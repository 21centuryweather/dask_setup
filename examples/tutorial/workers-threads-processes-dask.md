---
title: "Workers, threads and processes in Dask"
author: Sam Green
date: 2026-10-16
tags: [dask, dask_setup, hpc, gadi, python]
---

# Workers, threads and processes in Dask

*A field guide for HPC users, with `dask_setup` doing the bookkeeping.*

At every `dask_setup` tutorial the same question comes up within the first half hour: *what is the difference between a worker, a thread and a process, and why does `workload_type` change them?* This post is the long answer. There is a companion Jupyter notebook that lets you watch each idea happen on your own machine.

## TL;DR

- A **worker** is Dask's unit of *management*: it receives tasks from the scheduler, holds results in memory, and has its own memory limit.
- A **thread** is Dask's unit of *concurrency*: each worker runs `threads_per_worker` tasks at once, all sharing that worker's memory.
- A **process** is the operating system's unit of *isolation*: its own Python interpreter, its own memory, and its own **GIL**.
- Your total parallelism is `n_workers × threads_per_worker`. Match it to the cores PBS gave you, not the cores the node has.
- Whether to spend those slots on **processes** or **threads** depends on one question: *does the hot code hold Python's GIL?* If yes, use processes (`workload_type="cpu"`). If it releases the GIL, threads are cheaper (`workload_type="io"`).

## The three words

### Process

A process is a running program as the operating system sees it. Each Python process has its own interpreter, its own copy of every imported library, and its own private memory. Two processes cannot see each other's arrays; to share data, one has to **serialise** it (pickle it), send it, and the other has to rebuild it.

### Thread

A thread is a line of execution *inside* a process. Threads in the same process share all memory, so handing an array from one thread to another costs nothing. The catch is the **Global Interpreter Lock (GIL)**: within one process, only one thread can execute Python bytecode at a time. Threads only run truly in parallel while they are inside C code that has explicitly released the GIL (NumPy's heavy kernels, compression codecs, waiting on a socket or a disk).

### Worker

A worker is a Dask concept, not an OS one. It is a small server that:

- receives tasks from the scheduler,
- runs them on a pool of `threads_per_worker` threads,
- stores the results it produces until someone needs them,
- enforces a **memory limit**, spilling to disk and eventually pausing or restarting when it is exceeded.

How workers map onto processes is a cluster setting:

- `processes=True` (the `LocalCluster` default): each worker gets its **own process**, launched and babysat by a "nanny".
- `processes=False`: the worker lives **inside your notebook's own process**, as a set of threads alongside your client.

So one worker is always *at least one thread*, and *at most one process*.

## A mental model

Think of a shared office building.

- Each **process** is a separate office with a locked door. Whatever is on its desk (memory) is private; passing a document to another office means photocopying it and walking it down the hall (serialisation).
- Each **thread** is a person working in that office. People in the same office share the desk for free.
- The **GIL** is the single pen in each office. Only one person can *write Python* at a time; the others can still read, wait for the printer, or use a calculator that doesn't need the pen (C code that releases the GIL).
- A **worker** is the office manager: it takes jobs from reception (the scheduler), hands them to the people in the room, and complains when the desk gets too full (the memory limit).

More offices give you more pens but cost more photocopying and more desk space. More people per office are cheap, but they fight over the pen.

## Why the choice matters: the GIL and the copy

You always trade between two costs:

| | Threads (in one process) | Processes |
|---|---|---|
| Parallel Python code | No: one thread at a time holds the GIL | Yes: each process has its own GIL |
| Parallel GIL-releasing C code | Yes | Yes |
| Moving data between tasks | Free (shared memory) | Pickle + copy |
| Memory | One big pool | Split evenly between workers |
| Start-up and overhead | Low | Higher (one interpreter each) |

The companion notebook measures this directly. A pure-Python loop gets close to a linear speed-up on processes and almost none on threads. A SHA-256 hash (C code that releases the GIL) and a `time.sleep` (waiting, like I/O) speed up on both. Returning a 400 MB array to the notebook is near-instant from a threaded worker and noticeably slower from a separate process.

## What `dask_setup` chooses for you

`workload_type` is really a choice of topology. Here is what `dask_setup` builds on a Gadi job with `ncpus=48`:

| `workload_type` | Workers | Threads / worker | Processes? | Total slots | Use when the hot code... |
|---|---|---|---|---|---|
| `"cpu"` | 48 | 1 | yes | 48 | **holds** the GIL |
| `"mixed"` | 24 | 2 | yes | 48 | does a bit of both |
| `"io"` | 1 | 16 | no | 16 | **releases** the GIL |
| `"gpu"` | 1 per GPU | 2–8 | yes | — | runs on GPUs |

Two details are worth noticing:

- `"io"` deliberately caps at 16 threads. Threads that are mostly *waiting* don't need a core each, and one process avoids every copy.
- `max_workers=` caps the number of workers, so `setup_dask_client("cpu", max_workers=8)` gives you 8 processes × 1 thread.

## The NetCDF trap

Reading lots of NetCDF files *feels* like I/O, so people reach for `"io"`. For Dask it is the wrong choice. The netCDF4/HDF5 C library serialises reads behind a single process-wide lock, so 16 threads in one process take turns reading. With `"cpu"`, each process has its own lock and the reads really do run in parallel.

- **NetCDF / HDF5 input → `"cpu"`**
- **Zarr input → `"io"`** (Zarr's codecs release the GIL)

`dask_setup` warns you if it sees a NetCDF-backed dataset with `workload_type="io"`.

## Memory is per worker

This is the second most common surprise. The memory limit belongs to the **worker**, and is shared by all of that worker's threads.

On a 48-core, 190 GB Gadi job, `dask_setup` first holds back some memory for the OS and filesystem (by default 20% of RAM, clamped to 4–50 GiB), then splits the rest evenly:

- `"cpu"` → 48 workers with roughly 3 GiB each
- `"mixed"` → 24 workers with roughly 6 GiB each
- `"io"` → 1 worker with roughly 150 GiB

The same 1 GiB chunk that is comfortable in an `"io"` worker can push a `"cpu"` worker into spilling, because a task often needs its input chunk, its output chunk and a temporary or two at the same time. A practical rule: keep chunks several times smaller than `memory_limit / threads_per_worker`. `setup_dask_client(..., ds=ds)` and `recommend_chunks()` do this arithmetic for you.

## Don't stack threads on threads

NumPy's linear algebra calls into a BLAS library (OpenBLAS or MKL), which may start its *own* thread pool. Run 16 Dask threads that each start 48 BLAS threads and you have 768 threads fighting for 48 cores.

- In process-based clusters, Dask's nanny sets `OMP_NUM_THREADS`, `MKL_NUM_THREADS` and `OPENBLAS_NUM_THREADS` to 1 in each worker by default.
- In a threads-only (`"io"`) cluster the workers inherit your session's environment. If the hot path is BLAS-heavy, set those variables to 1 before you start Python (for example in your PBS script):

```bash
export OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
```

## A five-question checklist

1. **What did PBS give me?** `ncpus` and `mem` are your budget. `dask_setup` reads them from the job environment, so you don't oversubscribe a shared node.
2. **Does the hot code hold the GIL?** Pure Python, pandas `apply`, NetCDF/HDF5 reads → `"cpu"`. Zarr, cloud storage, NumPy-heavy maths → `"io"`. Not sure → `"mixed"`.
3. **How big is each worker's memory?** Divide by threads per worker; your chunks should fit several times over.
4. **Am I moving lots of data between tasks?** Shuffles, rechunks and big gathers favour fewer, larger workers.
5. **What did I actually get?** Print `client`, open the dashboard's *Workers* tab, or call `client.nthreads()`.

## Try it yourself

The companion notebook, `workers_threads_processes.ipynb`, walks through all of this with `dask_setup`:

- inspects a `"cpu"` and an `"io"` cluster to show which process and thread each task ran on,
- times three kinds of task (pure Python, GIL-releasing C, waiting) on both topologies,
- measures the cost of moving a large array out of a separate process,
- shows how memory per worker changes with topology on a simulated 48-core Gadi job.

```python
from dask_setup import setup_dask_client

# Pure-Python or NetCDF-heavy work: many processes, one thread each
client, cluster, dask_tmp = setup_dask_client("cpu")

# Zarr / object storage: one process, many threads
client, cluster, dask_tmp = setup_dask_client("io")
```

`dask_setup` is on PyPI and conda-forge (`pip install dask-setup`), and the source and wiki live at [github.com/21centuryweather/dask_setup](https://github.com/21centuryweather/dask_setup).
