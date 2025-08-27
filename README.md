# dask_setup

[![CI](https://github.com/21centuryweather/dask_setup/workflows/CI/badge.svg)](https://github.com/21centuryweather/dask_setup/actions)

dask_setup helper designed for **single‑node** Dask runs on **Gadi**, with smart defaults for CPU-/IO-bound workloads and automatic routing of all temp/spill files to **`$PBS_JOBFS`**. It's a drop‑in convenience wrapper around `dask.distributed.LocalCluster` + `Client` that minimizes trial‑and‑error and avoids common Out Of Memory (OOM) pitfalls.

**Python Support**: Requires Python 3.11+ | **Installation**: `pip install dask_setup` or `pip install -e .[dev]` for development

---

## What this function does (at a glance)

- **Detects resources** (cores & RAM) from PBS/SLURM env vars (fallback to `psutil`).
- **Reserves memory** for the OS/I/O caches (`reserve_mem_gb`) and splits the rest across workers.
- Chooses **process/thread topology** based on `workload_type` (`"cpu"`, `"io"`, `"mixed"`).
- Configures **aggressive but safe spilling** so tasks spill to disk before OOM.
- **Pins all temp + spill** to `$PBS_JOBFS` (node‑local SSD) instead of shared filesystems.
- Starts a **dashboard** (optional) and prints a ready‑to‑copy **SSH tunnel** command.
- Returns `(client, cluster, dask_local_dir)` so you can reuse the jobfs path for things like Rechunker temp stores.

---

## Signature and parameters

```python
def setup_dask_client(
    workload_type: str = "io",   # "cpu", "io", or "mixed"
    max_workers: int | None = None,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
    dashboard: bool = True,
    adaptive: bool = False,
    min_workers: int | None = None,
) -> tuple[Client, LocalCluster, str]:
    ...
```

- **workload_type**  
  - `"cpu"` → many processes, **1 thread** each (best for NumPy/Numba/xarray math; dodges GIL).  
  - `"io"` → **1 process** with **many threads** (8–16) for high‑throughput NetCDF/Zarr I/O.  
  - `"mixed"` → compromise: a few threads per process, several processes.
- **max_workers**: hard cap on workers (defaults to available cores; derived from PBS/SLURM if possible).
- **reserve_mem_gb**: memory held back for OS caches, filesystem metadata, etc.
- **max_mem_gb**: upper bound if you want less than full node RAM.
- **dashboard**: create a dashboard on a random free port (`:0`) and print an SSH tunnel hint.
- **adaptive/min_workers**: optional elasticity on a single node (often off for batch jobs).

**Returns:**  

- `client`: a connected `dask.distributed.Client`  
- `cluster`: the backing `LocalCluster`  
- `dask_local_dir`: the absolute path used for Dask temp/spill (under `$PBS_JOBFS` if available)

---

## Environment & resource detection

The function checks (in order) for scheduler hints, then falls back to `psutil`:

- **Cores**: `SLURM_CPUS_ON_NODE` → `NCPUS`/`PBS_NCPUS` → `psutil.cpu_count(logical=True)`  
- **Memory**: `SLURM_MEM_PER_NODE`/`SLURM_MEM_PER_CPU` (MB) → `PBS_VMEM`/`PBS_MEM` (e.g., `"300gb"`) → `psutil.virtual_memory().total`

It computes:

``` python
total_mem_gib = total_mem_bytes / 2**30
usable_mem_gb = clamp(max_mem_gb, total_mem) - reserve_mem_gb
mem_per_worker = max(1.0 GiB, usable_mem_gb / n_workers)
```

and passes `memory_limit=mem_per_worker_bytes` to each worker.

> **Why GiB/bytes?**  
> Dask is strict about `memory_limit`; using **bytes** avoids unit ambiguity and off‑by‑one rounding that can trip large tasks.

---

## Temp & spill routing (PBS job filesystem)

To avoid punishing shared storage and to **speed up shuffles/spills**, the function sets all relevant temp locations to **`$PBS_JOBFS`** (fallback: `TMPDIR` → `/tmp`). It ensures a unique path like:

``` python
$PBS_JOBFS/dask-<pid>/
```

and points **everything** at it:

- `TMPDIR` (respected by many libs)  
- `DASK_TEMPORARY_DIRECTORY`  
- Dask config keys:  
  - `temporary-directory`  
  - `distributed.worker.local-directory`

You’ll see subfolders like:

``` python
…/dask-<pid>/worker-{uuid}/spill/
```

> Tip: When using **Rechunker**, reuse `dask_local_dir` for `temp_store` so the heavy shuffle stays on jobfs.

---

## Process/thread topology by workload

### CPU‑bound (`workload_type="cpu"`)

- **processes=True**, **threads_per_worker=1**
- **n_workers ≈ cores** (but clamped by `max_workers`)
- Best for heavy compute/reductions (`.mean`, `apply_ufunc`, resampling)

``` text
[Node RAM] -> reserve_mem_gb -> usable
usable / n_workers -> memory_limit per worker
Workers: many processes, 1 thread each
```

### IO‑bound (`"io"`)

- **processes=False**, **threads_per_worker=8–16**, **n_workers=1**
- One process with many threads tends to maximize file I/O throughput

``` text
Single process
  └─ 8–16 threads
memory_limit ~ all usable_mem (since n_workers=1)
```

### Mixed (`"mixed"`)

- **processes=True**, **threads_per_worker=2**, several workers
- Good compromise for pipelines that both read and compute

---

## Memory safety: spill thresholds

Set once via `dask.config.set`:

- `worker.memory.target = 0.75`  → start spilling around 75% usage  
- `worker.memory.spill  = 0.85`  → spill aggressively  
- `worker.memory.pause  = 0.92`  → pause scheduling new tasks  
- `worker.memory.terminate = 0.98` → last‑resort kill (protects the job)

These **prevent OOM** when a few tasks inflate more than chunk‑size estimates.

---

## Dashboard & SSH tunnel

With `dashboard=True`, the cluster binds on a **random free port**. The helper prints something like:

``` bash
Dask dashboard: http://127.0.0.1:<PORT>/status
Tunnel from your laptop:
  ssh -N -L 8787:<SCHED_HOST>:<PORT> gadi.nci.org.au
Then open: http://localhost:8787
```

> On Gadi, run the SSH command **locally** (your laptop). If you’re inside a compute job, `<SCHED_HOST>` is the compute hostname shown in the printout.

---

## Typical usage patterns

### 1) Big xarray reductions (CPU)

```python
client, cluster, dask_tmp = setup_dask_client("cpu", reserve_mem_gb=60)
ds = ds.chunk({"time": 240, "y": 512, "x": 512})  # ~256–512 MiB/chunk
out = ds.mean(("y", "x")).compute()
```

### 2) Heavy I/O (open/concat many NetCDF/Zarr)

```python
client, cluster, dask_tmp = setup_dask_client("io", reserve_mem_gb=40)
ds = xr.open_mfdataset(files, engine="netcdf4", chunks={}, parallel=True)
# perform indexing/slicing/concat operations
```

### 3) Writing to Zarr in time windows (safe default)

```python
client, cluster, dask_tmp = setup_dask_client("cpu", max_workers=1, reserve_mem_gb=50)
step = 240
n = ds.sizes["time"]

# First window creates the store
ds.isel(time=slice(0, step)).to_zarr("out.zarr", mode="w", consolidated=True)

# Append by region (shards along time)
for start in range(step, n, step):
    stop = min(start + step, n)
    ds.isel(time=slice(start, stop)).to_zarr("out.zarr", mode="a",
        region={"time": slice(start, stop)})
```

### 4) Rechunk safely with Rechunker (spill to jobfs)

```python
client, cluster, dask_tmp = setup_dask_client("cpu", reserve_mem_gb=60)

tmp_store = f"{dask_tmp}/tmp_rechunk.zarr"
plan = rechunker.rechunk(
    ds.to_array().data,
    target_chunks={"time": 240, "y": 512, "x": 512},
    max_mem="6GB",                # < per-worker memory_limit
    target_store="out.zarr",
    temp_store=tmp_store,         # lives on $PBS_JOBFS
)
plan.execute()
```

---

## Troubleshooting & tips

- **“Task needs > memory_limit”**  
  Use fewer (fatter) workers for the heavy step:

  ```python
  client, cluster, _ = setup_dask_client("cpu", max_workers=1, reserve_mem_gb=60)
  ```

  and ensure chunk sizes yield **~256–512 MiB per chunk**.

- **Dashboard unreachable**  
  Use the printed `ssh -N -L` tunnel. If you’re in an interactive compute job, tunnel to the **compute node** hostname.

- **Shared FS thrashing**  
  Confirm spills go to jobfs: check the printed `temp/spill dir:` path and disk usage on `$PBS_JOBFS`.

- **PBS script snippet**

  ```bash
  #PBS -q normalsr
  #PBS -l ncpus=104
  #PBS -l mem=300gb
  #PBS -l jobfs=200gb
  #PBS -l walltime=12:00:00
  #PBS -l storage=gdata/hh5+gdata/gb02
  #PBS -l wd
  module use /g/data/hh5/public/modules/
  module load conda_concept/analysis3-unstable
  export TMPDIR="$PBS_JOBFS"  # optional, setup_dask_client also sets this
  python your_script.py
  ```

---

## Design limitations (by choice)

- **Single node only.** For multi‑node, use `dask-jobqueue` (e.g., `PBSCluster`) so each worker runs as a separate PBS job.
- **No GPU‑specific tuning.** If using CuPy/rapids, alter processes/threads and RMM settings accordingly.
- **Assumes POSIX paths.** On other systems, adapt the temp directory logic.

---

## Minimal code sketch (core ideas)

Below is an abridged version showing the essential settings (not the full implementation):

```python
def setup_dask_client(...):
    # 1) Decide temp root (prefer jobfs), make unique dir
    base_tmp = os.environ.get("PBS_JOBFS") or os.environ.get("TMPDIR") or "/tmp"
    dask_local_dir = Path(base_tmp) / f"dask-{os.getpid()}"
    dask_local_dir.mkdir(parents=True, exist_ok=True)

    # 2) Route temp/spills
    os.environ["TMPDIR"] = str(dask_local_dir)
    os.environ["DASK_TEMPORARY_DIRECTORY"] = str(dask_local_dir)
    dask.config.set({
        "temporary-directory": str(dask_local_dir),
        "distributed.worker.local-directory": str(dask_local_dir),
        "distributed.worker.memory.target": 0.75,
        "distributed.worker.memory.spill": 0.85,
        "distributed.worker.memory.pause": 0.92,
        "distributed.worker.memory.terminate": 0.98,
    })

    # 3) Detect cores/mem from PBS/SLURM or psutil
    logical_cores = ...
    total_mem_gib = ...
    usable_mem_gb = max(0.0, min(max_mem_gb or total_mem_gib, total_mem_gib) - reserve_mem_gb)

    # 4) Choose processes/threads by workload_type → compute n_workers
    # 5) Compute per-worker memory in BYTES
    mem_per_worker_bytes = int(max(1.0, usable_mem_gb / n_workers) * 2**30)

    # 6) Launch LocalCluster
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
        memory_limit=mem_per_worker_bytes,
        dashboard_address=":0",
        local_directory=str(dask_local_dir),
    )
    client = Client(cluster)
    return client, cluster, str(dask_local_dir)
```

---

## TL;DR

- Use `setup_dask_client("cpu")` for heavy math; `"io"` for large file I/O; `"mixed"` for pipelines with both.  
- All **spills/temp** are routed to **`$PBS_JOBFS`** for speed and to protect shared storage.  
- Memory is **safely split** across workers with **spilling thresholds** to avoid OOM.  
- The function prints a **dashboard** link and SSH tunnel recipe so you can monitor your jobs easily.
