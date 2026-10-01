# Multi-Node Support (v2.0)

`dask_setup` v2.0 extends the same simple API to multi-node HPC jobs. Two distinct workflows are supported:

| Situation | Correct function | What it does |
|-----------|-----------------|--------------|
| **Interactive session** (`qsub -I` / `salloc`) | `setup_interactive_cluster()` | Starts workers directly on already-allocated nodes — no new job submission |
| **Batch job** submitting worker jobs | `setup_pbs_cluster()` / `setup_slurm_cluster()` | Submits new PBS/SLURM child jobs via `dask-jobqueue` |

`setup_dask_client()` defaults to `mode="interactive"`: it uses whatever the current job already has and **never submits jobs**. Pass `mode="pbs"` / `mode="slurm"` (or `mode="auto"`) to submit worker jobs. *Changed in v2.3* — the default was `"auto"`, which submitted worker jobs whenever it was called from inside a batch job.

| Call | Laptop / login node | Single-node job | Multi-node job (interactive or batch) |
|------|--------------------|-----------------|---------------------------------------|
| `setup_dask_client()` (`mode="interactive"`) | `LocalCluster` | `LocalCluster` | `SSHCluster` across allocated nodes |
| `setup_dask_client(mode="auto")` | `LocalCluster` | interactive → `LocalCluster`; batch → **submits jobs** | interactive → `SSHCluster`; batch → **submits jobs** |
| `setup_dask_client(mode="pbs")` / `"slurm"` | submits jobs | submits jobs | submits jobs |

**Requires:** `pip install dask-jobqueue` or `conda install -c conda-forge dask-jobqueue` (only for the batch/`PBSCluster`/`SLURMCluster` path)

---

## Quick Start

### Interactive session (Jupyter notebook inside `qsub -I` or `salloc`)

```python
from dask_setup import setup_interactive_cluster

# Reads PBS_NODEFILE / SLURM_NODELIST to discover allocated nodes.
# Single node → LocalCluster.  Multiple nodes → SSHCluster.
client, cluster, dask_tmp = setup_interactive_cluster(workload_type="cpu")

try:
    result = ds.mean().compute()
finally:
    client.close()
    cluster.close()
```

Or via `setup_dask_client`, whose default `mode="interactive"` does the same thing:

```python
from dask_setup import setup_dask_client

client, cluster, dask_tmp = setup_dask_client(workload_type="cpu")
```

On a single node (or outside any job) this takes the ordinary local path, so `ds=`-based `workload_type="auto"` inference, `fallback_on_detection_failure` and `adaptive_memory` all apply. On a multi-node allocation it calls `setup_interactive_cluster` for you.

### Batch job — submitting worker jobs

```python
from dask_setup import setup_dask_client, MultiNodeConfig

client, cluster, shared_tmp = setup_dask_client(
    mode="auto",   # picks "pbs" / "slurm" from environment in a batch job;
                   # without it, the default "interactive" would not submit jobs
    multi_node_config=MultiNodeConfig(
        workers_per_node=4,
        cores_per_worker=12,
        mem_per_worker_gb=32.0,
        walltime="04:00:00",
        queue="normal",
    ),
)
try:
    result = ds.mean().compute()
finally:
    client.close()
    cluster.close()
```

### Explicit PBS or SLURM (batch)

```python
# PBS
client, cluster, shared_tmp = setup_dask_client(mode="pbs", multi_node_config=cfg)

# SLURM
client, cluster, shared_tmp = setup_dask_client(mode="slurm", multi_node_config=cfg)
```

### Direct helpers

```python
from dask_setup import setup_pbs_cluster, setup_slurm_cluster, MultiNodeConfig

cfg = MultiNodeConfig(workers_per_node=2, cores_per_worker=24, mem_per_worker_gb=64.0, walltime="06:00:00")

# PBS
client, cluster, shared_tmp = setup_pbs_cluster(cfg, n_workers=4)

# SLURM
client, cluster, shared_tmp = setup_slurm_cluster(cfg, n_workers=4)
```

### Worker initialisation — why the client can report 0 workers

`cluster.scale(n)` only *submits* jobs to the PBS/SLURM scheduler — it does not wait for them to be allocated and started. `Client(cluster)` connects to the Dask *scheduler* process immediately, but the scheduler has no workers yet because the HPC jobs are still queued. Without an explicit wait, `client.scheduler_info()` returns `{"workers": {}}`.

`setup_pbs_cluster` and `setup_slurm_cluster` handle this automatically: they block until at least one worker has connected before returning. Two parameters control the behaviour:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `wait_for_workers` | `True` | Block until workers are online. Set to `False` to return immediately. |
| `worker_timeout` | `300.0` | Seconds to wait. Raise this on queues with long wait times. |

```python
# Default — blocks up to 5 minutes
client, cluster, _ = setup_pbs_cluster(cfg, n_workers=4)

# Long queue — wait up to 20 minutes
client, cluster, _ = setup_pbs_cluster(cfg, n_workers=4, worker_timeout=1200.0)

# Return immediately and wait manually
client, cluster, _ = setup_pbs_cluster(cfg, n_workers=4, wait_for_workers=False)
client.wait_for_workers(cfg.workers_per_node * 4, timeout=600)
```

If no worker connects within `worker_timeout` seconds, a `TimeoutError` is raised with a diagnostic message listing the number of jobs submitted, expected worker count, scheduler address, and common remediation steps (checking the queue with `qstat`/`squeue`, verifying scheduler port reachability, and inspecting worker job logs).

---

## setup_interactive_cluster — Interactive Sessions

Use this when you already have resources allocated (Jupyter notebook inside `qsub -I` or `salloc`). It does **not** submit new jobs — it starts workers directly on the nodes assigned to your session.

```python
from dask_setup import setup_interactive_cluster

client, cluster, dask_tmp = setup_interactive_cluster(
    workload_type="cpu",     # "cpu", "io", "mixed", or "auto"
    wait_for_workers=True,   # block until workers are ready (default)
    worker_timeout=60.0,     # seconds — workers start almost immediately
)
```

### Node discovery

`setup_interactive_cluster` reads `PBS_NODEFILE` (PBS) or `SLURM_NODELIST` (SLURM) to find the allocated nodes:

| Unique nodes found | Cluster type | Notes |
|-------------------|--------------|-------|
| 0 or 1 | `LocalCluster` | Uses `PBS_NCPUS` / `PBS_MEM` (same as single-node `setup_dask_client`) |
| 2+ | `SSHCluster` | Workers started on all nodes via passwordless SSH |

On NCI Gadi, `PBS_NODEFILE` has one line *per CPU* — e.g. 48 lines of `gadi-cpu-clx-0012` for a 48-core node. The parser deduplicates and counts to determine cores per node.

### Worker topology (multi-node SSH path)

Worker count and thread count per node are derived from `workload_type` and the core count read from the nodefile:

| `workload_type` | Workers per node | Threads per worker |
|-----------------|------------------|--------------------|
| `"cpu"` | `cores_per_node` | 1 |
| `"io"` | 1 | `min(16, max(4, cores_per_node))` |
| `"mixed"` | `ceil(cores_per_node / 2)` | 2 |

Override with `workers_per_node=` and `threads_per_worker=` if needed.

### `mode="auto"` detection

`mode="auto"` is opt-in (the default is `"interactive"`). `setup_dask_client(mode="auto")` calls `detect_cluster_mode()` which distinguishes interactive from batch:

| Environment | Detected mode | Backend |
|-------------|--------------|---------|
| `PBS_ENVIRONMENT=PBS_INTERACTIVE` | `"interactive"` | `setup_interactive_cluster` |
| `PBS_JOBID` set, `PBS_ENVIRONMENT=PBS_BATCH` (or absent) | `"pbs"` | `setup_pbs_cluster` |
| `SLURM_JOB_ID` set, `SLURM_BATCH_FLAG` absent or `"0"` | `"interactive"` | `setup_interactive_cluster` |
| `SLURM_JOB_ID` set, `SLURM_BATCH_FLAG=1` | `"slurm"` | `setup_slurm_cluster` |
| None of the above | `"local"` | `LocalCluster` |

---

## MultiNodeConfig

`MultiNodeConfig` holds all the configuration needed for a multi-node PBS or SLURM job. It is a companion to `DaskSetupConfig` — not a subclass.

```python
from dask_setup import MultiNodeConfig

cfg = MultiNodeConfig(
    workload_type="cpu",        # "cpu", "io", "mixed", "gpu", "auto"
    workers_per_node=4,         # Dask worker processes per submitted job
    cores_per_worker=12,        # CPU cores per worker
    mem_per_worker_gb=32.0,     # RAM per worker (GiB)
    walltime="04:00:00",        # HH:MM:SS
    queue="normal",             # PBS queue / SLURM partition
    project="ab01",             # PBS -P / SLURM --account
    job_extra_directives=["-l storage=gdata/hh5"],  # extra #PBS / #SBATCH lines
    n_nodes=1,                  # nodes per submitted job (usually 1)
    shared_tmp_dir="/scratch/project/tmp",  # Lustre/GPFS path for shared stores
    env_extra=["module load python3"],      # shell lines, inserted verbatim
    adaptive=False,             # True → cluster.adapt(min_jobs, max_jobs)
    min_jobs=1,
    max_jobs=10,
)
```

### `env_extra`

Entries are **shell statements inserted verbatim** near the top of the worker
job script, before the worker starts. Write the whole statement, including
`export` where you need it:

```python
env_extra=[
    "module load conda/analysis3",     # not an assignment — no export
    "source activate myenv",
    "export OMP_NUM_THREADS=1",        # assignment — export it yourself
]
```

> **Changed in v2.2.** The script generators used to prefix every entry with
> `export`, turning `module load conda/analysis3` into
> `export module load conda/analysis3`, while the same list passed through
> `dask-jobqueue` was emitted correctly — so the one list meant two different
> things depending on the path. Both now emit verbatim. If you were writing
> bare `FOO=bar` entries and relying on the implicit prefix, add `export`.

### Useful properties

```python
cfg.total_cores_per_job   # workers_per_node * cores_per_worker
cfg.total_mem_gb_per_job  # workers_per_node * mem_per_worker_gb
```

`cores_per_worker` and `mem_per_worker_gb` are per **worker**; the totals above
are what the job reserves. *Changed in v2.2* — the per-worker figures were
being handed to `dask-jobqueue`, which divides its `cores`/`memory` arguments
by `processes`. A 4×12-core, 4×32 GiB job therefore started workers with
`--nthreads 3 --memory-limit 7.45GiB` instead of `--nthreads 12
--memory-limit 29.80GiB`: a 4× under-provision against the `ncpus=48,mem=128GB`
the job had actually reserved.

### Adaptive scaling

```python
cfg = MultiNodeConfig(
    workers_per_node=4,
    cores_per_worker=12,
    mem_per_worker_gb=32.0,
    walltime="04:00:00",
    adaptive=True,
    min_jobs=1,
    max_jobs=8,
)
client, cluster, _ = setup_pbs_cluster(cfg)
# cluster automatically submits/cancels jobs based on task queue
```

---

## Shared Temp Directory

On multi-node jobs, `$PBS_JOBFS` is per-node and cannot be shared. Use `SharedTempDir` for stores that must be visible to all workers (e.g. intermediate Rechunker stores, Zarr outputs):

```python
from dask_setup import SharedTempDir

shared_tmp = SharedTempDir(
    path="/scratch/project/my_analysis",
    create_subdirectory=True,   # creates dask_tmp_<JOBID>/ inside path
    cleanup_on_close=False,     # shared scratch is usually cleaned by scheduler
)
print(shared_tmp)  # /scratch/project/my_analysis/dask_tmp_12345.gadi

# Use directly as a path
rechunk_dataset(ds, target_chunks=chunks, client=client, dask_tmp=str(shared_tmp))
```

`SharedTempDir` implements `__fspath__()` so it can be used wherever a `Path` is accepted.

When `shared_tmp_dir` is set in `MultiNodeConfig`, it is automatically passed as `--local-directory` to each worker process.

---

## GPU Topology

`workload_type="gpu"` configures one worker process per CUDA-capable GPU with several CPU threads each for data loading and preprocessing.

```python
from dask_setup import setup_dask_client, DaskSetupConfig

# Single-node GPU setup
client, cluster, dask_tmp = setup_dask_client(
    workload_type="gpu",
    # CUDA_VISIBLE_DEVICES=0,1,2,3 → 4 workers, one per GPU
)
```

**GPU detection order:**
1. `CUDA_VISIBLE_DEVICES` environment variable (e.g. `"0,1,2,3"`)
2. `cupy.cuda.runtime.getDeviceCount()` (if CuPy is installed)
3. Graceful fallback to single-process multi-thread (like `"io"`) with a warning when no GPUs are found

**Thread allocation:** `max(2, min(8, total_cores // n_gpus))` threads per worker.

For multi-node GPU jobs, set `workload_type="gpu"` in your `MultiNodeConfig` and add `--gres=gpu:4` (SLURM) or `-l ngpus=4` (PBS) to `job_extra_directives`:

```python
cfg = MultiNodeConfig(
    workload_type="gpu",
    workers_per_node=4,     # one per GPU
    cores_per_worker=6,     # CPU threads per GPU worker
    mem_per_worker_gb=32.0,
    walltime="02:00:00",
    queue="gpuvolta",
    job_extra_directives=["-l ngpus=4"],
)
```

---

## Generating Job Scripts

The `dask-setup submit` CLI subcommand generates a ready-to-run PBS or SLURM job script without submitting it:

```bash
# PBS script (prints to stdout)
dask-setup submit my_analysis.py \
    --scheduler pbs \
    --workers-per-node 4 \
    --cores-per-worker 12 \
    --mem-per-worker 32 \
    --walltime 04:00:00 \
    --queue normal \
    --project ab01 \
    --extra-directive "-l storage=gdata/hh5+gdata/gb02"

# Write to file
dask-setup submit my_analysis.py --scheduler pbs ... --output job.sh
qsub job.sh
```

```bash
# SLURM script
dask-setup submit my_analysis.py \
    --scheduler slurm \
    --workers-per-node 2 \
    --cores-per-worker 20 \
    --mem-per-worker 64 \
    --walltime 08:00:00 \
    --queue compute \
    --project myaccount \
    --extra-directive "--gres=gpu:2"
```

**Full flag reference:**

| Flag | Default | Description |
|------|---------|-------------|
| `--scheduler` / `-S` | `pbs` | `pbs` or `slurm` |
| `--workload-type` / `-w` | `cpu` | `cpu`, `io`, `mixed`, `gpu` |
| `--workers-per-node` / `-W` | `1` | Dask worker processes per node |
| `--cores-per-worker` / `-c` | `1` | CPU cores per worker |
| `--mem-per-worker` / `-m` | `4.0` | RAM per worker (GiB) |
| `--walltime` / `-t` | `01:00:00` | `HH:MM:SS` |
| `--queue` / `-q` | `normal` | Queue / partition |
| `--project` / `-P` | | Account / project code |
| `--n-nodes` / `-N` | `1` | Nodes per job |
| `--shared-tmp-dir` | | Shared filesystem temp dir |
| `--extra-directive` / `-e` | | Extra directive (repeatable) |
| `--python` | `python3` | Python executable in job script |
| `--output` / `-o` | (stdout) | Write script to file |

In Python:

```python
from dask_setup import MultiNodeConfig
from dask_setup.multinode import generate_pbs_script, generate_slurm_script

cfg = MultiNodeConfig(workers_per_node=4, cores_per_worker=12, mem_per_worker_gb=32.0, walltime="04:00:00")

pbs_script = generate_pbs_script(cfg, script_path="my_analysis.py")
slurm_script = generate_slurm_script(cfg, script_path="my_analysis.py")

with open("job.sh", "w") as f:
    f.write(pbs_script)
```

---

## Environment Detection

`detect_cluster_mode()` inspects environment variables:

```python
from dask_setup import detect_cluster_mode

mode = detect_cluster_mode()  # "pbs", "slurm", or "local"
```

| Indicator | Mode |
|-----------|------|
| `SLURM_JOB_ID` or `SLURM_JOBID` or `SLURM_NODELIST` | `"slurm"` |
| `PBS_JOBID` or `PBS_NODEFILE` or `PBS_ENVIRONMENT` | `"pbs"` |
| None of the above | `"local"` |

SLURM indicators take priority when both are set.

---

## Complete PBS Example

**PBS job script** (or use `dask-setup submit` to generate one):

```bash
#!/bin/bash
#PBS -q normal
#PBS -l ncpus=48,mem=128GB,walltime=04:00:00
#PBS -l storage=gdata/hh5+scratch/ab01
#PBS -P ab01
#PBS -l wd

module load python3

python my_analysis.py
```

**`my_analysis.py`:**

```python
import xarray as xr
from dask_setup import setup_dask_client, MultiNodeConfig, recommend_io_chunks

client, cluster, shared_tmp = setup_dask_client(
    mode="auto",   # picks "pbs" automatically when PBS_JOBID is set;
                   # required, since the default "interactive" never submits jobs
    multi_node_config=MultiNodeConfig(
        workers_per_node=4,
        cores_per_worker=12,
        mem_per_worker_gb=32.0,
        walltime="04:00:00",
        queue="normal",
        project="ab01",
        job_extra_directives=["-l storage=gdata/hh5"],
        shared_tmp_dir="/scratch/ab01/tmp",
    ),
)

ds = xr.open_zarr("s3://bucket/era5.zarr")
rec = recommend_io_chunks(ds, path_or_url="s3://bucket/era5.zarr", access_pattern="compute")
result = ds.chunk(rec.chunks).mean(["lat", "lon"]).compute()
result.to_netcdf("/scratch/ab01/output/result.nc")

client.close()
cluster.close()
```

---

## Notes

- Multi-node clusters **require `dask-jobqueue`** (`pip install dask-jobqueue`). The package imports cleanly without it; a helpful `ImportError` is raised only when `setup_pbs_cluster` / `setup_slurm_cluster` are called.
- The returned `cluster` object is a live `PBSCluster` / `SLURMCluster`. Use `cluster.scale(n)` to change the number of active jobs, or `cluster.adapt(minimum=..., maximum=...)` for elastic scaling.
- `setup_dask_client()` returns a **4-tuple** `(client, cluster, tmp_path, chunks)` whenever `ds=` is passed, in every mode including `pbs`, `slurm` and `interactive`; a **3-tuple** `(client, cluster, tmp_path)` otherwise. *Changed in v2.2* — the multi-node paths used to return a 3-tuple even with `ds=`, so unpacking four values raised `ValueError: not enough values to unpack`.
- For GPU multi-node jobs, ensure `CUDA_VISIBLE_DEVICES` is set correctly inside each submitted job by adding it to `env_extra`.
- `setup_pbs_cluster` / `setup_slurm_cluster` **block by default** until at least one worker is online (`wait_for_workers=True`). If your queue has a long wait time and you want to return immediately, pass `wait_for_workers=False` and call `client.wait_for_workers()` yourself. See [Worker initialisation](#worker-initialisation--why-the-client-can-report-0-workers) above.
- **Inside an interactive session (`qsub -I` / `salloc`), use `setup_interactive_cluster` instead of `setup_pbs_cluster` / `setup_slurm_cluster`.** The batch helpers submit *new* PBS/SLURM jobs, which are queued and will not start immediately (or at all, depending on scheduler policy for nested jobs). `setup_interactive_cluster` starts workers directly on already-allocated nodes. `setup_dask_client()` does this by default (`mode="interactive"`), and `mode="auto"` selects it when `PBS_ENVIRONMENT=PBS_INTERACTIVE` is detected.
