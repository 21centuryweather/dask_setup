# Troubleshooting & Migration

## Common Issues

### "Task needs > memory_limit"

A single task is larger than the per-worker memory allocation. Options:

Use fewer, fatter workers:

```python
client, cluster, _ = setup_dask_client("cpu", max_workers=1, reserve_mem_gb=60)
```

Reduce chunk sizes to target 256–512 MiB per chunk:

```python
ds = ds.chunk({"time": 120, "y": 256, "x": 256})
```

Or increase available memory by lowering `reserve_mem_gb`:

```python
config = DaskSetupConfig(reserve_mem_gb=20.0)
client, cluster, _ = setup_dask_client(config=config)
```

You can also let `recommend_chunks()` calculate a safe size automatically:

```python
from dask_setup import recommend_chunks
chunks = recommend_chunks(ds, client, verbose=True)
ds = ds.chunk(chunks)
```

---

### Concatenating NetCDF files is far slower than expected

Symptom: opening a handful of daily NetCDF files, concatenating along `time` and
writing takes minutes, and single-threaded tools like NCO beat it comfortably.

Cause: `workload_type="io"`. It looks like the obvious choice for a file-reading
job, but it is the worst option for NetCDF. `"io"` runs **one process with many
threads**, and HDF5 (underneath NetCDF4) is usually not built thread-safe — so
xarray funnels every NetCDF read through a single process-wide lock. The threads
queue on that lock rather than reading in parallel, and zlib decompression is
serialised inside it too.

Fix: use `"cpu"`. Each process gets its own lock and GIL, so reads and
decompression run concurrently.

If you pass `ds=`, `dask_setup` detects this combination and warns for you:

```
WARNING [client] workload_type='io' is usually the wrong choice for NetCDF/HDF5 input
        (suggestion=use workload_type='cpu' (one lock per process) for NetCDF)
```

```python
# Slow: 12 threads queuing on one HDF5 lock
client, cluster, _ = setup_dask_client("io")

# Fast: 24 processes, each with its own lock
client, cluster, _ = setup_dask_client("cpu")
```

If it is not merely slow but appears to hang, check whether you also passed
`open_mfdataset(..., parallel=True)`. Combined with `"io"`, concurrent metadata
reads can kill the worker (`NetCDF: Can't open HDF5 attribute`); Dask then
recomputes the lost tasks silently, so repeated crashes look like a stall. The
dashboard's worker count dropping and recovering is the tell.

Converting the files to Zarr once makes everything downstream a genuinely
threaded workload, at which point `"io"` becomes the better choice.

---

### Getting more detail out of `dask_setup`

Set the log level before importing or calling anything:

```bash
export DASK_SETUP_LOG_LEVEL=DEBUG       # DEBUG, INFO, WARNING, ERROR, CRITICAL
export DASK_SETUP_LOG_FORMAT=structured # or "json" for machine-readable output
export DASK_SETUP_LOG_COLOR=auto        # true / false / auto
```

or from Python:

```python
from dask_setup import configure_logging

configure_logging(level="DEBUG")
```

Debug output covers resource detection, the topology decision, the memory
split, profile resolution and temp/spill routing — which is usually enough to
see why you got the cluster you got.

> **Changed in v2.2.** Neither of these worked before. `configure_logging()`
> returned early if logging had already been configured, and importing any
> `dask_setup` module configures it — so by the time you could call it, it was
> a no-op. The `DASK_SETUP_LOG_*` variables were documented in the source but
> nothing ever read them.

---

### Dashboard unreachable

The dashboard runs on a random port inside the compute job. Use the SSH tunnel printed at startup:

```bash
ssh -N -L 8787:<COMPUTE_HOST>:<PORT> gadi.nci.org.au
```

The login host in the printed hint is inferred from the compute node's DNS
domain, so it is right at non-NCI sites too. If the guess is wrong, set
`$DASK_SETUP_LOGIN_HOST`:

```bash
export DASK_SETUP_LOGIN_HOST=login.mycluster.edu
```

Then open `http://localhost:8787` in your browser. If you're in an interactive PBS job, `<COMPUTE_HOST>` is the compute node hostname (not the login node). The dashboard URL and tunnel command are printed each time `setup_dask_client()` is called with `dashboard=True`.

If you need a fixed port rather than a random one:

```python
config = DaskSetupConfig(dashboard_port=8787)
client, cluster, _ = setup_dask_client(config=config)
```

---

### Shared filesystem thrashing / slow spill

Check that spill files are going to `$PBS_JOBFS` and not to a shared filesystem. The startup log prints the chosen temp directory:

```
[setup_dask_client] temp/spill dir: /jobfs/local/<jobid>/pbs/dask-<pid>/
```

If the path shows `/scratch`, `/home`, or similar, `$PBS_JOBFS` is not set in your job environment. Verify your PBS script requests jobfs:

```bash
#PBS -l jobfs=200gb
```

On SLURM systems, the temp dir falls back to `$TMPDIR`, then `/tmp`. Configure `TMPDIR` in your job script to point to fast local storage if `$PBS_JOBFS` is not available.

---

### OOM (Out of Memory) crashes

In order of likelihood: oversized chunks, insufficient `reserve_mem_gb`, job memory underallocation.

Lower the memory threshold to start spilling earlier:

```python
config = DaskSetupConfig(memory_target=0.6, memory_spill=0.75)
client, cluster, _ = setup_dask_client(config=config)
```

Enable adaptive memory tuning to auto-tighten thresholds after startup:

```python
client, cluster, _ = setup_dask_client("cpu", adaptive_memory=True)
```

Or increase the job's memory allocation in your PBS/SLURM script and reduce `reserve_mem_gb` to give Dask more headroom.

---

### Workers fail to start

On some HPC systems, worker processes are killed by the scheduler if they appear to use too many resources at startup. Try capping the worker count:

```python
client, cluster, _ = setup_dask_client("cpu", max_workers=4)
```

Or switch to `workload_type="io"` which uses a single process with threads (no process-startup penalty):

```python
client, cluster, _ = setup_dask_client("io")
```

---

### `reserve_mem_gb` too high on a small machine

The default scales with the machine — 20% of RAM, clamped to 4–50 GiB and never
more than half the host — so a laptop reserves 4 GiB rather than the 50 GiB that
suits a Gadi node. Override it if you want something different:

```python
client, cluster, _ = setup_dask_client("cpu", reserve_mem_gb=4.0)
# or use the development profile:
client, cluster, _ = setup_dask_client(profile="development")
```

> **Changed in v2.2.** This default was written in v1.1 but never called, so
> every machine got a flat 50 GiB — which on a 16 GiB laptop reserves more than
> the whole host. If you had worked around this with an explicit
> `reserve_mem_gb`, that still wins and nothing changes for you.

---

### "Reduced workers N -> M to fit available memory"

Not an error. The worker count is fitted to usable memory before it is divided
up, so each worker gets at least 1 GiB for real rather than on paper. Before
v2.2 the per-worker floor was applied *after* the split, so 64 workers on a 64
GiB node with a 50 GiB reserve were each given 1 GiB — 64 GiB committed against
14 GiB actually usable, and the node went into swap or the OOM killer.

To get more workers, give them more memory to share: lower `reserve_mem_gb`,
raise `max_mem_gb`, or request a larger job allocation.

---

### PBS memory detected as absurdly large (e.g. 520 million GiB)

On NCI Gadi and other PBS Pro sites, `$PBS_MEM` is set to a **raw byte count** with no unit suffix (e.g. `"532575944704"` for 496 GiB). Older versions of `dask_setup` incorrectly treated bare integers as megabytes (the SLURM convention), inflating the detected memory by 1,048,576×:

```
INFO [resources] Resources detected via PBS (total_cores=104 | total_mem_gib=520093696.0)
```

This is fixed — bare integers in `$PBS_MEM` / `$PBS_VMEM` are now treated as bytes. If you see a realistic figure (e.g. `total_mem_gib=496.0`) the fix is active. Strings with explicit unit suffixes like `"496gib"` or `"512gb"` continue to work as before.

---

### `validate_chunks()` / `recommend_chunks()` warns about huge chunks that don't exist

`validate_chunks()` and the pre-recommendation check in `recommend_chunks()` estimate the in-memory footprint of one chunk by multiplying the chunk size across **all** dimensions. An older version of this code incorrectly multiplied the target dimension's chunk count by the **full extent** of every other dimension — as if those dimensions weren't chunked at all — producing wildly inflated estimates like 122,000 MiB and triggering false OOM warnings on datasets that were already well-chunked:

```
UserWarning: Dimension 'latitude' has very large chunks (122485 MiB).
             Consider rechunking to avoid memory issues.
```

This is fixed. The correct footprint for a chunk of shape `{time: 24, level: 1, lat: 90, lon: 180}` at float32 is `24 × 1 × 90 × 180 × 4 bytes ≈ 1.5 MiB`. If you were suppressing these warnings or avoiding `validate_chunks()` because of them, it is safe to re-enable them.

---

### `rechunk_dataset()` fails with `extract_zarr_variable_encoding() missing … 'zarr_format'`

The `rechunker` library calls an internal xarray function (`extract_zarr_variable_encoding`) that gained a required `zarr_format` keyword argument in newer xarray releases. Older rechunker versions don't pass it, producing:

```
TypeError: extract_zarr_variable_encoding() missing 1 required keyword-only argument: 'zarr_format'
RuntimeError: rechunk_dataset failed: extract_zarr_variable_encoding() ...
```

`rechunk_dataset()` now detects this incompatibility automatically and falls back to a native `xarray.to_zarr()` rechunking path. You will see a warning in the log but the function completes successfully:

```
WARNING [rechunk] rechunker is incompatible with the installed xarray version ...
         Falling back to native xarray.to_zarr() rechunking.
INFO    [rechunk] Rechunking complete (native fallback)
```

No changes to your code are needed. The `max_mem` parameter is ignored in fallback mode.

---

### Resource detection failure

If `$NCPUS`, `$PBS_MEM`, and `psutil` all fail, an error is raised. Set `fallback_on_detection_failure=True` to use a conservative 2-core, 8 GiB default instead:

```python
client, cluster, _ = setup_dask_client(
    "cpu",
    fallback_on_detection_failure=True,
)
```

---

## Multi-Node Issues (v2.0)

### `ImportError: dask-jobqueue is required for multi-node clusters`

Multi-node PBS/SLURM support requires `dask-jobqueue`:

```bash
pip install dask-jobqueue
# or
conda install -c conda-forge dask-jobqueue
```

The import is deferred — `from dask_setup import MultiNodeConfig` works without it; the error is raised only when `setup_pbs_cluster()` or `setup_slurm_cluster()` is called.

---

### Jobs submitted but workers never connect

On PBS, check that `$PBS_JOBID` is set in the submitted worker jobs. Verify network connectivity between the scheduler node and compute nodes. Add a `scheduler_options={"host": "<your-ip>"}` entry to `MultiNodeConfig` if the scheduler binds to the wrong interface.

Also check that `dask-jobqueue` is available in the compute-node environment — add the appropriate module load or conda activation to `env_extra`:

```python
cfg = MultiNodeConfig(
    workers_per_node=4,
    cores_per_worker=12,
    mem_per_worker_gb=32.0,
    walltime="04:00:00",
    env_extra=["module load python3", "source activate myenv"],
)
```

---

### Multi-node shared temp directory issues

`$PBS_JOBFS` is per-node and not visible to other nodes. Use `SharedTempDir` backed by Lustre/GPFS for stores that must be visible cluster-wide:

```python
from dask_setup import MultiNodeConfig

cfg = MultiNodeConfig(
    workers_per_node=4,
    cores_per_worker=12,
    mem_per_worker_gb=32.0,
    walltime="04:00:00",
    shared_tmp_dir="/scratch/project/tmp",  # Lustre path
)
```

See the [Multi-Node](multi-node.md) page for full `SharedTempDir` documentation.

---

### GPU workers not detecting GPUs

`dask_setup` detects GPUs from `CUDA_VISIBLE_DEVICES` first, then CuPy. For multi-node GPU jobs, ensure `CUDA_VISIBLE_DEVICES` is set correctly inside each submitted job by adding it to `env_extra`:

```python
cfg = MultiNodeConfig(
    workload_type="gpu",
    workers_per_node=4,
    cores_per_worker=6,
    mem_per_worker_gb=32.0,
    walltime="02:00:00",
    queue="gpuvolta",
    job_extra_directives=["-l ngpus=4"],
    env_extra=["export CUDA_VISIBLE_DEVICES=0,1,2,3"],
)
```

---

## PBS Job Template

```bash
#!/bin/bash
#PBS -q normalsr
#PBS -l ncpus=104
#PBS -l mem=300gb
#PBS -l jobfs=200gb
#PBS -l walltime=12:00:00
#PBS -l storage=gdata/hh5+gdata/gb02
#PBS -l wd

module use /g/data/hh5/public/modules/
module load conda_concept/analysis3-unstable

python your_script.py
```

`setup_dask_client()` reads `$PBS_JOBFS`, `$NCPUS`, and `$PBS_MEM` automatically. You do not need to set `TMPDIR` manually.

---

## Optimal Chunk Sizes

Target 256–512 MiB per chunk. Use `recommend_chunks()` to get a cluster-aware starting point:

```python
from dask_setup import recommend_chunks
chunks = recommend_chunks(ds, client, verbose=True)
ds = ds.chunk(chunks)
```

For storage-format-aware recommendations (Zarr, NetCDF, Kerchunk, Parquet), see [IO-Optimization](io-optimization.md).

---

## Migration Guide

### From v1.x to v2.0

The original `setup_dask_client()` positional API is fully backward compatible:

```python
# These still work exactly as before
client, cluster, dask_tmp = setup_dask_client("cpu")
client, cluster, dask_tmp = setup_dask_client("cpu", max_workers=8, reserve_mem_gb=60)
```

New capabilities available from v2.0:

```python
from dask_setup import setup_dask_client, DaskSetupConfig

# Named profile
client, cluster, dask_tmp = setup_dask_client(profile="climate_analysis")

# Full config object — most explicit option
config = DaskSetupConfig(
    workload_type="cpu",
    max_workers=8,
    reserve_mem_gb=60.0,
    spill_compression="lz4",
    suggest_chunks=True,
    io_format="zarr",
)
client, cluster, dask_tmp = setup_dask_client(config=config)

# Multi-node
from dask_setup import MultiNodeConfig
client, cluster, shared_tmp = setup_dask_client(
    mode="pbs",
    multi_node_config=MultiNodeConfig(
        workers_per_node=4,
        cores_per_worker=12,
        mem_per_worker_gb=32.0,
        walltime="04:00:00",
    ),
)
```

### What changed between versions

**v2.2** — Correctness release. Explicit `setup_dask_client()` arguments are no longer discarded when they happen to equal a default; `profile=` and `config=` reach the multi-node backends and return a 4-tuple with `ds=`; multi-node worker resources are no longer under-provisioned 4x; `reserve_mem_gb` reduces the worker count instead of over-committing; `tune_memory_thresholds()` applies to live workers; spill volume and `peak_memory_gib` report real numbers; profile inheritance cycles are rejected with a readable message; user profiles shadow builtins in `get_profile()` as they already did in `list_profiles()`; `configure_logging()` and `$DASK_SETUP_LOG_LEVEL` work; `env_extra` lines are emitted verbatim; the SSH tunnel hint is no longer hardcoded to Gadi.

**v2.1** — Bug fixes: PBS memory detection (`$PBS_MEM` bare integers now treated as bytes, not MB); false OOM warnings in `validate_chunks()` and `recommend_chunks()` when dataset is already chunked on all dimensions; `rechunk_dataset()` fallback to native `xarray.to_zarr()` when `rechunker` is incompatible with newer xarray (`zarr_format` argument); interactive PBS/SLURM session support (`mode="interactive"`, `setup_interactive_cluster()`); `wait_for_workers` on multi-node cluster setup.

**v2.0** — Multi-node PBS/SLURM support (`MultiNodeConfig`, `setup_pbs_cluster`, `setup_slurm_cluster`), GPU topology (`workload_type="gpu"`), `mode=` parameter on `setup_dask_client`, `dask-setup submit` CLI.

**v1.8** — Performance benchmarking (`benchmark_config`, `scaling_analysis`, `chunk_impact`, `run_synthetic_benchmark`, `dask-setup benchmark` CLI).

**v1.7** — Profile ecosystem: inheritance (`based_on:`), site-wide profiles (`/etc/dask_setup/profiles/`), profile versioning, URL import, JSON Schema.

**v1.6** — Zarr v3 + sharding, Kerchunk/VirtualiZarr, Parquet/Arrow recommendations, blosc2 codecs.

**v1.5** — Auto workload inference (`workload_type="auto"`), adaptive memory tuning (`adaptive_memory=True`), profile auto-selection (`profile="auto"`).

**v1.4** — Cluster summary on close, `cluster_report()`, Jupyter dashboard links.

**v1.3** — Jupyter/IPython detection, graceful `fallback_on_detection_failure`.

**v1.2** — `rechunk_dataset()`, `validate_chunks()`, dataset-aware setup.

**v1.1** — Context manager (`DaskClientContext`), structured logging. (The smart `reserve_mem_gb` default was written in this release but never called; it took effect in v2.2.)
