# dask_setup

HPC-tuned Dask helpers for single-node and multi-node runs on NCI Gadi and other PBS/SLURM systems.

---

## Quick Start

```python
from dask_setup import setup_dask_client

client, cluster, dask_tmp = setup_dask_client(workload_type="cpu")   # heavy compute
client, cluster, dask_tmp = setup_dask_client(workload_type="io")    # Zarr / object storage
client, cluster, dask_tmp = setup_dask_client(workload_type="mixed") # both
```

The default `mode="interactive"` uses the resources your job already has (a `LocalCluster` on one node or a laptop, an `SSHCluster` across a multi-node allocation) and never submits new jobs. See [Multi-Node](multi-node.md) to submit worker jobs.

`dask_tmp` is the spill/temp directory (`$PBS_JOBFS` when available). Pass it to Rechunker, Zarr, or anywhere you want fast local I/O.

### With a config object

```python
from dask_setup import setup_dask_client, DaskSetupConfig

config = DaskSetupConfig(
    workload_type="cpu",
    max_workers=8,
    reserve_mem_gb=32.0,
    spill_compression="lz4",
)
client, cluster, dask_tmp = setup_dask_client(config=config)
```

### Context manager

```python
from dask_setup import DaskClientContext

with DaskClientContext("cpu") as (client, cluster, dask_tmp):
    result = ds.mean().compute()
# cluster is closed automatically
```

### Multi-node (v2.0)

```python
from dask_setup import setup_dask_client, MultiNodeConfig

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

---

## Workload Types

| Type | Topology | Best for |
|------|----------|----------|
| `"cpu"` | Many processes, 1 thread each | NumPy/Numba math, xarray reductions, **and reading NetCDF/HDF5** |
| `"io"` | 1 process, 8–16 threads | Zarr, object storage (S3), HTTP — libraries that release the GIL |
| `"mixed"` | Processes with 2 threads each | Pipelines that both read and compute |
| `"gpu"` | 1 process per GPU, up to 8 threads | CuPy/RAPIDS CUDA workloads |
| `"auto"` | Inferred from dataset | Let `dask_setup` decide based on your data |

### Reading NetCDF? Use `"cpu"`, not `"io"`

The split is not "I/O vs compute" — it is **whether the library releases the GIL
and is thread-safe**. `"io"` runs one process with many threads, which only wins
when threads can genuinely overlap.

HDF5 (underneath NetCDF4) is usually not built thread-safe, so xarray routes
*every* NetCDF read through a single process-wide lock. Under `"io"` your threads
do not become parallel readers — they queue on that one lock, one at a time, and
zlib decompression is serialised inside it too. Separate processes each get their
own lock, so `"cpu"` is what actually parallelises NetCDF reads.

Zarr is the opposite: it decompresses via numcodecs, which releases the GIL, and
has no global lock, so threads scale well. Opening, concatenating and writing
NetCDF is a `"cpu"` job even though it feels like I/O.

> **Rule of thumb: NetCDF in → `"cpu"`. Zarr or object storage in → `"io"`.**
> Pass `ds=` and `dask_setup` will warn if you get this pair the wrong way round.
>
> Also avoid `open_mfdataset(..., parallel=True)` together with `"io"`: concurrent
> metadata reads can kill the worker outright, after which Dask silently
> recomputes the lost tasks and the job appears to hang rather than fail.

---

## Documentation

| Page | What's covered |
|------|----------------|
| [Configuration](configuration.md) | `DaskSetupConfig`, profiles, site-wide profiles, profile inheritance, CLI, JSON Schema |
| [Multi-Node](multi-node.md) | `MultiNodeConfig`, PBS/SLURM cluster setup, GPU topology, shared temp dirs, `dask-setup submit` |
| [IO-Optimization](io-optimization.md) | `recommend_chunks`, `recommend_io_chunks`, Zarr v3, Kerchunk, Parquet/Arrow, storage-aware chunking |
| [Benchmarking](benchmarking.md) | `benchmark_config`, `scaling_analysis`, `chunk_impact`, `dask-setup benchmark` |
| [Internals](internals.md) | Resource detection, topology decisions, temp/spill routing, module layout |
| [Troubleshooting](troubleshooting.md) | Common errors, OOM, spill issues, migration from older versions |
| [User Feedback](user-feedback.md) | Questions from users, the answers, and what changed as a result |
| [API Reference](api.rst) | Every public function and class, generated from the docstrings |
| [Changelog](changelog.md) | Per-release changes |

---

## Installation

From PyPI:

```bash
pip install dask-setup
```

From conda-forge:

```bash
conda install -c conda-forge dask-setup
```

For multi-node PBS/SLURM support:

```bash
pip install dask-setup dask-jobqueue
# or
conda install -c conda-forge dask-setup dask-jobqueue
```

For GPU workloads (CuPy auto-detection):

```bash
pip install dask-setup cupy-cuda12x   # match your CUDA version
```

---

## Version History

| Version | Highlights |
|---------|-----------|
| **2.2** | Correctness release — explicit arguments and `profile=`/`config=` now reach the cluster, multi-node worker sizing, live memory tuning, real spill/peak-memory reporting, working log configuration |
| **2.1** | Bug fixes — PBS memory detection, false OOM warnings, native rechunk fallback, `mode="interactive"` |
| **2.0** | Multi-node PBS/SLURM (`MultiNodeConfig`, `setup_pbs_cluster`, `setup_slurm_cluster`), GPU topology, `dask-setup submit` |
| **1.8** | Performance benchmarking (`benchmark_config`, `scaling_analysis`, `chunk_impact`, `dask-setup benchmark`) |
| **1.7** | Profile ecosystem — inheritance (`based_on:`), site-wide profiles, versioning, URL import, JSON Schema |
| **1.6** | Zarr v3 + sharding, Kerchunk/VirtualiZarr, Parquet/Arrow recommendations, blosc2 codecs |
| **1.5** | Auto workload inference, adaptive memory tuning, worker health callbacks, profile auto-selection |
| **1.4** | Cluster summary on close, `cluster_report()`, Jupyter dashboard links |
| **1.3** | Jupyter/IPython detection, graceful fallback on resource detection failure |
| **1.2** | `rechunk_dataset()`, `validate_chunks()`, dataset-aware setup |
| **1.1** | Context manager, structured logging, `py.typed` (the smart `reserve_mem_gb` default was written here but not wired up until 2.2) |

Full details in [CHANGELOG.md](https://github.com/21centuryweather/dask_setup/blob/main/CHANGELOG.md).

```{toctree}
:hidden:
:caption: User guide

configuration
multi-node
io-optimization
benchmarking
troubleshooting
user-feedback
```

```{toctree}
:hidden:
:caption: Reference

api
internals
changelog
```
