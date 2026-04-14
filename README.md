# dask_setup

[![CI](https://github.com/21centuryweather/dask_setup/workflows/CI/badge.svg)](https://github.com/21centuryweather/dask_setup/actions)

HPC-tuned Dask helpers for **NCI Gadi** and other PBS/SLURM systems. Wraps `dask.distributed.LocalCluster` + `Client` with sensible defaults for single-node jobs, and extends to multi-node PBS/SLURM clusters via `dask-jobqueue` in v2.0.

**Python 3.11+** | `pip install dask-setup`

---

## Quick Start

```python
from dask_setup import setup_dask_client

# Pick a workload type and go
client, cluster, dask_tmp = setup_dask_client(mode="interactive", workload_type="cpu")   # heavy compute
client, cluster, dask_tmp = setup_dask_client(mode="interactive", workload_type="io")    # heavy file I/O
client, cluster, dask_tmp = setup_dask_client(mode="interactive", workload_type="mixed") # both
```

`dask_tmp` is the path to the spill/temp directory (on `$PBS_JOBFS` if available). Pass it to Rechunker, Zarr, or anywhere else you want fast local I/O.

For more control, pass a `DaskSetupConfig` object or a named profile:

```python
from dask_setup import setup_dask_client, DaskSetupConfig

config = DaskSetupConfig(
    workload_type="cpu",
    max_workers=8,
    reserve_mem_gb=32.0,
    spill_compression="lz4",
)
client, cluster, dask_tmp = setup_dask_client(config=config)

# Or use a built-in profile
client, cluster, dask_tmp = setup_dask_client(profile="climate_analysis")
```

**Multi-node (v2.0):**

```python
from dask_setup import setup_dask_client, MultiNodeConfig

client, cluster, shared_tmp = setup_dask_client(
    mode="auto",   # detects PBS_JOBID / SLURM_JOB_ID, falls back to local
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
| `"cpu"` | Many processes, 1 thread each | NumPy/Numba math, xarray reductions |
| `"io"` | 1 process, 8–16 threads | Opening many NetCDF/Zarr files concurrently |
| `"mixed"` | Processes with 2 threads each | Pipelines that both read and compute |
| `"gpu"` | 1 process per GPU, up to 8 threads | CuPy/RAPIDS CUDA workloads |
| `"auto"` | Inferred from dataset | Let `dask_setup` decide based on your data |

---

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `workload_type` | `"io"` | Worker topology: `"cpu"`, `"io"`, `"mixed"`, `"gpu"`, `"auto"` |
| `max_workers` | all cores | Hard cap on worker count |
| `reserve_mem_gb` | auto (20% RAM) | Memory held back for OS/cache (GiB) |
| `max_mem_gb` | total RAM | Upper bound on Dask's total memory use |
| `dashboard` | `True` | Start dashboard and print SSH tunnel hint |
| `profile` | `None` | Named config profile |
| `config` | `None` | Pre-built `DaskSetupConfig` object — mutually exclusive with `profile` |
| `mode` | `"auto"` | `"local"`, `"pbs"`, `"slurm"`, or `"auto"` (v2.0) |
| `multi_node_config` | `None` | `MultiNodeConfig` for PBS/SLURM multi-node jobs (v2.0) |

---

## Common Patterns

**Big xarray reductions (CPU-bound)**
```python
client, cluster, dask_tmp = setup_dask_client("cpu", reserve_mem_gb=60)
ds = ds.chunk({"time": 240, "y": 512, "x": 512})
out = ds.mean(("y", "x")).compute()
```

**Opening many NetCDF/Zarr files (I/O-bound)**
```python
client, cluster, dask_tmp = setup_dask_client("io", reserve_mem_gb=40)
ds = xr.open_mfdataset(files, engine="netcdf4", chunks={}, parallel=True)
```

**Rechunking with Rechunker (spill stays on fast local storage)**
```python
client, cluster, dask_tmp = setup_dask_client("cpu", reserve_mem_gb=60)
plan = rechunker.rechunk(
    ds.to_array().data,
    target_chunks={"time": 240, "y": 512, "x": 512},
    max_mem="6GB",
    target_store="out.zarr",
    temp_store=f"{dask_tmp}/tmp_rechunk.zarr",
)
plan.execute()
```

**Writing to Zarr in time windows**
```python
client, cluster, dask_tmp = setup_dask_client("cpu", max_workers=1, reserve_mem_gb=50)
ds.isel(time=slice(0, 240)).to_zarr("out.zarr", mode="w", consolidated=True)
for start in range(240, ds.sizes["time"], 240):
    stop = min(start + 240, ds.sizes["time"])
    ds.isel(time=slice(start, stop)).to_zarr(
        "out.zarr", mode="a", region={"time": slice(start, stop)}
    )
```

---

## Built-in Profiles

Use `profile=` to load a named configuration:

```python
client, cluster, dask_tmp = setup_dask_client(profile="climate_analysis")
```

| Profile | Workload | Reserve | Notes |
|---------|----------|---------|-------|
| `climate_analysis` | cpu | 60 GB | Heavy compute with large arrays |
| `zarr_io_heavy` | io | 40 GB | Many Zarr files |
| `development` | mixed | 8 GB | Local testing, 2 workers max |
| `production` | mixed | 80 GB | Adaptive, dashboard off |
| `interactive` | mixed | 20 GB | Jupyter notebooks, 4 workers |

See [Configuration](https://github.com/21centuryweather/dask_setup/wiki/Configuration) for how to create and save your own profiles.

---

## Dashboard

With `dashboard=True` (default), the cluster prints an SSH tunnel command:

```
Dask dashboard: http://127.0.0.1:<PORT>/status
Tunnel from your laptop:
  ssh -N -L 8787:<COMPUTE_HOST>:<PORT> gadi.nci.org.au
Then open: http://localhost:8787
```

---

## CLI

```bash
# Profile management
dask-setup list                                       # list all profiles
dask-setup show climate_analysis                      # show profile details
dask-setup create my_profile --from-profile zarr_io_heavy
dask-setup validate my_profile
dask-setup export climate_analysis -o profile.yaml
dask-setup import https://example.com/team.yaml
dask-setup delete my_profile

# Synthetic benchmark
dask-setup benchmark --profile development --size small --operation mean

# Generate a PBS/SLURM job script (v2.0)
dask-setup submit my_analysis.py --scheduler pbs \
    --workers-per-node 4 --cores-per-worker 12 --mem-per-worker 32 \
    --walltime 04:00:00 --queue normal --project ab01
```

---

## Documentation

Full documentation lives in the [GitHub wiki](https://github.com/21centuryweather/dask_setup/wiki):

| Page | What's covered |
|------|----------------|
| [Configuration](https://github.com/21centuryweather/dask_setup/wiki/Configuration) | `DaskSetupConfig`, profiles, site-wide profiles, profile inheritance, CLI, JSON Schema |
| [Multi-Node](https://github.com/21centuryweather/dask_setup/wiki/Multi-Node) | `MultiNodeConfig`, PBS/SLURM cluster setup, GPU topology, shared temp dirs, `dask-setup submit` |
| [IO-Optimization](https://github.com/21centuryweather/dask_setup/wiki/IO-Optimization) | `recommend_chunks`, `recommend_io_chunks`, Zarr v3, Kerchunk, Parquet/Arrow, storage-aware chunking |
| [Benchmarking](https://github.com/21centuryweather/dask_setup/wiki/Benchmarking) | `benchmark_config`, `scaling_analysis`, `chunk_impact`, `dask-setup benchmark` |
| [Internals](https://github.com/21centuryweather/dask_setup/wiki/Internals) | Resource detection, topology decisions, temp/spill routing, module layout |
| [Troubleshooting](https://github.com/21centuryweather/dask_setup/wiki/Troubleshooting) | Common errors, OOM, multi-node issues, migration guide |

---

## Installation

```bash
pip install dask-setup
```

For multi-node PBS/SLURM support:

```bash
pip install dask-setup dask-jobqueue
```

For GPU workloads (CuPy auto-detection):

```bash
pip install dask-setup cupy-cuda12x   # match your CUDA version
```

---

## Contributing

Bug reports, feature requests, and pull requests are welcome. Please include tests and, for performance changes, benchmarks.

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.
