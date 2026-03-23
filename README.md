# dask_setup

[![CI](https://github.com/21centuryweather/dask_setup/workflows/CI/badge.svg)](https://github.com/21centuryweather/dask_setup/actions)

A single-node Dask setup helper for HPC environments (especially **NCI Gadi**). It wraps `dask.distributed.LocalCluster` + `Client` with sensible defaults for PBS/SLURM systems so you spend time on science, not configuration.

**Python 3.11+** | `pip install dask-setup`

---

## Quick Start

```python
from dask_setup import setup_dask_client

# Pick a workload type and go
client, cluster, dask_tmp = setup_dask_client("cpu")   # heavy compute
client, cluster, dask_tmp = setup_dask_client("io")    # heavy file I/O
client, cluster, dask_tmp = setup_dask_client("mixed") # both
```

For more control, pass a `DaskSetupConfig` object:

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

`dask_tmp` is the path to the spill/temp directory (on `$PBS_JOBFS` if available). Pass it to Rechunker, Zarr, or anywhere else you want fast local I/O.

---

## Workload Types

| Type | Workers | Topology | Best for |
|------|---------|----------|----------|
| `"cpu"` | ≈ all cores | many processes, 1 thread each | NumPy/Numba math, xarray reductions |
| `"io"` | 1 | 1 process, 8–16 threads | opening many NetCDF/Zarr files |
| `"mixed"` | cores / 2 | processes with 2 threads each | pipelines that both read and compute |

---

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `workload_type` | `"io"` | Worker topology (`"cpu"`, `"io"`, `"mixed"`) |
| `max_workers` | all cores | Hard cap on worker count |
| `reserve_mem_gb` | `50.0` | Memory held back for OS/cache (GiB) |
| `max_mem_gb` | total RAM | Upper bound on Dask's total memory use |
| `dashboard` | `True` | Start dashboard and print SSH tunnel hint |
| `profile` | `None` | Named config profile (see [docs/configuration.md](docs/configuration.md)) |
| `config` | `None` | Pre-built `DaskSetupConfig` object — mutually exclusive with `profile` |

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

See [docs/configuration.md](docs/configuration.md) for how to create and save your own profiles.

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
dask-setup list                                    # list all profiles
dask-setup show climate_analysis                   # show profile details
dask-setup create my_profile                       # create profile interactively
dask-setup create my_profile --from-profile zarr_io_heavy
dask-setup validate my_profile                     # validate a profile
dask-setup export climate_analysis -o profile.yaml # export to YAML
dask-setup delete my_profile
```

---

## Design Limits

- **Single node only.** For multi-node, use `dask-jobqueue` (`PBSCluster`, `SLURMCluster`).
- **No GPU tuning.** Adjust processes/threads and RMM settings manually for CuPy/RAPIDS.
- **POSIX paths only.** Temp directory logic assumes a POSIX filesystem.

---

## Documentation

| Topic | File |
|-------|------|
| `DaskSetupConfig` reference, profiles, CLI | [docs/configuration.md](docs/configuration.md) |
| How it works (resource detection, memory, topology) | [docs/internals.md](docs/internals.md) |
| I/O optimisation, xarray chunking | [docs/io_optimization.md](docs/io_optimization.md) |
| Error types and handling | [docs/ENHANCED_ERROR_HANDLING.md](docs/ENHANCED_ERROR_HANDLING.md) |
| Troubleshooting, PBS template, migration from v1.x | [docs/troubleshooting.md](docs/troubleshooting.md) |

---

## Contributing

Bug reports, feature requests, and pull requests are welcome. Please include tests and, for performance changes, benchmarks.

## License

Apache-2.0 — see [LICENSE](LICENSE) for details.
