# Benchmarking & Performance Analysis

`dask_setup` ships a benchmarking module (`dask_setup.benchmark`) with three complementary tools for measuring and comparing performance on real or synthetic datasets.

---

## Quick Overview

| Function | What it does |
|----------|-------------|
| `benchmark_config()` | A/B-test multiple `DaskSetupConfig` objects against the same xarray operation |
| `scaling_analysis()` | Sweep worker counts and measure parallel speedup and efficiency |
| `chunk_impact()` | Sweep chunk sizes on a fixed cluster to find the throughput sweet spot |
| `run_synthetic_benchmark()` | No-dataset synthetic benchmark — used by the CLI |

All functions share a common `BenchmarkResult` dataclass.

---

## benchmark_config()

Compare multiple configurations against the same xarray operation. Each configuration gets a fresh cluster so memory state, topology, and spill behaviour are fully independent.

```python
from dask_setup import DaskSetupConfig
from dask_setup.benchmark import benchmark_config

configs = {
    "io_profile":  DaskSetupConfig(workload_type="io",  reserve_mem_gb=40),
    "cpu_profile": DaskSetupConfig(workload_type="cpu", reserve_mem_gb=60),
    "mixed":       DaskSetupConfig(workload_type="mixed"),
}

results = benchmark_config(
    configs,
    my_ds,
    operation="mean",
    repeats=3,
    verbose=True,
)

for r in sorted(results, key=lambda r: r.wall_time_seconds):
    print(r.summary_line())
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `configs` | — | `dict[name, DaskSetupConfig]`, `list[DaskSetupConfig]`, or a single config |
| `ds` | — | xarray Dataset or DataArray (unchunked; each config rechunks independently) |
| `operation` | `"mean"` | String alias or `callable(ds) → lazy_result` |
| `repeats` | `1` | Timed repetitions per config — mean and std-dev are recorded |
| `warmup` | `False` | Run one un-timed warmup pass to eliminate cold-start JIT overhead |
| `verbose` | `False` | Print a summary line after each config completes |

### Built-in operation aliases

`"mean"`, `"sum"`, `"std"`, `"max"`, `"min"`, `"var"`, `"rechunk"`

For anything else, pass a callable:

```python
results = benchmark_config(
    configs,
    my_ds,
    operation=lambda ds: ds.rolling(time=10).mean(),
)
```

---

## scaling_analysis()

Sweep worker counts (e.g. 1, 2, 4, 8) and measure speedup and parallel efficiency relative to the single-worker baseline.

```python
from dask_setup import DaskSetupConfig
from dask_setup.benchmark import scaling_analysis

result = scaling_analysis(
    my_ds,
    operation="mean",
    worker_counts=(1, 2, 4, 8),
    base_config=DaskSetupConfig(workload_type="cpu"),
    repeats=2,
    plot=True,   # renders a speedup + efficiency figure if matplotlib is installed
    verbose=True,
)

print(result.summary())
print(f"\nBest: {result.best().summary_line()}")
```

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ds` | — | xarray Dataset or DataArray |
| `operation` | `"mean"` | String alias or callable |
| `worker_counts` | `(1, 2, 4, 8)` | Worker counts to sweep; first entry is the baseline |
| `base_config` | `DaskSetupConfig(workload_type="cpu")` | Config used for each run — `max_workers` is overridden per run |
| `repeats` | `1` | Timed repetitions per worker count |
| `warmup` | `False` | Un-timed warmup pass |
| `plot` | `False` | Display speedup + efficiency chart (requires matplotlib) |
| `verbose` | `False` | Print summary line after each count |

> **The sweep only means something for topologies that scale with worker
> count.** `decide_topology()` pins `n_workers=1` for `workload_type="io"` (and
> for `"gpu"` with no GPU present) regardless of `max_workers`, because those
> topologies are single-process-many-threads by design. Sweeping one of those
> builds the same cluster at every point and the resulting curve is timing
> noise; a warning is logged if it happens.
>
> **Changed in v2.2:** the default `base_config` was `DaskSetupConfig()`, whose
> `workload_type` is `"io"` — so the default sweep was exactly that degenerate
> case. It is now `"cpu"`.

> **Efficiency is relative to the worker *ratio*, not the absolute count**
> (*changed in v2.2*). A sweep starting anywhere other than 1 worker used to
> report a fraction of its true efficiency — a perfectly scaling `(4, 8)` sweep
> scored `0.25` at its own baseline instead of `1.0`.

### ScalingResult

```python
result.summary()         # formatted table: workers / wall / speedup / efficiency / tasks_per_s
result.worker_counts     # [1, 2, 4, 8]
result.speedups          # [1.0, 1.87, 3.4, 5.9]
result.efficiencies      # [1.0, 0.94, 0.85, 0.74]
result.wall_times        # [12.4, 6.6, 3.6, 2.1]
result.best()            # BenchmarkResult with lowest wall time
result.to_dataframe()    # pandas DataFrame (requires pandas)
result.plot()            # matplotlib Figure with speedup + efficiency subplots
```

---

## chunk_impact()

Fix a running cluster and sweep chunk sizes to find the throughput sweet spot. Unlike the other two functions, this one **does not** create or destroy clusters.

```python
from dask_setup import setup_dask_client
from dask_setup.benchmark import chunk_impact

client, cluster, _ = setup_dask_client("cpu")

result = chunk_impact(
    my_ds,
    client,
    operation="mean",
    chunk_sizes=[
        {"time": 30,  "lat": 180, "lon": 360},
        {"time": 60,  "lat": 180, "lon": 360},
        {"time": 90,  "lat": 360, "lon": 720},
        {"time": 120, "lat": 360, "lon": 720},
    ],
    repeats=2,
    plot=True,
    verbose=True,
)

print(result.summary())
print(f"\nRecommended: {result.recommended_chunks}")
```

If `chunk_sizes` is `None` and `auto_chunks=True` (the default), a five-point geometric sweep is generated automatically across all chunkable dimensions.

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `ds` | — | xarray Dataset or DataArray with known dimension sizes |
| `client` | — | Connected Dask client |
| `operation` | `"mean"` | String alias or callable |
| `chunk_sizes` | `None` | List of chunk-spec dicts; auto-generated when `None` and `auto_chunks=True` |
| `auto_chunks` | `True` | Generate a 5-point geometric sweep when `chunk_sizes` is `None` |
| `repeats` | `1` | Timed repetitions per chunk spec |
| `warmup` | `False` | Un-timed warmup pass |
| `plot` | `False` | Display wall-time vs chunk-size figure |
| `verbose` | `False` | Print summary after each chunk spec |

### ChunkImpactResult

```python
result.summary()              # formatted table: chunks / wall / mem / tasks_per_s
result.recommended_chunks     # dict of the fastest chunk spec
result.best()                 # BenchmarkResult with lowest wall time
result.to_dataframe()         # pandas DataFrame
result.plot(dim="time")       # wall-time vs chunk size for one dimension
```

---

## BenchmarkResult

All three functions return (or contain) `BenchmarkResult` objects.

```python
from dask_setup.benchmark import BenchmarkResult

r: BenchmarkResult

r.name                # label (config name, worker count, chunk spec, etc.)
r.wall_time_seconds   # mean elapsed time across all repeats
r.wall_time_std       # std-dev of per-repeat times (0.0 for a single repeat)
r.peak_memory_gib     # highest total worker RSS observed *during* the timed runs
r.spill_gib           # total data written to disk spill storage
r.n_tasks             # number of dask-graph tasks
r.n_workers           # number of active workers
r.tasks_per_second    # derived: n_tasks / wall_time_seconds
r.errors              # list of non-fatal error messages
r.extra               # additional metadata dict

r.summary_line()      # one-line human-readable summary
r.to_dict()           # JSON-serialisable dict of all fields
```

> **`peak_memory_gib` is sampled every 0.2 s while the operation runs**
> (*changed in v2.2*). It used to be read from a cluster report taken *after*
> `.compute()` returned, by which point the workers had already released the
> data — so the "peak" was near zero for exactly the workloads whose memory use
> matters. If in-flight sampling is unavailable (an older `distributed`), the
> post-run reading is used and a note saying so is appended to `r.errors`.
>
> `spill_gib` was likewise always `0.0`: it was read from worker metric keys
> that current `distributed` does not publish. It now reads `spilled_bytes`.


---

## run_synthetic_benchmark()

A no-dataset synthetic benchmark backed by `dask.array`. No xarray or real data required. This is the backend for the `dask-setup benchmark` CLI subcommand.

```python
from dask_setup.benchmark import run_synthetic_benchmark

result = run_synthetic_benchmark(
    profile_name="climate_analysis",
    operation="mean",
    ds_size="medium",
    repeats=3,
    verbose=True,
)
print(result.summary())
```

### Dataset sizes

| `ds_size` | Array shape | Chunk shape |
|-----------|-------------|-------------|
| `"tiny"` | (200, 200, 10) | (50, 50, 10) |
| `"small"` | (500, 500, 20) | (100, 100, 20) |
| `"medium"` | (1000, 1000, 50) | (200, 200, 25) |
| `"large"` | (2000, 2000, 100) | (400, 400, 25) |

Arrays are float32, pre-seeded with `numpy.random.seed(42)` for reproducibility.

### SyntheticBenchmarkResult

```python
result.profile_name       # "climate_analysis"
result.operation          # "mean"
result.ds_size            # "medium"
result.array_shape        # (1000, 1000, 50)
result.wall_time_seconds  # 3.71
result.peak_memory_gib    # 0.42
result.spill_gib          # 0.0
result.n_tasks            # 250
result.n_workers          # 4
result.tasks_per_second   # 67.4

result.summary()          # formatted multi-line report
```

---

## dask-setup benchmark CLI

Run a synthetic benchmark against any profile without writing any Python:

```bash
# Benchmark the built-in development profile (2 workers, lightweight)
dask-setup benchmark --profile development --size small --operation mean

# Benchmark a custom profile with 3 timed repeats
dask-setup benchmark --profile climate_analysis --size medium --repeats 3

# Large array, std operation
dask-setup benchmark --profile zarr_io_heavy --size large --operation std
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--profile` / `-p` | `"development"` | Profile name (builtin or user-defined) |
| `--size` / `-s` | `"small"` | Dataset size: `tiny`, `small`, `medium`, `large` |
| `--operation` / `-O` | `"mean"` | Operation: `mean`, `sum`, `std`, `max`, `min` |
| `--repeats` / `-r` | `1` | Number of timed repetitions |

Output is the `SyntheticBenchmarkResult.summary()` printed to stdout.

---

## Comparing All Built-in Profiles

```python
from dask_setup import DaskSetupConfig
from dask_setup.benchmark import run_synthetic_benchmark

profiles = ["development", "interactive", "zarr_io_heavy", "climate_analysis"]

for p in profiles:
    result = run_synthetic_benchmark(profile_name=p, ds_size="medium")
    print(f"{p:20s}  {result.wall_time_seconds:.2f}s  "
          f"{result.tasks_per_second:.0f} tasks/s")
```

---

## matplotlib Integration

`scaling_analysis()` and `chunk_impact()` can render comparison plots directly:

```python
# Inline in a Jupyter notebook
import matplotlib
matplotlib.use("inline")

scaling = scaling_analysis(ds, worker_counts=(1, 2, 4, 8, 16), plot=True)
impact  = chunk_impact(ds, client, plot=True)
```

Or generate figures manually for embedding in reports:

```python
fig1 = scaling.plot(title="ERA5 scaling on Gadi normalsr")
fig2 = impact.plot(dim="time", title="Chunk size sweep — temperature variable")

fig1.savefig("scaling.png", dpi=150)
fig2.savefig("chunks.png", dpi=150)
```
