# How dask_setup Works

This page describes what `setup_dask_client()` does under the hood and how the library is structured internally.

---

## Resource Detection

Resources are detected in priority order.

**CPU cores:**
1. `SLURM_CPUS_ON_NODE`
2. `NCPUS` or `PBS_NCPUS`
3. `psutil.cpu_count(logical=True)`

**Memory:**
1. `SLURM_MEM_PER_NODE` or `SLURM_MEM_PER_CPU` (in MB)
2. `PBS_VMEM` or `PBS_MEM` (e.g. `"300gb"`, `"1.5gib"`)
3. `psutil.virtual_memory().total`

The memory parser handles binary/decimal unit prefixes (`gb` vs `gib`), space-separated strings (`"16 GB"`), and pure integers (treated as MB for SLURM compatibility).

**Fallback behaviour:** If all detection methods fail and `fallback_on_detection_failure=True` is set, a conservative default (2 cores, 8 GiB) is used instead of raising an error.

**Memory allocation formula:**

```python
total_mem_gib = total_mem_bytes / (1024**3)
effective_total_gb = min(max_mem_gb or total_mem_gib, total_mem_gib)
usable_mem_gb = max(0.0, effective_total_gb - reserve_mem_gb)

# Fit the worker count to the memory before dividing it up, so the 1 GiB
# per-worker floor below can never commit more than the node has.
n_workers = max(1, min(n_workers, int(usable_mem_gb // 1.0)))

mem_per_worker_gb = max(1.0, usable_mem_gb / n_workers)
mem_per_worker_bytes = int(mem_per_worker_gb * (1024**3))
```

Memory is passed to each worker as bytes. Dask is strict about `memory_limit` units, so bytes avoids off-by-one rounding errors.

**Worker count is fitted to memory.** The `max(1.0, ...)` floor exists so a
worker is never given an unusably small limit, but on its own it over-commits:
64 workers on a 64 GiB node with a 50 GiB reserve used to get 1 GiB each — 64
GiB committed against 14 GiB usable. The worker count is now reduced first (to
14 in that example), and the reduction is logged and printed rather than done
silently.

**Smart `reserve_mem_gb` default:** When `reserve_mem_gb` is not supplied, the
value is 20% of total RAM, clamped to [4 GiB, 50 GiB], and then capped at half
the machine with a 1 GiB floor. The half-machine cap matters on small hosts: on
a 4 GiB container the 4 GiB minimum would otherwise reserve everything. A
profile, a `config=` object or an explicit argument all override it.

---

## Temp & Spill Directory Routing

All Dask temp and spill files are routed to fast local storage. The priority is:

```
$PBS_JOBFS  →  $TMPDIR  →  /tmp
```

A unique directory is created per-process:

```
{base_tmp}/dask-{pid}/
```

The following locations all point at this directory:

- `os.environ["TMPDIR"]`
- `os.environ["DASK_TEMPORARY_DIRECTORY"]`
- `dask.config["temporary-directory"]`
- `dask.config["distributed.worker.local-directory"]`

Workers create per-worker subdirectories inside this path (e.g. `worker-abc123/spill/`).

> **Multi-node note:** On multi-node PBS/SLURM jobs, `$PBS_JOBFS` is per-node and not visible to other nodes. Use `SharedTempDir` (backed by Lustre/GPFS) for stores that must be visible cluster-wide — see [Multi-Node](multi-node.md).

---

## Worker Topology

### CPU-bound (`workload_type="cpu"`)

```
processes=True, threads_per_worker=1, n_workers ≈ core count
```

Each worker is a separate process with one thread. This avoids Python's GIL for compute-heavy operations. Memory is split evenly.

### I/O-bound (`workload_type="io"`)

```
processes=False, threads_per_worker=8–16, n_workers=1
```

A single process with many threads, avoiding process startup and inter-process
transfer. Thread count is `min(16, max(4, ceil(cores / 2)))`.

**This only helps when the reading library releases the GIL and is thread-safe.**
That holds for Zarr (numcodecs releases the GIL, no global lock), object storage
and HTTP. It does *not* hold for NetCDF4/HDF5: HDF5 is usually not built
thread-safe, so xarray serialises every NetCDF read through one process-wide
lock. Under `"io"`, threads therefore queue on that lock instead of reading in
parallel, and zlib decompression is serialised inside it as well — which is
normally the dominant cost for compressed climate files.

For NetCDF, use `"cpu"`: each process gets its own lock and its own GIL, so reads
and decompression actually run concurrently. Combining `"io"` with
`open_mfdataset(..., parallel=True)` is worse still — concurrent metadata reads
can kill the worker, and Dask's silent recompute of the lost tasks turns a
crash into an apparent hang.

### Mixed (`workload_type="mixed"`)

```
processes=True, threads_per_worker=2, n_workers ≈ cores / 2
```

A compromise: several processes with a small thread pool each. Good for pipelines that both open files and perform non-trivial computation.

### GPU (`workload_type="gpu"`)

```
processes=True, threads_per_worker=2–8, n_workers = GPU count
```

One worker process per CUDA-capable GPU, with several CPU threads for data loading and preprocessing. GPU count is detected from `CUDA_VISIBLE_DEVICES` first, then `cupy.cuda.runtime.getDeviceCount()`. Falls back to a single-process multi-thread worker (like `"io"`) when no GPUs are found, with a warning.

### Auto (`workload_type="auto"`)

Inspects the dataset's variables, dtypes, and dimension structure to choose
`"cpu"`, `"io"`, or `"mixed"` by scoring two signals against each other:

- **CPU signals:** CF-convention dimension names (`time`, `lat`, `lon`, `lev`, …)
  and a high proportion of floating-point variables.
- **I/O signals:** mostly integer, byte or boolean variables, and many small
  variables.

A typical geoscience dataset — float32 fields on `time`/`lat`/`lon` — scores as
`"cpu"`, which is the right answer for NetCDF input. The heuristic is deliberately
conservative and returns `"mixed"` whenever the signal is ambiguous or no dataset
is supplied.

---

## Dimension Classification (`chunk_domain`)

`recommend_chunks()` accepts an optional `chunk_domain` parameter (`"spatial"` or `"temporal"`) that pins one group of dimensions to a single full-size chunk (xarray `-1`) while confining the size-fitting algorithm to the other group.

### Pattern Matching

Two module-level tuples in `xarray.py` drive classification:

**`_TEMPORAL_PATTERNS`** — a substring is matched (case-insensitive) against the dimension name:

```
"time", "date", "step", "record", "sample", "day", "month", "year", "hour"
```

**`_SPATIAL_PATTERNS`** — same substring matching:

```
"lat", "lon", "latitude", "longitude",
"x", "y", "z",
"north", "south", "east", "west",
"altitude", "depth", "level", "lev", "height", "pressure",
"ni", "nj"
```

Any dimension that matches neither list is placed in an `"other"` bucket and treated as a free dimension (eligible for size-fitting) regardless of `chunk_domain`.

### `_classify_dimensions(dims)`

```python
def _classify_dimensions(dims: dict[str, int]) -> dict[str, list[str]]:
    ...
```

Returns `{"temporal": [...], "spatial": [...], "other": [...]}`. Called once at the start of `_calculate_optimal_chunks()` when `chunk_domain` is not `None`.

### Locked vs Free Dimensions

Inside `_calculate_optimal_chunks()`:

- **locked dims** — the group corresponding to the *opposite* domain (e.g. temporal dims are locked when `chunk_domain="spatial"`). Locked dims always emit `-1` in the final chunk dict and are excluded from all size-fitting loops.
- **free dims** — the group matching the requested domain, plus any `"other"` dims. Free dims are passed through the normal workload-type sizing algorithm.

The locked/free split is applied before the workload-type branch (`io` / `cpu` / `mixed`), so `chunk_domain` composes cleanly with all existing strategies.

### Output

The returned `ChunkRecommendation` includes two extra keys in `dataset_info`:

| Key | Value |
|-----|-------|
| `"chunk_domain"` | `"spatial"`, `"temporal"`, or `None` |
| `"locked_dims"` | list of dimension names that were set to `-1` |

`_format_chunk_report()` surfaces these in the `verbose=True` output:

```
 Chunk domain: spatial
 Fully loaded (lock=-1): ['time']
```

---

## Memory Safety Thresholds

Set via `dask.config.set()` before the cluster is created:

| Threshold | Default | Meaning |
|-----------|---------|---------|
| `memory.target` | `0.75` | Start moving data to disk at 75% worker memory |
| `memory.spill` | `0.85` | Spill aggressively at 85% |
| `memory.pause` | `0.92` | Pause scheduling new tasks at 92% |
| `memory.terminate` | `0.98` | Kill the worker as a last resort at 98% |

These prevent OOM crashes when tasks temporarily use more memory than chunk-size estimates predict.

### Adaptive Memory Tuning

Pass `adaptive_memory=True` to call `tune_memory_thresholds()` once after the cluster is ready. This reads the (initially zero) spill stats and tightens thresholds slightly, giving workers more head-room from the start:

```python
client, cluster, dask_tmp = setup_dask_client("cpu", adaptive_memory=True)
```

Thresholds are applied to the **live** workers, not just to `dask.config`.
`WorkerMemoryManager` reads `distributed.worker.memory.target` and `.spill`
once at construction and sizes the `SpillBuffer`'s eviction threshold from them
at the same moment, so a running worker never re-reads the config. Tuning
updates the manager and resizes the buffer directly, then writes the config too
so anything started later agrees.


---

## Post-Run Reporting

When `client.close()` is called (or the context manager exits), a one-line summary is printed: total wall time, peak memory per worker, total spill volume, and tasks executed.

For a richer report:

```python
from dask_setup import cluster_report

report = cluster_report(client)
print(report.summary())
# Returns a ClusterReport with .memory_highwatermarks, .spill_stats, .task_counts
```

---

## Mode Dispatch (v2.0)

`setup_dask_client()` accepts a `mode=` parameter that determines which backend is used:

| mode | Backend |
|------|---------|
| `"local"` | `dask.distributed.LocalCluster` (single node) |
| `"pbs"` | `dask-jobqueue.PBSCluster` (multi-node) |
| `"slurm"` | `dask-jobqueue.SLURMCluster` (multi-node) |
| `"auto"` | Detects from environment (`PBS_JOBID` → pbs, `SLURM_JOB_ID` → slurm, else local) |

When a multi-node mode is selected, the function returns immediately after setting up the cluster — the rest of the single-node setup (topology, memory spec, LocalCluster) is skipped.

---

## Module Layout

```
src/dask_setup/
├── __init__.py         # Public API — all exports live here
├── client.py           # setup_dask_client(), DaskClientContext, _resolve_configuration()
├── cluster.py          # create_cluster(), configure_dask_settings(), calculate_memory_spec()
├── config.py           # DaskSetupConfig, ConfigProfile dataclasses
├── config_manager.py   # ConfigManager — load/save/list/import profiles (YAML + builtins)
├── cli.py              # dask-setup CLI (argparse): list, show, create, validate, delete,
│                       #   export, import, schema, benchmark, submit
├── dashboard.py        # print_dashboard_info() — SSH tunnel hint, Jupyter HTML link
├── environment.py      # is_jupyter(), get_environment_type()
├── error_handling.py   # Enhanced error classes with context + HPC-specific suggestions
├── exceptions.py       # Base exception hierarchy
├── io_patterns.py      # ZarrOptimizer, ZarrV3Optimizer, NetCDFOptimizer,
│                       #   KerchunkOptimizer, recommend_io_chunks(), detect_storage_format()
├── logging.py          # Structured levelled logging via get_logger()
├── multinode.py        # MultiNodeConfig, SharedTempDir, setup_pbs_cluster(),
│                       #   setup_slurm_cluster(), detect_cluster_mode(),
│                       #   generate_pbs_script(), generate_slurm_script()
├── parquet.py          # recommend_parquet_chunks(), ParquetRecommendation
├── rechunk.py          # rechunk_dataset() — safe Rechunker wrapper
├── reporting.py        # ClusterReport, cluster_report()
├── resources.py        # detect_resources() — PBS/SLURM/psutil + memory parser
├── schema/
│   └── profile_schema.json   # JSON Schema (draft-07) for profile YAML
├── schema.py           # PROFILE_SCHEMA constant, get_profile_schema()
├── tempdir.py          # create_dask_temp_dir()
├── topology.py         # decide_topology(), validate_topology(), _count_gpus()
├── tune.py             # tune_memory_thresholds(), MemoryTuneResult
├── types.py            # NamedTuples: ResourceSpec, TopologySpec, MemorySpec
├── workload.py         # infer_workload_type() — auto workload detection from xarray dataset
├── xarray.py           # recommend_chunks(), validate_chunks(), ChunkRecommendation,
│                       #   _classify_dimensions(), _TEMPORAL_PATTERNS, _SPATIAL_PATTERNS
└── benchmark.py        # benchmark_config(), scaling_analysis(), chunk_impact(),
                        #   run_synthetic_benchmark(), BenchmarkResult, ScalingResult,
                        #   ChunkImpactResult, SyntheticBenchmarkResult
```

---

## Call Flow (Single-Node)

```
setup_dask_client(...)                # mode defaults to "interactive"
│
├── detect_cluster_mode()           # if mode="auto"
│    └── returns "local" / "pbs" / "slurm" / "interactive"
│
├── discover_allocated_nodes()      # if mode="interactive"
│    └── ≤1 node (or no job) → treated as "local"
│
├── [multi-node path]               # "pbs", "slurm", or "interactive" on >1 node
│    └── setup_pbs_cluster() / setup_slurm_cluster() / setup_interactive_cluster()
│
└── [single-node path]
     ├── _resolve_configuration()   # merge profile + kwargs → DaskSetupConfig
     ├── detect_resources()         # SLURM → PBS → psutil → fallback
     ├── infer_workload_type()      # only if workload_type="auto"
     ├── create_dask_temp_dir()     # PBS_JOBFS → TMPDIR → /tmp
     ├── decide_topology()          # DaskSetupConfig → TopologySpec
     ├── validate_topology()
     ├── calculate_memory_spec()    # TopologySpec + resources → MemorySpec
     ├── configure_dask_settings()  # dask.config.set(...)
     ├── LocalCluster(...)
     ├── Client(cluster)
     ├── tune_memory_thresholds()   # only if adaptive_memory=True
     ├── print_dashboard_info()     # SSH hint or Jupyter HTML
     └── return (client, cluster, str(temp_dir))
```
