# dask_setup

[![CI](https://github.com/21centuryweather/dask_setup/workflows/CI/badge.svg)](https://github.com/21centuryweather/dask_setup/actions)

A comprehensive **single‑node** Dask setup helper designed for **HPC environments** (especially **Gadi**), with intelligent defaults for CPU-/I/O-bound workloads, advanced configuration management, storage format optimization, and user-friendly error handling. It's a feature-rich wrapper around `dask.distributed.LocalCluster` + `Client` that eliminates trial‑and‑error and provides expert-level optimization out of the box.

**Python Support**: Requires Python 3.11+ | **Installation**: `pip install dask_setup` or `pip install -e .[dev]` for development

## Key Features

- **Smart Resource Detection**: Automatic PBS/SLURM/Kubernetes environment detection
- **Advanced Configuration**: Profile-based config management with validation
- **Workload Optimization**: Specialized topologies for CPU/I/O/mixed workloads  
- **Storage Format Intelligence**: Zarr/NetCDF-aware chunking and compression
- **Xarray Integration**: Intelligent chunking recommendations
- **Enhanced Error Handling**: Context-aware error messages with actionable suggestions
- **Memory Safety**: Aggressive spilling prevents OOM crashes
- **HPC-Optimized**: Routes temp/spill to `$PBS_JOBFS` for maximum performance
- **Dashboard Integration**: Easy SSH tunnel setup for monitoring
- **Battle-Tested**: 500+ tests, 90%+ coverage, production-ready

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

## Quick Start

```python
from dask_setup import setup_dask_client

# Simple usage - optimal defaults
client, cluster, dask_tmp = setup_dask_client("cpu")

# Advanced usage with configuration
from dask_setup import setup_dask_client, DaskSetupConfig

config = DaskSetupConfig(
    workload_type="cpu",
    max_workers=8,
    reserve_mem_gb=32.0,
    spill_compression="lz4",
    suggest_chunks=True  # Show xarray chunking recommendations
)
client, cluster, dask_tmp = setup_dask_client(config=config)
```

## Signature and Parameters

### Basic Function

```python
def setup_dask_client(
    workload_type: str = "io",         # "cpu", "io", or "mixed"
    max_workers: int | None = None,     # Limit worker count
    reserve_mem_gb: float = 50.0,       # Memory reserved for OS
    max_mem_gb: float | None = None,    # Total memory cap
    dashboard: bool = True,             # Enable dashboard
    adaptive: bool = False,             # Enable adaptive scaling
    min_workers: int | None = None,     # Minimum workers for adaptive
    profile: str | None = None,         # Configuration profile name
    suggest_chunks: bool = False,       # Show chunking recommendations
) -> tuple[Client, LocalCluster, str]:
    ...
```

### Advanced Configuration Class

```python
@dataclass
class DaskSetupConfig:
    # Core parameters
    workload_type: str = "io"
    max_workers: int | None = None
    reserve_mem_gb: float = 50.0
    
    # Memory management (advanced)
    memory_target: float = 0.75      # Start spilling at 75%
    memory_spill: float = 0.85        # Aggressive spilling
    memory_pause: float = 0.92        # Pause new tasks
    memory_terminate: float = 0.98    # Kill worker (last resort)
    
    # Compression & I/O
    spill_compression: str = "auto"   # lz4, zstd, snappy, etc.
    comm_compression: bool = False    # Worker communication compression
    spill_threads: int | None = None  # Parallel spill I/O threads
    
    # I/O optimization
    io_format: str | None = None      # "zarr", "netcdf", or auto-detect
    io_target_chunk_mb: tuple[float, float] = (128, 512)
    io_access_pattern: str = "auto"   # "sequential", "random", "streaming"
    io_storage_location: str = "auto" # "local", "cloud", "network"
    
    # Integration
    suggest_chunks: bool = False      # Xarray chunking recommendations
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

## Configuration Management

`dask_setup` provides a comprehensive configuration system with profiles, validation, and environment-specific optimizations:

### Configuration Profiles

```python
from dask_setup import ConfigManager, DaskSetupConfig

# Use built-in profiles
client, cluster, tmp = setup_dask_client(profile="cpu_intensive")

# Create and save custom profiles
config = DaskSetupConfig(
    workload_type="io",
    reserve_mem_gb=40.0,
    spill_compression="lz4",
    name="my_io_profile",
    description="Optimized for large NetCDF processing"
)

manager = ConfigManager()
manager.save_profile("my_io_profile", config)

# List and manage profiles
profiles = manager.list_profiles()
print(profiles["cpu_intensive"].description)
```

### Built-in Profiles

- **`cpu_intensive`**: Heavy computation, many processes, minimal I/O
- **`io_heavy`**: Large file processing, optimized for Zarr/NetCDF
- **`memory_conservative`**: Lower memory usage, safer for shared systems
- **`balanced`**: Good default for mixed workloads

## Storage Format Intelligence & I/O Patterns

`dask_setup` includes sophisticated storage format detection and optimization:

### Automatic Format Detection & Optimization

```python
from dask_setup import recommend_io_chunks

# Automatic format detection and optimization
chunks = recommend_io_chunks(
    ds,                                    # Your xarray dataset
    path_or_url="s3://bucket/data.zarr",  # Storage path for format detection
    access_pattern="sequential",          # Access pattern optimization
    verbose=True                          # Show detailed recommendations
)

ds_optimized = ds.chunk(chunks)
```

### Format-Specific Optimizations

#### Zarr Optimization

- **Cloud-friendly chunking**: Larger chunks (128-512MB) for better throughput
- **Compression codecs**: zstd for cloud, lz4 for local storage
- **Consolidation**: Automatic metadata consolidation
- **Filters**: Bit rounding for floating-point compression

#### NetCDF Optimization

- **HDF5-aware chunking**: Moderate chunk sizes (64-256MB) for HDF5 backend
- **Unlimited dimension handling**: Conservative chunking of time dimensions
- **Compression**: zlib with shuffle filter, precision control
- **Remote access**: Optimized caching for HTTP/S3 access

### Storage Location Intelligence

```python
# Cloud storage optimization
chunks = recommend_io_chunks(
    ds, 
    path_or_url="s3://climate-data/temperature.zarr",
    storage_location="cloud",           # Optimized for cloud access
    target_chunk_mb=(256, 512)         # Larger chunks for fewer requests
)

# Local high-performance storage
chunks = recommend_io_chunks(
    ds,
    path_or_url="/scratch/data.nc", 
    storage_location="local",           # Optimized for local NVMe/SSD
    access_pattern="random"             # Random access pattern
)
```

## Advanced Memory & Compression Management

For improved performance and efficient resource usage, `dask_setup` supports comprehensive compression and I/O optimization:

### Configuring compression and parallel I/O

While the basic `setup_dask_client()` function uses sensible defaults, you can configure compression and parallel I/O through the `DaskSetupConfig` class:

```python
from dask_setup.config import DaskSetupConfig
from dask_setup.client import setup_dask_client

# Enable LZ4 spill compression, communication compression, and parallel spill I/O
config = DaskSetupConfig(
    workload_type="cpu",
    spill_compression="lz4",      # Options: "auto", "lz4", "zstd", "snappy", etc.
    comm_compression=True,        # Enable worker-to-worker compression
    spill_threads=4,              # Use 4 threads for parallel spill I/O
    reserve_mem_gb=50.0
)

client, cluster, dask_tmp = setup_dask_client(config=config)
```

### Compression algorithms

Supported spill compression algorithms:

- `"auto"` (default): Let Dask choose automatically
- `"lz4"`: Fast compression, good for most workloads
- `"zstd"`: Better compression ratio, slightly slower
- `"snappy"`: Very fast, lower compression ratio
- `"gzip"`, `"blosc"`, `"zlib"`, `"bz2"`, `"lzma"`: Other options

**Recommendations:**

- **CPU-intensive workloads**: Use `"lz4"` or `"zstd"` for good balance of speed and compression
- **I/O-intensive workloads**: Consider `"lz4"` or `"snappy"` for minimal CPU overhead
- **Large spill files**: Use `"zstd"` for maximum disk space savings

### Parallel spill I/O configuration

The `spill_threads` parameter controls how many threads Dask uses for spill operations (reading/writing data to/from disk when memory is full):

- `None` (default): Use Dask's default behavior
- `1-16`: Number of threads for parallel spill I/O

**Recommendations:**

- **Fast storage (SSD/NVMe)**: Use `4-8` threads to maximize throughput
- **Network storage**: Use `2-4` threads to avoid overwhelming the network
- **Slow/shared storage**: Use `1-2` threads to minimize contention
- **High-memory workloads**: Higher thread counts can help when frequent spilling occurs

**Example configurations:**

```python
# For workloads with frequent spilling on fast local storage
config = DaskSetupConfig(
    workload_type="cpu",
    spill_threads=6,
    spill_compression="lz4",
    reserve_mem_gb=30.0
)

# For I/O-intensive workloads on shared storage
config = DaskSetupConfig(
    workload_type="io", 
    spill_threads=2,
    spill_compression="snappy",  # Fast compression
    reserve_mem_gb=40.0
)
```

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

### 5) Intelligent xarray chunking with storage optimization

```python
from dask_setup import setup_dask_client, recommend_chunks, recommend_io_chunks

# Option 1: Get chunking guidance when setting up cluster
client, cluster, dask_tmp = setup_dask_client("cpu", suggest_chunks=True)
# Automatically prints chunking recommendations and usage examples

# Option 2: Standard chunking recommendations
chunks = recommend_chunks(ds, client, verbose=True)
ds_optimized = ds.chunk(chunks)

# Option 3: Storage-format-aware chunking (NEW!)
chunks = recommend_io_chunks(
    ds, 
    path_or_url="data.zarr",           # Format auto-detection
    storage_location="cloud",          # Cloud-optimized chunking
    access_pattern="sequential",       # Access pattern optimization
    verbose=True                       # Detailed recommendations
)
ds_optimized = ds.chunk(chunks)

# Option 4: Integrated storage-aware chunking in setup
chunks = recommend_chunks(
    ds, 
    client,
    storage_format="zarr",             # Zarr-specific optimizations
    storage_path="s3://bucket/data.zarr",
    verbose=True
)
```

#### Chunking Intelligence Features

The chunking system provides multiple levels of optimization:

**Standard `recommend_chunks`**:

- Target 256-512 MiB per chunk for memory efficiency
- Respect worker memory limits (60% safety factor)
- Workload-aware strategies:
  - `"cpu"`: Square-ish chunks for compute-heavy operations
  - `"io"`: Stream-friendly chunks along record dimensions
  - `"mixed"`: Balanced approach for mixed workloads
  - `"auto"`: Detect based on dataset dimensions

**Advanced `recommend_io_chunks`**:

- **Format-specific optimization**: Zarr vs NetCDF chunking strategies
- **Storage location awareness**: Cloud vs local vs network optimization
- **Access pattern tuning**: Sequential, random, streaming patterns
- **Compression recommendations**: Format-appropriate compression codecs
- **Throughput estimation**: Predicted I/O performance
- **Warning system**: Alerts for suboptimal configurations

## Enhanced Error Handling

`dask_setup` features a comprehensive error handling system that provides clear, actionable guidance:

### User-Friendly Error Messages

Instead of cryptic errors, get helpful guidance:

```python
# Bad configuration triggers helpful error
try:
    config = DaskSetupConfig(
        max_workers=-5,              # Invalid
        reserve_mem_gb=1000.0,       # Too high
        workload_type="invalid"      # Invalid type
    )
except ConfigurationValidationError as e:
    print(e)
    #    [CONFIG_VALIDATION] Configuration validation failed:
    #    • max_workers: must be positive (current: -5)
    #    • reserve_mem_gb: exceeds total system memory (16.0 GB)
    #    • workload_type: must be one of ['cpu', 'io', 'mixed']
    # 
    #    Suggestions:
    #    1. Set max_workers to a value between 1 and 10
    #    2. Try reserve_mem_gb=3 (20% of total memory, capped at 10 GB)
    #    3. Use workload_type='io' for I/O intensive tasks
    # 
    #  Documentation: https://github.com/dask-contrib/dask_setup#configuration
```

### Error Types with Context

- **`ConfigurationValidationError`**: Field-specific validation with targeted fixes
- **`ResourceConstraintError`**: Memory/CPU limits with environment-specific advice
- **`DependencyError`**: Missing packages with installation instructions
- **`StorageConfigurationError`**: Storage format and access issues with solutions
- **`ClusterSetupError`**: Cluster startup problems with diagnostic information

### Environment-Aware Suggestions

Errors adapt to your environment:

```python
# On HPC systems, get PBS/SLURM-specific advice
try:
    setup_dask_client(max_workers=1000)  # Too many workers
except ResourceConstraintError as e:
    print(e)
    #   [RESOURCE_CONSTRAINT] Insufficient CPU cores for configuration
    # 
    #   Suggestions:
    #    1. Request more CPUs in your job submission script (e.g., #PBS -l ncpus=32)
    #    2. Reduce max_workers to 10
    #    3. Use workload_type='io' for I/O bound tasks (uses fewer cores)
```

### Diagnostic Information

Every error includes full environmental context:

- System resources (CPU, memory)
- HPC environment detection (SLURM/PBS/local)
- Available dependencies (numpy, xarray, zarr, etc.)
- Configuration details and current values

---

## Testing & Quality Assurance

`dask_setup` is thoroughly tested and production-ready:

- **500+ tests** covering all functionality
- **90%+ code coverage** requirement
- **Integration tests** with real HPC environments
- **Performance benchmarks** for optimization validation
- **Cross-platform testing** (Linux, macOS, Windows)
- **Dependency matrix testing** (multiple Python/package versions)

---

## Monitoring & Observability

### Advanced Dashboard Features

```python
# Custom dashboard configuration
config = DaskSetupConfig(
    dashboard=True,
    dashboard_port=8787,            # Specific port
    silence_logs=False              # Show detailed logging
)
client, cluster, tmp = setup_dask_client(config=config)

# Get diagnostic information
from dask_setup.error_handling import ErrorContext
context = ErrorContext()
print(context.get_environment_summary())
#   Environment: SLURM
#   Python: 3.11.5  
#   Memory: 128.0 GB
#   CPUs: 48
#   Dependencies:
#    • dask: 2024.1.0
#    • xarray: 2024.1.1
#    • zarr: 2.16.1
```

---

## CLI Integration

`dask_setup` includes command-line tools for configuration management:

```bash
# List available profiles
dask-setup profiles list

# Show profile details
dask-setup profiles show cpu_intensive

# Create new profile interactively  
dask-setup profiles create my_profile

# Validate configuration
dask-setup config validate my_config.yaml

# Export profile for sharing
dask-setup profiles export cpu_intensive > cpu_profile.yaml
```

---

## Troubleshooting & Tips

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

## Migration from v1.x

For users upgrading from earlier versions:

```python
# Old usage (still works)
client, cluster, tmp = setup_dask_client("cpu", max_workers=8)

# New features available
from dask_setup import setup_dask_client, DaskSetupConfig

# Profile-based configuration
client, cluster, tmp = setup_dask_client(profile="cpu_intensive")

# Advanced configuration
config = DaskSetupConfig(
    workload_type="cpu",
    max_workers=8,
    spill_compression="lz4",     # NEW: Compression options
    suggest_chunks=True,         # NEW: Xarray integration  
    io_format="zarr"            # NEW: Storage optimization
)
client, cluster, tmp = setup_dask_client(config=config)

# Storage-aware chunking
from dask_setup import recommend_io_chunks  # NEW
chunks = recommend_io_chunks(ds, "data.zarr", verbose=True)
```

---

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

- **Bug reports**: Use GitHub issues with error context
- **Feature requests**: Describe use case and expected behavior
- **Pull requests**: Include tests and documentation
- **Performance improvements**: Provide benchmarks

---

## License

MIT License - see [LICENSE](LICENSE) for details.

---

## TL;DR

**For quick results:**

- `setup_dask_client("cpu")` → Heavy math/computation
- `setup_dask_client("io")` → Large file I/O (NetCDF/Zarr)
- `setup_dask_client("mixed")` → Mixed workloads
- `setup_dask_client(profile="cpu_intensive")` → Use optimized profiles

**Key benefits:**

- **Intelligent defaults** eliminate configuration guesswork
- **Storage format optimization** for Zarr/NetCDF performance
- **User-friendly errors** with actionable suggestions
- **Memory safety** with spilling prevents OOM crashes
- **HPC-optimized** routes temp/spill to `$PBS_JOBFS`
- **Comprehensive monitoring** with dashboard and diagnostics
- **Production-ready** with 500+ tests and 90%+ coverage
