# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.4.0] - 2026-03-23

### Added

- **Post-run cluster reporting (`reporting.py`).** New `reporting` module with
  two public exports: `ClusterReport` (a dataclass snapshot of cluster metrics)
  and `cluster_report(client, start_time=None)` (the collection function).
  `ClusterReport` fields: `wall_time_seconds`, `memory_per_worker_gib`,
  `total_spill_gib`, and `total_tasks`.  Derived properties (`wall_time_str`,
  `peak_memory_gib`, `total_memory_gib`) and helper methods
  (`summary_line()`, `to_dict()`) are also available.  All metric collection
  is defensive — fields that cannot be retrieved from the scheduler (due to
  Dask version differences) are left at their zero/empty defaults rather than
  raising.  Both symbols are exported from the top-level package.
- **Cluster summary on context-manager exit.** `DaskClientContext.__enter__`
  now records the wall-clock start time via `time.monotonic()`.
  `DaskClientContext.__exit__` calls `cluster_report()` before closing the
  client and emits the result as a single `logger.info()` line
  (`"Cluster run summary: wall=… | workers=… | mem=… GiB/max | …"`).
  This gives users an automatic post-run summary without any extra code.
- **Environment summary in `ClusterSetupError`.** `ClusterSetupError` now
  overrides `_format_error_message()` to append a `🔍 Environment:` section
  (produced by `ErrorContext.get_environment_summary()`) to the formatted
  exception message.  The section includes platform, Python version, memory,
  CPU count, and detected package versions — making it immediately visible in
  tracebacks and log output without separately calling `get_diagnostic_info()`.

## [1.3.0] - 2026-03-23

### Added

- **Jupyter / IPython detection (`environment.py`).** New `environment` module
  with two public helpers: `is_jupyter()` (cached boolean) and
  `get_environment_type()` (returns `"jupyter"`, `"ipython"`, or `"script"`).
  Detection is based on the IPython shell class name (`ZMQInteractiveShell`
  covers JupyterLab, classic Notebook, VSCode notebooks, and Google Colab).
  Both functions are exported from the top-level package.
- **Clickable dashboard link in Jupyter.** `print_dashboard_info()` in
  `dashboard.py` now checks `is_jupyter()` at call time. When inside a Jupyter
  kernel, it uses `IPython.display.HTML` to render a styled, clickable anchor
  tag instead of printing the SSH tunnel hint. Falls back to the plain-text
  message if `IPython.display` is unavailable despite being in a Jupyter
  environment. The new `display_jupyter_dashboard(client)` function is also
  available for direct use.
- **Graceful resource detection degradation (`fallback_on_detection_failure`).**
  `setup_dask_client()` now accepts `fallback_on_detection_failure=True` (also
  settable via `DaskSetupConfig`).  When enabled and all resource detection
  methods fail (SLURM, PBS, and psutil all raise), a conservative hardcoded
  fallback is used (2 cores, 8 GiB) rather than raising
  `ResourceDetectionError`. A warning is logged identifying the fallback.
  Default `False` preserves existing behaviour. The `detect_resources()`
  function in `resources.py` gains the corresponding `fallback=` parameter.

## [1.2.0] - 2026-03-23

### Added

- **`setup_dask_client(ds=)` parameter.** Pass an xarray Dataset or DataArray to
  `setup_dask_client()` to get automatic chunk recommendations for that specific
  dataset and cluster configuration.  When `ds=` is provided, the function returns
  a 4-tuple `(client, cluster, tmp, chunks)` where *chunks* is a `dict[str, int]`
  ready to pass to `ds.chunk(chunks)`.
- **`suggest_chunks` now returns a value.** When `ds=` is provided and
  `suggest_chunks=True`, the detailed chunk report is printed **and** the chunk
  dict is returned in the 4-tuple.  Without `ds=`, `suggest_chunks=True` continues
  to print generic cluster-based guidance.
- **`validate_chunks(ds, client)`.** New standalone function that compares a
  dataset's existing chunk sizes to the cluster's per-worker memory limit and emits
  `UserWarning` for chunks that are too large (OOM risk) or too small (task overhead).
  Called automatically when `ds=` is passed to `setup_dask_client()`.
- **`rechunk_dataset(ds, target_chunks, client, dask_tmp)`.** New rechunking helper
  that wraps the optional `rechunker` library.  Routes both the intermediate temp
  store and the output Zarr store to `dask_tmp` (typically `$PBS_JOBFS`) for fast
  local I/O.  Handles temp-store cleanup and provides clear error messages.
  Requires: `pip install rechunker zarr`.
- **`ChunkRecommendation` exported.** The `ChunkRecommendation` dataclass from
  `xarray.py` is now part of the public API and included in `__all__`.
- **`DaskClientContext` supports 4-tuple unpacking.**  When `ds=` is in the
  context-manager kwargs, `__enter__` returns `(client, cluster, tmp, chunks)`.
  The `chunks` attribute on the context object is also set.

### Changed

- **`setup_dask_client` return type is now conditional.**  Mypy/pyright users
  benefit from `@overload` stubs: passing `ds=None` (default) infers a 3-tuple;
  passing a dataset infers a 4-tuple.

## [1.1.0] - 2026-03-23

### Added

- **Context manager support.** `DaskClientContext` class allows
  `with DaskClientContext(...) as (client, cluster, tmp):` so clusters are always
  closed cleanly, even on exception. Exported from the top-level package.
- **Smart `reserve_mem_gb` default.** The default is now computed from the
  running system's total RAM: 20 % of total RAM, clamped to [4 GiB, 50 GiB].
  This replaces the previous hardcoded 50 GiB default which failed immediately on
  laptops and small VMs. Explicit values still override the smart default.
  The `DaskSetupConfig` dataclass retains 50.0 as its own field default for users
  who instantiate it directly.
- **`py.typed` marker.** The package now ships a `py.typed` file (PEP 561) so that
  mypy, pyright, and other type checkers recognise it as fully typed.
- **Python 3.13 in CI matrix.** Tests now run on Python 3.11, 3.12, and 3.13.

### Changed

- **Structured logging throughout.** `resources`, `cluster`, `topology`, and `client`
  modules now use the package's own `DaskSetupLogger` (via `get_logger()`) instead of
  a mix of bare `print()` calls and stdlib loggers. Setup progress — resource detection,
  topology selection, memory allocation, and the cluster summary — is now emitted via
  `logger.info()` and `logger.debug()` with structured context fields, making it easy
  to filter or redirect log output. Users can call `configure_logging(level="DEBUG")`
  to see the full trace, or `configure_logging(level="ERROR")` to silence everything.

### Fixed

- **Incorrect Dask config key in CHANGELOG v1.0.0.** The v1.0.0 entry incorrectly
  stated that `DaskSetupConfig.spill_threads` applied the Dask key
  `distributed.p2p.threads`. The correct key — which controls each worker's I/O thread
  pool used for spill read/write — is `distributed.worker.io-threads`. The code has
  been correct since the bug was introduced; only the CHANGELOG entry was wrong.

## [1.0.0] - 2025-01-17

### Added

- Configurable spill compression for worker memory spill files via `DaskSetupConfig.spill_compression`.
  - Applies Dask key `distributed.worker.memory.spill-compression`.
  - Supported values include: `auto`, `lz4`, `zstd`, `snappy`, `gzip`, `blosc`, `zlib`, `bz2`, `lzma`.
  - Default: `auto`.
- Optional communication compression toggle via `DaskSetupConfig.comm_compression`.
  - Applies Dask key `distributed.comm.compression`.
  - Boolean flag; default: `False`.
- Configurable parallel spill I/O via `DaskSetupConfig.spill_threads`.
  - Applies Dask key `distributed.worker.io-threads` to control the size of each worker's
    I/O thread pool used for spill read/write operations.
  - Accepts positive integers (1-16) or `None` for default behavior.
  - Default: `None`.
  - Note: `distributed.p2p.threads` (peer-to-peer shuffle) was incorrectly cited here in
    the original release entry; the key in use has always been `distributed.worker.io-threads`.
- Propagation of compression and parallel I/O settings through `configure_dask_settings`, `create_cluster`, and client setup, with summary output.
- New tests covering validation, serialization, Dask config application, and integration paths for compression and parallel I/O options.
- Improved test isolation and robustness for PBS environment detection to avoid flaky results.

### Enhanced

- Improved memory parsing capabilities in resource detection:
  - Added support for space-separated memory formats (e.g., "16 GB", "1.5 TiB").
  - Enhanced validation with overflow protection (8 EiB maximum).
  - Better error messages and warnings for problematic memory values.
  - Comprehensive support for binary vs decimal units (KiB vs KB, etc.).
  - Improved SLURM memory detection with fallback handling.
  - Robust whitespace normalization and format validation.
- Test suite enhancements including warning suppressions for deprecated distributed internal attributes.
- Improved mock fixture architecture to synchronize psutil mock responses across modules.

### Fixed

- Fixed flaky PBS environment detection test by making worker count assertion more resilient (accepting range based on environment).
- Fixed deprecation warnings from distributed library during tests by suppressing FutureWarnings related to `worker.nthreads`.
- Removed unused imports and variables.
- Fixed linting and formatting issues across multiple files.
