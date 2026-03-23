# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.8.0] - 2026-03-23

### Added

- **`benchmark_config(configs, ds, operation)` function (`benchmark.py`).** A/B-tests
  multiple `DaskSetupConfig` objects against the same xarray operation. Each config gets
  a fresh cluster, and results are collected as `BenchmarkResult` objects with wall time,
  memory usage, spill volume, and tasks/second. Accepts 1–5 timed repeats and logs
  per-repeat progress. Returns a list of `BenchmarkResult` entries in the order supplied.
- **`scaling_analysis(ds, operation, worker_counts, …)` function (`benchmark.py`).**
  Sweeps worker counts (default `[1, 2, 4, 8]`) using the same config and dataset,
  computes speedup and parallel efficiency at each count, and returns a `ScalingResult`
  containing per-count `BenchmarkResult` objects plus speedup/efficiency lists.
  Optional `plot=True` renders a matplotlib figure with dual axes (wall time and speedup).
- **`chunk_impact(ds, client, operation, chunk_sizes, …)` function (`benchmark.py`).**
  Fixes a running cluster and sweeps a list of chunk-size dicts (or generates them
  automatically with `_generate_auto_chunks`). Returns a `ChunkImpactResult` with
  per-spec `BenchmarkResult` objects. Optional `plot=True` renders a wall-time curve.
- **`BenchmarkResult` dataclass (`benchmark.py`).** Shared result carrier for all three
  analysis functions. Fields: `name`, `wall_time_seconds`, `wall_time_std`,
  `peak_memory_gib`, `spill_gib`, `n_tasks`, `n_workers`, `tasks_per_second`, `errors`,
  `extra`. Has `summary_line()` for one-line console output and `to_dict()` for
  JSON-serialisable output.
- **`ScalingResult` dataclass (`benchmark.py`).** Returned by `scaling_analysis()`.
  Provides `wall_times` property, `best()` method, `summary()` multi-line table, and
  optional `plot()` method.
- **`ChunkImpactResult` dataclass (`benchmark.py`).** Returned by `chunk_impact()`.
  Provides `optimal()` method, `summary()` table, and optional `plot()` method.
- **`run_synthetic_benchmark(profile, operation, ds_size, repeats)` function
  (`benchmark.py`).** Runs a benchmark against a named profile using only `dask.array`
  — no xarray dataset required. Supports four dataset sizes (`tiny`, `small`, `medium`,
  `large`). Returns a `SyntheticBenchmarkResult` with a human-readable `summary()`.
- **`dask-setup benchmark` CLI subcommand (`cli.py`).** Runs `run_synthetic_benchmark`
  from the command line. Flags: `--profile` (default `development`), `--operation`
  (default `mean`), `--size` (default `small`), `--repeats` (default `1`). Prints the
  `SyntheticBenchmarkResult.summary()` to stdout.
- **`BenchmarkResult`, `ScalingResult`, `ChunkImpactResult`, `benchmark_config`,
  `scaling_analysis`, `chunk_impact`, and `run_synthetic_benchmark` exported** from the
  top-level package (`__init__.py`). Import guarded so the package degrades gracefully
  when `dask.distributed` is unavailable.

## [1.7.0] - 2026-03-23

### Added

- **Profile inheritance (`based_on:` key).** YAML profiles now accept a
  `based_on: <parent_name>` field. Only the fields listed under `config:` in
  the child profile override the parent's values; everything else is inherited.
  Works with builtin, site-wide, and user profiles. Circular chains and chains
  exceeding 16 levels are detected and rejected with a descriptive error.
  `ConfigProfile` gains `based_on: str | None` and `profile_version: str | None`
  fields.
- **Site-wide profiles.** `ConfigManager` now loads profiles from
  `/etc/dask_setup/profiles/` (or the directory pointed to by
  `$DASK_SETUP_PROFILE_DIR`) between builtins and user profiles. Site admins
  can ship system-optimised defaults that users can override locally.
  `ConfigManager.__init__` accepts a new `site_profiles_dir` parameter.
- **Profile versioning.** Every profile saved by `save_profile()` now carries a
  `version: "1.7"` field (the current `PROFILE_FORMAT_VERSION` constant).
  `load_profile_from_file()` emits a `UserWarning` when loading a profile whose
  version string is newer than the installed package understands.
  `PROFILE_FORMAT_VERSION` is exported from the top-level package.
- **Profile import via URL / local file (`dask-setup import`).** New CLI
  subcommand: `dask-setup import <URL_OR_PATH> [--name <name>] [--force]`.
  Supports HTTP/HTTPS URLs and local file paths. Uses only stdlib
  `urllib.request` — no extra dependencies. `ConfigManager` exposes the
  corresponding `import_profile_from_url(url, name_override, force)` method.
- **JSON Schema for profiles (`dask-setup schema`).** A JSON Schema (draft-07)
  describing the profile YAML format is now shipped in
  `dask_setup/schema/profile_schema.json`. `ConfigManager.get_profile_schema()`
  returns it as a Python dict. `dask-setup schema [--output file]` prints it
  to stdout or writes it to a file for use with editors. `PROFILE_SCHEMA` is
  exported from the top-level package.

### Changed

- `ConfigManager.list_profiles()` now merges profiles in three layers —
  builtins < site-wide < user — so user profiles always win on name conflicts.
- `ConfigManager.get_profile()` searches user → site-wide → builtin (same
  precedence order as `list_profiles`).
- `format_profile_details()` in the CLI now shows `Based on:` and
  `Format version:` lines when present.

## [1.6.0] - 2026-03-23

### Added

- **`ZarrV3Optimizer` class (`io_patterns.py`).** Handles the zarr-python ≥ 3.0
  API, including sharding via `zarr.codecs.ShardingCodec` and the updated codec
  pipeline. Detects v3 stores via `zarr.json` metadata or the `zarr_format=3`
  store attribute. When the outer chunk exceeds 64 MiB, a sharding config
  (outer/inner shapes, index codec) is returned in `IORecommendation.extra["sharding"]`.
  Preferred over `ZarrOptimizer` when zarr-python ≥ 3.0 is detected.
- **`KerchunkOptimizer` class (`io_patterns.py`).** Detects datasets opened via
  Kerchunk or VirtualiZarr reference stores (fsspec `ReferenceFileSystem`,
  `ManifestArray`, `.json` reference files). Returns the existing chunk layout
  unchanged — rechunking would require a full data copy — and adds an
  informational warning about fixed byte-range boundaries.
- **`detect_storage_format()` extended.** Now returns `"zarr_v3"` for Zarr v3
  stores and `"kerchunk"` for Kerchunk/VirtualiZarr reference datasets. Ordering
  ensures kerchunk is checked before zarr (kerchunk presents a zarr-like
  interface), and `ZarrV3Optimizer` is preferred over `ZarrOptimizer` when v3 is
  detected.
- **`recommend_io_chunks()` dispatches to new optimizers.** Routes `"zarr_v3"`
  to `ZarrV3Optimizer` and `"kerchunk"` to `KerchunkOptimizer`. Populates
  `IORecommendation.extra["sharding"]` for Zarr v3 stores when sharding is
  appropriate. Adds a Kerchunk-specific warning when that format is detected.
- **`IORecommendation.extra` field.** New `dict[str, Any]` field on
  `IORecommendation` for format-specific extras. Currently populated by
  `ZarrV3Optimizer` with sharding configuration when applicable.
- **`recommend_parquet_chunks(df, client, …)` helper (`parquet.py`).** Parquet /
  Arrow partition-size recommendations for Dask DataFrame workloads — analogous
  to `recommend_io_chunks()` for xarray datasets. Estimates bytes-per-row from
  `memory_usage()` or dtype sizes, respects per-worker memory limits when a
  `client` is provided, auto-selects compression (`snappy` local, `zstd` cloud),
  and warns on wide tables. Returns `rows_per_partition` by default or a full
  `ParquetRecommendation` when `verbose=True`.
- **`ParquetRecommendation` dataclass (`parquet.py`).** Returned by
  `recommend_parquet_chunks()` when `verbose=True`. Fields: `rows_per_partition`,
  `compression`, `storage_options`, `estimated_partition_mb`, `warnings`, `extra`
  (includes `row_group_size` and `write_metadata_file` hints). Has `.summary()`.
- **blosc2 codec variants in `VALID_COMPRESSION_ALGORITHMS`.** Added `"blosc2"`,
  `"blosc2:lz4"`, `"blosc2:lz4hc"`, `"blosc2:blosclz"`, `"blosc2:zstd"`,
  `"blosc2:zlib"`, and `"blosc2:snappy"` — first-class codecs in Zarr v3 /
  blosc2 package. `ZarrV3Optimizer` automatically uses `blosc2:zstd` or
  `blosc2:lz4` when `blosc2` is importable.
- **`VALID_IO_FORMATS` extended.** Now includes `"zarr_v3"`, `"kerchunk"`, and
  `"parquet"` alongside the existing `"zarr"` and `"netcdf"` entries.

### Changed

- `ZarrV3Optimizer`, `KerchunkOptimizer`, `ParquetRecommendation`, and
  `recommend_parquet_chunks` are now exported from the top-level package.

## [1.5.0] - 2026-03-23

### Added

- **`workload_type="auto"` in `setup_dask_client()`.** New sentinel value for
  `workload_type`. When passed, `setup_dask_client()` calls
  `infer_workload_type(ds)` to choose between `"cpu"`, `"io"`, or `"mixed"`
  automatically. Falls back to `"mixed"` if no dataset is provided.
  Accepted by `DaskSetupConfig` so it can be stored in profiles.
- **`infer_workload_type(ds)` helper (`workload.py`).** Inspects an xarray
  Dataset or DataArray's dimension names (time, lat, lon, lev …), variable
  dtypes (float-dominant → CPU; int/bool-dominant → I/O), and
  bytes-per-variable ratio. Returns `"cpu"`, `"io"`, or `"mixed"`. Defaults
  to `"mixed"` when evidence is ambiguous (score margin ≤ 2) or `ds=None`.
  Exported from the top-level package.
- **`tune_memory_thresholds(client, strategy="auto", …)` (`tune.py`).** One-shot
  dynamic memory threshold adjustment. Reads current spill volume from the
  scheduler; tightens worker `memory.target`/`memory.spill` when spill is low
  (extra head-room), loosens them when spill is heavy (less disk write
  amplification). Strategy can be forced with `"tighten"`, `"loosen"`, or
  disabled with `"off"`. Returns `MemoryTuneResult` with `.summary()` and
  per-field detail. Exported from the top-level package.
- **`MemoryTuneResult` dataclass (`tune.py`).** Returned by
  `tune_memory_thresholds()`. Fields: `strategy`, `old_target`, `new_target`,
  `old_spill`, `new_spill`, `spill_gib_observed`, `rationale`,
  `workers_updated`. Exported from the top-level package.
- **`DaskSetupConfig(adaptive_memory=True)` opt-in.** When set, calls
  `tune_memory_thresholds(client, strategy="tighten")` immediately after the
  cluster is created so workers start with tighter thresholds from the first
  task. Also accepted as a keyword argument to `setup_dask_client()`.
- **`register_worker_callbacks(client, on_worker_death=…, on_worker_added=…)`
  (`callbacks.py`).** Installs a `SchedulerPlugin` that fires user callables
  when workers join or leave the cluster. Worker address is passed as the sole
  argument. Exceptions inside callbacks are caught and logged so a buggy
  callback cannot crash the scheduler. Exported from the top-level package.
- **`profile="auto"` in `setup_dask_client()`.** Inspects resources and
  environment then delegates to `ConfigManager.auto_select_profile()`.
  Selection rules (first match): Jupyter → `"interactive"`; small machine
  (≤ 8 cores or ≤ 16 GiB) → `"development"`; PBS + JOBFS + ≥ 16 cores →
  `"zarr_io_heavy"`; large HPC (≥ 48 cores or ≥ 128 GiB) →
  `"climate_analysis"`; general HPC → `"production"`; fallback →
  `"development"`.
- **`ConfigManager.auto_select_profile(resources)`.** New public method
  implementing the profile auto-selection logic above.

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
