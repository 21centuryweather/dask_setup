# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.2.0] - 2026-08-27

Correctness release. Most of these are settings that were accepted, validated
and then silently discarded — the library reported doing one thing and did
another. Several fixes therefore *change behaviour*; they are listed separately
below.

### Fixed

#### Configuration reaching the cluster

- **Explicit arguments to `setup_dask_client()` were discarded when they equalled
  a default.** Whether an argument had been supplied was decided by comparing it
  against the default value, so `workload_type="io"`, `dashboard=True` or
  `reserve_mem_gb=50.0` were indistinguishable from "not supplied" and were
  dropped in favour of the profile. Every configuration parameter now defaults
  to `None` as a real sentinel, and `DaskSetupConfig.merge_overrides()` applies
  only the keys the caller actually named.
- **`profile=` and `config=` never reached the PBS, SLURM or interactive
  backends.** Those modes returned before configuration was resolved.
- **`setup_dask_client(ds=...)` raised `ValueError: not enough values to unpack`
  on the multi-node paths**, which returned a 3-tuple even when a dataset was
  supplied. All modes now return a 4-tuple with `ds=` and a 3-tuple without.
- **`get_profile()` checked builtins before user profiles**, so
  `dask-setup show <name>` displayed your profile while
  `setup_dask_client(profile=<name>)` used the builtin. Both now resolve
  user → site → builtin.
- **`MultiNodeConfig` overrides dropped `env_extra` and `scheduler_options`**,
  because they round-tripped through `to_dict()`, which omits both. `module
  load` lines vanished from generated worker job scripts.

#### Memory and cluster sizing

- **Multi-node jobs under-provisioned workers by a factor of `processes`.**
  `cores` and `memory` are per-*job* in `dask-jobqueue`, which divides both by
  `processes`; the per-worker figures were being passed instead. A 4×12-core,
  4×32 GiB job started workers with `--nthreads 3 --memory-limit 7.45GiB`
  rather than `--nthreads 12 --memory-limit 29.80GiB`, against the
  `ncpus=48,mem=128GB` it had reserved.
- **`reserve_mem_gb` was defeated by the per-worker memory floor.** The 1 GiB
  minimum was applied after the split, so 64 workers on a 64 GiB node with a
  50 GiB reserve each got 1 GiB — 64 GiB committed against 14 GiB usable. The
  worker count is now fitted to memory first, and the reduction is logged.
- **`tune_memory_thresholds()` had no effect on running workers.** It wrote
  `dask.config`, but `WorkerMemoryManager` reads the thresholds once at
  construction and sizes the `SpillBuffer`'s eviction threshold from them at the
  same moment. Thresholds are now applied to the live manager and buffer.
- **Spill volume always read `0.0`.** The worker metric keys queried
  (`spilled_memory`, `spill`) are not published by current `distributed`; the
  key is `spilled_bytes`, whose value is a `{"memory", "disk"}` mapping. This
  also meant `tune_memory_thresholds(strategy="auto")` could never reach its
  `loosen` branch.
- **`SLURM_MEM_PER_NODE=0`** — SLURM's encoding for "no memory limit requested"
  — parsed to a literal zero-byte budget *and* suppressed the psutil fallback,
  which was guarded on `is None`. Same bug on the PBS path. Non-positive core
  counts are likewise rejected so detection falls through instead of producing
  an unusable spec.

#### Hangs, crashes and dead code

- **`recommend_chunks()` could loop forever** on a `mixed` workload when every
  spatial dimension was already at a chunk size of 1: the reduction step chose
  from unfiltered candidates and `max(1, 1 // 2) == 1` never made progress.
- **Circular profile inheritance recursed until `RecursionError`**, producing a
  57 KB error message. The chain was threaded into `load_profile_from_file` but
  parents resolved via `get_profile`, which restarted it empty, leaving both the
  cycle check and the depth cap dead. Cycles now raise
  `InvalidConfigurationError: Circular profile inheritance detected: a -> b -> a`.
- **`configure_logging()` was a no-op after import**, so there was no way to
  enable debug logging: it returned early if logging was already configured, and
  importing any `dask_setup` module configures it. The documented
  `DASK_SETUP_LOG_LEVEL`, `DASK_SETUP_LOG_FORMAT` and `DASK_SETUP_LOG_COLOR`
  variables were never read by anything.
- **`ErrorContext` crashed while building an error report** if zarr or netCDF4
  were installed but broken — only `ImportError` was caught, and a missing
  shared library raises `OSError`.
- **`create_dask_temp_dir()` nested on repeat calls** (`dask-<pid>/dask-<pid>`)
  because it overwrote `TMPDIR` with its own result and read it back next time.

#### Benchmarking

- **`scaling_analysis()` swept a topology that cannot scale.** The default
  config left `workload_type="io"`, for which `decide_topology()` pins
  `n_workers=1` regardless of `max_workers` — so every point built the same
  one-worker cluster and the "scaling curve" was timing noise. The default is
  now `"cpu"`, and a sweep whose worker count never changes logs a warning.
- **Parallel efficiency divided by the absolute worker count** rather than the
  worker ratio, so a perfectly scaling `(4, 8)` sweep scored `0.25` at its own
  baseline instead of `1.0`.
- **`peak_memory_gib` was not a peak.** It came from a cluster report taken
  after `.compute()` returned, by which point the workers had released the data.
  Memory is now sampled every 0.2 s during the timed runs.

#### Smaller fixes

- Generated job scripts quote interpolated paths (`shlex.quote`), so a
  `shared_tmp_dir` or script path containing a space no longer breaks the script.
- `dask-setup create-profile --from-profile` copied by reference and renamed the
  source profile in place — for a builtin, for the rest of the process.
- Dimension classification no longer matches `"x"`, `"y"`, `"z"`, `"ni"`, `"nj"`
  and `"lev"` as substrings, which made `proxy`, `flux`, `max` and `zone` spatial
  dimensions and let them drive chunk sizing.
- The dashboard SSH tunnel hint no longer hardcodes `gadi.nci.org.au`; it is
  inferred from the node's DNS domain and can be set with `$DASK_SETUP_LOGIN_HOST`.
- `_rechunk_native()` no longer claims memory safety it cannot provide, and warns
  when a rechunk enlarges chunks (the unbounded case).
- PBS cluster construction no longer emits a `FutureWarning` on every launch
  (`project` → `account`, `env_extra` → `job_script_prologue`).

### Changed

These change observable behaviour. Explicit settings you already pass still win
in every case.

- **`reserve_mem_gb` now defaults to 20% of RAM**, clamped to [4, 50] GiB, capped
  at half the machine with a 1 GiB floor. The machine-aware default was written
  in v1.1 but never called, so every host got a flat 50 GiB — which on a 16 GiB
  laptop reserves more than the whole machine.
- **`silence_logs=False` (the default) now leaves Dask at its own `WARNING`
  level** instead of `ERROR`. The setting was collected and validated but never
  passed to `create_cluster`, so worker warnings — memory pressure, paused
  workers — were always suppressed.
- **`env_extra` lines are emitted verbatim** in generated job scripts, matching
  how `dask-jobqueue` treats them. They used to be prefixed with `export`, which
  turned `module load conda` into `export module load conda` on that path while
  the `dask-jobqueue` path ran it correctly. **If you relied on the implicit
  prefix for bare `FOO=bar` entries, add `export` yourself.**
- **A user profile now shadows a builtin of the same name** in `get_profile()`,
  matching what `list_profiles()` and `dask-setup list` already displayed.
- **PBS/SLURM modes warn about settings the backend cannot apply**
  (`reserve_mem_gb`, spill compression, …) instead of dropping them silently.

### Documentation

- Corrected the claim that `profile=` and `config=` compose. They occupy the
  same precedence layer: whichever is passed supplies the base, and if both are
  passed the profile wins outright. Layering them would require knowing which
  fields a profile's YAML actually set, which `ConfigProfile` does not record.
- README and wiki updated for every behaviour change above; added documentation
  for log configuration, `env_extra` semantics and the per-job resource model.

### Internal

- 100 new tests (946 total, up from 784), including new suites for `reporting`,
  `tune`, `logging` and `rechunk`, which had none.
- Release workflow now runs the test matrix, verifies the tag matches
  `pyproject.toml`, runs `twine check --strict`, and creates a GitHub Release
  before publishing to PyPI — all triggered by pushing a `v*` tag.

## [2.0.0] - 2026-03-23

### Added

- **`MultiNodeConfig` dataclass (`multinode.py`).** Companion to `DaskSetupConfig` for
  multi-node PBS/SLURM jobs. Fields include `workload_type`, `workers_per_node`,
  `cores_per_worker`, `mem_per_worker_gb`, `walltime`, `queue`, `project`,
  `job_extra_directives`, `n_nodes`, `shared_tmp_dir`, `env_extra`, `adaptive`,
  `min_jobs`, and `max_jobs`. Provides `total_cores_per_job`, `total_mem_gb_per_job`
  properties and a `to_dict()` serialiser. Full validation on construction.
- **`setup_pbs_cluster(config, **kwargs)` function (`multinode.py`).** Wraps
  `dask-jobqueue.PBSCluster` with `MultiNodeConfig` defaults, shared temp directory
  support, and adaptive scaling. Returns `(client, cluster, shared_tmp)`.
- **`setup_slurm_cluster(config, **kwargs)` function (`multinode.py`).** Mirrors
  `setup_pbs_cluster` for SLURM via `SLURMCluster`.
- **`SharedTempDir` dataclass (`multinode.py`).** Wraps a Lustre/GPFS shared filesystem
  path for stores that must be visible to all workers (e.g. Rechunker targets).
  Optionally creates a job-specific subdirectory (`dask_tmp_<JOBID>`) and supports
  cleanup on close. Implements `__fspath__` for transparent use as a `Path`.
- **`detect_cluster_mode()` function (`multinode.py`).** Inspects the environment for
  SLURM (`SLURM_JOB_ID`, etc.) and PBS (`PBS_JOBID`, etc.) indicators and returns
  `"slurm"`, `"pbs"`, or `"local"`.
- **`generate_pbs_script(cfg, script_path, …)` function (`multinode.py`).** Generates
  a ready-to-run `#PBS` job script string from a `MultiNodeConfig`.
- **`generate_slurm_script(cfg, script_path, …)` function (`multinode.py`).** Generates
  a `#SBATCH` job script string.
- **`mode=` parameter on `setup_dask_client()` (`client.py`).** Accepts `"local"`,
  `"pbs"`, `"slurm"`, or `"auto"` (default). In `"auto"` mode, calls
  `detect_cluster_mode()` and dispatches to the appropriate backend. Multi-node modes
  return a `(client, cluster, tmp_path)` 3-tuple.
- **`multi_node_config=` parameter on `setup_dask_client()` (`client.py`).** A
  `MultiNodeConfig` instance to use when a multi-node mode is selected. When `None`, a
  minimal config is built from `workload_type`.
- **`workload_type="gpu"` topology (`topology.py`).** New topology for CUDA-accelerated
  workloads. Detects GPU count via `CUDA_VISIBLE_DEVICES` or `cupy`. Configures one
  worker process per GPU with `ceil(total_cores / n_gpus)` CPU threads (clamped to
  2–8). Falls back gracefully to a single-threaded worker when no GPUs are detected,
  with a warning. `DaskSetupConfig.VALID_WORKLOAD_TYPES` updated to include `"gpu"`.
- **`_count_gpus()` helper (`topology.py`).** Counts CUDA-capable GPUs from
  `CUDA_VISIBLE_DEVICES` (falls back to `cupy.cuda.runtime.getDeviceCount()`).
- **`dask-setup submit` CLI subcommand (`cli.py`).** Generates PBS or SLURM job
  scripts from the command line. Key flags: `--scheduler` (`pbs`/`slurm`),
  `--workload-type`, `--workers-per-node`, `--cores-per-worker`, `--mem-per-worker`,
  `--walltime`, `--queue`, `--project`, `--n-nodes`, `--shared-tmp-dir`,
  `--extra-directive` (repeatable), `--python`, `--output`.
- **All new v2.0 symbols exported** from the top-level package (`__init__.py`):
  `MultiNodeConfig`, `SharedTempDir`, `detect_cluster_mode`, `setup_pbs_cluster`,
  `setup_slurm_cluster`, `generate_pbs_script`, `generate_slurm_script`.

### Changed

- `setup_dask_client()` gains `mode="auto"` and `multi_node_config=None` parameters.
  Existing callers are unaffected (defaults preserve previous behaviour).
- `DaskSetupConfig.VALID_WORKLOAD_TYPES` now includes `"gpu"`.
- `decide_topology()` and `validate_topology()` updated to handle `"gpu"` workload type.
  The high-threads warning in `validate_topology` is suppressed for GPU topologies.

### Notes

- Multi-node PBS/SLURM support requires `pip install dask-jobqueue`. The package
  imports cleanly without it; a helpful `ImportError` is raised only when
  `setup_pbs_cluster` / `setup_slurm_cluster` are called.
- GPU topology requires `pip install cupy-cudaXXX` (matching your CUDA version) for
  GPU auto-detection; falls back gracefully when CuPy is absent.

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
