# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
  - Applies Dask key `distributed.p2p.threads` to control number of threads for spill operations.
  - Accepts positive integers (1-16) or `None` for default behavior.
  - Default: `None`.
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
