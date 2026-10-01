API Reference
=============

Everything here is importable from the top-level package, e.g.
``from dask_setup import setup_dask_client``. Helpers backed by optional
dependencies (xarray, dask-jobqueue, zarr) raise an ``ImportError`` naming the
install command when the dependency is missing.

Cluster setup
-------------

.. autofunction:: dask_setup.client.setup_dask_client

.. autoclass:: dask_setup.client.DaskClientContext

Configuration and profiles
--------------------------

.. autoclass:: dask_setup.config.DaskSetupConfig

.. autoclass:: dask_setup.config_manager.ConfigManager

Multi-node
----------

.. autoclass:: dask_setup.multinode.MultiNodeConfig

.. autoclass:: dask_setup.multinode.SharedTempDir

.. autofunction:: dask_setup.multinode.detect_cluster_mode

.. autofunction:: dask_setup.multinode.setup_interactive_cluster

.. autofunction:: dask_setup.multinode.setup_pbs_cluster

.. autofunction:: dask_setup.multinode.setup_slurm_cluster

.. autofunction:: dask_setup.multinode.generate_pbs_script

.. autofunction:: dask_setup.multinode.generate_slurm_script

Chunking and xarray
-------------------

.. autofunction:: dask_setup.xarray.recommend_chunks

.. autofunction:: dask_setup.xarray.validate_chunks

.. autoclass:: dask_setup.xarray.ChunkRecommendation

.. autofunction:: dask_setup.rechunk.rechunk_dataset

I/O patterns
------------

.. autofunction:: dask_setup.io_patterns.recommend_io_chunks

.. autofunction:: dask_setup.io_patterns.detect_storage_format

.. autoclass:: dask_setup.io_patterns.IORecommendation

.. autoclass:: dask_setup.io_patterns.ZarrOptimizer

.. autoclass:: dask_setup.io_patterns.ZarrV3Optimizer

.. autoclass:: dask_setup.io_patterns.NetCDFOptimizer

.. autoclass:: dask_setup.io_patterns.KerchunkOptimizer

.. autofunction:: dask_setup.parquet.recommend_parquet_chunks

.. autoclass:: dask_setup.parquet.ParquetRecommendation

Workload inference and tuning
-----------------------------

.. autofunction:: dask_setup.workload.infer_workload_type

.. autofunction:: dask_setup.tune.tune_memory_thresholds

.. autoclass:: dask_setup.tune.MemoryTuneResult

.. autofunction:: dask_setup.callbacks.register_worker_callbacks

Reporting
---------

.. autofunction:: dask_setup.reporting.cluster_report

.. autoclass:: dask_setup.reporting.ClusterReport

Benchmarking
------------

.. autofunction:: dask_setup.benchmark.benchmark_config

.. autofunction:: dask_setup.benchmark.scaling_analysis

.. autofunction:: dask_setup.benchmark.chunk_impact

.. autofunction:: dask_setup.benchmark.run_synthetic_benchmark

.. autoclass:: dask_setup.benchmark.BenchmarkResult

.. autoclass:: dask_setup.benchmark.ScalingResult

.. autoclass:: dask_setup.benchmark.ChunkImpactResult

Environment and logging
-----------------------

.. autofunction:: dask_setup.environment.is_jupyter

.. autofunction:: dask_setup.environment.get_environment_type

.. autofunction:: dask_setup.logging.configure_logging

.. autofunction:: dask_setup.logging.get_logger

Errors
------

.. automodule:: dask_setup.exceptions

.. autoclass:: dask_setup.error_handling.ErrorContext

.. autoclass:: dask_setup.error_handling.EnhancedDaskSetupError

.. autoclass:: dask_setup.error_handling.ConfigurationValidationError

.. autoclass:: dask_setup.error_handling.ResourceConstraintError

.. autoclass:: dask_setup.error_handling.DependencyError

.. autoclass:: dask_setup.error_handling.StorageConfigurationError

.. autoclass:: dask_setup.error_handling.ClusterSetupError

.. autofunction:: dask_setup.error_handling.create_user_friendly_error

.. autofunction:: dask_setup.error_handling.format_exception_chain
