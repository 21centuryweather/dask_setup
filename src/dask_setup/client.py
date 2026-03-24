"""Main client setup orchestration for dask_setup."""

from __future__ import annotations

import time
import types
from typing import TYPE_CHECKING, Any, overload

import psutil
from dask.distributed import Client, LocalCluster

from .cluster import calculate_memory_spec, create_cluster
from .config import DaskSetupConfig
from .config_manager import ConfigManager
from .dashboard import print_dashboard_info
from .exceptions import InsufficientResourcesError
from .logging import get_logger
from .multinode import MultiNodeConfig, detect_cluster_mode
from .resources import detect_resources
from .tempdir import create_dask_temp_dir
from .topology import decide_topology, validate_topology

if TYPE_CHECKING:
    import xarray as xr

from .workload import infer_workload_type

logger = get_logger("client")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_smart_reserve_default() -> float:
    """Compute a sensible reserve_mem_gb default based on available system RAM.

    Formula: 20 % of total RAM, minimum 4 GiB, maximum 50 GiB.

    Examples
    --------
    - 16 GiB laptop  → max(4.0, 3.2)  = 4.0 GiB  (clamped to minimum)
    - 64 GiB workstation → 12.8 GiB
    - 300 GiB Gadi node  → min(50.0, 60.0) = 50.0 GiB  (clamped to maximum)

    Falls back to 50.0 GiB if psutil fails for any reason.
    """
    try:
        total_ram_gb = psutil.virtual_memory().total / (1024**3)
        return min(50.0, max(4.0, total_ram_gb * 0.20))
    except Exception:
        return 50.0  # Safe HPC fallback if psutil is unexpectedly unavailable


def _resolve_configuration(
    config: DaskSetupConfig | None = None,
    profile: str | None = None,
    workload_type: str = "io",
    max_workers: int | None = None,
    reserve_mem_gb: float | None = None,
    max_mem_gb: float | None = None,
    dashboard: bool = True,
    adaptive: bool = False,
    min_workers: int | None = None,
    suggest_chunks: bool = False,
    fallback_on_detection_failure: bool = False,
    adaptive_memory: bool = False,
) -> DaskSetupConfig:
    """Resolve final configuration from a config object, profile, and explicit parameters.

    Priority order (highest to lowest):

    1. Explicit keyword parameters passed to ``setup_dask_client()``
    2. Config object (``config=``) **or** profile (``profile=``)
    3. Defaults — ``reserve_mem_gb`` uses a smart default (20 % RAM, 4–50 GiB)

    ``config`` and ``profile`` are mutually exclusive.

    Args:
        config: A pre-built DaskSetupConfig object to use as the base configuration.
        profile: Profile name to load from disk/builtins as the base configuration.
        workload_type: Workload type override.
        max_workers: Maximum workers cap.
        reserve_mem_gb: Memory to reserve (GiB). ``None`` → compute smart default.
        max_mem_gb: Total memory cap (GiB).
        dashboard: Whether to start the dashboard.
        adaptive: Enable adaptive scaling.
        min_workers: Minimum workers when adaptive=True.
        suggest_chunks: Print xarray chunking hints after setup.

    Returns:
        Resolved DaskSetupConfig

    Note:
        The heuristic that detects "explicitly set" parameters compares each value
        against its default.  Edge case: if you deliberately pass a value equal to
        the default (e.g. ``dashboard=True``) it will be treated as "not set" and a
        base_config value will take precedence.  Use ``config=DaskSetupConfig(...)``
        to avoid this ambiguity entirely.
    """
    if config is not None and profile is not None:
        raise ValueError(
            "Cannot specify both 'config' and 'profile'. "
            "Pass a DaskSetupConfig object via 'config=' OR a profile name via 'profile=', not both."
        )

    # Build defaults using the environment-aware smart reserve
    smart_reserve = _compute_smart_reserve_default()
    defaults = DaskSetupConfig(reserve_mem_gb=smart_reserve)
    logger.debug("Smart reserve default computed", reserve_mem_gb=smart_reserve)

    # Resolve the base configuration: either a provided config object or a loaded profile
    base_config = None

    if config is not None:
        # Caller supplied a ready-made DaskSetupConfig — use it as the profile-level base
        base_config = config
        logger.debug("Using caller-provided DaskSetupConfig as base")
    elif profile is not None:
        manager = ConfigManager()
        profile_obj = manager.get_profile(profile)
        if profile_obj is None:
            available = list(manager.list_profiles().keys())
            raise ValueError(f"Profile '{profile}' not found. Available profiles: {available}")
        base_config = profile_obj.config
        logger.debug("Loaded profile as base", profile=profile)

    # Collect explicitly-provided keyword overrides.
    #
    # Note: This heuristic compares each value against its default to decide whether it was
    # explicitly set. Edge case: if you deliberately pass a value that *equals* the default
    # (e.g. reserve_mem_gb equal to the computed smart default) it will be treated as "not set"
    # and a base_config value will take precedence. To avoid this, use a DaskSetupConfig
    # object via the 'config=' parameter instead.
    explicit_params: dict[str, Any] = {}

    if workload_type != "io":
        explicit_params["workload_type"] = workload_type
    if max_workers is not None:
        explicit_params["max_workers"] = max_workers
    if reserve_mem_gb is not None:
        # None means "use the smart default"; an explicit float means "user chose this"
        explicit_params["reserve_mem_gb"] = reserve_mem_gb
    if max_mem_gb is not None:
        explicit_params["max_mem_gb"] = max_mem_gb
    if dashboard is not True:
        explicit_params["dashboard"] = dashboard
    if adaptive is not False:
        explicit_params["adaptive"] = adaptive
    if min_workers is not None:
        explicit_params["min_workers"] = min_workers
    if suggest_chunks is not False:
        explicit_params["suggest_chunks"] = suggest_chunks
    if fallback_on_detection_failure:
        explicit_params["fallback_on_detection_failure"] = fallback_on_detection_failure
    if adaptive_memory:
        explicit_params["adaptive_memory"] = adaptive_memory

    explicit_config = DaskSetupConfig(**explicit_params) if explicit_params else None

    # Merge configurations: defaults < base_config < explicit overrides
    final_config = defaults
    if base_config:
        final_config = final_config.merge_with(base_config)
    if explicit_config:
        final_config = final_config.merge_with(explicit_config)

    return final_config


# ---------------------------------------------------------------------------
# Public API — context manager
# ---------------------------------------------------------------------------


class DaskClientContext:
    """Context manager wrapper for :func:`setup_dask_client`.

    Ensures the Dask client and cluster are closed cleanly on exit — even
    when an exception is raised inside the ``with`` block.

    All keyword arguments accepted by :func:`setup_dask_client` are valid.

    When a dataset is passed via ``ds=``, the context manager yields a
    4-tuple ``(client, cluster, tmp, chunks)`` where *chunks* is the
    recommended chunk dictionary for that dataset.  Without ``ds=``, the
    familiar 3-tuple ``(client, cluster, tmp)`` is yielded.

    Examples
    --------
    ::

        from dask_setup import DaskClientContext

        # 3-tuple form (no dataset)
        with DaskClientContext(workload_type="cpu") as (client, cluster, tmp):
            result = client.compute(ds.mean())

        # 4-tuple form (with dataset — chunks auto-computed)
        with DaskClientContext(ds=my_ds, suggest_chunks=True) as (client, cluster, tmp, chunks):
            ds_opt = my_ds.chunk(chunks)
            result = client.compute(ds_opt.mean())

    The context manager does **not** delete the spill/temp directory on exit
    so that users can inspect spill artefacts for debugging if needed.
    """

    def __init__(self, **kwargs: Any) -> None:
        self._kwargs = kwargs
        self._client: Client | None = None
        self._cluster: LocalCluster | None = None
        self._tmp_dir: str | None = None
        self.chunks: dict[str, int] | None = None
        self._start_time: float | None = None

    def __enter__(self) -> tuple:
        self._start_time = time.monotonic()
        result = setup_dask_client(**self._kwargs)
        self._client = result[0]
        self._cluster = result[1]
        self._tmp_dir = result[2]
        if len(result) == 4:
            self.chunks = result[3]
        return result

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: types.TracebackType | None,
    ) -> None:
        # Collect run statistics before closing the client
        if self._client is not None:
            try:
                from .reporting import cluster_report

                report = cluster_report(self._client, start_time=self._start_time)
                logger.info("Cluster run summary: " + report.summary_line())
            except Exception as e:
                logger.debug("Could not collect cluster report on exit", error=str(e))

        if self._client is not None:
            try:
                self._client.close()
                logger.debug("Dask client closed via context manager")
            except Exception as e:
                logger.warning("Failed to close Dask client cleanly", error=str(e))
        if self._cluster is not None:
            try:
                self._cluster.close()
                logger.debug("Dask cluster closed via context manager")
            except Exception as e:
                logger.warning("Failed to close Dask cluster cleanly", error=str(e))
        # Return None (falsy) — do not suppress any exception from the with block


# ---------------------------------------------------------------------------
# Public API — main entry point
# ---------------------------------------------------------------------------

# Overloads give callers accurate return-type information at static-analysis time.
# When ds=None (default), the return is a 3-tuple.
# When ds is an xarray object, the return is a 4-tuple that also contains the
# computed chunk dictionary.


@overload
def setup_dask_client(
    workload_type: str = ...,
    max_workers: int | None = ...,
    reserve_mem_gb: float | None = ...,
    max_mem_gb: float | None = ...,
    dashboard: bool = ...,
    adaptive: bool = ...,
    min_workers: int | None = ...,
    profile: str | None = ...,
    suggest_chunks: bool = ...,
    config: DaskSetupConfig | None = ...,
    ds: None = ...,
    fallback_on_detection_failure: bool = ...,
    adaptive_memory: bool = ...,
    mode: str = ...,
    multi_node_config: MultiNodeConfig | None = ...,
) -> tuple[Client, LocalCluster, str]: ...


@overload
def setup_dask_client(
    workload_type: str = ...,
    max_workers: int | None = ...,
    reserve_mem_gb: float | None = ...,
    max_mem_gb: float | None = ...,
    dashboard: bool = ...,
    adaptive: bool = ...,
    min_workers: int | None = ...,
    profile: str | None = ...,
    suggest_chunks: bool = ...,
    config: DaskSetupConfig | None = ...,
    ds: xr.Dataset | xr.DataArray = ...,
    fallback_on_detection_failure: bool = ...,
    adaptive_memory: bool = ...,
    mode: str = ...,
    multi_node_config: MultiNodeConfig | None = ...,
) -> tuple[Client, LocalCluster, str, dict[str, int]]: ...


def setup_dask_client(
    workload_type: str = "io",
    max_workers: int | None = None,
    reserve_mem_gb: float | None = None,
    max_mem_gb: float | None = None,
    dashboard: bool = True,
    adaptive: bool = False,
    min_workers: int | None = None,
    profile: str | None = None,
    suggest_chunks: bool = False,
    config: DaskSetupConfig | None = None,
    ds: Any = None,  # xr.Dataset | xr.DataArray | None
    fallback_on_detection_failure: bool = False,
    adaptive_memory: bool = False,
    mode: str = "auto",
    multi_node_config: MultiNodeConfig | None = None,
) -> tuple[Client, LocalCluster, str] | tuple[Client, LocalCluster, str, dict[str, int]]:
    """Create a single-node Dask LocalCluster tuned for HPC login/compute nodes.

    Routes temp/spill to ``$PBS_JOBFS`` when present.

    Parameters
    ----------
    workload_type : {"cpu","io","mixed"}
        Shape worker topology for CPU-bound, I/O-bound, or mixed workloads.
    max_workers : int or None
        Cap on worker processes. Defaults to all logical cores available.
    reserve_mem_gb : float or None
        Memory to reserve for OS / cache / filesystem (GiB).
        When ``None`` (the default), a smart default is chosen automatically:
        20 % of total RAM, clamped to [4 GiB, 50 GiB].
        Pass an explicit value to override (e.g. ``reserve_mem_gb=8.0``).
    max_mem_gb : float or None
        Cap total memory used by Dask. Default is node total.
    dashboard : bool
        If True, start a dashboard on a random free port and print an SSH tunnel hint.
    adaptive : bool
        Enable single-node adaptive scaling (elastic number of workers).
    min_workers : int or None
        Minimum workers when adaptive=True.
    profile : str or None
        Name of configuration profile to use. Profile settings are overridden by
        explicit parameters. Mutually exclusive with ``config``.
    suggest_chunks : bool
        If True, print xarray chunking recommendations after cluster setup.
        When ``ds=`` is also provided, the recommendations are computed from
        the actual dataset.  Without ``ds=``, generic guidance is printed.
        Requires xarray and numpy to be installed.
    config : DaskSetupConfig or None
        A pre-built configuration object. When provided, all other parameters
        except explicit overrides are ignored. Mutually exclusive with ``profile``.
        This is the recommended way to avoid the default-value ambiguity that
        occurs with individual keyword parameters.
    ds : xr.Dataset, xr.DataArray, or None
        Optional xarray dataset.  When provided:

        - Chunk validation is run automatically (warnings emitted for
          dangerously large or very small existing chunks).
        - Chunk recommendations are computed for the specific dataset and
          cluster configuration, and returned as the fourth element of the
          return tuple.
        - If ``suggest_chunks=True``, the recommendations are also printed.

        Requires xarray and numpy to be installed.
    fallback_on_detection_failure : bool
        If ``True``, use conservative hardcoded defaults (2 cores, 8 GiB) when
        all resource detection methods fail, instead of raising
        :exc:`ResourceDetectionError`.  A warning is logged.
        Default ``False`` preserves the existing behaviour of raising on failure.
    adaptive_memory : bool
        If ``True``, call :func:`~dask_setup.tune.tune_memory_thresholds` once
        after the cluster is ready.  This reads the (initially zero) spill stats
        and tightens the worker ``memory.target`` / ``memory.spill`` thresholds
        slightly, giving workers more head-room from the start.  Default
        ``False``.
    mode : {"auto", "local", "pbs", "slurm"}
        Backend selection.

        - ``"local"`` — always use a single-node ``LocalCluster`` (default
          behaviour prior to v2.0).
        - ``"pbs"`` — launch via ``dask-jobqueue.PBSCluster``.  Requires
          ``pip install dask-jobqueue``.
        - ``"slurm"`` — launch via ``dask-jobqueue.SLURMCluster``.
        - ``"auto"`` (default) — inspect the environment and choose
          ``"pbs"`` if ``PBS_JOBID`` is set, ``"slurm"`` if
          ``SLURM_JOB_ID`` is set, or ``"local"`` otherwise.
    multi_node_config : MultiNodeConfig or None
        Configuration for the multi-node backend (``mode="pbs"`` or
        ``"slurm"``).  Ignored when ``mode="local"``.  When ``None`` and a
        multi-node mode is selected, a default :class:`MultiNodeConfig` is
        constructed from *workload_type*.

    Returns
    -------
    When ``ds`` is ``None`` (default):
        ``(client, cluster, dask_local_dir)`` — a 3-tuple.

    When ``ds`` is provided:
        ``(client, cluster, dask_local_dir, chunks)`` — a 4-tuple, where
        *chunks* is a ``dict[str, int]`` of recommended chunk sizes for that
        dataset (can be passed directly to ``ds.chunk(chunks)``).

    Raises
    ------
    InvalidConfigurationError
        If workload_type is invalid or parameters are inconsistent.
    InsufficientResourcesError
        If system resources are insufficient for the requested configuration.
    ResourceDetectionError
        If resource detection fails completely.

    See Also
    --------
    DaskClientContext : Context manager version that closes the cluster on exit.
    validate_chunks   : Validate existing chunking against cluster memory limits.
    recommend_chunks  : Standalone chunk recommendation function.
    rechunk_dataset   : Rechunk a dataset to new target chunk sizes.

    Examples
    --------
    ::

        # Basic usage (3-tuple)
        client, cluster, tmp = setup_dask_client(workload_type="io")

        # Basic usage (3-tuple)
        client, cluster, tmp = setup_dask_client(workload_type="io")

        # With dataset — chunk validation + recommendations (4-tuple)
        ds = xr.open_zarr("era5.zarr")
        client, cluster, tmp, chunks = setup_dask_client(ds=ds, suggest_chunks=True)
        ds_opt = ds.chunk(chunks)

        # Multi-node PBS — auto-detects from environment
        client, cluster, tmp = setup_dask_client(
            mode="pbs",
            multi_node_config=MultiNodeConfig(
                workers_per_node=4,
                cores_per_worker=12,
                mem_per_worker_gb=32.0,
                walltime="04:00:00",
            ),
        )
    """
    from .environment import get_environment_type, is_jupyter

    env_type = get_environment_type()

    # ------------------------------------------------------------------
    # Multi-node dispatch — if mode != "local", hand off to the
    # appropriate dask-jobqueue backend and return early.
    # ------------------------------------------------------------------
    resolved_mode = mode
    if resolved_mode == "auto":
        resolved_mode = detect_cluster_mode()
        logger.debug("Mode auto-resolved", mode=resolved_mode)

    if resolved_mode in {"pbs", "slurm"}:
        from .multinode import setup_pbs_cluster, setup_slurm_cluster

        mn_cfg = multi_node_config
        if mn_cfg is None:
            # Build a minimal MultiNodeConfig from whatever was passed
            mn_cfg = MultiNodeConfig(workload_type=workload_type)

        logger.info("Multi-node cluster mode", mode=resolved_mode)
        if resolved_mode == "pbs":
            client, cluster, shared_tmp = setup_pbs_cluster(mn_cfg)
        else:
            client, cluster, shared_tmp = setup_slurm_cluster(mn_cfg)

        tmp_path = str(shared_tmp) if shared_tmp is not None else ""
        # Multi-node path always returns a 3-tuple (no chunk recommendation)
        return client, cluster, tmp_path  # type: ignore[return-value]

    logger.info("Starting Dask client setup", workload_type=workload_type, environment=env_type)

    # --- Profile auto-selection -----------------------------------------
    # Must happen before _resolve_configuration so the selected profile name
    # can be passed in. Requires a preliminary resource detection pass.
    if profile == "auto":
        try:
            _pre_resources = detect_resources(fallback=fallback_on_detection_failure)
        except Exception:
            _pre_resources = None
        from .config_manager import ConfigManager as _CM

        profile = _CM().auto_select_profile(_pre_resources)
        logger.info("Auto-selected profile", profile=profile)

    # Load and merge configuration
    config = _resolve_configuration(
        config=config,
        profile=profile,
        workload_type=workload_type,
        max_workers=max_workers,
        reserve_mem_gb=reserve_mem_gb,
        max_mem_gb=max_mem_gb,
        dashboard=dashboard,
        adaptive=adaptive,
        min_workers=min_workers,
        suggest_chunks=suggest_chunks,
        fallback_on_detection_failure=fallback_on_detection_failure,
        adaptive_memory=adaptive_memory,
    )
    logger.debug(
        "Configuration resolved",
        workload_type=config.workload_type,
        reserve_mem_gb=config.reserve_mem_gb,
    )

    # Detect system resources
    resources = detect_resources(fallback=config.fallback_on_detection_failure)
    logger.debug(
        "Resources detected",
        total_cores=resources.total_cores,
        total_mem_gib=f"{resources.total_mem_bytes / (1024**3):.1f}",
        method=resources.detection_method,
    )
    if resources.detection_method == "fallback":
        logger.warning(
            "Using fallback resource defaults — cluster will be conservative. "
            "Pass fallback_on_detection_failure=False to raise an error instead.",
            cores=resources.total_cores,
            mem_gib=f"{resources.total_mem_bytes / (1024**3):.1f}",
        )
    if is_jupyter():
        logger.debug("Jupyter environment detected — dashboard link will be rendered as HTML")

    # --- Workload type auto-inference -----------------------------------
    # Resolve "auto" to a concrete type now that we have resources + ds.
    # We patch config with the inferred type rather than creating a whole
    # new DaskSetupConfig to avoid a second validation pass.
    if config.workload_type == "auto":
        inferred_wt = infer_workload_type(ds)
        config.workload_type = inferred_wt
        logger.info(
            "workload_type='auto' resolved via dataset inspection",
            workload_type=inferred_wt,
        )

    # Create temporary directory for spill files (use config for base dir if specified)
    temp_dir = create_dask_temp_dir(base_dir=config.temp_base_dir)
    logger.debug("Temp/spill directory created", path=str(temp_dir))

    # Decide worker topology based on workload type
    topology = decide_topology(
        workload_type=config.workload_type,
        total_cores=resources.total_cores,
        max_workers=config.max_workers,
    )

    # Validate topology makes sense
    validate_topology(topology, resources.total_cores)

    # Calculate memory allocation
    try:
        memory_spec = calculate_memory_spec(
            total_mem_bytes=resources.total_mem_bytes,
            n_workers=topology.n_workers,
            reserve_mem_gb=config.reserve_mem_gb,
            max_mem_gb=config.max_mem_gb,
        )
    except ValueError as e:
        # Extract memory values for better error reporting
        total_gib = resources.total_mem_bytes / (1024**3)
        available_gb = total_gib - config.reserve_mem_gb
        required_gb = topology.n_workers * 1.0  # Rough estimate: 1 GB per worker minimum

        # Generate suggested actions based on the configuration
        suggestions = []
        if config.reserve_mem_gb > available_gb / 2:  # Reserve more than half of available
            suggestions.append(
                f"Reduce reserve_mem_gb from {config.reserve_mem_gb:.1f} GB to {available_gb * 0.3:.1f} GB"
            )
        if topology.n_workers > 1:
            suggestions.append(
                f"Limit max_workers to 1 or 2 workers instead of {topology.n_workers}"
            )
        if not suggestions:  # Fallback suggestions
            suggestions = [
                "Close other applications to free up memory",
                "Request a larger memory allocation for your job",
            ]

        raise InsufficientResourcesError(
            required_mem=required_gb, available_mem=available_gb, suggested_actions=suggestions
        ) from e

    # Create the cluster
    dashboard_address = ":0" if config.dashboard else None
    if config.dashboard and config.dashboard_port:
        dashboard_address = f":{config.dashboard_port}"

    # Map the boolean silence_logs config to a logging level.
    # True  → suppress everything except errors (logging.ERROR)
    # False → show warnings and above (logging.WARNING), which is a reasonable HPC default
    import logging as _stdlib_logging

    silence_logs_level = _stdlib_logging.ERROR if config.silence_logs else _stdlib_logging.WARNING

    cluster = create_cluster(
        topology=topology,
        memory_spec=memory_spec,
        temp_dir=temp_dir,
        dashboard_address=dashboard_address,
        silence_logs=silence_logs_level,
        adaptive=config.adaptive,
        min_workers=config.min_workers,
        memory_target=config.memory_target,
        memory_spill=config.memory_spill,
        memory_pause=config.memory_pause,
        memory_terminate=config.memory_terminate,
        spill_compression=config.spill_compression,
        comm_compression=config.comm_compression,
        spill_threads=config.spill_threads,
    )

    # Connect client
    client = Client(cluster)

    # --- Adaptive memory threshold tuning (opt-in) ----------------------
    if config.adaptive_memory:
        try:
            from .tune import tune_memory_thresholds

            tune_result = tune_memory_thresholds(client, strategy="tighten")
            logger.info("Adaptive memory tuning: " + tune_result.summary())
        except Exception as e:
            logger.warning("adaptive_memory tuning failed; continuing with defaults", error=str(e))

    # Print dashboard info if enabled
    if config.dashboard:
        print_dashboard_info(client, silent=config.silence_logs)

    # Log setup summary via structured logger
    spill_threads_str = (
        f" | spill_threads={config.spill_threads}" if config.spill_threads is not None else ""
    )
    logger.info(f"Temp/spill dir: {temp_dir}")
    logger.info(
        f"Workers: {topology.n_workers}"
        f" | threads/worker: {topology.threads_per_worker}"
        f" | processes: {topology.processes}"
    )
    logger.info(
        f"Mem: total ~{memory_spec.total_mem_gib:.1f} GiB"
        f" | usable ~{memory_spec.usable_mem_gb:.1f} GiB"
        f" | per-worker ~{memory_spec.mem_per_worker_bytes / (1024**3):.1f} GiB"
    )
    logger.info(
        f"Compression: spill={config.spill_compression}"
        f" | comm={config.comm_compression}{spill_threads_str}"
    )

    # --- Dataset-aware chunking (ds= parameter) ------------------------------
    chunk_recommendations: dict[str, int] | None = None

    if ds is not None:
        try:
            from .xarray import recommend_chunks, validate_chunks

            # First, warn about any problematic existing chunking
            validate_chunks(ds, client=client)

            # Compute chunk recommendations for this specific dataset + cluster
            raw = recommend_chunks(
                ds,
                client=client,
                workload_type=config.workload_type,
                verbose=config.suggest_chunks,
            )
            # recommend_chunks returns ChunkRecommendation when verbose=True, dict otherwise
            chunk_recommendations = raw.chunks if hasattr(raw, "chunks") else raw
            logger.info("Chunk recommendations computed", chunks=str(chunk_recommendations))

        except ImportError:
            logger.warning(
                "ds= provided but xarray/numpy are not installed — "
                "skipping chunk validation and recommendations"
            )

    elif config.suggest_chunks:
        # No dataset provided — print generic cluster-based guidance
        logger.info("=" * 60)
        logger.info("Xarray Chunking Recommendations")
        logger.info("=" * 60)
        logger.info(
            "To get optimal chunking suggestions for your xarray datasets:\n"
            "\n"
            "  from dask_setup import recommend_chunks\n"
            "  chunks = recommend_chunks(your_dataset, client, verbose=True)\n"
            "  ds_optimized = your_dataset.chunk(chunks)\n"
            "\n"
            "Or pass your dataset to setup_dask_client for automatic recommendations:\n"
            "\n"
            "  client, cluster, tmp, chunks = setup_dask_client(ds=your_dataset)"
        )
        logger.info(
            f"Based on your current cluster:\n"
            f"  workload_type:      {config.workload_type}\n"
            f"  target chunk size:  256-512 MiB\n"
            f"  safety factor:      60% of worker memory"
            f" ({memory_spec.mem_per_worker_bytes / (1024**3) * 0.6:.1f} GiB max per chunk)\n"
            f"  workers available:  {topology.n_workers}"
        )
        logger.info("=" * 60)

    # --- Return --------------------------------------------------------------
    logger.info("Dask client ready")

    if ds is not None:
        return client, cluster, str(temp_dir), chunk_recommendations or {}
    return client, cluster, str(temp_dir)
