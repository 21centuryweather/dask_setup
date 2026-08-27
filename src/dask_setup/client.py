"""Main client setup orchestration for dask_setup."""

from __future__ import annotations

import logging
import time
import types
from typing import TYPE_CHECKING, Any, overload

import psutil
from dask.distributed import Client, LocalCluster

from .cluster import (
    MIN_MEM_PER_WORKER_GB,
    calculate_memory_spec,
    compute_usable_mem_gb,
    create_cluster,
    fit_workers_to_memory,
)
from .config import DaskSetupConfig
from .config_manager import ConfigManager
from .dashboard import print_dashboard_info
from .exceptions import InsufficientResourcesError
from .logging import get_logger
from .multinode import (
    MultiNodeConfig,
    detect_cluster_mode,
    setup_interactive_cluster,
    setup_pbs_cluster,
    setup_slurm_cluster,
)
from .resources import detect_resources
from .tempdir import create_dask_temp_dir
from .topology import decide_topology, validate_topology

if TYPE_CHECKING:
    import xarray as xr

from .workload import infer_workload_type

logger = get_logger("client")

#: Fallback workload type for code paths that need a concrete value before the
#: configuration has been resolved (the multi-node dispatch in particular).
#: Matches the ``DaskSetupConfig.workload_type`` field default.
_DEFAULT_WORKLOAD_TYPE = "io"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _compute_smart_reserve_default() -> float:
    """Compute a sensible reserve_mem_gb default based on available system RAM.

    Formula: 20 % of total RAM, clamped to [4 GiB, 50 GiB], and never more
    than half the machine.

    Examples
    --------
    - 1 GiB container    → max(1.0, 0.5)   =  1.0 GiB  (validation floor)
    - 4 GiB container    → min(4.0, 2.0)   =  2.0 GiB  (half-machine cap)
    - 16 GiB laptop      → max(4.0, 3.2)   =  4.0 GiB  (clamped to minimum)
    - 64 GiB workstation → 12.8 GiB
    - 300 GiB Gadi node  → min(50.0, 60.0) = 50.0 GiB  (clamped to maximum)

    The half-machine cap matters on small hosts: without it the 4 GiB floor
    reserves the entire machine on a 4 GiB container, leaving nothing at all
    for workers -- the same failure the flat 50 GiB default caused on a laptop,
    just further down the scale.

    Falls back to 50.0 GiB if psutil fails for any reason.
    """
    try:
        total_ram_gb = psutil.virtual_memory().total / (1024**3)
        smart_reserve = min(50.0, max(4.0, total_ram_gb * 0.20))
        # DaskSetupConfig validates reserve_mem_gb into [1.0, 1000.0]; the
        # half-machine cap must not push the default below its own floor.
        return max(1.0, min(smart_reserve, total_ram_gb * 0.5))
    except Exception:
        return 50.0  # Safe HPC fallback if psutil is unexpectedly unavailable


def _chunk_recommendations_for(
    ds: Any,
    client: Client,
    workload_type: str,
    verbose: bool,
) -> dict[str, int]:
    """Validate *ds*'s current chunking and return recommendations for it.

    Returns an empty dict when xarray/numpy are unavailable, so callers can
    always honour the documented 4-tuple return shape.
    """
    try:
        from .xarray import recommend_chunks, validate_chunks

        # First, warn about any problematic existing chunking
        validate_chunks(ds, client=client)

        raw = recommend_chunks(
            ds,
            client=client,
            workload_type=workload_type,
            verbose=verbose,
        )
        chunks = raw.chunks if hasattr(raw, "chunks") else raw
        logger.info("Chunk recommendations computed", chunks=str(chunks))
        return dict(chunks)
    except ImportError:
        logger.warning(
            "ds= provided but xarray/numpy are not installed — "
            "skipping chunk validation and recommendations"
        )
        return {}


def _multi_node_config_from(config: DaskSetupConfig) -> MultiNodeConfig:
    """Build a :class:`MultiNodeConfig` from a resolved single-node config.

    Used when a multi-node mode is selected without an explicit
    ``multi_node_config=``. Only the fields the two objects genuinely share
    are carried across; job-shape settings (cores per worker, walltime, queue,
    project) have no single-node equivalent and keep their defaults.
    """
    workload_type = config.workload_type
    if workload_type == "auto":
        # MultiNodeConfig accepts "auto", but the backends need something
        # concrete and there is no dataset to infer from at this point.
        workload_type = _DEFAULT_WORKLOAD_TYPE

    kwargs: dict[str, Any] = {"workload_type": workload_type}
    if config.max_workers is not None:
        kwargs["workers_per_node"] = config.max_workers
    if config.adaptive:
        kwargs["adaptive"] = True
        if config.min_workers is not None:
            kwargs["min_jobs"] = config.min_workers
            kwargs["max_jobs"] = max(config.min_workers, MultiNodeConfig().max_jobs)
    if config.temp_base_dir:
        kwargs["shared_tmp_dir"] = config.temp_base_dir

    return MultiNodeConfig(**kwargs)


#: Config fields that only a single-node LocalCluster can honour.  On the
#: multi-node paths these are the caller's job-script's business instead, so
#: say so rather than pretending they were applied.
_LOCAL_ONLY_CONFIG_FIELDS: tuple[tuple[str, Any], ...] = (
    # reserve_mem_gb is deliberately absent: its default is machine-aware
    # (see _compute_smart_reserve_default), so a value-vs-default comparison
    # would flag it as "explicitly set" on every machine with under 250 GiB of
    # RAM and warn about a setting the caller never touched.  It is handled
    # separately in _warn_unsupported_multinode_options.
    ("max_mem_gb", None),
    ("memory_target", 0.75),
    ("memory_spill", 0.85),
    ("spill_compression", "auto"),
    ("spill_threads", None),
    ("dashboard_port", None),
)


def _warn_unsupported_multinode_options(
    resolved_mode: str,
    config: DaskSetupConfig,
) -> None:
    """Log which resolved settings the selected backend cannot apply.

    Only for ``"pbs"`` / ``"slurm"``, where worker resources come from the
    submitted job script rather than from this process.  ``"interactive"`` on a
    single allocated node goes through the normal local path and honours
    everything; on a multi-node allocation the SSHCluster uses per-node
    defaults, which :func:`~dask_setup.multinode.setup_interactive_cluster`
    documents.
    """
    if resolved_mode not in {"pbs", "slurm"}:
        return

    ignored = [
        name
        for name, default in _LOCAL_ONLY_CONFIG_FIELDS
        if getattr(config, name, default) != default
    ]
    # Only report reserve_mem_gb when it differs from both the static default
    # and the machine-aware one this process would have chosen on its own.
    reserve = getattr(config, "reserve_mem_gb", None)
    if reserve is not None and reserve not in (50.0, _compute_smart_reserve_default()):
        ignored.insert(0, "reserve_mem_gb")
    if ignored:
        logger.warning(
            "Some settings do not apply to this cluster mode and were not used",
            mode=resolved_mode,
            ignored=",".join(ignored),
            hint="set these via MultiNodeConfig or the worker job script instead",
        )


def _resolve_configuration(
    profile: str | None = None,
    workload_type: str | None = None,
    max_workers: int | None = None,
    reserve_mem_gb: float | None = None,
    max_mem_gb: float | None = None,
    dashboard: bool | None = None,
    adaptive: bool | None = None,
    min_workers: int | None = None,
    suggest_chunks: bool | None = None,
    input_config: DaskSetupConfig | None = None,
) -> DaskSetupConfig:
    """Resolve final configuration from a profile and explicit parameters.

    Priority order (highest to lowest):

    1. Explicit keyword parameters passed to ``setup_dask_client()``
    2. Profile (``profile=``) **or** ``config=`` object (*input_config*)
    3. Library defaults (:class:`DaskSetupConfig` field defaults)

    Every parameter below uses ``None`` to mean "not supplied by the caller".
    Only non-``None`` values are treated as explicit overrides, so the base
    configuration keeps every field the caller did not name.

    .. note::

       ``profile=`` and ``config=`` do **not** layer on top of each other --
       whichever is given supplies the base, and if both are given the profile
       wins outright.  Layering them properly would need to know which fields a
       profile's YAML actually set: a :class:`ConfigProfile` holds a fully
       populated :class:`DaskSetupConfig`, so merging one over a caller's
       config object would apply the profile's *untouched defaults* too and
       silently wipe settings the caller had deliberately chosen.  Pass one or
       the other, and use explicit keyword arguments for the differences.

    Args:
        profile: Profile name to load from disk/builtins as the base configuration.
        workload_type: Workload type override, or ``None`` to inherit.
        max_workers: Maximum workers cap, or ``None`` to inherit.
        reserve_mem_gb: Memory to reserve (GiB), or ``None`` to inherit.
        max_mem_gb: Total memory cap (GiB), or ``None`` to inherit.
        dashboard: Whether to start the dashboard, or ``None`` to inherit.
        adaptive: Enable adaptive scaling, or ``None`` to inherit.
        min_workers: Minimum workers when adaptive=True, or ``None`` to inherit.
        suggest_chunks: Print xarray chunking hints, or ``None`` to inherit.
        input_config: Pre-built configuration supplied via ``config=``.

    Returns:
        Resolved DaskSetupConfig
    """
    # The library default for reserve_mem_gb scales with the machine.  A flat
    # 50 GiB is right for a Gadi node and catastrophic on a 16 GiB laptop,
    # where it reserves the entire machine and leaves nothing for workers.
    # DaskSetupConfig keeps a static default because profiles serialise to
    # JSON and need a concrete number; the machine-aware value is applied here,
    # at the bottom of the precedence stack, so any profile, config object or
    # explicit argument still overrides it.
    defaults = DaskSetupConfig(reserve_mem_gb=_compute_smart_reserve_default())
    logger.debug("Configuration defaults set", reserve_mem_gb=defaults.reserve_mem_gb)

    # Resolve the base configuration.
    # Priority (lowest → highest): defaults < input_config < profile < explicit params.
    # input_config is a pre-built DaskSetupConfig passed directly by the caller
    # (e.g. from scaling_analysis / benchmark_config).  It sits above the library
    # defaults but below any named profile so that profiles can still override it,
    # and below explicit keyword arguments so those always win.
    base_config = input_config  # may be None

    if profile is not None:
        manager = ConfigManager()
        profile_obj = manager.get_profile(profile)
        if profile_obj is None:
            available = list(manager.list_profiles().keys())
            raise ValueError(f"Profile '{profile}' not found. Available profiles: {available}")
        base_config = profile_obj.config
        logger.debug("Loaded profile as base", profile=profile)

    # Collect explicitly-provided keyword overrides.  A parameter is "explicit"
    # if and only if the caller passed something other than None — no value
    # comparison, so passing a value that happens to equal the library default
    # still overrides the profile.
    candidates: dict[str, Any] = {
        "workload_type": workload_type,
        "max_workers": max_workers,
        "reserve_mem_gb": reserve_mem_gb,
        "max_mem_gb": max_mem_gb,
        "dashboard": dashboard,
        "adaptive": adaptive,
        "min_workers": min_workers,
        "suggest_chunks": suggest_chunks,
    }
    explicit_params: dict[str, Any] = {k: v for k, v in candidates.items() if v is not None}

    # Merge configurations: defaults < input_config < profile < explicit overrides.
    #
    # The explicit layer is applied with merge_overrides() rather than
    # merge_with(): building a DaskSetupConfig from explicit_params would also
    # carry every untouched dataclass default (workload_type="io",
    # memory_target=0.75, spill_compression="auto", ...), and merge_with()
    # applies all non-None fields — which would wipe the profile.
    final_config = defaults
    if base_config:
        final_config = final_config.merge_with(base_config)
    final_config = final_config.merge_overrides(explicit_params)

    if explicit_params:
        logger.debug("Explicit overrides applied", fields=",".join(sorted(explicit_params)))

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
    workload_type: str | None = ...,
    max_workers: int | None = ...,
    reserve_mem_gb: float | None = ...,
    max_mem_gb: float | None = ...,
    dashboard: bool | None = ...,
    adaptive: bool | None = ...,
    min_workers: int | None = ...,
    profile: str | None = ...,
    suggest_chunks: bool | None = ...,
    config: DaskSetupConfig | None = ...,
    ds: None = ...,
    fallback_on_detection_failure: bool = ...,
    adaptive_memory: bool = ...,
    mode: str = ...,
    multi_node_config: MultiNodeConfig | None = ...,
) -> tuple[Client, LocalCluster, str]: ...


@overload
def setup_dask_client(
    workload_type: str | None = ...,
    max_workers: int | None = ...,
    reserve_mem_gb: float | None = ...,
    max_mem_gb: float | None = ...,
    dashboard: bool | None = ...,
    adaptive: bool | None = ...,
    min_workers: int | None = ...,
    profile: str | None = ...,
    suggest_chunks: bool | None = ...,
    config: DaskSetupConfig | None = ...,
    ds: xr.Dataset | xr.DataArray = ...,
    fallback_on_detection_failure: bool = ...,
    adaptive_memory: bool = ...,
    mode: str = ...,
    multi_node_config: MultiNodeConfig | None = ...,
) -> tuple[Client, LocalCluster, str, dict[str, int]]: ...


def setup_dask_client(
    workload_type: str | None = None,
    max_workers: int | None = None,
    reserve_mem_gb: float | None = None,
    max_mem_gb: float | None = None,
    dashboard: bool | None = None,
    adaptive: bool | None = None,
    min_workers: int | None = None,
    profile: str | None = None,
    suggest_chunks: bool | None = None,
    config: DaskSetupConfig | None = None,
    ds: Any = None,  # xr.Dataset | xr.DataArray | None
    fallback_on_detection_failure: bool = False,
    adaptive_memory: bool = False,
    mode: str = "auto",
    multi_node_config: MultiNodeConfig | None = None,
) -> tuple[Client, LocalCluster, str] | tuple[Client, LocalCluster, str, dict[str, int]]:
    """Create a single-node Dask LocalCluster tuned for HPC login/compute nodes.

    Routes temp/spill to ``$PBS_JOBFS`` when present.

    Every configuration parameter below defaults to ``None``, meaning "not
    supplied".  Values are resolved in this order, highest priority first:

    1. Parameters you pass explicitly here
    2. ``profile=``
    3. ``config=``
    4. :class:`~dask_setup.config.DaskSetupConfig` field defaults

    A parameter you leave at ``None`` keeps whatever the profile or config
    object specifies; a parameter you pass always wins, even when the value
    happens to equal the library default.

    Parameters
    ----------
    workload_type : {"cpu","io","mixed","gpu","auto"} or None
        Shape worker topology for CPU-bound, I/O-bound, or mixed workloads.
        ``None`` inherits from the profile/config, falling back to ``"io"``.
    max_workers : int or None
        Cap on worker processes. Defaults to all logical cores available.
    reserve_mem_gb : float or None
        Memory to reserve for OS / cache / filesystem (GiB).
        ``None`` inherits from the profile/config, falling back to a
        machine-aware default: 20% of total RAM, clamped to [4.0, 50.0] GiB.
    max_mem_gb : float or None
        Cap total memory used by Dask. Default is node total.
    dashboard : bool or None
        If True, start a dashboard on a random free port and print an SSH
        tunnel hint. ``None`` inherits, falling back to True.
    adaptive : bool or None
        Enable single-node adaptive scaling (elastic number of workers).
        ``None`` inherits, falling back to False.
    min_workers : int or None
        Minimum workers when adaptive=True.
    profile : str or None
        Name of configuration profile to use.  Profile settings override
        ``config=`` and are in turn overridden by explicit parameters.  Pass
        ``"auto"`` to select a profile from the detected resources.
    suggest_chunks : bool
        If True, print xarray chunking recommendations after cluster setup.
        When ``ds=`` is also provided, the recommendations are computed from
        the actual dataset.  Without ``ds=``, generic guidance is printed.
        Requires xarray and numpy to be installed.
    config : DaskSetupConfig or None
        A pre-built configuration object supplying every field at once.  This
        is the recommended way to avoid the default-value ambiguity that occurs
        with individual keyword parameters.

        The full precedence, lowest to highest, is::

            library defaults  <  config= or profile=  <  explicit keywords

        ``config=`` and ``profile=`` occupy the same layer rather than stacking:
        passing both is allowed, but the profile supplies the base and the
        config object is ignored.  Use explicit keyword arguments for anything
        you want on top.
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
    mode : {"auto", "local", "pbs", "slurm", "interactive"}
        Backend selection.

        - ``"local"`` — always use a single-node ``LocalCluster`` (default
          behaviour prior to v2.0).
        - ``"pbs"`` — launch via ``dask-jobqueue.PBSCluster`` (submits new
          batch jobs).  Requires ``pip install dask-jobqueue``.
        - ``"slurm"`` — launch via ``dask-jobqueue.SLURMCluster`` (submits
          new batch jobs).
        - ``"interactive"`` — use resources already allocated in the current
          interactive PBS (``qsub -I``) or SLURM (``salloc``) session.
          Single-node allocations create a ``LocalCluster``; multi-node
          allocations create an ``SSHCluster`` across all nodes in
          ``PBS_NODEFILE`` / ``SLURM_NODELIST``.
        - ``"auto"`` (default) — inspect the environment and choose
          ``"interactive"`` when inside a PBS interactive job
          (``PBS_ENVIRONMENT=PBS_INTERACTIVE``) or a SLURM interactive
          allocation (``SLURM_BATCH_FLAG`` not set to ``"1"``);
          ``"pbs"`` when ``PBS_JOBID`` is set in a batch job;
          ``"slurm"`` when ``SLURM_JOB_ID`` is set in a batch job;
          ``"local"`` otherwise.
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

    # ------------------------------------------------------------------
    # Multi-node dispatch — if the resolved mode is not "local", hand off to
    # the appropriate backend and return early.
    #
    # Configuration is resolved *before* this dispatch so the multi-node
    # backends see the caller's profile/config too. They previously read the
    # raw workload_type keyword and dropped everything else on the floor.
    # ------------------------------------------------------------------
    if resolved_mode != "local":
        mn_resolved = _resolve_configuration(
            profile=profile,
            workload_type=workload_type,
            max_workers=max_workers,
            reserve_mem_gb=reserve_mem_gb,
            max_mem_gb=max_mem_gb,
            dashboard=dashboard,
            adaptive=adaptive,
            min_workers=min_workers,
            suggest_chunks=suggest_chunks,
            input_config=config,
        )
        _warn_unsupported_multinode_options(resolved_mode, mn_resolved)

        if resolved_mode == "interactive":
            logger.info("Interactive cluster mode — using already-allocated nodes")
            client, cluster, tmp_path = setup_interactive_cluster(
                workload_type=mn_resolved.workload_type,
                workers_per_node=mn_resolved.max_workers,
                config=mn_resolved,
            )
        else:
            mn_cfg = multi_node_config
            if mn_cfg is None:
                mn_cfg = _multi_node_config_from(mn_resolved)
                logger.debug(
                    "Built MultiNodeConfig from the resolved configuration",
                    workload_type=mn_cfg.workload_type,
                    adaptive=mn_cfg.adaptive,
                )

            logger.info("Multi-node cluster mode", mode=resolved_mode)
            if resolved_mode == "pbs":
                client, cluster, shared_tmp = setup_pbs_cluster(mn_cfg)
            else:
                client, cluster, shared_tmp = setup_slurm_cluster(mn_cfg)
            tmp_path = str(shared_tmp) if shared_tmp is not None else ""

        # Chunk recommendations need a live client, which we now have — so
        # the ds= contract (4-tuple) holds on these paths too.
        if ds is not None:
            chunks = _chunk_recommendations_for(
                ds, client, mn_resolved.workload_type, mn_resolved.suggest_chunks
            )
            return client, cluster, tmp_path, chunks  # type: ignore[return-value]
        return client, cluster, tmp_path  # type: ignore[return-value]

    logger.info(
        "Starting Dask client setup",
        workload_type=workload_type or _DEFAULT_WORKLOAD_TYPE,
        environment=env_type,
    )

    # Load and merge configuration.
    # Save the caller-supplied config object before the local variable is
    # rebound by _resolve_configuration so it can be used as the base layer.
    # Every keyword below is forwarded verbatim: None means "caller did not
    # supply this", so profile/config values survive for unnamed parameters.
    _input_config = config

    config = _resolve_configuration(
        profile=profile,
        workload_type=workload_type,
        max_workers=max_workers,
        reserve_mem_gb=reserve_mem_gb,
        max_mem_gb=max_mem_gb,
        dashboard=dashboard,
        adaptive=adaptive,
        min_workers=min_workers,
        suggest_chunks=suggest_chunks,
        input_config=_input_config,
    )

    # Apply additional config-level settings that don't go through _resolve_configuration
    config.fallback_on_detection_failure = fallback_on_detection_failure
    config.adaptive_memory = adaptive_memory

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

    # --- Fit the worker count to available memory -----------------------
    # Dask's memory_limit is per-worker, so N workers each floored at the
    # minimum budget commit N * minimum in total.  On a core-rich but
    # memory-tight node that silently exceeds the node's RAM and eats the
    # whole reservation.  Reduce the worker count instead.
    try:
        _usable_gb = compute_usable_mem_gb(
            total_mem_bytes=resources.total_mem_bytes,
            reserve_mem_gb=config.reserve_mem_gb,
            max_mem_gb=config.max_mem_gb,
        )
    except ValueError:
        # Leave the reporting of this to calculate_memory_spec below, which
        # raises the same error with the full InsufficientResourcesError context.
        _usable_gb = None

    if _usable_gb is not None:
        fitted_workers = fit_workers_to_memory(_usable_gb, topology.n_workers)
        if fitted_workers < topology.n_workers:
            logger.warning(
                "Reducing worker count to fit available memory",
                requested_workers=topology.n_workers,
                fitted_workers=fitted_workers,
                usable_mem_gib=f"{_usable_gb:.1f}",
                min_per_worker_gib=f"{MIN_MEM_PER_WORKER_GB:.1f}",
                hint="raise max_mem_gb or lower reserve_mem_gb for more workers",
            )
            print(
                f"[setup_dask_client] Reduced workers {topology.n_workers} -> {fitted_workers} "
                f"so each keeps at least {MIN_MEM_PER_WORKER_GB:.1f} GiB "
                f"({_usable_gb:.1f} GiB usable)."
            )
            topology = topology._replace(n_workers=fitted_workers)

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

    cluster = create_cluster(
        topology=topology,
        memory_spec=memory_spec,
        temp_dir=temp_dir,
        dashboard_address=dashboard_address,
        # config.silence_logs was collected, validated and serialised but never
        # reached the cluster: every run got logging.ERROR regardless. False
        # (the default) now means distributed's own default level, so worker
        # warnings -- memory pressure, paused workers -- are visible again.
        silence_logs=logging.ERROR if config.silence_logs else logging.WARNING,
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

    # Log and print setup summary
    spill_threads_str = (
        f" | spill_threads={config.spill_threads}" if config.spill_threads is not None else ""
    )
    summary_lines = [
        "[setup_dask_client] Configuration summary",
        f"temp/spill dir: {temp_dir}",
        f"Workers: {topology.n_workers} | threads/worker: {topology.threads_per_worker} | processes: {topology.processes}",
        f"Memory: total ~{memory_spec.total_mem_gib:.1f} GiB | usable ~{memory_spec.usable_mem_gb:.1f} GiB | per-worker ~{memory_spec.mem_per_worker_bytes / (1024**3):.1f} GiB",
        f"Compression: spill={config.spill_compression} | comm={config.comm_compression}{spill_threads_str}",
    ]

    # Print summary to console
    for line in summary_lines:
        print(line)

    # Also log via structured logger
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
        chunk_recommendations = _chunk_recommendations_for(
            ds, client, config.workload_type, config.suggest_chunks
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
