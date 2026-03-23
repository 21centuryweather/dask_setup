"""Main client setup orchestration for dask_setup."""

from __future__ import annotations

import logging

from dask.distributed import Client, LocalCluster

from .cluster import calculate_memory_spec, create_cluster
from .config import DaskSetupConfig
from .config_manager import ConfigManager
from .dashboard import print_dashboard_info
from .exceptions import InsufficientResourcesError
from .resources import detect_resources
from .tempdir import create_dask_temp_dir
from .topology import decide_topology, validate_topology


def _resolve_configuration(
    config: DaskSetupConfig | None = None,
    profile: str | None = None,
    workload_type: str = "io",
    max_workers: int | None = None,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
    dashboard: bool = True,
    adaptive: bool = False,
    min_workers: int | None = None,
    suggest_chunks: bool = False,
) -> DaskSetupConfig:
    """Resolve final configuration from a config object, profile, and explicit parameters.

    Priority order (highest to lowest):
    1. Explicit keyword parameters passed to setup_dask_client()
    2. Config object (if provided via ``config=``)  OR  profile (if provided via ``profile=``)
    3. Default values

    ``config`` and ``profile`` are mutually exclusive.

    Args:
        config: A pre-built DaskSetupConfig object to use as the base configuration.
        profile: Profile name to load from disk/builtins as the base configuration.
        workload_type: Workload type override (only applied when it differs from the default "io").
        **kwargs: Additional explicit overrides forwarded from setup_dask_client().

    Returns:
        Resolved DaskSetupConfig
    """
    if config is not None and profile is not None:
        raise ValueError(
            "Cannot specify both 'config' and 'profile'. "
            "Pass a DaskSetupConfig object via 'config=' OR a profile name via 'profile=', not both."
        )

    # Start with defaults
    defaults = DaskSetupConfig()

    # Resolve the base configuration: either a provided config object or a loaded profile
    base_config = None

    if config is not None:
        # Caller supplied a ready-made DaskSetupConfig — use it as the profile-level base
        base_config = config
    elif profile is not None:
        manager = ConfigManager()
        profile_obj = manager.get_profile(profile)
        if profile_obj is None:
            available = list(manager.list_profiles().keys())
            raise ValueError(f"Profile '{profile}' not found. Available profiles: {available}")
        base_config = profile_obj.config

    # Collect explicitly-provided keyword overrides.
    #
    # Note: This heuristic compares each value against its default to decide whether it was
    # explicitly set. Edge case: if you deliberately pass a value that *equals* the default
    # (e.g. reserve_mem_gb=50.0 when the default is 50.0) it will be treated as "not set"
    # and a base_config value will take precedence. To avoid this, use a DaskSetupConfig
    # object via the 'config=' parameter instead.
    explicit_params = {}

    if workload_type != "io":
        explicit_params["workload_type"] = workload_type
    if max_workers is not None:
        explicit_params["max_workers"] = max_workers
    if reserve_mem_gb != 50.0:
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

    explicit_config = DaskSetupConfig(**explicit_params) if explicit_params else None

    # Merge configurations: defaults < base_config < explicit overrides
    final_config = defaults
    if base_config:
        final_config = final_config.merge_with(base_config)
    if explicit_config:
        final_config = final_config.merge_with(explicit_config)

    return final_config


def setup_dask_client(
    workload_type: str = "io",
    max_workers: int | None = None,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
    dashboard: bool = True,
    adaptive: bool = False,
    min_workers: int | None = None,
    profile: str | None = None,
    suggest_chunks: bool = False,
    config: DaskSetupConfig | None = None,
) -> tuple[Client, LocalCluster, str]:
    """Create a single-node Dask LocalCluster tuned for HPC login/compute nodes.

    Routes temp/spill to $PBS_JOBFS when present.

    Parameters
    ----------
    workload_type : {"cpu","io","mixed"}
        Shape worker topology for CPU-bound, I/O-bound, or mixed workloads.
    max_workers : int or None
        Cap on worker processes. Defaults to all logical cores available.
    reserve_mem_gb : float
        Memory to reserve for OS / cache / filesystem (GiB).
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
        Requires xarray and numpy to be installed.
    config : DaskSetupConfig or None
        A pre-built configuration object. When provided, all other parameters
        except explicit overrides are ignored. Mutually exclusive with ``profile``.
        This is the recommended way to avoid the default-value ambiguity that
        occurs with individual keyword parameters.

    Returns
    -------
    client : dask.distributed.Client
    cluster : dask.distributed.LocalCluster
    dask_local_dir : str
        Absolute path to the temp/spill directory (under $PBS_JOBFS if available).

    Raises
    ------
    InvalidConfigurationError
        If workload_type is invalid or parameters are inconsistent.
    InsufficientResourcesError
        If system resources are insufficient for the requested configuration.
    ResourceDetectionError
        If resource detection fails completely.
    """
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
    )

    # Detect system resources
    resources = detect_resources()

    # Create temporary directory for spill files (use config for base dir if specified)
    temp_dir = create_dask_temp_dir(base_dir=config.temp_base_dir)

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
    silence_logs_level = logging.ERROR if config.silence_logs else logging.WARNING

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

    # Print dashboard info if enabled
    if config.dashboard:
        print_dashboard_info(client, silent=config.silence_logs)
        if not config.silence_logs:
            print()  # Add blank line

    # Print summary information
    spill_threads_str = (
        f" | spill_threads={config.spill_threads}" if config.spill_threads is not None else ""
    )
    print(
        f"[setup_dask_client] temp/spill dir: {temp_dir}\\n"
        f"Workers: {topology.n_workers} | threads/worker: {topology.threads_per_worker} | processes: {topology.processes}\\n"
        f"Mem: total ~{memory_spec.total_mem_gib:.1f} GiB | usable ~{memory_spec.usable_mem_gb:.1f} GiB | per-worker ~{memory_spec.mem_per_worker_bytes / (1024**3):.1f} GiB\\n"
        f"Compression: spill={config.spill_compression} | comm={config.comm_compression}{spill_threads_str}"
    )

    # Print xarray chunking suggestions if enabled
    if config.suggest_chunks:
        try:
            # Try to import xarray module to check availability

            print("\n" + "=" * 60)
            print(" Xarray Chunking Recommendations")
            print("=" * 60)
            print(
                "To get optimal chunking suggestions for your xarray datasets:\n"
                "\n"
                "  from dask_setup import recommend_chunks\n"
                "  chunks = recommend_chunks(your_dataset, client, verbose=True)\n"
                "  ds_optimized = your_dataset.chunk(chunks)\n"
                "\n"
                "Or use the standalone function:\n"
                "\n"
                "  chunks = recommend_chunks(ds, workload_type='cpu')  # or 'io', 'mixed'\n"
                "\n"
                f"Based on your current cluster configuration:\n"
                f"• Workload type: {config.workload_type}\n"
                f"• Target chunk size: 256-512 MiB per chunk\n"
                f"• Safety factor: 60% of worker memory ({memory_spec.mem_per_worker_bytes / (1024**3) * 0.6:.1f} GiB max per chunk)\n"
                f"• {topology.n_workers} workers available for parallelization\n"
            )
            print("=" * 60)

        except ImportError:
            print(
                "\n Xarray integration requires xarray and numpy to be installed.\n"
                "Install with: pip install xarray numpy"
            )

    return client, cluster, str(temp_dir)
