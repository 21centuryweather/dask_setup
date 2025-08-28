"""Main client setup orchestration for dask_setup."""

from __future__ import annotations

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
    """Resolve final configuration from profile and explicit parameters.

    Priority order (highest to lowest):
    1. Explicit parameters passed to setup_dask_client()
    2. Profile configuration (if specified)
    3. Default values

    Args:
        profile: Profile name to load
        **kwargs: Explicit parameters from setup_dask_client()

    Returns:
        Resolved DaskSetupConfig
    """
    # Start with defaults
    defaults = DaskSetupConfig()

    # Load profile if specified
    profile_config = None
    if profile:
        manager = ConfigManager()
        profile_obj = manager.get_profile(profile)
        if profile_obj is None:
            available = list(manager.list_profiles().keys())
            raise ValueError(f"Profile '{profile}' not found. Available profiles: {available}")
        profile_config = profile_obj.config

    # Create explicit config from parameters (only non-default values)
    explicit_params = {}

    # Only include parameters that were explicitly passed (not defaults)
    # We'll use a simple heuristic - if it matches the default, assume it wasn't set
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

    # Merge configurations: defaults < profile < explicit
    final_config = defaults
    if profile_config:
        final_config = final_config.merge_with(profile_config)
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
        explicit parameters.
    suggest_chunks : bool
        If True, print xarray chunking recommendations after cluster setup.
        Requires xarray and numpy to be installed.

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

    cluster = create_cluster(
        topology=topology,
        memory_spec=memory_spec,
        temp_dir=temp_dir,
        dashboard_address=dashboard_address,
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
            print("💡 Xarray Chunking Recommendations")
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
                "\n⚠️  Xarray integration requires xarray and numpy to be installed.\n"
                "Install with: pip install xarray numpy"
            )

    return client, cluster, str(temp_dir)
