"""Dask cluster creation and configuration."""

from __future__ import annotations

import logging
from pathlib import Path

import dask
from dask.distributed import LocalCluster

from .logging import get_logger
from .types import MemorySpec, TopologySpec

logger = get_logger("cluster")


#: Smallest per-worker memory budget worth creating a worker for.  A Dask
#: worker with a few hundred MiB spends its life spilling and is slower than
#: not having it at all.
MIN_MEM_PER_WORKER_GB: float = 1.0


def compute_usable_mem_gb(
    total_mem_bytes: int,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
) -> float:
    """Return the memory (GiB) available to Dask after reservations and caps.

    Args:
        total_mem_bytes: Total system memory in bytes
        reserve_mem_gb: Memory to reserve for the OS / page cache in GiB
        max_mem_gb: Optional cap on total memory usage in GiB

    Returns:
        Usable memory in GiB.

    Raises:
        ValueError: If nothing is left after the reservation.
    """
    total_mem_gib = total_mem_bytes / (1024**3)
    effective_total_gb = min(max_mem_gb or total_mem_gib, total_mem_gib)
    usable_mem_gb = max(0.0, effective_total_gb - reserve_mem_gb)

    if usable_mem_gb <= 0:
        raise ValueError(
            f"Not enough memory after reserving {reserve_mem_gb:.1f} GiB from "
            f"{total_mem_gib:.1f} GiB total. Lower reserve_mem_gb or increase available memory."
        )

    return usable_mem_gb


def fit_workers_to_memory(
    usable_mem_gb: float,
    n_workers: int,
    min_mem_per_worker_gb: float = MIN_MEM_PER_WORKER_GB,
) -> int:
    """Return the largest worker count that fits in *usable_mem_gb*.

    Splitting memory across more workers than it can feed does not create
    capacity — it just hands every worker a budget too small to work with, and
    since Dask's ``memory_limit`` is per-worker, raising each one back up to a
    usable floor commits more memory than the node has.  Reducing the worker
    count is the only way to keep the reservation meaningful.

    Args:
        usable_mem_gb: Memory available to Dask, from :func:`compute_usable_mem_gb`.
        n_workers: Worker count the topology asked for.
        min_mem_per_worker_gb: Smallest acceptable per-worker budget.

    Returns:
        A worker count in ``[1, n_workers]``.  Never returns 0: one
        under-provisioned worker still beats a cluster with none, and
        :func:`compute_usable_mem_gb` has already rejected the truly
        hopeless cases.
    """
    if n_workers <= 0:
        return 1
    max_fit = int(usable_mem_gb // min_mem_per_worker_gb)
    return max(1, min(n_workers, max_fit))


def calculate_memory_spec(
    total_mem_bytes: int,
    n_workers: int,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
) -> MemorySpec:
    """Calculate memory allocation for Dask workers.

    Args:
        total_mem_bytes: Total system memory in bytes
        n_workers: Number of workers that will be created.  Pass a count that
            already fits the available memory — see :func:`fit_workers_to_memory`.
        reserve_mem_gb: Memory to reserve for system in GiB
        max_mem_gb: Optional cap on total memory usage in GiB

    Returns:
        MemorySpec with calculated memory allocation

    Raises:
        ValueError: If insufficient memory is available

    Note:
        If *n_workers* is too high for the available memory, the per-worker
        budget is raised to :data:`MIN_MEM_PER_WORKER_GB` and a warning is
        logged — the resulting cluster commits more memory than the node has.
        Callers should clamp the worker count with :func:`fit_workers_to_memory`
        first; :func:`~dask_setup.client.setup_dask_client` does.
    """
    total_mem_gib = total_mem_bytes / (1024**3)
    usable_mem_gb = compute_usable_mem_gb(total_mem_bytes, reserve_mem_gb, max_mem_gb)

    mem_per_worker_gb = usable_mem_gb / n_workers
    if mem_per_worker_gb < MIN_MEM_PER_WORKER_GB:
        # Raising the floor here over-commits: n_workers * MIN > usable.
        # Say so rather than letting the reservation quietly evaporate.
        mem_per_worker_gb = MIN_MEM_PER_WORKER_GB
        logger.warning(
            "Per-worker memory raised to the minimum, which over-commits this node",
            n_workers=n_workers,
            usable_mem_gib=f"{usable_mem_gb:.1f}",
            min_per_worker_gib=f"{MIN_MEM_PER_WORKER_GB:.1f}",
            committed_gib=f"{n_workers * MIN_MEM_PER_WORKER_GB:.1f}",
            hint="reduce the worker count with fit_workers_to_memory()",
        )
    mem_per_worker_bytes = int(mem_per_worker_gb * (1024**3))

    return MemorySpec(
        total_mem_gib=total_mem_gib,
        usable_mem_gb=usable_mem_gb,
        mem_per_worker_bytes=mem_per_worker_bytes,
        reserved_mem_gb=reserve_mem_gb,
    )


def configure_dask_settings(
    temp_dir: Path,
    memory_target: float = 0.75,
    memory_spill: float = 0.85,
    memory_pause: float = 0.92,
    memory_terminate: float = 0.98,
    spill_compression: str = "auto",
    comm_compression: bool = False,
    spill_threads: int | None = None,
) -> None:
    """Configure Dask global settings for optimal HPC performance.

    Args:
        temp_dir: Temporary directory for spill files
        memory_target: Memory target threshold for spilling (0.0-1.0)
        memory_spill: Memory spill threshold for aggressive spilling (0.0-1.0)
        memory_pause: Memory pause threshold for pausing new tasks (0.0-1.0)
        memory_terminate: Memory terminate threshold for killing workers (0.0-1.0)
        spill_compression: Compression algorithm for spill files ('auto', 'lz4', 'zstd', etc.)
        comm_compression: Whether to enable network communication compression
        spill_threads: Number of threads for parallel spill I/O operations (None for default)
    """
    temp_dir_str = str(temp_dir)

    config_dict = {
        # Temporary file locations
        "temporary-directory": temp_dir_str,
        "distributed.worker.local-directory": temp_dir_str,
        # Memory management thresholds (configurable)
        "distributed.worker.memory.target": memory_target,
        "distributed.worker.memory.spill": memory_spill,
        "distributed.worker.memory.pause": memory_pause,
        "distributed.worker.memory.terminate": memory_terminate,
        # Compression settings
        "distributed.worker.memory.spill-compression": spill_compression,
        "distributed.comm.compression": comm_compression,
        # Process spawning (more reliable on HPC systems)
        "distributed.worker.multiprocessing-method": "spawn",
        # Array optimization
        "array.slicing.split_large_chunks": True,
    }

    # Add spill threads configuration if specified.
    # "distributed.p2p.threads" controls the number of threads used for
    # peer-to-peer operations and spill I/O operations.
    if spill_threads is not None:
        config_dict["distributed.p2p.threads"] = spill_threads

    logger.debug(
        "Dask global settings applied",
        temp_dir=temp_dir_str,
        memory_target=memory_target,
        memory_spill=memory_spill,
        spill_compression=spill_compression,
        spill_threads=spill_threads,
    )
    dask.config.set(config_dict)


def create_cluster(
    topology: TopologySpec,
    memory_spec: MemorySpec,
    temp_dir: Path,
    dashboard_address: str | None = ":0",
    silence_logs: int = logging.ERROR,
    adaptive: bool = False,
    min_workers: int | None = None,
    memory_target: float = 0.75,
    memory_spill: float = 0.85,
    memory_pause: float = 0.92,
    memory_terminate: float = 0.98,
    spill_compression: str = "auto",
    comm_compression: bool = False,
    spill_threads: int | None = None,
) -> LocalCluster:
    """Create and configure a Dask LocalCluster.

    Args:
        topology: Worker topology specification
        memory_spec: Memory allocation specification
        temp_dir: Temporary directory for worker files
        dashboard_address: Dashboard bind address (None to disable)
        silence_logs: Log level to suppress worker output
        adaptive: Whether to enable adaptive scaling
        min_workers: Minimum workers for adaptive scaling
        memory_target: Memory target threshold for spilling (0.0-1.0)
        memory_spill: Memory spill threshold for aggressive spilling (0.0-1.0)
        memory_pause: Memory pause threshold for pausing new tasks (0.0-1.0)
        memory_terminate: Memory terminate threshold for killing workers (0.0-1.0)
        spill_compression: Compression algorithm for spill files ('auto', 'lz4', 'zstd', etc.)
        comm_compression: Whether to enable network communication compression
        spill_threads: Number of threads for parallel spill I/O operations (None for default)

    Returns:
        Configured LocalCluster instance
    """
    # Configure global Dask settings with compression
    configure_dask_settings(
        temp_dir=temp_dir,
        memory_target=memory_target,
        memory_spill=memory_spill,
        memory_pause=memory_pause,
        memory_terminate=memory_terminate,
        spill_compression=spill_compression,
        comm_compression=comm_compression,
        spill_threads=spill_threads,
    )

    # Create the cluster
    cluster = LocalCluster(
        n_workers=topology.n_workers,
        threads_per_worker=topology.threads_per_worker,
        processes=topology.processes,
        memory_limit=memory_spec.mem_per_worker_bytes,
        dashboard_address=dashboard_address,
        local_directory=str(temp_dir),
        silence_logs=silence_logs,
    )

    logger.debug(
        "LocalCluster created",
        n_workers=topology.n_workers,
        threads_per_worker=topology.threads_per_worker,
        mem_per_worker_gib=f"{memory_spec.mem_per_worker_bytes / (1024**3):.1f}",
    )

    # Enable adaptive scaling if requested
    if adaptive:
        min_w = min_workers if min_workers is not None else max(1, topology.n_workers // 2)
        cluster.adapt(minimum=min_w, maximum=topology.n_workers, wait_count=2)
        logger.debug("Adaptive scaling enabled", minimum=min_w, maximum=topology.n_workers)

    return cluster
