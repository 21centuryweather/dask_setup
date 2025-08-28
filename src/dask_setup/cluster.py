"""Dask cluster creation and configuration."""

from __future__ import annotations

import logging
from pathlib import Path

import dask
from dask.distributed import LocalCluster

from .types import MemorySpec, TopologySpec


def calculate_memory_spec(
    total_mem_bytes: int,
    n_workers: int,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
) -> MemorySpec:
    """Calculate memory allocation for Dask workers.

    Args:
        total_mem_bytes: Total system memory in bytes
        n_workers: Number of workers that will be created
        reserve_mem_gb: Memory to reserve for system in GiB
        max_mem_gb: Optional cap on total memory usage in GiB

    Returns:
        MemorySpec with calculated memory allocation

    Raises:
        ValueError: If insufficient memory is available
    """
    total_mem_gib = total_mem_bytes / (1024**3)

    # Apply max_mem_gb cap if specified
    effective_total_gb = min(max_mem_gb or total_mem_gib, total_mem_gib)

    # Calculate usable memory after reservation
    usable_mem_gb = max(0.0, effective_total_gb - reserve_mem_gb)

    if usable_mem_gb <= 0:
        raise ValueError(
            f"Not enough memory after reserving {reserve_mem_gb:.1f} GiB from "
            f"{total_mem_gib:.1f} GiB total. Lower reserve_mem_gb or increase available memory."
        )

    # Calculate per-worker memory (minimum 1 GiB per worker)
    mem_per_worker_gb = max(1.0, usable_mem_gb / n_workers)
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

    # Add spill threads configuration if specified
    if spill_threads is not None:
        config_dict["distributed.p2p.threads"] = spill_threads

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

    # Enable adaptive scaling if requested
    if adaptive:
        min_w = min_workers if min_workers is not None else max(1, topology.n_workers // 2)
        cluster.adapt(minimum=min_w, maximum=topology.n_workers, wait_count=2)

    return cluster
