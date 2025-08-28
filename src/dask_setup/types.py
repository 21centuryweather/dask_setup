"""Type definitions for the dask_setup package."""

from __future__ import annotations

from typing import NamedTuple


class ResourceSpec(NamedTuple):
    """Resource specification detected from the environment.

    Attributes:
        total_cores: Number of logical CPU cores available
        total_mem_bytes: Total memory available in bytes
        detection_method: How resources were detected ("PBS", "SLURM", "psutil")
    """

    total_cores: int
    total_mem_bytes: int
    detection_method: str


class TopologySpec(NamedTuple):
    """Worker topology specification based on workload type.

    Attributes:
        n_workers: Number of worker processes/threads to create
        threads_per_worker: Number of threads per worker
        processes: Whether to use processes (True) or threads (False)
        workload_type: The workload type that generated this topology
    """

    n_workers: int
    threads_per_worker: int
    processes: bool
    workload_type: str


class MemorySpec(NamedTuple):
    """Memory allocation specification.

    Attributes:
        total_mem_gib: Total memory available in GiB
        usable_mem_gb: Memory available for Dask after reservations in GiB
        mem_per_worker_bytes: Memory limit per worker in bytes
        reserved_mem_gb: Memory reserved for system in GiB
    """

    total_mem_gib: float
    usable_mem_gb: float
    mem_per_worker_bytes: int
    reserved_mem_gb: float
