"""Worker topology decision logic for dask_setup."""

from __future__ import annotations

import math

from .exceptions import InvalidConfigurationError
from .types import TopologySpec


def decide_topology(
    workload_type: str, total_cores: int, max_workers: int | None = None
) -> TopologySpec:
    """Decide worker topology based on workload type and available resources.

    Workload type determines the process/thread configuration:
    - "cpu": Many processes with 1 thread each (good for NumPy/compute)
    - "io": Single process with many threads (good for I/O operations)
    - "mixed": Balanced approach with multiple processes and threads

    Args:
        workload_type: Type of workload ("cpu", "io", "mixed")
        total_cores: Total logical CPU cores available
        max_workers: Optional limit on number of workers

    Returns:
        TopologySpec with worker configuration

    Raises:
        InvalidConfigurationError: If workload_type is invalid or parameters are inconsistent
    """
    # Validate workload type
    if workload_type not in {"cpu", "io", "mixed"}:
        raise InvalidConfigurationError(
            f"workload_type must be 'cpu', 'io', or 'mixed', got '{workload_type}'"
        )

    # Validate total_cores
    if total_cores <= 0:
        raise InvalidConfigurationError(f"total_cores must be positive, got {total_cores}")

    # Default max_workers to total_cores if not specified
    if max_workers is None:
        max_workers = total_cores
    elif max_workers <= 0:
        raise InvalidConfigurationError(f"max_workers must be positive, got {max_workers}")

    # Decide topology based on workload type
    if workload_type == "cpu":
        # CPU-bound: many processes, 1 thread each
        # Good for NumPy operations, avoids GIL
        processes = True
        threads_per_worker = 1
        n_workers = min(max_workers, total_cores)

    elif workload_type == "io":
        # I/O-bound: single process, many threads
        # Good for file I/O, network operations
        processes = False
        n_workers = 1
        # Choose thread count: 8-16 threads, but clamped by available cores
        threads_per_worker = min(16, max(4, math.ceil(total_cores / 2)))

    else:  # workload_type == "mixed"
        # Mixed workload: moderate number of processes with 2 threads each
        # Balanced approach for compute + I/O pipelines
        processes = True
        threads_per_worker = 2
        n_workers = max(1, min(max_workers, total_cores // threads_per_worker))

    # Ensure we have at least 1 worker
    n_workers = max(1, n_workers)

    return TopologySpec(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
        workload_type=workload_type,
    )


def validate_topology(topology: TopologySpec, total_cores: int) -> None:
    """Validate that topology configuration is reasonable.

    Args:
        topology: Topology specification to validate
        total_cores: Total logical CPU cores available

    Raises:
        InvalidConfigurationError: If topology is invalid or unreasonable
    """
    if topology.n_workers <= 0:
        raise InvalidConfigurationError(f"n_workers must be positive, got {topology.n_workers}")

    if topology.threads_per_worker <= 0:
        raise InvalidConfigurationError(
            f"threads_per_worker must be positive, got {topology.threads_per_worker}"
        )

    # Check if we're oversubscribing CPU cores significantly
    total_threads = topology.n_workers * topology.threads_per_worker
    if total_threads > total_cores * 2:
        raise InvalidConfigurationError(
            f"Topology requests {total_threads} total threads but only {total_cores} "
            f"cores available. This may cause severe oversubscription."
        )

    # Warn about configurations that might be suboptimal
    # (These are warnings, not errors, so we don't raise exceptions)
    if topology.processes and topology.threads_per_worker > 4:
        # Many threads per process can reduce benefits of multiprocessing
        pass

    if not topology.processes and topology.n_workers > 1:
        # Multiple workers without processes doesn't make sense
        raise InvalidConfigurationError(
            "Cannot have multiple workers (n_workers > 1) when processes=False"
        )
