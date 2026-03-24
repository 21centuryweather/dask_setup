"""Worker topology decision logic for dask_setup."""

from __future__ import annotations

import math

from .exceptions import InvalidConfigurationError
from .logging import get_logger
from .types import TopologySpec

logger = get_logger("topology")


def _count_gpus() -> int:
    """Return the number of CUDA-capable GPUs visible to this process.

    Checks (in order):
    1. ``CUDA_VISIBLE_DEVICES`` environment variable
    2. ``cupy.cuda.runtime.getDeviceCount()`` (if CuPy is installed)
    3. Falls back to 0 if neither source is available.
    """
    import os

    cvd = os.getenv("CUDA_VISIBLE_DEVICES", "")
    if cvd and cvd != "-1":
        # "0,1,2" or "NoDevFiles" etc.
        ids = [x.strip() for x in cvd.split(",") if x.strip().isdigit()]
        if ids:
            return len(ids)

    try:
        import cupy  # type: ignore[import-untyped]

        return cupy.cuda.runtime.getDeviceCount()
    except Exception:
        pass

    return 0


def decide_topology(
    workload_type: str, total_cores: int, max_workers: int | None = None
) -> TopologySpec:
    """Decide worker topology based on workload type and available resources.

    Workload type determines the process/thread configuration:
    - "cpu": Many processes with 1 thread each (good for NumPy/compute)
    - "io": Single process with many threads (good for I/O operations)
    - "mixed": Balanced approach with multiple processes and threads
    - "gpu": One process per GPU, multiple CPU threads per worker (CuPy/RAPIDS)

    Args:
        workload_type: Type of workload ("cpu", "io", "mixed", "gpu")
        total_cores: Total logical CPU cores available
        max_workers: Optional limit on number of workers

    Returns:
        TopologySpec with worker configuration

    Raises:
        InvalidConfigurationError: If workload_type is invalid or parameters are inconsistent
    """
    # Validate workload type
    # "auto" should be resolved to a concrete type before calling decide_topology
    if workload_type == "auto":
        raise InvalidConfigurationError(
            "workload_type='auto' must be resolved to 'cpu', 'io', 'mixed', or 'gpu' before "
            "decide_topology() is called. Pass a dataset via ds= to setup_dask_client(), "
            "or call infer_workload_type(ds) yourself and pass the result explicitly."
        )
    if workload_type not in {"cpu", "io", "mixed", "gpu"}:
        raise InvalidConfigurationError(
            f"workload_type must be 'cpu', 'io', 'mixed', or 'gpu', got '{workload_type}'"
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

    elif workload_type == "gpu":
        # GPU workload: one worker process per GPU, several CPU threads each.
        # CuPy / RAPIDS operations are single-threaded on the GPU but use CPU
        # threads for data loading and preprocessing.
        n_gpus = _count_gpus()
        if n_gpus == 0:
            logger.warning(
                "workload_type='gpu' requested but no CUDA-capable GPUs detected. "
                "Falling back to a single CPU worker. Install CuPy or set "
                "CUDA_VISIBLE_DEVICES to specify GPUs."
            )
            # Graceful fallback: single-process multi-thread (like "io")
            processes = False
            n_workers = 1
            threads_per_worker = min(16, max(4, math.ceil(total_cores / 2)))
        else:
            # One process per GPU; allocate CPU threads evenly across GPUs
            n_workers_requested = n_gpus if max_workers is None else min(max_workers, n_gpus)
            n_workers = max(1, n_workers_requested)
            processes = True
            # Divide available cores across GPU workers (min 2, max 8)
            threads_per_worker = max(2, min(8, total_cores // n_workers))
        logger.debug("GPU topology", n_gpus=n_gpus, n_workers=n_workers)

    else:  # workload_type == "mixed"
        # Mixed workload: moderate number of processes with 2 threads each
        # Balanced approach for compute + I/O pipelines
        processes = True
        threads_per_worker = 2
        n_workers = max(1, min(max_workers, total_cores // threads_per_worker))

    # Ensure we have at least 1 worker
    n_workers = max(1, n_workers)

    topology = TopologySpec(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
        workload_type=workload_type,
    )
    logger.debug(
        "Topology decided",
        workload_type=workload_type,
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
    )
    return topology


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
    # GPU topology legitimately uses processes=True with multiple threads per worker
    is_gpu = topology.workload_type == "gpu"
    if topology.processes and topology.threads_per_worker > 4 and not is_gpu:
        logger.warning(
            "High threads_per_worker with multiple processes — consider workload_type='io'",
            threads_per_worker=topology.threads_per_worker,
            n_workers=topology.n_workers,
        )

    if not topology.processes and topology.n_workers > 1:
        # Multiple workers without processes doesn't make sense
        raise InvalidConfigurationError(
            "Cannot have multiple workers (n_workers > 1) when processes=False"
        )
