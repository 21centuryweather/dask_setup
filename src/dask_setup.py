"""
setup_dask_client() — single-node Dask helper tuned for NCI Gadi.

- Detects cores/RAM from PBS/SLURM env, else psutil.
- Routes temp/spill to $PBS_JOBFS if available (fallback TMPDIR or /tmp).
- Picks processes/threads by workload_type ("cpu", "io", "mixed").
- Sets spill thresholds to avoid OOM.
- Returns (client, cluster, dask_local_dir).
"""

from __future__ import annotations

import logging
import math
import os
import socket
from pathlib import Path

import dask
import psutil
from dask.distributed import Client, LocalCluster


def setup_dask_client(
    workload_type: str = "io",  # "cpu", "io", or "mixed"
    max_workers: int | None = None,
    reserve_mem_gb: float = 50.0,
    max_mem_gb: float | None = None,
    dashboard: bool = True,
    adaptive: bool = False,
    min_workers: int | None = None,
) -> tuple[Client, LocalCluster, str]:
    """
    Create a single-node Dask LocalCluster tuned for HPC login/compute nodes.
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

    Returns
    -------
    client : dask.distributed.Client
    cluster : dask.distributed.LocalCluster
    dask_local_dir : str
        Absolute path to the temp/spill directory (under $PBS_JOBFS if available).
    """
    assert workload_type in {"cpu", "io", "mixed"}, "Invalid workload_type"

    # ---------- Resolve temp/spill directory (prefer PBS job filesystem) ----------
    base_tmp = os.environ.get("PBS_JOBFS") or os.environ.get("TMPDIR") or "/tmp"
    dask_local_dir = Path(base_tmp) / f"dask-{os.getpid()}"
    dask_local_dir.mkdir(parents=True, exist_ok=True)

    # Point Dask + Python temp to this location
    os.environ["TMPDIR"] = str(dask_local_dir)  # many libs honour this
    os.environ["DASK_TEMPORARY_DIRECTORY"] = str(dask_local_dir)
    dask.config.set(
        {
            "temporary-directory": str(dask_local_dir),  # dask.array + general temp
            "distributed.worker.local-directory": str(dask_local_dir),  # spill, worker dirs
            "distributed.worker.memory.target": 0.75,  # spill to disk at 75%
            "distributed.worker.memory.spill": 0.85,  # more aggressive spilling
            "distributed.worker.memory.pause": 0.92,  # pause new tasks
            "distributed.worker.memory.terminate": 0.98,  # last resort
            "distributed.worker.multiprocessing-method": "spawn",
            "array.slicing.split_large_chunks": True,
        }
    )

    # ---------- Detect resources (prefer scheduler env, else psutil) ----------
    slurm_cpus = os.getenv("SLURM_CPUS_ON_NODE")
    slurm_mem_mb = os.getenv("SLURM_MEM_PER_NODE") or os.getenv("SLURM_MEM_PER_CPU")
    pbs_ncpus = os.getenv("NCPUS") or os.getenv("PBS_NCPUS")
    pbs_mem = os.getenv("PBS_VMEM") or os.getenv("PBS_MEM")

    def _parse_mem_bytes(s: str | None) -> int | None:
        if not s:
            return None
        try:
            low = s.lower()
            if low.endswith("gb"):
                return int(low[:-2]) * (1024**3)
            if low.endswith("mb"):
                return int(low[:-2]) * (1024**2)
            return int(s)
        except Exception:
            return None

    if slurm_cpus:
        logical_cores = int(slurm_cpus)
    elif pbs_ncpus and pbs_ncpus.isdigit():
        logical_cores = int(pbs_ncpus)
    else:
        logical_cores = psutil.cpu_count(logical=True)

    if slurm_mem_mb and slurm_mem_mb.isdigit():
        total_mem_bytes = int(slurm_mem_mb) * 1024 * 1024
    else:
        total_mem_bytes = _parse_mem_bytes(pbs_mem) or psutil.virtual_memory().total

    total_mem_gib = total_mem_bytes / (1024**3)

    if max_workers is None:
        max_workers = logical_cores

    max_mem_gb = total_mem_gib if max_mem_gb is None else min(max_mem_gb, total_mem_gib)

    usable_mem_gb = max(0.0, max_mem_gb - max(0.0, reserve_mem_gb))

    # ---------- Topology by workload ----------
    if workload_type == "cpu":
        processes = True
        threads_per_worker = 1
        n_workers = min(max_workers, logical_cores)
    elif workload_type == "io":
        processes = False  # single proc, many threads for I/O
        threads_per_worker = min(16, max(4, math.ceil(logical_cores / 2)))
        n_workers = 1
    else:  # "mixed"
        processes = True
        threads_per_worker = 2
        n_workers = max(1, min(max_workers, logical_cores // threads_per_worker))

    n_workers = max(1, n_workers)

    # Split memory across workers (in bytes)
    mem_per_worker_gb = max(1.0, usable_mem_gb / n_workers)  # ≥ 1 GiB/worker
    mem_per_worker_bytes = int(mem_per_worker_gb * (1024**3))
    if mem_per_worker_bytes <= 0:
        raise ValueError(
            f"Not enough memory after reserving {reserve_mem_gb} GiB. "
            f"Lower reserve_mem_gb or raise max_mem_gb."
        )

    # ---------- Cluster ----------
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=threads_per_worker,
        processes=processes,
        memory_limit=mem_per_worker_bytes,
        dashboard_address=":0" if dashboard else None,  # random open port
        local_directory=str(dask_local_dir),  # primary spill dir
        silence_logs=logging.ERROR,
    )
    client = Client(cluster)

    # Optional single-node adaptive
    if adaptive:
        min_w = min_workers if min_workers is not None else max(1, n_workers // 2)
        cluster.adapt(minimum=min_w, maximum=n_workers, wait_count=2)

    # Helpful summary + tunnel hint
    if dashboard:
        host = socket.gethostname()
        link = client.dashboard_link  # usually http://127.0.0.1:<port>/status
        port = link.rsplit(":", 1)[-1].split("/")[0] if ":" in link else "8787"
        print(
            f"Dask dashboard: {link}\n"
            f"Tunnel from your laptop (run locally):\n"
            f"  ssh -N -L 8787:{host}:{port} gadi.nci.org.au\n"
            f"Then open: http://localhost:8787\n"
        )

    print(
        f"[setup_dask_client] temp/spill dir: {dask_local_dir}\n"
        f"Workers: {n_workers} | threads/worker: {threads_per_worker} | processes: {processes}\n"
        f"Mem: total ~{total_mem_gib:.1f} GiB | usable ~{usable_mem_gb:.1f} GiB | per-worker ~{mem_per_worker_gb:.1f} GiB"
    )

    return client, cluster, str(dask_local_dir)
