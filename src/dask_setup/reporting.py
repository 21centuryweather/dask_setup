"""Post-run cluster reporting and statistics.

Provides :class:`ClusterReport` (a snapshot of cluster metrics at close time)
and :func:`cluster_report` (the function that collects those metrics from a
live Dask client).

Typical usage — standalone::

    from dask_setup import setup_dask_client, cluster_report

    client, cluster, tmp = setup_dask_client()
    # ... do work ...
    report = cluster_report(client)
    print(report.summary_line())
    client.close(); cluster.close()

The :class:`~dask_setup.client.DaskClientContext` context manager records the
start time automatically and logs the summary line when it exits.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from .logging import get_logger

if TYPE_CHECKING:
    from dask.distributed import Client

logger = get_logger("reporting")


@dataclass
class ClusterReport:
    """Summary statistics collected from a Dask cluster at close time.

    Attributes
    ----------
    wall_time_seconds : float
        Elapsed wall-clock time since the cluster was started (or since the
        start_time passed to :func:`cluster_report`).  ``0.0`` if no start
        time was provided.
    memory_per_worker_gib : dict[str, float]
        Managed memory (GiB) per worker address, sampled at report time.
        Keys are worker addresses (e.g. ``"tcp://127.0.0.1:12345"``).
    total_spill_gib : float
        Total bytes written to disk spill storage across all workers (GiB).
        ``0.0`` if spill metrics are unavailable in this Dask version.
    total_tasks : int
        Number of tasks present in the scheduler graph at report time.
        This is a snapshot — it is not an all-time cumulative count.
    """

    wall_time_seconds: float = 0.0
    memory_per_worker_gib: dict[str, float] = field(default_factory=dict)
    total_spill_gib: float = 0.0
    total_tasks: int = 0

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def wall_time_str(self) -> str:
        """Human-readable elapsed wall time (e.g. ``"2h 5m 30s"``)."""
        seconds = self.wall_time_seconds
        hours, rem = divmod(int(seconds), 3600)
        minutes, secs = divmod(rem, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{seconds:.1f}s"

    @property
    def peak_memory_gib(self) -> float:
        """Maximum memory across all workers (GiB), or ``0.0`` if unknown."""
        if not self.memory_per_worker_gib:
            return 0.0
        return max(self.memory_per_worker_gib.values())

    @property
    def total_memory_gib(self) -> float:
        """Sum of managed memory across all workers (GiB)."""
        return sum(self.memory_per_worker_gib.values())

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------

    def summary_line(self) -> str:
        """One-line summary suitable for logging or printing.

        Example output::

            wall=1m 23s | workers=4 | mem=3.21 GiB/max | spill=0.00 GiB | tasks=1842
        """
        parts: list[str] = []

        if self.wall_time_seconds:
            parts.append(f"wall={self.wall_time_str}")

        n_workers = len(self.memory_per_worker_gib)
        if n_workers:
            parts.append(f"workers={n_workers}")
            parts.append(f"mem={self.peak_memory_gib:.2f} GiB/max")

        if self.total_spill_gib > 0.0:
            parts.append(f"spill={self.total_spill_gib:.2f} GiB")

        if self.total_tasks:
            parts.append(f"tasks={self.total_tasks}")

        return " | ".join(parts) if parts else "no metrics collected"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dictionary of all report fields.

        Returns
        -------
        dict
            All fields including derived properties (``wall_time_str``,
            ``peak_memory_gib``, ``total_memory_gib``).
        """
        return {
            "wall_time_seconds": self.wall_time_seconds,
            "wall_time_str": self.wall_time_str,
            "memory_per_worker_gib": dict(self.memory_per_worker_gib),
            "peak_memory_gib": self.peak_memory_gib,
            "total_memory_gib": self.total_memory_gib,
            "total_spill_gib": self.total_spill_gib,
            "total_tasks": self.total_tasks,
        }


# ---------------------------------------------------------------------------
# Public collection function
# ---------------------------------------------------------------------------


def cluster_report(
    client: "Client",
    start_time: float | None = None,
) -> ClusterReport:
    """Collect post-run statistics from a connected Dask client.

    Queries the distributed scheduler for per-worker memory, spill volume,
    and task counts.  All metric collection is done defensively — any field
    that cannot be retrieved (e.g. because this Dask version does not expose
    the metric) is left at its zero/empty default rather than raising.

    Parameters
    ----------
    client : dask.distributed.Client
        A connected Dask client.  The scheduler must still be reachable; call
        this function *before* ``client.close()``.
    start_time : float or None
        Wall-clock start time as returned by :func:`time.monotonic`.  When
        provided, ``ClusterReport.wall_time_seconds`` is computed as
        ``time.monotonic() - start_time``.  Pass ``None`` (the default) to
        leave ``wall_time_seconds`` at ``0.0``.

    Returns
    -------
    ClusterReport
        Snapshot of cluster metrics at the time of the call.

    Examples
    --------
    ::

        import time
        from dask_setup import setup_dask_client, cluster_report

        t0 = time.monotonic()
        client, cluster, tmp = setup_dask_client()
        # ... do work ...
        report = cluster_report(client, start_time=t0)
        print(report.summary_line())
    """
    report = ClusterReport()

    # Wall time
    if start_time is not None:
        report.wall_time_seconds = time.monotonic() - start_time

    # --- Scheduler info ------------------------------------------------------
    try:
        info = client.scheduler_info()
        workers: dict[str, Any] = info.get("workers", {})

        # Per-worker memory and spill
        total_spill_bytes = 0
        for worker_addr, worker_info in workers.items():
            metrics: dict[str, Any] = worker_info.get("metrics", {})

            # Memory: prefer managed_bytes (newer Dask), fall back to "memory"
            mem_bytes = (
                metrics.get("managed_bytes")
                or metrics.get("managed")
                or metrics.get("memory")
                or 0
            )
            if mem_bytes:
                report.memory_per_worker_gib[worker_addr] = mem_bytes / (1024**3)

            # Spill: try several key names used across Dask versions
            spill_val = metrics.get("spilled_memory") or metrics.get("spill")
            if isinstance(spill_val, dict):
                # Newer format: {"disk": bytes, "memory": bytes}
                total_spill_bytes += spill_val.get("disk", 0) or 0
            elif isinstance(spill_val, (int, float)):
                total_spill_bytes += int(spill_val)

        report.total_spill_gib = total_spill_bytes / (1024**3)

    except Exception as e:
        logger.debug("Could not collect worker metrics from scheduler_info", error=str(e))

    # --- Task count ----------------------------------------------------------
    try:
        # run_on_scheduler gives direct access to the scheduler object
        task_count = client.run_on_scheduler(lambda dask_scheduler: len(dask_scheduler.tasks))
        report.total_tasks = task_count
    except Exception as e:
        logger.debug("Could not collect task count from scheduler", error=str(e))

    logger.debug(
        "Cluster report collected",
        wall_time_s=f"{report.wall_time_seconds:.1f}",
        workers=len(report.memory_per_worker_gib),
        peak_mem_gib=f"{report.peak_memory_gib:.2f}",
        spill_gib=f"{report.total_spill_gib:.2f}",
        tasks=report.total_tasks,
    )

    return report
