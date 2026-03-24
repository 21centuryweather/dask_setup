"""Dynamic memory threshold tuning based on observed spill behaviour.

After a cluster has been running for a while, :func:`tune_memory_thresholds`
reads the current spill volume from the scheduler and nudges the Dask worker
memory thresholds in the appropriate direction:

- **Low / no spilling**: tighten ``memory.target`` (and ``memory.spill``)
  downward so workers keep more headroom and the GC has less work to do.
- **Heavy spilling**: loosen the thresholds upward so workers buffer more
  data in RAM before spilling, reducing disk I/O.

The function is a one-shot adjustment — call it after an initial batch of
tasks to adapt the cluster to your specific workload.  It is also called
automatically by :func:`~dask_setup.client.setup_dask_client` when
``DaskSetupConfig(adaptive_memory=True)`` is set.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .logging import get_logger

if TYPE_CHECKING:
    from dask.distributed import Client

logger = get_logger("tune")

# Safe bounds for memory thresholds — never go outside these ranges
_MIN_TARGET: float = 0.40
_MAX_TARGET: float = 0.90
_MIN_SPILL: float = 0.50
_MAX_SPILL: float = 0.95


@dataclass
class MemoryTuneResult:
    """Result from a memory threshold tuning pass.

    Attributes
    ----------
    strategy : str
        Effective strategy used: ``"tighten"``, ``"loosen"``, or ``"off"``.
    old_target : float
        ``memory.target`` value before adjustment (fraction, 0–1).
    new_target : float
        ``memory.target`` value after adjustment.
    old_spill : float
        ``memory.spill`` value before adjustment.
    new_spill : float
        ``memory.spill`` value after adjustment.
    spill_gib_observed : float
        Total spill volume observed across all workers at tune time (GiB).
    rationale : str
        Human-readable explanation of the decision.
    workers_updated : int
        Number of workers whose thresholds were successfully updated.
        ``0`` if no change was needed or if the update failed.
    """

    strategy: str
    old_target: float
    new_target: float
    old_spill: float
    new_spill: float
    spill_gib_observed: float
    rationale: str
    workers_updated: int

    def summary(self) -> str:
        """One-line human-readable summary of the tuning result.

        Examples
        --------
        ``"Memory thresholds adjusted on 4 worker(s): target 75%→70%, spill 85%→80% (low spill...)"``
        ``"No changes made (strategy='off'; no changes made)"``
        """
        if self.workers_updated == 0:
            return f"No changes made ({self.rationale})"
        return (
            f"Memory thresholds adjusted on {self.workers_updated} worker(s): "
            f"target {self.old_target:.0%}→{self.new_target:.0%}, "
            f"spill {self.old_spill:.0%}→{self.new_spill:.0%} "
            f"({self.rationale})"
        )


def tune_memory_thresholds(
    client: Client,
    strategy: str = "auto",
    spill_threshold_gib: float = 0.5,
    tighten_by: float = 0.05,
    loosen_by: float = 0.05,
) -> MemoryTuneResult:
    """Adjust Dask worker memory thresholds based on observed spill behaviour.

    Reads current spill metrics from the scheduler, then applies threshold
    adjustments on every worker:

    - **No / low spilling** (spill < *spill_threshold_gib*): tighten
      ``memory.target`` and ``memory.spill`` downward by *tighten_by* so
      workers retain more head-room before the GC kicks in.
    - **Heavy spilling** (spill ≥ *spill_threshold_gib*): loosen the same
      thresholds upward by *loosen_by* so workers buffer more data in RAM
      before reaching the spill threshold, reducing disk write amplification.

    Adjustments are clamped to safe ranges: target ∈ [0.40, 0.90],
    spill ∈ [0.50, 0.95].  If the current value is already at the boundary
    no change is applied.

    Parameters
    ----------
    client : dask.distributed.Client
        A connected Dask client. The scheduler must still be reachable.
    strategy : {"auto", "tighten", "loosen", "off"}
        ``"auto"`` (default) reads spill stats and chooses the direction.
        ``"tighten"`` / ``"loosen"`` force the direction regardless.
        ``"off"`` returns immediately without making any changes.
    spill_threshold_gib : float
        Spill volume (GiB) above which ``"auto"`` mode switches to the
        loosen direction. Default: 0.5 GiB.
    tighten_by : float
        Fraction to reduce thresholds by when tightening (default 0.05 = 5 pp).
    loosen_by : float
        Fraction to increase thresholds by when loosening (default 0.05 = 5 pp).

    Returns
    -------
    MemoryTuneResult
        Snapshot of what changed and why. Call ``.summary()`` for a
        one-liner suitable for logging.

    Examples
    --------
    ::

        from dask_setup import setup_dask_client, tune_memory_thresholds

        client, cluster, tmp = setup_dask_client()

        # Run a sample batch of tasks first
        client.compute(ds.isel(time=slice(0, 10)).mean()).result()

        # Now tune based on what was observed
        result = tune_memory_thresholds(client)
        print(result.summary())
    """
    _VALID = {"auto", "tighten", "loosen", "off"}
    if strategy not in _VALID:
        raise ValueError(f"strategy must be one of {_VALID}, got {strategy!r}")

    if strategy == "off":
        return MemoryTuneResult(
            strategy="off",
            old_target=0.0,
            new_target=0.0,
            old_spill=0.0,
            new_spill=0.0,
            spill_gib_observed=0.0,
            rationale="strategy='off'; no changes made",
            workers_updated=0,
        )

    # ------------------------------------------------------------------
    # 1. Read current thresholds from a representative worker
    # ------------------------------------------------------------------
    old_target: float = 0.75
    old_spill: float = 0.85

    try:
        worker_thresholds: dict = client.run(
            lambda: {
                "target": __import__("dask").config.get("distributed.worker.memory.target", 0.75),
                "spill": __import__("dask").config.get("distributed.worker.memory.spill", 0.85),
            }
        )
        if worker_thresholds:
            sample = next(iter(worker_thresholds.values()))
            old_target = float(sample.get("target", 0.75))
            old_spill = float(sample.get("spill", 0.85))
    except Exception as e:
        logger.debug("Could not read current memory thresholds from workers", error=str(e))

    # ------------------------------------------------------------------
    # 2. Collect spill volume from the scheduler
    # ------------------------------------------------------------------
    spill_gib: float = 0.0

    try:
        info = client.scheduler_info()
        total_spill_bytes = 0
        for worker_info in info.get("workers", {}).values():
            metrics = worker_info.get("metrics", {})
            spill_val = metrics.get("spilled_memory") or metrics.get("spill")
            if isinstance(spill_val, dict):
                total_spill_bytes += spill_val.get("disk", 0) or 0
            elif isinstance(spill_val, (int, float)):
                total_spill_bytes += int(spill_val)
        spill_gib = total_spill_bytes / (1024**3)
    except Exception as e:
        logger.debug("Could not read spill stats from scheduler", error=str(e))

    # ------------------------------------------------------------------
    # 3. Decide direction
    # ------------------------------------------------------------------
    if strategy == "auto":
        effective = "loosen" if spill_gib >= spill_threshold_gib else "tighten"
        logger.debug(
            "Auto-strategy resolved",
            spill_gib=f"{spill_gib:.3f}",
            threshold_gib=spill_threshold_gib,
            chosen=effective,
        )
    else:
        effective = strategy

    # ------------------------------------------------------------------
    # 4. Compute new thresholds (clamped)
    # ------------------------------------------------------------------
    if effective == "tighten":
        new_target = max(_MIN_TARGET, old_target - tighten_by)
        new_spill = max(_MIN_SPILL, old_spill - tighten_by)
        rationale = (
            f"low spill ({spill_gib:.2f} GiB < {spill_threshold_gib:.2f} GiB threshold); "
            "tightened thresholds for extra head-room"
        )
    else:  # loosen
        new_target = min(_MAX_TARGET, old_target + loosen_by)
        new_spill = min(_MAX_SPILL, old_spill + loosen_by)
        rationale = (
            f"high spill ({spill_gib:.2f} GiB ≥ {spill_threshold_gib:.2f} GiB threshold); "
            "loosened thresholds to reduce disk write amplification"
        )

    # ------------------------------------------------------------------
    # 5. Apply on all workers (only if values actually changed)
    # ------------------------------------------------------------------
    workers_updated = 0

    if abs(new_target - old_target) > 1e-6 or abs(new_spill - old_spill) > 1e-6:
        # Capture in locals so the lambda closure captures the right values
        _new_target = new_target
        _new_spill = new_spill

        try:

            def _apply(new_target=_new_target, new_spill=_new_spill):
                import dask

                dask.config.set(
                    {
                        "distributed.worker.memory.target": new_target,
                        "distributed.worker.memory.spill": new_spill,
                    }
                )

            client.run(_apply)
            workers_updated = len(client.scheduler_info().get("workers", {}))
            logger.info(
                "Worker memory thresholds updated",
                old_target=f"{old_target:.0%}",
                new_target=f"{new_target:.0%}",
                old_spill=f"{old_spill:.0%}",
                new_spill=f"{new_spill:.0%}",
                workers=workers_updated,
            )
        except Exception as e:
            logger.warning("Failed to apply memory thresholds to workers", error=str(e))
            rationale += f" [apply failed: {e}]"
    else:
        rationale = "thresholds already at boundary; no change applied"

    return MemoryTuneResult(
        strategy=effective,
        old_target=old_target,
        new_target=new_target,
        old_spill=old_spill,
        new_spill=new_spill,
        spill_gib_observed=spill_gib,
        rationale=rationale,
        workers_updated=workers_updated,
    )
