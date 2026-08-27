"""Unit tests for dask_setup.tune memory threshold tuning."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dask_setup.tune import _apply_thresholds, tune_memory_thresholds


class FakeSpillBuffer:
    """Stand-in for distributed's SpillBuffer (a zict.Buffer subclass).

    Only models what the tuner touches: the eviction threshold ``n`` and the
    ``evict_until_below_target`` hook.
    """

    def __init__(self, n: int) -> None:
        self.n = n
        self.evictions = 0

    def evict_until_below_target(self) -> None:
        self.evictions += 1


class FakeMemoryManager:
    """Stand-in for distributed.worker_memory.WorkerMemoryManager."""

    def __init__(self, memory_limit: int = 8 * 1024**3, target: float = 0.75, spill: float = 0.85):
        self.memory_limit = memory_limit
        self.memory_target_fraction = target
        self.memory_spill_fraction = spill
        self.data = FakeSpillBuffer(n=int(memory_limit * target))


class FakeWorker:
    def __init__(self, manager: FakeMemoryManager | None = None) -> None:
        if manager is not None:
            self.memory_manager = manager


class TestApplyThresholds:
    """The worker-side apply must change the live manager, not just dask.config.

    Regression guard: the tuner used to call ``dask.config.set`` inside
    ``client.run`` and report success. ``WorkerMemoryManager.__init__`` reads
    those keys exactly once at construction, so a running worker never saw the
    change — the whole function was a no-op that logged a threshold update.
    """

    @pytest.mark.unit
    def test_updates_live_manager_fractions(self):
        manager = FakeMemoryManager(target=0.75, spill=0.85)

        assert _apply_thresholds(FakeWorker(manager), new_target=0.60, new_spill=0.70) is True

        assert manager.memory_target_fraction == 0.60
        assert manager.memory_spill_fraction == 0.70

    @pytest.mark.unit
    def test_resizes_the_spill_buffer_threshold(self):
        """The SpillBuffer caches memory_limit * target at construction."""
        limit = 8 * 1024**3
        manager = FakeMemoryManager(memory_limit=limit, target=0.75)
        assert manager.data.n == int(limit * 0.75)

        _apply_thresholds(FakeWorker(manager), new_target=0.60, new_spill=0.70)

        assert manager.data.n == int(limit * 0.60)
        # ...and the buffer is asked to act on the tighter threshold now
        assert manager.data.evictions == 1

    @pytest.mark.unit
    def test_also_updates_dask_config(self):
        """Config stays in sync so restarted workers agree with live ones."""
        import dask

        manager = FakeMemoryManager()
        with dask.config.set({"distributed.worker.memory.target": 0.75}):
            _apply_thresholds(FakeWorker(manager), new_target=0.61, new_spill=0.71)
            assert dask.config.get("distributed.worker.memory.target") == 0.61
            assert dask.config.get("distributed.worker.memory.spill") == 0.71

    @pytest.mark.unit
    def test_reports_failure_when_no_manager_is_exposed(self):
        """A worker without a memory manager must not be counted as updated."""
        assert _apply_thresholds(FakeWorker(None), new_target=0.60, new_spill=0.70) is False
        assert _apply_thresholds(None, new_target=0.60, new_spill=0.70) is False


class TestTuneMemoryThresholds:
    """The reported result must reflect what actually landed on the workers."""

    @staticmethod
    def _client(run_result):
        client = MagicMock()
        client.run.side_effect = [
            # First call reads current thresholds
            {"tcp://w1": {"target": 0.75, "spill": 0.85}},
            # Second call applies them
            run_result,
        ]
        client.scheduler_info.return_value = {"workers": {"tcp://w1": {}, "tcp://w2": {}}}
        return client

    @pytest.mark.unit
    def test_counts_only_workers_that_confirmed(self):
        client = self._client({"tcp://w1": True, "tcp://w2": True})

        result = tune_memory_thresholds(client, strategy="tighten")

        assert result.workers_updated == 2
        assert result.new_target < result.old_target

    @pytest.mark.unit
    def test_does_not_claim_success_for_workers_that_refused(self):
        """A worker returning False must not be counted, and must be surfaced."""
        client = self._client({"tcp://w1": True, "tcp://w2": False})

        result = tune_memory_thresholds(client, strategy="tighten")

        assert result.workers_updated == 1
        assert "1/2" in result.rationale

    @pytest.mark.unit
    def test_off_strategy_is_a_no_op(self):
        client = MagicMock()

        result = tune_memory_thresholds(client, strategy="off")

        assert result.strategy == "off"
        assert result.workers_updated == 0
        client.run.assert_not_called()

    @pytest.mark.unit
    def test_rejects_unknown_strategy(self):
        with pytest.raises(ValueError, match="strategy must be one of"):
            tune_memory_thresholds(MagicMock(), strategy="sideways")


@pytest.mark.integration
@pytest.mark.slow
def test_thresholds_change_on_a_real_cluster():
    """End-to-end: the live worker's fractions must actually move."""
    distributed = pytest.importorskip("distributed")

    cluster = distributed.LocalCluster(
        n_workers=1, threads_per_worker=1, processes=False, dashboard_address=None, silence_logs=50
    )
    client = distributed.Client(cluster)
    try:

        def read(dask_worker):
            mm = dask_worker.memory_manager
            return (mm.memory_target_fraction, mm.memory_spill_fraction)

        before = next(iter(client.run(read).values()))
        result = tune_memory_thresholds(client, strategy="tighten", tighten_by=0.05)
        after = next(iter(client.run(read).values()))

        assert result.workers_updated == 1
        assert after[0] == pytest.approx(before[0] - 0.05)
        assert after[1] == pytest.approx(before[1] - 0.05)
    finally:
        client.close()
        cluster.close()
