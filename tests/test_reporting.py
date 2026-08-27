"""Unit tests for dask_setup.reporting."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from dask_setup.reporting import ClusterReport, cluster_report, worker_spill_bytes


class TestWorkerSpillBytes:
    """Spill must be read from the key distributed actually publishes.

    Regression guard: the reader looked for ``spilled_memory`` / ``spill``,
    neither of which exists on current distributed. Spill therefore always
    reported 0.0 GiB — in ClusterReport, in BenchmarkResult, and in the
    tune_memory_thresholds decision, whose "loosen" branch was unreachable.
    """

    @pytest.mark.unit
    def test_reads_current_spilled_bytes_mapping(self):
        """distributed >= 2023.3 publishes {"memory": ..., "disk": ...}."""
        metrics = {"spilled_bytes": {"memory": 128_000_000, "disk": 128_003_712}}
        # The disk figure is the spill volume, not the in-memory size
        assert worker_spill_bytes(metrics) == 128_003_712

    @pytest.mark.unit
    @pytest.mark.parametrize("key", ["spilled_memory", "spill"])
    def test_still_reads_legacy_keys(self, key):
        assert worker_spill_bytes({key: {"disk": 4096}}) == 4096
        assert worker_spill_bytes({key: 2048}) == 2048

    @pytest.mark.unit
    def test_prefers_the_current_key_when_several_are_present(self):
        metrics = {"spilled_bytes": {"disk": 999}, "spill": 1}
        assert worker_spill_bytes(metrics) == 999

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "metrics",
        [
            {},
            {"managed_bytes": 1024},  # unrelated keys only
            {"spilled_bytes": {}},  # present but empty
            {"spilled_bytes": {"memory": 500}},  # no disk figure
            {"spilled_bytes": None},
        ],
    )
    def test_missing_or_unusable_reads_as_zero(self, metrics):
        """A future rename must degrade to 'no data', never raise."""
        assert worker_spill_bytes(metrics) == 0

    @pytest.mark.unit
    def test_booleans_are_not_treated_as_counts(self):
        assert worker_spill_bytes({"spill": True}) == 0


class TestClusterReport:
    @pytest.mark.unit
    def test_sums_spill_across_workers(self):
        client = MagicMock()
        client.scheduler_info.return_value = {
            "workers": {
                "tcp://w1": {"metrics": {"managed_bytes": 2**30, "spilled_bytes": {"disk": 2**30}}},
                "tcp://w2": {
                    "metrics": {"managed_bytes": 2**30, "spilled_bytes": {"disk": 2 * 2**30}}
                },
            }
        }
        client.run_on_scheduler.return_value = 42

        report = cluster_report(client)

        assert report.total_spill_gib == pytest.approx(3.0)
        assert report.peak_memory_gib == pytest.approx(1.0)
        assert report.total_tasks == 42
        assert "spill=3.00 GiB" in report.summary_line()

    @pytest.mark.unit
    def test_survives_an_unreachable_scheduler(self):
        """Metric collection is best-effort and must never raise."""
        client = MagicMock()
        client.scheduler_info.side_effect = OSError("scheduler gone")
        client.run_on_scheduler.side_effect = OSError("scheduler gone")

        report = cluster_report(client)

        assert report == ClusterReport()
        assert report.summary_line() == "no metrics collected"

    @pytest.mark.unit
    def test_zero_spill_is_omitted_from_the_summary(self):
        client = MagicMock()
        client.scheduler_info.return_value = {
            "workers": {"tcp://w1": {"metrics": {"managed_bytes": 2**30}}}
        }
        client.run_on_scheduler.return_value = 0

        report = cluster_report(client)

        assert report.total_spill_gib == 0.0
        assert "spill" not in report.summary_line()
