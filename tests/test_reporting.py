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
    def test_spilled_data_is_not_also_counted_as_memory(self):
        """managed_bytes includes spilled data; memory must exclude it.

        Real metrics from a 1 GiB worker holding 1.6 GB, 1.344 GB of it
        spilled: the worker's resident managed data is 0.256 GB, not 1.6 GB
        (which is more than its memory limit).
        """
        client = MagicMock()
        client.scheduler_info.return_value = {
            "workers": {
                "tcp://w1": {
                    "metrics": {
                        "managed_bytes": 1_600_000_000,
                        "spilled_bytes": {"memory": 1_344_000_000, "disk": 1_344_009_744},
                        "memory": 802_062_336,
                    }
                }
            }
        }
        client.run_on_scheduler.return_value = 0

        report = cluster_report(client)

        assert report.peak_memory_gib == pytest.approx(256_000_000 / 2**30)
        assert report.total_spill_gib == pytest.approx(1_344_009_744 / 2**30)

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


class TestSchedulerWorkers:
    """distributed 2025 truncates scheduler_info() to 5 workers by default.

    cluster_report, tune_memory_thresholds, recommend_chunks and the benchmarks
    all summed or counted over that truncated list, so on any node running more
    than 5 workers they undercounted memory and spill and reported 5 workers.
    """

    @pytest.mark.unit
    def test_requests_every_worker(self):
        from dask_setup.reporting import scheduler_workers

        client = MagicMock()
        client.scheduler_info.return_value = {"workers": {"tcp://w1": {}}}

        assert scheduler_workers(client) == {"tcp://w1": {}}
        client.scheduler_info.assert_called_once_with(n_workers=-1)

    @pytest.mark.unit
    def test_older_distributed_without_n_workers(self):
        from dask_setup.reporting import scheduler_workers

        def scheduler_info(**kwargs):
            if kwargs:
                raise TypeError("identity() got an unexpected keyword argument 'n_workers'")
            return {"workers": {"tcp://w1": {}, "tcp://w2": {}}}

        client = MagicMock()
        client.scheduler_info.side_effect = scheduler_info

        assert len(scheduler_workers(client)) == 2

    @pytest.mark.integration
    def test_cluster_report_sees_more_than_five_workers(self):
        from distributed import Client, LocalCluster

        with (
            LocalCluster(
                n_workers=8, threads_per_worker=1, processes=False, dashboard_address=None
            ) as cluster,
            Client(cluster) as client,
        ):
            client.wait_for_workers(8)
            client.submit(sum, [1, 2]).result()  # give every worker a heartbeat to report

            report = cluster_report(client)

        assert len(report.memory_per_worker_gib) == 8
