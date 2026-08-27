"""Tests for the performance benchmarking module (v1.8)."""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest


@contextlib.contextmanager
def _fake_sampler(payload):
    """Stand-in for _sample_peak_memory yielding a fixed sample result."""
    yield payload


# ---------------------------------------------------------------------------
# Helpers — lightweight stubs so tests run without a real Dask cluster
# ---------------------------------------------------------------------------


def _make_client_stub(n_workers: int = 2) -> MagicMock:
    """Return a minimal mock Dask Client."""
    client = MagicMock()
    scheduler_info = {
        "workers": {
            f"tcp://127.0.0.1:{5000 + i}": {
                "memory_limit": 2 * 1024**3,
                "metrics": {
                    "memory": 512 * 1024**2,
                    "spilled_nbytes": {"disk": 0},
                },
            }
            for i in range(n_workers)
        }
    }
    client.scheduler_info.return_value = scheduler_info
    client.close = MagicMock()
    client.__enter__ = lambda s: s
    client.__exit__ = MagicMock(return_value=False)
    return client


# ---------------------------------------------------------------------------
# BenchmarkResult tests
# ---------------------------------------------------------------------------


class TestBenchmarkResult:
    def test_basic_fields(self):
        from dask_setup.benchmark import BenchmarkResult

        r = BenchmarkResult(name="test", wall_time_seconds=1.5)
        assert r.name == "test"
        assert r.wall_time_seconds == 1.5
        assert r.wall_time_std == 0.0
        assert r.peak_memory_gib == 0.0
        assert r.spill_gib == 0.0
        assert r.n_tasks == 0
        assert r.n_workers == 0
        assert r.errors == []
        assert r.extra == {}

    def test_tasks_per_second_auto_computed(self):
        from dask_setup.benchmark import BenchmarkResult

        r = BenchmarkResult(name="t", wall_time_seconds=2.0, n_tasks=100)
        assert r.tasks_per_second == pytest.approx(50.0)

    def test_tasks_per_second_zero_wall_time(self):
        from dask_setup.benchmark import BenchmarkResult

        r = BenchmarkResult(name="t", wall_time_seconds=0.0, n_tasks=100)
        # zero wall time — should not compute (division by zero)
        assert r.tasks_per_second == 0.0

    def test_summary_line_minimal(self):
        from dask_setup.benchmark import BenchmarkResult

        r = BenchmarkResult(name="cfg", wall_time_seconds=3.14)
        line = r.summary_line()
        assert "3.14s" in line
        assert "cfg" in line

    def test_summary_line_all_fields(self):
        from dask_setup.benchmark import BenchmarkResult

        r = BenchmarkResult(
            name="full",
            wall_time_seconds=5.0,
            wall_time_std=0.5,
            peak_memory_gib=2.0,
            spill_gib=0.3,
            n_tasks=200,
            n_workers=4,
            tasks_per_second=40.0,
            errors=["oops"],
        )
        line = r.summary_line()
        assert "±0.50s" in line
        assert "workers=4" in line
        assert "mem=2.00 GiB/peak" in line
        assert "spill=0.30 GiB" in line
        assert "tasks/s=40.0" in line
        assert "errors=1" in line

    def test_to_dict_round_trip(self):
        from dask_setup.benchmark import BenchmarkResult

        r = BenchmarkResult(
            name="d",
            wall_time_seconds=1.0,
            n_tasks=10,
            errors=["e1"],
            extra={"k": "v"},
        )
        d = r.to_dict()
        assert d["name"] == "d"
        assert d["wall_time_seconds"] == 1.0
        assert d["n_tasks"] == 10
        assert d["errors"] == ["e1"]
        assert d["extra"] == {"k": "v"}


# ---------------------------------------------------------------------------
# ScalingResult tests
# ---------------------------------------------------------------------------


class TestScalingResult:
    def _make_scaling_result(self):
        from dask_setup.benchmark import BenchmarkResult, ScalingResult

        results = [
            BenchmarkResult(name="w1", wall_time_seconds=8.0, n_workers=1, n_tasks=100),
            BenchmarkResult(name="w2", wall_time_seconds=4.5, n_workers=2, n_tasks=100),
            BenchmarkResult(name="w4", wall_time_seconds=2.5, n_workers=4, n_tasks=100),
        ]
        worker_counts = [1, 2, 4]
        speedups = [1.0, 8.0 / 4.5, 8.0 / 2.5]
        efficiencies = [speedups[i] / worker_counts[i] for i in range(3)]
        return ScalingResult(
            results=results,
            worker_counts=worker_counts,
            speedups=speedups,
            efficiencies=efficiencies,
        )

    def test_wall_times_property(self):
        sr = self._make_scaling_result()
        assert sr.wall_times == [8.0, 4.5, 2.5]

    def test_best_result(self):
        sr = self._make_scaling_result()
        best = sr.best()
        assert best.name == "w4"
        assert best.wall_time_seconds == 2.5

    def test_summary_contains_headers(self):
        sr = self._make_scaling_result()
        s = sr.summary()
        assert "Workers" in s
        assert "Speedup" in s
        assert "Efficiency" in s

    def test_speedups_and_efficiencies_length(self):
        sr = self._make_scaling_result()
        assert len(sr.speedups) == 3
        assert len(sr.efficiencies) == 3
        # baseline speedup
        assert sr.speedups[0] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# ChunkImpactResult tests
# ---------------------------------------------------------------------------


class TestChunkImpactResult:
    def _make_chunk_result(self):
        from dask_setup.benchmark import BenchmarkResult, ChunkImpactResult

        results = [
            BenchmarkResult(
                name="32 MiB",
                wall_time_seconds=5.0,
                n_tasks=50,
                extra={"chunk_size_mb": 32},
            ),
            BenchmarkResult(
                name="128 MiB",
                wall_time_seconds=2.0,
                n_tasks=20,
                extra={"chunk_size_mb": 128},
            ),
            BenchmarkResult(
                name="512 MiB",
                wall_time_seconds=3.5,
                n_tasks=10,
                extra={"chunk_size_mb": 512},
            ),
        ]
        return ChunkImpactResult(
            results=results,
            chunk_specs=[{"x": 100}, {"x": 400}, {"x": 1600}],
        )

    def test_optimal_result(self):
        ci = self._make_chunk_result()
        opt = ci.optimal()
        assert opt.name == "128 MiB"
        assert opt.wall_time_seconds == 2.0

    def test_summary_contains_optimal(self):
        ci = self._make_chunk_result()
        s = ci.summary()
        assert "128 MiB" in s or "optimal" in s.lower()

    def test_results_length(self):
        ci = self._make_chunk_result()
        assert len(ci.results) == 3
        assert len(ci.chunk_specs) == 3


# ---------------------------------------------------------------------------
# SyntheticBenchmarkResult tests
# ---------------------------------------------------------------------------


class TestSyntheticBenchmarkResult:
    def test_summary_basic(self):
        from dask_setup.benchmark import SyntheticBenchmarkResult

        r = SyntheticBenchmarkResult(
            profile_name="development",
            operation="mean",
            ds_size="tiny",
            array_shape=(200, 200, 10),
            wall_time_seconds=0.5,
            peak_memory_gib=0.1,
            spill_gib=0.0,
            n_tasks=8,
            n_workers=2,
            tasks_per_second=16.0,
        )
        s = r.summary()
        assert "development" in s
        assert "mean" in s
        assert "0.50s" in s
        assert "(200, 200, 10)" in s

    def test_summary_shows_spill_when_nonzero(self):
        from dask_setup.benchmark import SyntheticBenchmarkResult

        r = SyntheticBenchmarkResult(
            profile_name="p",
            operation="sum",
            ds_size="small",
            array_shape=(500, 500, 20),
            wall_time_seconds=1.0,
            peak_memory_gib=0.5,
            spill_gib=0.2,
            n_tasks=20,
            n_workers=2,
            tasks_per_second=20.0,
        )
        s = r.summary()
        assert "0.20 GiB" in s

    def test_summary_shows_errors(self):
        from dask_setup.benchmark import SyntheticBenchmarkResult

        r = SyntheticBenchmarkResult(
            profile_name="p",
            operation="max",
            ds_size="tiny",
            array_shape=(200, 200, 10),
            wall_time_seconds=0.3,
            peak_memory_gib=0.0,
            spill_gib=0.0,
            n_tasks=4,
            n_workers=1,
            tasks_per_second=13.3,
            errors=["worker died"],
        )
        s = r.summary()
        assert "worker died" in s


# ---------------------------------------------------------------------------
# _get_builtin_op / _resolve_operation tests
# ---------------------------------------------------------------------------


class TestOperationHelpers:
    def test_resolve_string_mean(self):
        from dask_setup.benchmark import _resolve_operation

        try:
            import xarray as xr
        except ImportError:
            pytest.skip("xarray not installed")

        import numpy as np
        import xarray as xr

        ds = xr.Dataset({"t": (["x", "y"], np.ones((4, 4)))})
        op = _resolve_operation("mean")
        result = op(ds)
        # Should produce a lazy xarray object
        assert result is not None

    def test_resolve_callable(self):
        from dask_setup.benchmark import _resolve_operation

        fn = lambda x: x  # noqa: E731
        assert _resolve_operation(fn) is fn

    def test_resolve_unknown_string_raises(self):
        from dask_setup.benchmark import _resolve_operation

        with pytest.raises((ValueError, KeyError)):
            _resolve_operation("nonexistent_op")


# ---------------------------------------------------------------------------
# _generate_auto_chunks tests
# ---------------------------------------------------------------------------


class TestGenerateAutoChunks:
    def test_returns_list_of_dicts(self):
        from dask_setup.benchmark import _generate_auto_chunks

        dims = {"x": 1000, "y": 500, "time": 100}
        chunks_list = _generate_auto_chunks(dims)
        assert isinstance(chunks_list, list)
        assert len(chunks_list) > 0
        for spec in chunks_list:
            assert isinstance(spec, dict)

    def test_chunk_values_are_positive_ints(self):
        from dask_setup.benchmark import _generate_auto_chunks

        dims = {"lat": 720, "lon": 1440, "lev": 30}
        for spec in _generate_auto_chunks(dims):
            for _dim, size in spec.items():
                assert isinstance(size, int)
                assert size > 0

    def test_chunk_values_do_not_exceed_dim_size(self):
        from dask_setup.benchmark import _generate_auto_chunks

        dims = {"x": 50, "y": 50}
        for spec in _generate_auto_chunks(dims):
            for _dim, size in spec.items():
                assert size <= dims[_dim]


# ---------------------------------------------------------------------------
# CLI: cmd_benchmark
# ---------------------------------------------------------------------------


class TestCLIBenchmark:
    def test_benchmark_subcommand_registered(self):
        """Verify 'benchmark' is a registered sub-command."""
        from dask_setup.cli import create_parser

        parser = create_parser()
        # Parse with benchmark subcommand and check defaults exist
        args = parser.parse_args(
            [
                "benchmark",
                "--profile",
                "development",
                "--operation",
                "mean",
                "--size",
                "tiny",
                "--repeats",
                "1",
            ]
        )
        assert args.profile == "development"
        assert args.operation == "mean"
        assert args.size == "tiny"
        assert args.repeats == 1

    def test_benchmark_defaults(self):
        from dask_setup.cli import create_parser

        parser = create_parser()
        args = parser.parse_args(["benchmark"])
        assert args.profile == "development"
        assert args.operation == "mean"
        assert args.size == "small"
        assert args.repeats == 1

    def test_benchmark_invalid_operation(self):
        """Invalid operation should cause argparse to error."""

        from dask_setup.cli import create_parser

        parser = create_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["benchmark", "--operation", "invalid"])

    def test_cmd_benchmark_calls_run_synthetic(self):
        """cmd_benchmark should delegate to run_synthetic_benchmark."""
        import argparse

        from dask_setup.benchmark import SyntheticBenchmarkResult
        from dask_setup.cli import cmd_benchmark

        mock_result = SyntheticBenchmarkResult(
            profile_name="development",
            operation="mean",
            ds_size="tiny",
            array_shape=(200, 200, 10),
            wall_time_seconds=0.1,
            peak_memory_gib=0.0,
            spill_gib=0.0,
            n_tasks=4,
            n_workers=1,
            tasks_per_second=40.0,
        )

        args = argparse.Namespace(
            profile="development",
            operation="mean",
            size="tiny",
            repeats=1,
        )

        with patch(
            "dask_setup.benchmark.run_synthetic_benchmark", return_value=mock_result
        ) as mock_fn:
            rc = cmd_benchmark(args)
            assert rc == 0
            mock_fn.assert_called_once_with(
                profile_name="development",
                operation="mean",
                ds_size="tiny",
                repeats=1,
                verbose=True,
            )

    def test_cmd_benchmark_handles_exception(self, capsys):
        """cmd_benchmark should return non-zero on error."""
        import argparse

        from dask_setup.cli import cmd_benchmark

        args = argparse.Namespace(
            profile="nonexistent_profile",
            operation="mean",
            size="tiny",
            repeats=1,
        )

        with patch(
            "dask_setup.benchmark.run_synthetic_benchmark",
            side_effect=ValueError("Unknown profile"),
        ):
            rc = cmd_benchmark(args)
            assert rc != 0


# ---------------------------------------------------------------------------
# _count_tasks
# ---------------------------------------------------------------------------


class TestCountTasks:
    def test_returns_int(self):
        from dask_setup.benchmark import _count_tasks

        try:
            import dask.array as da
        except ImportError:
            pytest.skip("dask not installed")

        import dask.array as da
        import numpy as np

        arr = da.from_array(np.ones((100, 100)), chunks=(50, 50))
        lazy = arr.mean()
        n = _count_tasks(lazy)
        assert isinstance(n, int)
        assert n > 0

    def test_returns_zero_for_non_dask(self):
        from dask_setup.benchmark import _count_tasks

        assert _count_tasks(42) == 0
        assert _count_tasks("string") == 0
        assert _count_tasks(None) == 0


# ---------------------------------------------------------------------------
# __init__ exports
# ---------------------------------------------------------------------------


def test_benchmark_symbols_exported():
    """All v1.8 benchmark symbols should be importable from dask_setup."""
    import dask_setup

    # Dataclass types
    assert hasattr(dask_setup, "BenchmarkResult")
    assert hasattr(dask_setup, "ScalingResult")
    assert hasattr(dask_setup, "ChunkImpactResult")

    # Functions
    assert hasattr(dask_setup, "benchmark_config")
    assert hasattr(dask_setup, "scaling_analysis")
    assert hasattr(dask_setup, "chunk_impact")
    assert hasattr(dask_setup, "run_synthetic_benchmark")


def test_version_is_2():
    import dask_setup

    assert dask_setup.__version__.startswith("2.")


class TestScalingSweepActuallyScales:
    """The sweep's default config pinned every run to a single worker.

    decide_topology() sets n_workers=1 for workload_type="io" regardless of
    max_workers, and "io" is DaskSetupConfig's default -- so scaling_analysis
    built the same one-worker cluster at every point and reported the resulting
    timing noise as a scaling curve.
    """

    @pytest.mark.unit
    def test_default_base_config_uses_a_scaling_workload_type(self):
        from dask_setup.benchmark import scaling_analysis
        from dask_setup.config import DaskSetupConfig

        captured: list[DaskSetupConfig] = []

        def fake_setup(*_args, config=None, **_kwargs):
            captured.append(config)
            raise RuntimeError("stop here — we only want the config")

        with patch("dask_setup.client.setup_dask_client", side_effect=fake_setup):
            scaling_analysis(MagicMock(), worker_counts=(1, 2, 4))

        assert captured, "setup_dask_client was never called"
        assert all(c.workload_type == "cpu" for c in captured)

    @pytest.mark.unit
    def test_requested_worker_count_reaches_the_config(self):
        from dask_setup.benchmark import scaling_analysis

        captured = []

        def fake_setup(*_args, config=None, **_kwargs):
            captured.append((config.max_workers, config.adaptive))
            raise RuntimeError("stop")

        with patch("dask_setup.client.setup_dask_client", side_effect=fake_setup):
            scaling_analysis(MagicMock(), worker_counts=(1, 2, 8))

        assert [n for n, _ in captured] == [1, 2, 8]
        assert all(adaptive is False for _, adaptive in captured)

    @pytest.mark.unit
    def test_topology_honours_the_sweep_for_cpu_but_not_io(self):
        """The reason the default had to change, stated as an assertion."""
        from dask_setup.topology import decide_topology

        io_counts = [decide_topology("io", 32, max_workers=n).n_workers for n in (1, 2, 4, 8)]
        cpu_counts = [decide_topology("cpu", 32, max_workers=n).n_workers for n in (1, 2, 4, 8)]

        assert io_counts == [1, 1, 1, 1]
        assert cpu_counts == [1, 2, 4, 8]


class TestScalingEfficiencyNormalisation:
    """Efficiency divided by the absolute worker count, not the worker ratio.

    A sweep starting anywhere other than 1 worker therefore reported a
    fraction of its true efficiency: a perfectly scaling (4, 8) sweep scored
    0.25 at its own baseline instead of 1.0.
    """

    @staticmethod
    def _sweep(counts, times):
        from dask_setup.benchmark import BenchmarkResult, scaling_analysis

        results = iter(
            BenchmarkResult(name=f"workers={n}", wall_time_seconds=t, n_workers=n)
            for n, t in zip(counts, times, strict=True)
        )

        with (
            patch(
                "dask_setup.client.setup_dask_client",
                return_value=(MagicMock(), MagicMock(), "/tmp"),
            ),
            patch("dask_setup.benchmark._measure_one", side_effect=lambda **_k: next(results)),
        ):
            return scaling_analysis(MagicMock(), worker_counts=counts)

    @pytest.mark.unit
    def test_baseline_efficiency_is_one_when_the_sweep_starts_at_one(self):
        scaling = self._sweep((1, 2, 4), (8.0, 4.0, 2.0))
        assert scaling.efficiencies[0] == pytest.approx(1.0)

    @pytest.mark.unit
    def test_baseline_efficiency_is_one_when_the_sweep_starts_at_four(self):
        """Used to report 0.25 for a baseline that is by definition 100%."""
        scaling = self._sweep((4, 8), (8.0, 4.0))
        assert scaling.efficiencies[0] == pytest.approx(1.0)

    @pytest.mark.unit
    def test_perfect_scaling_from_a_non_unit_baseline_stays_at_one(self):
        scaling = self._sweep((4, 8, 16), (8.0, 4.0, 2.0))
        assert scaling.efficiencies == pytest.approx([1.0, 1.0, 1.0])

    @pytest.mark.unit
    def test_half_efficiency_is_reported_as_half(self):
        # 4 -> 8 workers halves nothing: same time means efficiency 0.5
        scaling = self._sweep((4, 8), (8.0, 8.0))
        assert scaling.efficiencies[1] == pytest.approx(0.5)

    @pytest.mark.unit
    def test_speedups_are_unchanged(self):
        scaling = self._sweep((4, 8), (8.0, 4.0))
        assert scaling.speedups == pytest.approx([1.0, 2.0])


class TestPeakMemoryIsSampledDuringTheRun:
    """peak_memory_gib came from a report taken after .compute() returned.

    By then the workers have released the data, so the "peak" was near zero
    for exactly the workloads whose memory use matters.
    """

    @pytest.mark.unit
    def test_sampled_peak_is_preferred_over_the_post_run_reading(self):
        from dask_setup.benchmark import _measure_one
        from dask_setup.reporting import ClusterReport

        client = MagicMock()
        client.scheduler_info.return_value = {"workers": {"a": {}}}

        after_the_fact = ClusterReport(memory_per_worker_gib={"a": 0.01})

        with (
            patch("dask_setup.reporting.cluster_report", return_value=after_the_fact),
            patch(
                "dask_setup.benchmark._sample_peak_memory",
                lambda _c: _fake_sampler({"peak_gib": 7.5, "sampled": True}),
            ),
        ):
            result = _measure_one(
                ds=MagicMock(),
                operation_fn=lambda d: MagicMock(),
                client=client,
                repeats=1,
                warmup=False,
                name="t",
            )

        assert result.peak_memory_gib == pytest.approx(7.5)
        assert not any("lower bound" in e for e in result.errors)

    @pytest.mark.unit
    def test_unsampled_run_says_so_instead_of_reporting_a_fake_peak(self):
        from dask_setup.benchmark import _measure_one
        from dask_setup.reporting import ClusterReport

        client = MagicMock()
        client.scheduler_info.return_value = {"workers": {"a": {}}}

        with (
            patch(
                "dask_setup.reporting.cluster_report",
                return_value=ClusterReport(memory_per_worker_gib={"a": 0.01}),
            ),
            patch(
                "dask_setup.benchmark._sample_peak_memory",
                lambda _c: _fake_sampler({"peak_gib": 0.0, "sampled": False}),
            ),
        ):
            result = _measure_one(
                ds=MagicMock(),
                operation_fn=lambda d: MagicMock(),
                client=client,
                repeats=1,
                warmup=False,
                name="t",
            )

        assert any("lower bound" in e for e in result.errors)

    @pytest.mark.unit
    def test_sampler_survives_a_missing_memorysampler(self):
        """An older distributed must degrade, not break the benchmark."""
        import builtins

        from dask_setup.benchmark import _sample_peak_memory

        real_import = builtins.__import__

        def no_sampler(name, *a, **k):
            if name == "distributed.diagnostics":
                raise ImportError("no MemorySampler here")
            return real_import(name, *a, **k)

        with (
            patch.object(builtins, "__import__", side_effect=no_sampler),
            _sample_peak_memory(MagicMock()) as sampled,
        ):
            pass

        assert sampled["sampled"] is False
        assert sampled["peak_gib"] == 0.0
