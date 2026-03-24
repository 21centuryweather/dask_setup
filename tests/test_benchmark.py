"""Tests for the performance benchmarking module (v1.8)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
