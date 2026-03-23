"""Performance benchmarking and configuration comparison utilities.

This module provides three complementary analysis tools for tuning Dask
configurations on HPC datasets:

:func:`benchmark_config`
    A/B-test multiple :class:`~dask_setup.config.DaskSetupConfig` objects
    against the same xarray operation.  Each config gets a fresh cluster so
    results are directly comparable.

:func:`scaling_analysis`
    Sweep worker counts (1, 2, 4, 8, …) and measure parallel speedup and
    efficiency.  Returns a :class:`ScalingResult` with optional matplotlib
    plot.

:func:`chunk_impact`
    Fix a running cluster and sweep chunk sizes to find the throughput sweet
    spot for a specific dataset and operation.  Returns a
    :class:`ChunkImpactResult` with optional plot.

A synthetic benchmark requiring no dataset is available via the
``dask-setup benchmark`` CLI subcommand (see :func:`run_synthetic_benchmark`).

All three functions share a common :class:`BenchmarkResult` dataclass.

Example::

    from dask_setup import DaskSetupConfig
    from dask_setup.benchmark import benchmark_config

    configs = {
        "io_profile":  DaskSetupConfig(workload_type="io",  reserve_mem_gb=40),
        "cpu_profile": DaskSetupConfig(workload_type="cpu", reserve_mem_gb=60),
    }
    results = benchmark_config(configs, my_ds, operation="mean")
    for r in results:
        print(r.summary_line())
"""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Sequence

if TYPE_CHECKING:
    from dask.distributed import Client

    from .config import DaskSetupConfig

__all__ = [
    "BenchmarkResult",
    "ScalingResult",
    "ChunkImpactResult",
    "benchmark_config",
    "scaling_analysis",
    "chunk_impact",
    "run_synthetic_benchmark",
]

# ---------------------------------------------------------------------------
# Supported string operation aliases
# ---------------------------------------------------------------------------

#: Mapping of string alias → callable factory.
#: Each factory receives the dataset/array and returns a lazy Dask expression.
_BUILTIN_OPS: dict[str, Callable[[Any], Any]] = {}  # populated lazily


def _get_builtin_op(name: str) -> Callable[[Any], Any]:
    """Return a lazy-operation callable for the given string alias."""
    ops = {
        "mean": lambda ds: ds.mean(),
        "sum": lambda ds: ds.sum(),
        "std": lambda ds: ds.std(),
        "max": lambda ds: ds.max(),
        "min": lambda ds: ds.min(),
        "var": lambda ds: ds.var(),
        "rechunk": lambda ds: ds.rechunk(),  # rechunk to auto-determined sizes
    }
    if name not in ops:
        raise ValueError(
            f"Unknown operation alias {name!r}. "
            f"Valid aliases: {sorted(ops)}. "
            "You can also pass any callable: operation=lambda ds: ds.rolling(time=10).mean()"
        )
    return ops[name]


def _resolve_operation(operation: Callable[[Any], Any] | str) -> Callable[[Any], Any]:
    if callable(operation):
        return operation
    return _get_builtin_op(str(operation))


def _count_tasks(obj: Any) -> int:
    """Return the number of tasks in *obj*'s dask graph (best effort)."""
    try:
        return len(dict(obj.__dask_graph__()))
    except Exception:
        try:
            return len(obj.__dask_graph__())
        except Exception:
            return 0


# ---------------------------------------------------------------------------
# BenchmarkResult — one timed run
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    """Metrics for a single benchmarked configuration / parameter setting.

    Attributes
    ----------
    name : str
        Label for this result (config name, worker count, chunk spec, etc.).
    wall_time_seconds : float
        Mean elapsed time for the operation across all repeats.
    wall_time_std : float
        Standard deviation of per-repeat wall times (0.0 for a single repeat).
    peak_memory_gib : float
        Maximum in-memory data across all workers at the end of the run.
    spill_gib : float
        Total data written to disk spill storage during the run.
    n_tasks : int
        Number of dask-graph tasks the operation required.
    n_workers : int
        Number of workers active during this run.
    tasks_per_second : float
        Derived: ``n_tasks / wall_time_seconds`` (0.0 if either is unknown).
    errors : list[str]
        Any non-fatal error messages collected during the run.
    extra : dict[str, Any]
        Additional metadata attached by the calling function
        (e.g. ``{"chunk_size_mb": 128}`` from :func:`chunk_impact`).
    """

    name: str
    wall_time_seconds: float
    wall_time_std: float = 0.0
    peak_memory_gib: float = 0.0
    spill_gib: float = 0.0
    n_tasks: int = 0
    n_workers: int = 0
    tasks_per_second: float = 0.0
    errors: list[str] = field(default_factory=list)
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.tasks_per_second == 0.0 and self.n_tasks > 0 and self.wall_time_seconds > 0:
            self.tasks_per_second = self.n_tasks / self.wall_time_seconds

    def summary_line(self) -> str:
        """One-line human-readable summary."""
        parts = [
            f"{self.name!r:24s}",
            f"wall={self.wall_time_seconds:.2f}s",
        ]
        if self.wall_time_std > 0:
            parts[-1] += f" ±{self.wall_time_std:.2f}s"
        if self.n_workers:
            parts.append(f"workers={self.n_workers}")
        if self.peak_memory_gib:
            parts.append(f"mem={self.peak_memory_gib:.2f} GiB/peak")
        if self.spill_gib:
            parts.append(f"spill={self.spill_gib:.2f} GiB")
        if self.tasks_per_second:
            parts.append(f"tasks/s={self.tasks_per_second:.1f}")
        if self.errors:
            parts.append(f"errors={len(self.errors)}")
        return " | ".join(parts)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable dictionary of all fields."""
        return {
            "name": self.name,
            "wall_time_seconds": self.wall_time_seconds,
            "wall_time_std": self.wall_time_std,
            "peak_memory_gib": self.peak_memory_gib,
            "spill_gib": self.spill_gib,
            "n_tasks": self.n_tasks,
            "n_workers": self.n_workers,
            "tasks_per_second": self.tasks_per_second,
            "errors": list(self.errors),
            "extra": dict(self.extra),
        }


# ---------------------------------------------------------------------------
# ScalingResult — collection from scaling_analysis()
# ---------------------------------------------------------------------------


@dataclass
class ScalingResult:
    """Results of a worker-count scaling sweep.

    Attributes
    ----------
    results : list[BenchmarkResult]
        One entry per worker count, in the order they were benchmarked.
    worker_counts : list[int]
        The worker counts that were swept.
    speedups : list[float]
        ``wall_times[0] / wall_times[i]`` relative to the single-worker run.
    efficiencies : list[float]
        ``speedups[i] / worker_counts[i]`` — ideal scaling gives 1.0.
    """

    results: list[BenchmarkResult]
    worker_counts: list[int]
    speedups: list[float]
    efficiencies: list[float]

    @property
    def wall_times(self) -> list[float]:
        """Wall times in the same order as :attr:`worker_counts`."""
        return [r.wall_time_seconds for r in self.results]

    def best(self) -> BenchmarkResult:
        """Return the result with the shortest wall time."""
        return min(self.results, key=lambda r: r.wall_time_seconds)

    def summary(self) -> str:
        """Multi-line summary table."""
        lines = [
            f"{'Workers':>8}  {'Wall (s)':>10}  {'Speedup':>8}  {'Efficiency':>10}  {'Tasks/s':>8}",
            "-" * 54,
        ]
        for r, nw, sp, eff in zip(
            self.results, self.worker_counts, self.speedups, self.efficiencies
        ):
            lines.append(
                f"{nw:>8}  {r.wall_time_seconds:>10.2f}  {sp:>8.2f}x  "
                f"{eff:>9.1%}  {r.tasks_per_second:>8.1f}"
            )
        return "\n".join(lines)

    def to_dataframe(self) -> Any:
        """Return results as a ``pandas.DataFrame``.

        Raises :exc:`ImportError` if pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "ScalingResult.to_dataframe() requires pandas. "
                "Install with: pip install pandas"
            ) from e

        records = []
        for r, nw, sp, eff in zip(
            self.results, self.worker_counts, self.speedups, self.efficiencies
        ):
            records.append(
                {
                    "workers": nw,
                    "wall_time_s": r.wall_time_seconds,
                    "wall_time_std": r.wall_time_std,
                    "speedup": sp,
                    "efficiency": eff,
                    "peak_memory_gib": r.peak_memory_gib,
                    "spill_gib": r.spill_gib,
                    "tasks_per_second": r.tasks_per_second,
                }
            )
        return pd.DataFrame(records)

    def plot(
        self,
        figsize: tuple[float, float] = (10, 4),
        title: str = "Scaling Analysis",
    ) -> Any:
        """Plot speedup and efficiency curves.

        Returns the matplotlib ``Figure``, or ``None`` if matplotlib is not
        installed.
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            return None

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=figsize)

        ax1.plot(self.worker_counts, self.speedups, "o-", color="steelblue", label="Actual")
        ax1.plot(
            self.worker_counts, self.worker_counts, "--", color="grey", alpha=0.5, label="Ideal"
        )
        ax1.set_xlabel("Workers")
        ax1.set_ylabel("Speedup")
        ax1.set_title("Speedup")
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        ax2.plot(self.worker_counts, [e * 100 for e in self.efficiencies], "s-", color="tomato")
        ax2.axhline(100, linestyle="--", color="grey", alpha=0.5)
        ax2.set_xlabel("Workers")
        ax2.set_ylabel("Efficiency (%)")
        ax2.set_title("Parallel Efficiency")
        ax2.set_ylim(0, 110)
        ax2.grid(True, alpha=0.3)

        fig.suptitle(title)
        fig.tight_layout()
        return fig


# ---------------------------------------------------------------------------
# ChunkImpactResult — collection from chunk_impact()
# ---------------------------------------------------------------------------


@dataclass
class ChunkImpactResult:
    """Results of a chunk-size sweep on a fixed cluster.

    Attributes
    ----------
    results : list[BenchmarkResult]
        One entry per chunk specification, in the order they were tried.
    chunk_sizes : list[dict[str, int]]
        The chunk specs that were swept.
    recommended_chunks : dict[str, int]
        The chunk spec with the best (lowest) wall time.
    """

    results: list[BenchmarkResult]
    chunk_sizes: list[dict[str, int]]
    recommended_chunks: dict[str, int]

    @property
    def wall_times(self) -> list[float]:
        return [r.wall_time_seconds for r in self.results]

    def best(self) -> BenchmarkResult:
        return min(self.results, key=lambda r: r.wall_time_seconds)

    def summary(self) -> str:
        lines = [
            f"{'Chunks':^36}  {'Wall (s)':>10}  {'Mem GiB':>8}  {'Tasks/s':>8}",
            "-" * 68,
        ]
        for r, cs in zip(self.results, self.chunk_sizes):
            chunk_str = str(cs)[:34]
            lines.append(
                f"{chunk_str:<36}  {r.wall_time_seconds:>10.2f}  "
                f"{r.peak_memory_gib:>8.2f}  {r.tasks_per_second:>8.1f}"
            )
        lines.append(f"\nRecommended: {self.recommended_chunks}")
        return "\n".join(lines)

    def to_dataframe(self) -> Any:
        """Return results as a ``pandas.DataFrame``.

        Raises :exc:`ImportError` if pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError as e:
            raise ImportError(
                "ChunkImpactResult.to_dataframe() requires pandas. "
                "Install with: pip install pandas"
            ) from e

        records = []
        for r, cs in zip(self.results, self.chunk_sizes):
            row = {
                "chunks": str(cs),
                "wall_time_s": r.wall_time_seconds,
                "wall_time_std": r.wall_time_std,
                "peak_memory_gib": r.peak_memory_gib,
                "spill_gib": r.spill_gib,
                "tasks_per_second": r.tasks_per_second,
            }
            row.update(cs)
            records.append(row)
        return pd.DataFrame(records)

    def plot(
        self,
        dim: str | None = None,
        figsize: tuple[float, float] = (8, 4),
        title: str = "Chunk Size Impact",
    ) -> Any:
        """Plot wall time vs chunk size for one dimension.

        Parameters
        ----------
        dim:
            Dimension name to use as the x-axis.  If ``None``, uses the first
            dimension found in :attr:`chunk_sizes`.

        Returns the matplotlib ``Figure``, or ``None`` if matplotlib is not
        installed or no suitable dimension is found.
        """
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            return None

        if not self.chunk_sizes:
            return None

        if dim is None:
            dim = next(iter(self.chunk_sizes[0]), None)
        if dim is None:
            return None

        xs = [cs.get(dim, 0) for cs in self.chunk_sizes]
        ys = self.wall_times

        fig, ax = plt.subplots(figsize=figsize)
        ax.plot(xs, ys, "o-", color="mediumseagreen")
        best_x = self.recommended_chunks.get(dim, 0)
        if best_x:
            ax.axvline(best_x, linestyle="--", color="tomato", label=f"Best: {dim}={best_x}")
            ax.legend()
        ax.set_xlabel(f"Chunk size ({dim})")
        ax.set_ylabel("Wall time (s)")
        ax.set_title(title)
        ax.grid(True, alpha=0.3)
        fig.tight_layout()
        return fig


# ---------------------------------------------------------------------------
# Core measurement helper
# ---------------------------------------------------------------------------


def _measure_one(
    ds: Any,
    operation_fn: Callable[[Any], Any],
    client: "Client",
    repeats: int,
    warmup: bool,
    name: str,
    extra: dict[str, Any] | None = None,
) -> BenchmarkResult:
    """Time one operation on *ds* using the connected *client*.

    Parameters
    ----------
    ds:
        A dask-backed xarray Dataset/DataArray (or dask.array).
    operation_fn:
        Callable that takes *ds* and returns a lazy Dask expression.
    client:
        Connected Dask client.
    repeats:
        Number of timed repetitions. Mean and std-dev are computed.
    warmup:
        If ``True``, run one un-timed warmup iteration before measurement.
    name:
        Label for the returned :class:`BenchmarkResult`.
    extra:
        Additional metadata to attach to the result.
    """
    errors: list[str] = []
    n_tasks = 0
    n_workers = 0
    times: list[float] = []
    peak_mem = 0.0
    spill = 0.0

    # Count tasks from the lazy graph
    try:
        lazy = operation_fn(ds)
        n_tasks = _count_tasks(lazy)
    except Exception as e:
        errors.append(f"Task count failed: {e}")

    # Worker count
    try:
        n_workers = len(client.scheduler_info().get("workers", {}))
    except Exception:
        pass

    # Optional warmup
    if warmup:
        try:
            operation_fn(ds).compute()
        except Exception as e:
            errors.append(f"Warmup failed: {e}")

    # Timed runs
    for i in range(repeats):
        t0 = time.monotonic()
        try:
            operation_fn(ds).compute()
        except Exception as e:
            errors.append(f"Run {i + 1}/{repeats} failed: {e}")
            times.append(float("inf"))
        else:
            times.append(time.monotonic() - t0)

    # Filter infinities (failed runs)
    valid_times = [t for t in times if t != float("inf")]
    wall_mean = statistics.mean(valid_times) if valid_times else float("nan")
    wall_std = statistics.stdev(valid_times) if len(valid_times) > 1 else 0.0

    # Post-run cluster metrics
    try:
        from .reporting import cluster_report

        report = cluster_report(client)
        peak_mem = report.peak_memory_gib
        spill = report.total_spill_gib
    except Exception as e:
        errors.append(f"Metrics collection failed: {e}")

    return BenchmarkResult(
        name=name,
        wall_time_seconds=wall_mean,
        wall_time_std=wall_std,
        peak_memory_gib=peak_mem,
        spill_gib=spill,
        n_tasks=n_tasks,
        n_workers=n_workers,
        errors=errors,
        extra=extra or {},
    )


# ---------------------------------------------------------------------------
# benchmark_config
# ---------------------------------------------------------------------------


def benchmark_config(
    configs: "dict[str, DaskSetupConfig] | list[DaskSetupConfig] | DaskSetupConfig",
    ds: Any,
    operation: "Callable[[Any], Any] | str" = "mean",
    *,
    repeats: int = 1,
    warmup: bool = False,
    fallback_on_detection_failure: bool = True,
    verbose: bool = False,
) -> list[BenchmarkResult]:
    """A/B-test multiple configurations against the same xarray operation.

    Each configuration is run in a fresh cluster so that memory state,
    worker topology, and spill behaviour are fully independent between runs.

    Parameters
    ----------
    configs:
        Configurations to compare.  May be:

        - A ``dict[name, DaskSetupConfig]`` — explicit names.
        - A ``list[DaskSetupConfig]`` — names are ``"config_0"``, etc.
        - A single ``DaskSetupConfig`` — returns a one-element list.

    ds:
        xarray Dataset or DataArray to benchmark against.  Should **not**
        already be chunked — :func:`benchmark_config` will chunk it according
        to each config's recommended chunks (or leave it as-is if no chunking
        is suggested).
    operation:
        Operation to time.  Either a callable ``fn(ds) -> lazy_result`` or
        one of the string aliases: ``"mean"``, ``"sum"``, ``"std"``,
        ``"max"``, ``"min"``, ``"var"``, ``"rechunk"``.
    repeats:
        Number of timed repetitions per configuration.  Mean and std-dev
        of wall time are stored in each :class:`BenchmarkResult`.
    warmup:
        If ``True``, run one un-timed warmup pass before the timed repeats.
        Useful for eliminating cold-start overhead (e.g. JIT compilation).
    fallback_on_detection_failure:
        Passed to :func:`~dask_setup.client.setup_dask_client`.
    verbose:
        If ``True``, print a summary line after each configuration completes.

    Returns
    -------
    list[BenchmarkResult]
        One entry per configuration, in the same order as *configs*.

    Examples
    --------
    ::

        from dask_setup import DaskSetupConfig
        from dask_setup.benchmark import benchmark_config

        results = benchmark_config(
            {
                "io":  DaskSetupConfig(workload_type="io"),
                "cpu": DaskSetupConfig(workload_type="cpu"),
            },
            my_ds,
            operation="mean",
            repeats=3,
        )
        for r in sorted(results, key=lambda r: r.wall_time_seconds):
            print(r.summary_line())
    """
    from .client import setup_dask_client

    # Normalise configs to dict[name, config]
    if hasattr(configs, "workload_type"):
        # Single DaskSetupConfig
        configs_dict: dict[str, Any] = {"config_0": configs}
    elif isinstance(configs, list):
        configs_dict = {f"config_{i}": c for i, c in enumerate(configs)}
    else:
        configs_dict = dict(configs)

    operation_fn = _resolve_operation(operation)
    results: list[BenchmarkResult] = []

    for name, cfg in configs_dict.items():
        client = cluster = None
        errors: list[str] = []
        result: BenchmarkResult | None = None

        try:
            client, cluster, _tmp = setup_dask_client(
                config=cfg,
                fallback_on_detection_failure=fallback_on_detection_failure,
                dashboard=False,  # suppress dashboard during benchmarks
            )

            result = _measure_one(
                ds=ds,
                operation_fn=operation_fn,
                client=client,
                repeats=repeats,
                warmup=warmup,
                name=name,
            )

        except Exception as e:
            errors.append(f"Cluster setup failed: {e}")
            result = BenchmarkResult(
                name=name,
                wall_time_seconds=float("nan"),
                errors=errors,
            )
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            if cluster is not None:
                try:
                    cluster.close()
                except Exception:
                    pass

        if result is None:
            result = BenchmarkResult(name=name, wall_time_seconds=float("nan"), errors=errors)

        results.append(result)

        if verbose:
            print(result.summary_line())

    return results


# ---------------------------------------------------------------------------
# scaling_analysis
# ---------------------------------------------------------------------------


def scaling_analysis(
    ds: Any,
    operation: "Callable[[Any], Any] | str" = "mean",
    worker_counts: Sequence[int] = (1, 2, 4, 8),
    *,
    base_config: "DaskSetupConfig | None" = None,
    repeats: int = 1,
    warmup: bool = False,
    fallback_on_detection_failure: bool = True,
    plot: bool = False,
    verbose: bool = False,
) -> ScalingResult:
    """Measure parallel scaling across different worker counts.

    For each entry in *worker_counts*, a fresh cluster is created with
    ``max_workers`` set to that count and the operation is timed.  The
    resulting :class:`ScalingResult` includes speedup and efficiency
    relative to the single-worker baseline.

    Parameters
    ----------
    ds:
        xarray Dataset or DataArray to benchmark.
    operation:
        Callable or string alias (see :func:`benchmark_config`).
    worker_counts:
        Sequence of worker counts to sweep.  The first entry is used as the
        baseline for speedup calculations.
    base_config:
        Base :class:`~dask_setup.config.DaskSetupConfig` to use for each
        run.  ``max_workers`` is overridden per run.  Defaults to
        ``DaskSetupConfig()`` (library defaults).
    repeats:
        Timed repetitions per worker count.
    warmup:
        Run one un-timed warmup pass before timing.
    fallback_on_detection_failure:
        Passed to :func:`~dask_setup.client.setup_dask_client`.
    plot:
        If ``True``, call :meth:`ScalingResult.plot` and display the figure
        (requires matplotlib).
    verbose:
        Print a summary line after each worker count.

    Returns
    -------
    ScalingResult
        Scaling metrics for all worker counts.
    """
    from .client import setup_dask_client
    from .config import DaskSetupConfig

    operation_fn = _resolve_operation(operation)
    counts = list(worker_counts)

    if base_config is None:
        base_config = DaskSetupConfig(fallback_on_detection_failure=True)

    raw_results: list[BenchmarkResult] = []

    for nw in counts:
        # Clone config with this worker count
        cfg_dict = base_config.to_dict()
        cfg_dict["max_workers"] = nw
        # Disable adaptive — we want a fixed worker count
        cfg_dict["adaptive"] = False
        cfg = DaskSetupConfig.from_dict(cfg_dict)

        client = cluster = None
        result: BenchmarkResult | None = None
        errors: list[str] = []

        try:
            client, cluster, _tmp = setup_dask_client(
                config=cfg,
                fallback_on_detection_failure=fallback_on_detection_failure,
                dashboard=False,
            )
            result = _measure_one(
                ds=ds,
                operation_fn=operation_fn,
                client=client,
                repeats=repeats,
                warmup=warmup,
                name=f"workers={nw}",
            )
        except Exception as e:
            errors.append(f"Cluster setup failed: {e}")
            result = BenchmarkResult(
                name=f"workers={nw}",
                wall_time_seconds=float("nan"),
                errors=errors,
            )
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            if cluster is not None:
                try:
                    cluster.close()
                except Exception:
                    pass

        raw_results.append(result)

        if verbose:
            print(result.summary_line())

    # Compute speedup and efficiency relative to the first (baseline) run
    baseline_time = raw_results[0].wall_time_seconds if raw_results else 1.0
    if not baseline_time or baseline_time != baseline_time:  # nan check
        baseline_time = 1.0

    speedups = []
    efficiencies = []
    for r, nw in zip(raw_results, counts):
        if r.wall_time_seconds and r.wall_time_seconds == r.wall_time_seconds:
            sp = baseline_time / r.wall_time_seconds
        else:
            sp = float("nan")
        speedups.append(sp)
        efficiencies.append(sp / nw if nw > 0 else float("nan"))

    scaling = ScalingResult(
        results=raw_results,
        worker_counts=counts,
        speedups=speedups,
        efficiencies=efficiencies,
    )

    if plot:
        fig = scaling.plot()
        if fig is not None:
            try:
                import matplotlib.pyplot as plt

                plt.show()
            except Exception:
                pass

    return scaling


# ---------------------------------------------------------------------------
# chunk_impact
# ---------------------------------------------------------------------------


def chunk_impact(
    ds: Any,
    client: "Client",
    operation: "Callable[[Any], Any] | str" = "mean",
    chunk_sizes: list[dict[str, int]] | None = None,
    *,
    auto_chunks: bool = True,
    repeats: int = 1,
    warmup: bool = False,
    plot: bool = False,
    verbose: bool = False,
) -> ChunkImpactResult:
    """Sweep chunk sizes on a fixed cluster to find the throughput sweet spot.

    Unlike :func:`benchmark_config` and :func:`scaling_analysis`, this
    function **does not** create or destroy clusters — it uses the *client*
    you provide.  The dataset is rechunked before each measurement.

    Parameters
    ----------
    ds:
        xarray Dataset or DataArray.  Must have known dimension sizes.
    client:
        Connected Dask client.  The cluster remains running throughout.
    operation:
        Callable or string alias (see :func:`benchmark_config`).
    chunk_sizes:
        List of chunk-size dicts to try (e.g.
        ``[{"time": 10, "lat": 50}, {"time": 20, "lat": 100}]``).
        If ``None`` and ``auto_chunks=True``, a geometric series of five
        sizes is generated automatically.
    auto_chunks:
        Generate chunk sizes automatically when *chunk_sizes* is ``None``.
    repeats:
        Timed repetitions per chunk size.
    warmup:
        Run one un-timed warmup pass per chunk size.
    plot:
        Display the wall-time vs chunk-size plot (requires matplotlib).
    verbose:
        Print a summary line after each chunk size is tested.

    Returns
    -------
    ChunkImpactResult
        Timing results for each chunk spec, plus the recommended spec.
    """
    operation_fn = _resolve_operation(operation)

    # Determine dimension sizes
    try:
        if hasattr(ds, "sizes"):
            dims: dict[str, int] = dict(ds.sizes)
        elif hasattr(ds, "dims") and hasattr(ds, "shape"):
            dims = dict(zip(ds.dims, ds.shape, strict=False))
        else:
            dims = {}
    except Exception:
        dims = {}

    # Generate chunk size sweep if not provided
    if chunk_sizes is None:
        if auto_chunks and dims:
            chunk_sizes = _generate_auto_chunks(dims)
        else:
            chunk_sizes = [{}]  # single run with no rechunking

    raw_results: list[BenchmarkResult] = []

    for cs in chunk_sizes:
        errors: list[str] = []
        ds_chunked = ds

        if cs:
            try:
                ds_chunked = ds.chunk(cs)
            except Exception as e:
                errors.append(f"Rechunk failed for {cs}: {e}")

        result = _measure_one(
            ds=ds_chunked,
            operation_fn=operation_fn,
            client=client,
            repeats=repeats,
            warmup=warmup,
            name=str(cs),
            extra={"chunks": cs},
        )
        result.errors.extend(errors)
        raw_results.append(result)

        if verbose:
            print(result.summary_line())

    # Recommended = fastest valid run
    valid = [
        (r, cs)
        for r, cs in zip(raw_results, chunk_sizes)
        if r.wall_time_seconds == r.wall_time_seconds  # not nan
    ]
    if valid:
        best_result, best_chunks = min(valid, key=lambda x: x[0].wall_time_seconds)
    else:
        best_chunks = chunk_sizes[0] if chunk_sizes else {}

    impact = ChunkImpactResult(
        results=raw_results,
        chunk_sizes=chunk_sizes,
        recommended_chunks=best_chunks,
    )

    if plot:
        first_dim = next(iter(dims), None)
        fig = impact.plot(dim=first_dim)
        if fig is not None:
            try:
                import matplotlib.pyplot as plt

                plt.show()
            except Exception:
                pass

    return impact


def _generate_auto_chunks(dims: dict[str, int]) -> list[dict[str, int]]:
    """Generate a 5-point geometric sweep of chunk sizes for *dims*."""
    # Use ×0.125, ×0.25, ×0.5, ×1, ×2 of the full dimension size,
    # clamped so each chunk has at least 1 element and at most the full dim.
    base_fractions = [0.125, 0.25, 0.5, 1.0, 2.0]
    # We only chunk dimensions with > 1 element; leave tiny dims unchunked
    chunkable = {k: v for k, v in dims.items() if v > 1}
    if not chunkable:
        return [{}]

    result = []
    for frac in base_fractions:
        cs = {}
        for dim, size in chunkable.items():
            chunk_val = max(1, min(size, int(size * frac)))
            cs[dim] = chunk_val
        result.append(cs)

    # Deduplicate while preserving order
    seen: list[dict[str, int]] = []
    for cs in result:
        if cs not in seen:
            seen.append(cs)
    return seen


# ---------------------------------------------------------------------------
# run_synthetic_benchmark — used by dask-setup benchmark CLI
# ---------------------------------------------------------------------------


@dataclass
class SyntheticBenchmarkResult:
    """Result of a CLI synthetic benchmark run.

    Uses ``dask.array`` directly — no xarray needed.
    """

    profile_name: str
    operation: str
    ds_size: str
    array_shape: tuple[int, ...]
    wall_time_seconds: float
    peak_memory_gib: float
    spill_gib: float
    n_tasks: int
    n_workers: int
    tasks_per_second: float
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        lines = [
            f"Profile   : {self.profile_name}",
            f"Operation : {self.operation}",
            f"Array     : {self.array_shape} (size={self.ds_size})",
            f"Workers   : {self.n_workers}",
            f"Wall time : {self.wall_time_seconds:.2f}s",
            f"Tasks     : {self.n_tasks}  ({self.tasks_per_second:.1f} tasks/s)",
            f"Peak mem  : {self.peak_memory_gib:.2f} GiB",
        ]
        if self.spill_gib:
            lines.append(f"Spill     : {self.spill_gib:.2f} GiB")
        if self.errors:
            lines.append(f"Errors    : {'; '.join(self.errors)}")
        return "\n".join(lines)


# Synthetic dataset shapes: (rows, cols, depth)
_SYNTHETIC_SHAPES: dict[str, tuple[int, ...]] = {
    "tiny": (200, 200, 10),
    "small": (500, 500, 20),
    "medium": (1000, 1000, 50),
    "large": (2000, 2000, 100),
}

# Chunk sizes to use with each shape: roughly 32 MiB per float64 chunk
_SYNTHETIC_CHUNKS: dict[str, tuple[int, ...]] = {
    "tiny": (50, 50, 10),
    "small": (100, 100, 20),
    "medium": (200, 200, 25),
    "large": (400, 400, 25),
}


def run_synthetic_benchmark(
    profile_name: str = "development",
    operation: str = "mean",
    ds_size: str = "small",
    repeats: int = 1,
    fallback_on_detection_failure: bool = True,
    verbose: bool = False,
) -> SyntheticBenchmarkResult:
    """Run a synthetic benchmark using ``dask.array`` (no xarray required).

    Creates a random floating-point array of the given size, sets up a Dask
    cluster from the named profile, and times the requested operation.

    This function is the backend for the ``dask-setup benchmark`` CLI
    subcommand.

    Parameters
    ----------
    profile_name:
        Name of a builtin or user profile to use.  ``"development"`` is the
        safe default for quick tests.
    operation:
        One of ``"mean"``, ``"sum"``, ``"std"``, ``"max"``, ``"min"``.
    ds_size:
        One of ``"tiny"``, ``"small"``, ``"medium"``, ``"large"``.
    repeats:
        Timed repetitions.
    fallback_on_detection_failure:
        Passed to :func:`~dask_setup.client.setup_dask_client`.
    verbose:
        Print progress messages.

    Returns
    -------
    SyntheticBenchmarkResult
    """
    import dask.array as da
    import numpy as np

    from .client import setup_dask_client
    from .config_manager import ConfigManager
    from .reporting import cluster_report

    if ds_size not in _SYNTHETIC_SHAPES:
        raise ValueError(
            f"Unknown ds_size {ds_size!r}. Valid: {sorted(_SYNTHETIC_SHAPES)}"
        )

    shape = _SYNTHETIC_SHAPES[ds_size]
    chunks = _SYNTHETIC_CHUNKS[ds_size]

    if verbose:
        print(f"Creating synthetic array {shape} chunked {chunks} …")

    np.random.seed(42)
    arr = da.from_array(np.random.rand(*shape).astype(np.float32), chunks=chunks)

    # Resolve dask operation
    da_ops: dict[str, Callable[[Any], Any]] = {
        "mean": lambda a: a.mean(),
        "sum": lambda a: a.sum(),
        "std": lambda a: a.std(),
        "max": lambda a: a.max(),
        "min": lambda a: a.min(),
    }
    if operation not in da_ops:
        raise ValueError(
            f"Unknown operation {operation!r} for synthetic benchmark. "
            f"Valid: {sorted(da_ops)}"
        )
    op_fn = da_ops[operation]

    # Task count
    lazy = op_fn(arr)
    n_tasks = _count_tasks(lazy)

    errors: list[str] = []
    times: list[float] = []
    n_workers = 0
    peak_mem = 0.0
    spill = 0.0

    # Load profile
    manager = ConfigManager()
    profile = manager.get_profile(profile_name)
    if profile is None:
        raise ValueError(
            f"Profile {profile_name!r} not found. "
            f"Available: {sorted(manager.list_profiles())}"
        )

    client = cluster_obj = None
    try:
        client, cluster_obj, _tmp = setup_dask_client(
            config=profile.config,
            fallback_on_detection_failure=fallback_on_detection_failure,
            dashboard=False,
        )

        n_workers = len(client.scheduler_info().get("workers", {}))

        if verbose:
            print(f"Cluster ready: {n_workers} workers. Running {operation!r} × {repeats} …")

        for i in range(repeats):
            t0 = time.monotonic()
            try:
                op_fn(arr).compute()
            except Exception as e:
                errors.append(f"Run {i + 1} failed: {e}")
                times.append(float("inf"))
            else:
                times.append(time.monotonic() - t0)

        report = cluster_report(client)
        peak_mem = report.peak_memory_gib
        spill = report.total_spill_gib

    except Exception as e:
        errors.append(f"Cluster error: {e}")
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        if cluster_obj is not None:
            try:
                cluster_obj.close()
            except Exception:
                pass

    valid_times = [t for t in times if t != float("inf")]
    wall = statistics.mean(valid_times) if valid_times else float("nan")
    tps = n_tasks / wall if (n_tasks > 0 and wall == wall and wall > 0) else 0.0

    return SyntheticBenchmarkResult(
        profile_name=profile_name,
        operation=operation,
        ds_size=ds_size,
        array_shape=shape,
        wall_time_seconds=wall,
        peak_memory_gib=peak_mem,
        spill_gib=spill,
        n_tasks=n_tasks,
        n_workers=n_workers,
        tasks_per_second=tps,
        errors=errors,
    )
