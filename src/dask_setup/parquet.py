"""Parquet / Arrow partition recommendations for Dask DataFrame workloads.

Provides :func:`recommend_parquet_chunks` — the Dask-DataFrame analogue of
:func:`~dask_setup.io_patterns.recommend_io_chunks` for xarray datasets.

Key design choices
------------------
- Partition size, not chunk shape, is the primary tuning parameter for Dask
  DataFrames.  The goal is ~128 – 512 MiB per partition for most workloads.
- Column selection matters: wide tables with many unused columns inflate
  partition sizes unnecessarily; the helper warns when this is likely.
- Compression defaults follow Arrow / Parquet best practices: ``snappy`` for
  speed-sensitive HPC pipelines, ``zstd`` when storage footprint matters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dask.distributed import Client

try:
    from .error_handling import DependencyError
except ImportError:

    class DependencyError(Exception):  # type: ignore[no-redef]
        pass


__all__ = [
    "ParquetRecommendation",
    "recommend_parquet_chunks",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default target partition size range (min, max) in MiB.
_DEFAULT_PARTITION_MB: tuple[float, float] = (128.0, 512.0)

#: Warn when average row size times this factor exceeds the upper target.
_WIDE_TABLE_WARN_COLUMNS: int = 200

#: Average bytes per row assumed when schema info is unavailable.
_FALLBACK_ROW_BYTES: int = 256


# ---------------------------------------------------------------------------
# Public dataclass
# ---------------------------------------------------------------------------


@dataclass
class ParquetRecommendation:
    """Container for Parquet / Arrow partition recommendations.

    Attributes
    ----------
    rows_per_partition:
        Recommended number of rows per Dask partition.
    compression:
        Recommended compression codec and options dict, suitable for passing
        to ``dask.dataframe.to_parquet(..., compression=...)``.
    storage_options:
        Recommended ``storage_options`` dict for cloud or network targets.
    estimated_partition_mb:
        Estimated size of each partition in MiB after applying the
        recommended row count.
    warnings:
        List of advisory messages (empty when all looks healthy).
    extra:
        Format-specific extras, e.g. ``row_group_size`` for Parquet v2.
    """

    rows_per_partition: int
    compression: str
    storage_options: dict[str, Any]
    estimated_partition_mb: float
    warnings: list[str]
    extra: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return (
            f"ParquetRecommendation("
            f"rows_per_partition={self.rows_per_partition:,}, "
            f"compression='{self.compression}', "
            f"~{self.estimated_partition_mb:.1f} MiB/partition)"
        )

    def summary(self) -> str:
        """Return a one-line human-readable summary."""
        warn_str = f"  [{len(self.warnings)} warning(s)]" if self.warnings else ""
        return (
            f"Parquet: {self.rows_per_partition:,} rows/partition "
            f"(~{self.estimated_partition_mb:.1f} MiB), "
            f"compression={self.compression}{warn_str}"
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def recommend_parquet_chunks(
    df: Any,
    client: "Client | None" = None,
    path_or_url: str | None = None,
    target_partition_mb: tuple[float, float] = _DEFAULT_PARTITION_MB,
    compression: str = "auto",
    storage_location: str = "auto",
    verbose: bool = False,
) -> ParquetRecommendation | int:
    """Recommend partition sizes for Dask DataFrame Parquet workloads.

    Parameters
    ----------
    df:
        A Dask or pandas DataFrame (or any object with ``dtypes``, ``columns``,
        and optionally ``memory_usage()``).
    client:
        Optional Dask :class:`~dask.distributed.Client`.  When provided,
        per-worker memory limits are used to tighten the upper partition bound.
    path_or_url:
        Destination path or URL.  Used to infer storage location and to
        recommend appropriate ``storage_options``.
    target_partition_mb:
        ``(min_mb, max_mb)`` target partition size range.  Defaults to
        ``(128, 512)``.
    compression:
        Parquet compression codec.  ``"auto"`` picks ``"snappy"`` for local
        storage and ``"zstd"`` for cloud / network.
    storage_location:
        One of ``"local"``, ``"cloud"``, ``"network"``, or ``"auto"``.
    verbose:
        If ``True``, return the full :class:`ParquetRecommendation` and print
        a report.  If ``False`` (default), return ``rows_per_partition`` only.

    Returns
    -------
    :class:`ParquetRecommendation` when *verbose* is ``True``, otherwise the
    recommended ``rows_per_partition`` as an :class:`int`.
    """
    warnings_list: list[str] = []

    # ------------------------------------------------------------------
    # Resolve storage location
    # ------------------------------------------------------------------
    if storage_location == "auto":
        if path_or_url:
            if any(
                path_or_url.startswith(p) for p in ["s3://", "gs://", "azure://", "abfs://"]
            ):
                storage_location = "cloud"
            elif any(path_or_url.startswith(p) for p in ["http://", "https://", "ftp://"]):
                storage_location = "network"
            else:
                storage_location = "local"
        else:
            storage_location = "local"

    # ------------------------------------------------------------------
    # Resolve compression
    # ------------------------------------------------------------------
    if compression == "auto":
        compression = "zstd" if storage_location in ("cloud", "network") else "snappy"

    # ------------------------------------------------------------------
    # Estimate bytes per row
    # ------------------------------------------------------------------
    bytes_per_row = _estimate_bytes_per_row(df, warnings_list)

    # ------------------------------------------------------------------
    # Tighten upper bound using per-worker memory if client is available
    # ------------------------------------------------------------------
    max_partition_mb = target_partition_mb[1]
    if client is not None:
        try:
            info = client.scheduler_info()
            workers = info.get("workers", {})
            if workers:
                min_worker_mem_bytes = min(
                    w.get("memory_limit", float("inf")) for w in workers.values()
                )
                if min_worker_mem_bytes < float("inf"):
                    # Target at most 10 % of the smallest worker's memory per partition
                    worker_cap_mb = (min_worker_mem_bytes * 0.10) / (1024 * 1024)
                    if worker_cap_mb < max_partition_mb:
                        max_partition_mb = worker_cap_mb
        except Exception:
            pass  # Non-fatal — proceed with defaults

    # ------------------------------------------------------------------
    # Compute rows per partition
    # ------------------------------------------------------------------
    target_max_bytes = int(max_partition_mb * 1024 * 1024)
    target_min_bytes = int(target_partition_mb[0] * 1024 * 1024)

    if bytes_per_row <= 0:
        bytes_per_row = _FALLBACK_ROW_BYTES
        warnings_list.append(
            "Could not determine row size — using fallback estimate of "
            f"{_FALLBACK_ROW_BYTES} bytes/row."
        )

    rows_per_partition = max(1, target_max_bytes // bytes_per_row)
    estimated_mb = (rows_per_partition * bytes_per_row) / (1024 * 1024)

    # Warn if resulting partition is smaller than the minimum target
    if estimated_mb < target_partition_mb[0]:
        warnings_list.append(
            f"Estimated partition size ({estimated_mb:.1f} MiB) is below the "
            f"minimum target ({target_partition_mb[0]:.0f} MiB).  "
            "Consider coalescing partitions with df.repartition()."
        )

    # ------------------------------------------------------------------
    # Storage options
    # ------------------------------------------------------------------
    storage_options = _build_storage_options(path_or_url or "", storage_location)

    # ------------------------------------------------------------------
    # Extra hints (row group size = partition size for single-file writes)
    # ------------------------------------------------------------------
    extra: dict[str, Any] = {
        "row_group_size": rows_per_partition,
        "write_metadata_file": storage_location != "cloud",  # avoid small-file overhead
    }

    # ------------------------------------------------------------------
    # Build recommendation
    # ------------------------------------------------------------------
    rec = ParquetRecommendation(
        rows_per_partition=rows_per_partition,
        compression=compression,
        storage_options=storage_options,
        estimated_partition_mb=estimated_mb,
        warnings=warnings_list,
        extra=extra,
    )

    if verbose:
        _print_report(rec, storage_location)
        return rec

    return rows_per_partition


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _estimate_bytes_per_row(df: Any, warnings_list: list[str]) -> int:
    """Return an estimate of the in-memory bytes per row for *df*."""
    # pandas / Dask DataFrame: use memory_usage() if available
    try:
        # Dask DataFrames expose _meta which is a pandas DataFrame
        meta = getattr(df, "_meta", df)
        if hasattr(meta, "memory_usage"):
            mem = meta.memory_usage(deep=True)
            nrows = len(meta)
            if nrows > 0:
                total_bytes = int(mem.sum())
                return max(1, total_bytes // nrows)
    except Exception:
        pass

    # Fallback: sum dtype itemsizes across columns
    try:
        import numpy as np

        dtypes = getattr(df, "dtypes", None)
        if dtypes is not None:
            total = sum(
                np.dtype(dt).itemsize
                for dt in dtypes
                if hasattr(dt, "itemsize") or _is_numeric_dtype_str(str(dt))
            )
            n_cols = len(list(dtypes))
            if n_cols > _WIDE_TABLE_WARN_COLUMNS:
                warnings_list.append(
                    f"Wide table detected ({n_cols} columns). "
                    "Consider selecting only required columns before writing."
                )
            if total > 0:
                return total
    except Exception:
        pass

    return _FALLBACK_ROW_BYTES


def _is_numeric_dtype_str(dtype_str: str) -> bool:
    """Return True if the dtype string looks numeric."""
    return any(t in dtype_str for t in ["int", "float", "uint", "complex"])


def _build_storage_options(path_or_url: str, storage_location: str) -> dict[str, Any]:
    """Build storage_options suitable for dask.dataframe.to_parquet."""
    options: dict[str, Any] = {}
    if path_or_url.startswith("s3://"):
        options.update(
            {
                "anon": False,
                "default_cache_type": "readahead",
            }
        )
    elif path_or_url.startswith("gs://"):
        options.update({"default_cache_type": "readahead"})
    elif path_or_url.startswith(("http://", "https://")):
        options.update({"default_cache_type": "readahead"})
    return options


def _print_report(rec: ParquetRecommendation, storage_location: str) -> None:
    """Print a human-readable Parquet recommendation report."""
    print(" Parquet Partition Recommendations")
    print("=" * 42)
    print(f" Storage location : {storage_location}")
    print(f" Rows per partition: {rec.rows_per_partition:,}")
    print(f" Est. partition size: {rec.estimated_partition_mb:.1f} MiB")
    print(f" Compression       : {rec.compression}")
    print(f" Row group size    : {rec.extra.get('row_group_size', 'N/A'):,}")

    if rec.storage_options:
        print(f" Storage options   : {rec.storage_options}")

    if rec.warnings:
        print("\n Warnings:")
        for w in rec.warnings:
            print(f"  • {w}")

    print("\n Usage:")
    print(
        f"   df.to_parquet(path, compression='{rec.compression}', "
        f"write_index=False)"
    )
    print(f"   # Or repartition first: df.repartition(npartitions=...)")
