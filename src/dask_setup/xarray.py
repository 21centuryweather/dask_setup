"""Xarray integration helpers for automatic chunk size recommendations.

Provides intelligent chunking recommendations based on:
- Available worker memory and cluster topology
- Dataset characteristics (dimensions, dtypes, current chunking)
- Workload type (CPU-bound, I/O-bound, or mixed)

The primary goal is to optimize chunk sizes for memory efficiency while maintaining
good parallelization, typically targeting 256-512 MiB chunks per worker.
"""

from __future__ import annotations

import math
import warnings
from typing import TYPE_CHECKING, Any

try:
    from .error_handling import DependencyError
except ImportError:
    # Fallback for when error_handling isn't available
    class DependencyError(Exception):
        pass


if TYPE_CHECKING:
    from dask.distributed import Client

try:
    import numpy as np
except ImportError:
    np = None

try:
    import xarray as xr
except ImportError:
    xr = None

import psutil

__all__ = ["recommend_chunks"]


class XarrayDependencyError(ImportError):
    """Raised when xarray functionality is used but xarray is not available."""


class ChunkRecommendation:
    """Container for chunk recommendation results."""

    def __init__(
        self,
        chunks: dict[str, int],
        estimated_chunk_mb: float,
        total_chunks: int,
        warnings_list: list[str] | None = None,
        dataset_info: dict[str, Any] | None = None,
    ):
        self.chunks = chunks
        self.estimated_chunk_mb = estimated_chunk_mb
        self.total_chunks = total_chunks
        self.warnings = warnings_list or []
        self.dataset_info = dataset_info or {}

    def __repr__(self) -> str:
        return (
            f"ChunkRecommendation(chunks={self.chunks}, "
            f"estimated_chunk_mb={self.estimated_chunk_mb:.1f}, "
            f"total_chunks={self.total_chunks})"
        )


def _ensure_xarray_available() -> None:
    """Check if xarray is available, raise helpful error if not."""
    if xr is None:
        raise DependencyError(missing_package="xarray", feature="xarray chunking recommendations")
    if np is None:
        raise DependencyError(missing_package="numpy", feature="xarray chunking recommendations")


def _get_cluster_info(client: Client | None) -> dict[str, Any]:
    """Extract worker configuration from a Dask client or fallback to system info.

    Args:
        client: Active Dask client, or None to use system defaults

    Returns:
        Dict with keys: n_workers, threads_per_worker, memory_limit_bytes, total_memory_bytes
    """
    if client is not None:
        try:
            # Get info from active cluster
            scheduler_info = client.scheduler_info()
            workers = scheduler_info.get("workers", {})

            if not workers:
                # Fallback if no workers detected
                n_workers = 1
                threads_per_worker = 1
                memory_limit_bytes = int(psutil.virtual_memory().total * 0.8)
            else:
                # Extract from first worker (assume homogeneous cluster)
                first_worker = next(iter(workers.values()))
                n_workers = len(workers)
                threads_per_worker = first_worker.get("nthreads", 1)
                memory_limit_bytes = first_worker.get("memory_limit", 0)

                # Fallback for memory limit if not set
                if memory_limit_bytes <= 0:
                    total_system_mem = psutil.virtual_memory().total
                    memory_limit_bytes = int(total_system_mem * 0.8 / n_workers)

            total_memory_bytes = n_workers * memory_limit_bytes

        except Exception:
            # Fallback on any client query failure
            n_workers = 1
            threads_per_worker = 1
            total_memory_bytes = int(psutil.virtual_memory().total * 0.8)
            memory_limit_bytes = total_memory_bytes
    else:
        # No client provided - use system defaults
        n_workers = psutil.cpu_count(logical=False) or 4  # physical cores
        threads_per_worker = 1
        total_memory_bytes = int(psutil.virtual_memory().total * 0.8)
        memory_limit_bytes = total_memory_bytes // n_workers

    return {
        "n_workers": n_workers,
        "threads_per_worker": threads_per_worker,
        "memory_limit_bytes": memory_limit_bytes,
        "total_memory_bytes": total_memory_bytes,
    }


def _analyze_dataset(ds: xr.Dataset | xr.DataArray) -> dict[str, Any]:
    """Analyze xarray dataset characteristics for chunking decisions.

    Args:
        ds: xarray Dataset or DataArray to analyze

    Returns:
        Dict with dataset analysis results
    """
    _ensure_xarray_available()

    # Handle both Dataset and DataArray
    if isinstance(ds, xr.DataArray):
        data_vars = {ds.name or "data": ds}
        dims = dict(zip(ds.dims, ds.shape, strict=False))
        coords = ds.coords
    else:
        data_vars = ds.data_vars
        dims = dict(ds.sizes)  # Use sizes to get {dim_name: length}
        coords = ds.coords

    # Analyze variables
    var_info = {}
    total_uncompressed_bytes = 0

    for var_name, var in data_vars.items():
        if not hasattr(var, "dtype") or not hasattr(var, "shape"):
            continue

        dtype_size = np.dtype(var.dtype).itemsize
        var_size = int(np.prod(var.shape)) * dtype_size
        total_uncompressed_bytes += var_size

        var_info[var_name] = {
            "dtype": str(var.dtype),
            "shape": var.shape,
            "dims": list(var.dims),
            "size_bytes": var_size,
            "current_chunks": getattr(var, "chunks", None),
        }

    # Get current chunking state
    current_chunking = {}
    is_currently_chunked = False

    if hasattr(ds, "chunks") and ds.chunks is not None:
        # Dataset/DataArray with existing chunks
        try:
            if isinstance(ds, xr.DataArray) and hasattr(ds.data, "chunks"):
                # DataArray with dask array backend
                if ds.data.chunks is not None:
                    for i, dim_name in enumerate(ds.dims):
                        if i < len(ds.data.chunks):
                            current_chunking[dim_name] = ds.data.chunks[i]
                    is_currently_chunked = True
            else:
                # Dataset chunks - this should be a dict of {dim: chunk_tuple}
                if hasattr(ds.chunks, "items"):  # Ensure it's dict-like
                    current_chunking = dict(ds.chunks)
                    is_currently_chunked = bool(current_chunking)
        except (TypeError, ValueError, AttributeError):
            # Fallback - could be an unchunked object or chunks=None
            current_chunking = {}
            is_currently_chunked = False

    return {
        "dims": dims,
        "coords": dict(coords),
        "variables": var_info,
        "total_uncompressed_bytes": total_uncompressed_bytes,
        "current_chunking": current_chunking,
        "is_currently_chunked": is_currently_chunked,
    }


def _calculate_optimal_chunks(
    dataset_info: dict[str, Any],
    cluster_info: dict[str, Any],
    workload_type: str = "auto",
    target_chunk_mb: tuple[float, float] = (256, 512),
    safety_factor: float = 0.6,
) -> ChunkRecommendation:
    """Calculate optimal chunk sizes based on dataset and cluster characteristics.

    Args:
        dataset_info: Dataset analysis from _analyze_dataset
        cluster_info: Cluster info from _get_cluster_info
        workload_type: "cpu", "io", "mixed", or "auto"
        target_chunk_mb: (min, max) target chunk size in MiB
        safety_factor: Fraction of worker memory to use (0.6 = 60%)

    Returns:
        ChunkRecommendation object
    """
    dims = dataset_info["dims"]
    current_chunking = dataset_info["current_chunking"]
    variables = dataset_info["variables"]

    if not variables:
        # No data variables to chunk
        return ChunkRecommendation({}, 0.0, 0)

    # Calculate memory constraints
    worker_memory_bytes = cluster_info["memory_limit_bytes"]
    max_chunk_bytes = int(worker_memory_bytes * safety_factor)
    int(target_chunk_mb[0] * 1024 * 1024)
    target_max_bytes = int(target_chunk_mb[1] * 1024 * 1024)

    # Ensure target fits within memory constraint
    effective_target_max = min(target_max_bytes, max_chunk_bytes)

    # Auto-detect workload type if needed
    if workload_type == "auto":
        # Simple heuristic: if dataset has time-like dimension, assume I/O-heavy
        time_like_dims = [
            d for d in dims if any(t in d.lower() for t in ["time", "date", "step", "record"])
        ]
        if time_like_dims:
            workload_type = "io"
        elif len(dims) >= 3:
            workload_type = "mixed"
        else:
            workload_type = "cpu"

    # Choose chunking strategy based on workload type
    recommended_chunks = {}
    warnings_list = []

    # Get the largest variable to base chunking on
    main_var = max(variables.values(), key=lambda v: v["size_bytes"])
    main_var_dims = main_var["dims"]

    # Start with current chunking or full dimensions
    working_chunks = {}
    for dim in main_var_dims:
        if dim in current_chunking:
            # Use first chunk size as representative
            current_chunk_sizes = current_chunking[dim]
            if isinstance(current_chunk_sizes, list | tuple) and current_chunk_sizes:
                working_chunks[dim] = current_chunk_sizes[0]
            else:
                working_chunks[dim] = dims[dim]
        else:
            working_chunks[dim] = dims[dim]

    # Calculate bytes per element for the main variable
    dtype_size = np.dtype(main_var["dtype"]).itemsize

    def _estimate_chunk_bytes(chunk_dict: dict[str, int]) -> float:
        """Estimate memory usage for a chunk configuration."""
        chunk_size = dtype_size
        for dim in main_var_dims:
            chunk_size *= chunk_dict.get(dim, dims[dim])
        return float(chunk_size)

    # Apply chunking strategy based on workload type
    if workload_type == "io":
        # I/O workloads: favor streaming patterns, chunk along record dimensions
        record_dims = [
            d
            for d in main_var_dims
            if any(t in d.lower() for t in ["time", "date", "step", "record", "sample"])
        ]

        if record_dims:
            # Chunk along record dimension to enable streaming
            primary_dim = record_dims[0]
            # Keep other dimensions unchunked for efficient I/O
            for dim in main_var_dims:
                if dim != primary_dim:
                    working_chunks[dim] = dims[dim]

            # Calculate optimal chunk size for primary dimension
            other_elements = 1
            for dim in main_var_dims:
                if dim != primary_dim:
                    other_elements *= dims[dim]

            bytes_per_primary_element = dtype_size * other_elements
            optimal_primary_chunks = min(
                dims[primary_dim], max(1, effective_target_max // bytes_per_primary_element)
            )
            working_chunks[primary_dim] = optimal_primary_chunks

    elif workload_type == "cpu":
        # CPU workloads: favor square-ish chunks for compute efficiency
        # Try to make chunks roughly equal in all dimensions
        current_bytes = _estimate_chunk_bytes(working_chunks)

        while current_bytes > effective_target_max and any(
            working_chunks[d] > 1 for d in main_var_dims
        ):
            # Find largest chunkable dimension
            largest_dim = max(
                [d for d in main_var_dims if working_chunks[d] > 1],
                key=lambda d: working_chunks[d],
                default=None,
            )
            if largest_dim is None:
                break

            # Halve the largest dimension
            working_chunks[largest_dim] = max(1, working_chunks[largest_dim] // 2)
            current_bytes = _estimate_chunk_bytes(working_chunks)

    else:  # mixed workload
        # Mixed: balance between streaming and compute efficiency
        # Prefer to chunk 2D spatial dimensions while keeping time contiguous
        spatial_dims = [
            d
            for d in main_var_dims
            if any(t in d.lower() for t in ["x", "y", "z", "lat", "lon", "latitude", "longitude"])
        ]
        time_dims = [
            d for d in main_var_dims if any(t in d.lower() for t in ["time", "date", "step"])
        ]

        # Keep time dimensions large, chunk spatial dimensions
        current_bytes = _estimate_chunk_bytes(working_chunks)

        while current_bytes > effective_target_max:
            # Prefer to chunk spatial dimensions first
            candidate_dims = (
                spatial_dims
                if spatial_dims
                else [d for d in main_var_dims if working_chunks[d] > 1 and d not in time_dims]
            )

            if not candidate_dims:
                # Fall back to chunking any dimension
                candidate_dims = [d for d in main_var_dims if working_chunks[d] > 1]

            if not candidate_dims:
                break

            # Chunk the largest spatial dimension
            largest_dim = max(candidate_dims, key=lambda d: working_chunks[d])
            working_chunks[largest_dim] = max(1, working_chunks[largest_dim] // 2)
            current_bytes = _estimate_chunk_bytes(working_chunks)

    # Ensure minimum parallelism
    n_workers = int(cluster_info["n_workers"])  # Ensure it's an int, not Mock
    total_chunks = 1
    for dim in main_var_dims:
        chunk_size = working_chunks[dim]
        n_chunks_in_dim = max(1, math.ceil(dims[dim] / chunk_size))
        total_chunks *= n_chunks_in_dim

    # Adjust if we have too few chunks for good parallelism
    if total_chunks < n_workers and any(working_chunks[d] < dims[d] for d in main_var_dims):
        warnings_list.append(
            f"Generated {total_chunks} chunks for {n_workers} workers. "
            "Consider using fewer workers or rechunking manually for better parallelism."
        )

    # Cap at reasonable chunk count to avoid task graph explosion
    max_reasonable_chunks = max(n_workers * 4, 32)
    if total_chunks > max_reasonable_chunks:
        warnings_list.append(
            f"Generated {total_chunks} chunks, which may create a large task graph. "
            "Consider using larger chunk sizes if memory permits."
        )

    # Extract final recommendations (only include dimensions that are actually chunked)
    for dim in main_var_dims:
        chunk_size = working_chunks[dim]
        if chunk_size < dims[dim]:  # Only include if actually chunking
            recommended_chunks[dim] = chunk_size

    # Final chunk size estimate
    final_chunk_bytes = _estimate_chunk_bytes(working_chunks)
    final_chunk_mb = final_chunk_bytes / (1024 * 1024)

    # Add warnings for problematic chunk sizes
    if final_chunk_mb > target_chunk_mb[1] * 2:
        warnings_list.append(
            f"Recommended chunk size ({final_chunk_mb:.1f} MiB) is quite large. "
            "Consider reducing chunk dimensions for better memory efficiency."
        )
    elif final_chunk_mb < target_chunk_mb[0] / 2:
        warnings_list.append(
            f"Recommended chunk size ({final_chunk_mb:.1f} MiB) is quite small. "
            "This may lead to high task overhead."
        )

    return ChunkRecommendation(
        chunks=recommended_chunks,
        estimated_chunk_mb=final_chunk_mb,
        total_chunks=total_chunks,
        warnings_list=warnings_list,
        dataset_info={"workload_type": workload_type, "safety_factor": safety_factor},
    )


def _format_chunk_report(
    recommendation: ChunkRecommendation,
    dataset_info: dict[str, Any],
    verbose: bool = True,
) -> str:
    """Format a human-readable report of chunk recommendations.

    Args:
        recommendation: ChunkRecommendation object
        dataset_info: Dataset analysis results
        verbose: Whether to include detailed comparison

    Returns:
        Formatted report string
    """
    lines = []

    # Header
    lines.append("📊 Xarray Chunking Recommendations")
    lines.append("=" * 35)

    # Recommended chunks
    if recommendation.chunks:
        lines.append(f" Recommended chunks: {dict(recommendation.chunks)}")
        lines.append(f" Estimated chunk size: {recommendation.estimated_chunk_mb:.1f} MiB")
        lines.append(f" Total chunks: {recommendation.total_chunks}")
    else:
        lines.append(" No chunking recommended (dataset fits comfortably in memory)")

    # Current vs recommended comparison
    if verbose and dataset_info["is_currently_chunked"]:
        lines.append("")
        lines.append("Current vs Recommended:")
        lines.append("-" * 25)

        current = dataset_info["current_chunking"]
        for dim in set(current.keys()) | set(recommendation.chunks.keys()):
            current_val = current.get(dim, "unchunked")
            recommended_val = recommendation.chunks.get(dim, "unchunked")
            lines.append(f"  {dim}: {current_val} → {recommended_val}")

    # Warnings
    if recommendation.warnings:
        lines.append("")
        lines.append(" Warnings:")
        for warning in recommendation.warnings:
            lines.append(f"  • {warning}")

    # Usage example
    if recommendation.chunks:
        lines.append("")
        lines.append(" Usage:")
        lines.append(f"   ds_chunked = ds.chunk({dict(recommendation.chunks)})")

    return "\n".join(lines)


def recommend_chunks(
    ds: xr.Dataset | xr.DataArray,
    client: Client | None = None,
    workload_type: str = "auto",
    target_chunk_mb: tuple[float, float] = (256, 512),
    safety_factor: float = 0.6,
    verbose: bool = False,
    storage_format: str | None = None,
    storage_path: str | None = None,
    **kwargs: Any,
) -> dict[str, int] | ChunkRecommendation:
    """Recommend optimal chunk sizes for an xarray Dataset or DataArray.

    Analyzes the dataset characteristics and available Dask worker resources to
    suggest chunk sizes that balance memory efficiency with good parallelization.
    Optionally considers storage format for I/O-optimized chunking.

    Args:
        ds: xarray Dataset or DataArray to analyze
        client: Active Dask client (to detect worker configuration), or None to use system defaults
        workload_type: Chunking strategy - "cpu" (compute-heavy), "io" (I/O-heavy),
                      "mixed" (balanced), or "auto" (detect automatically)
        target_chunk_mb: (min, max) target chunk size in MiB. Default: (256, 512)
        safety_factor: Fraction of worker memory to use for chunks. Default: 0.6 (60%)
        verbose: If True, print detailed report and return ChunkRecommendation object.
                If False, return simple dict of chunk sizes.
        storage_format: Optional storage format hint ("zarr", "netcdf") for I/O optimization
        storage_path: Optional storage path/URL for format detection and location optimization
        **kwargs: Additional parameters (reserved for future use)

    Returns:
        If verbose=False: Dict mapping dimension names to recommended chunk sizes
        If verbose=True: ChunkRecommendation object with full details

    Raises:
        XarrayDependencyError: If xarray or numpy are not installed

    Examples:
        Basic usage:
        >>> chunks = recommend_chunks(ds, client)
        >>> ds_chunked = ds.chunk(chunks)

        With detailed analysis:
        >>> recommendation = recommend_chunks(ds, client, verbose=True)
        >>> print(recommendation)  # Shows detailed report
        >>> ds_chunked = ds.chunk(recommendation.chunks)

        For I/O-heavy workloads:
        >>> chunks = recommend_chunks(ds, workload_type="io")
    """
    _ensure_xarray_available()

    # If storage format/path provided, use I/O-optimized chunking
    if storage_format or storage_path:
        try:
            from .io_patterns import recommend_io_chunks

            # Determine storage location for I/O optimization
            if storage_path:
                if any(
                    storage_path.startswith(prefix) for prefix in ["s3://", "gs://", "azure://"]
                ):
                    storage_location = "cloud"
                elif any(storage_path.startswith(prefix) for prefix in ["http://", "https://"]):
                    storage_location = "network"
                else:
                    storage_location = "local"
            else:
                storage_location = "auto"

            # Use I/O optimization with format-specific chunking
            io_chunks = recommend_io_chunks(
                ds=ds,
                path_or_url=storage_path,
                client=client,
                format_hint=storage_format,
                access_pattern="compute" if workload_type == "cpu" else workload_type,
                target_chunk_mb=target_chunk_mb,
                storage_location=storage_location,
                verbose=verbose,
            )

            if verbose:
                return io_chunks  # IORecommendation object
            else:
                return io_chunks  # dict

        except ImportError:
            # Fall back to standard chunking if I/O patterns module not available
            pass

    # Standard chunking approach
    # Analyze dataset and cluster
    dataset_info = _analyze_dataset(ds)
    cluster_info = _get_cluster_info(client)

    # Calculate recommendations
    recommendation = _calculate_optimal_chunks(
        dataset_info=dataset_info,
        cluster_info=cluster_info,
        workload_type=workload_type,
        target_chunk_mb=target_chunk_mb,
        safety_factor=safety_factor,
    )

    # Emit warnings for clearly suboptimal existing chunking
    if dataset_info["is_currently_chunked"]:
        current_chunking = dataset_info["current_chunking"]
        variables = dataset_info["variables"]

        if variables:
            main_var = max(variables.values(), key=lambda v: v["size_bytes"])
            dtype_size = np.dtype(main_var["dtype"]).itemsize

            # Check for very large existing chunks
            for dim, chunk_sizes in current_chunking.items():
                if isinstance(chunk_sizes, list | tuple) and chunk_sizes:
                    max_chunk_size = max(chunk_sizes)
                    # Rough estimate of chunk memory usage
                    elements_in_chunk = max_chunk_size
                    for other_dim in main_var["dims"]:
                        if other_dim != dim:
                            elements_in_chunk *= dataset_info["dims"][other_dim]

                    chunk_mb = (elements_in_chunk * dtype_size) / (1024 * 1024)

                    if chunk_mb > 2048:  # > 2 GiB
                        warnings.warn(
                            f"Dimension '{dim}' has very large chunks ({chunk_mb:.0f} MiB). "
                            "Consider rechunking to avoid memory issues.",
                            UserWarning,
                            stacklevel=2,
                        )
                    elif chunk_mb < 10:  # < 10 MiB
                        warnings.warn(
                            f"Dimension '{dim}' has very small chunks ({chunk_mb:.1f} MiB). "
                            "This may create high task overhead.",
                            UserWarning,
                            stacklevel=2,
                        )

    # Return results
    if verbose:
        # Print report and return full recommendation object
        report = _format_chunk_report(recommendation, dataset_info, verbose=True)
        print(report)
        return recommendation
    else:
        # Return simple chunk dict
        return dict(recommendation.chunks)
