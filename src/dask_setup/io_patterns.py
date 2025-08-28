"""I/O optimization patterns for scientific data formats.

Provides specialized optimization strategies for different storage formats:
- Zarr: Cloud-native array storage with optimal chunking and compression
- NetCDF: Traditional scientific data format with HDF5 backend optimizations
- Cloud storage: AWS S3, Google Cloud Storage, Azure Blob optimizations

Key features:
- Format-specific chunking recommendations
- Compression codec selection
- Cloud storage access patterns
- Concurrent I/O optimization
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

try:
    from .error_handling import DependencyError, StorageConfigurationError
except ImportError:
    # Fallback for when error_handling isn't available
    class DependencyError(Exception):
        pass

    class StorageConfigurationError(Exception):
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

__all__ = [
    "IOOptimizer",
    "ZarrOptimizer",
    "NetCDFOptimizer",
    "detect_storage_format",
    "recommend_io_chunks",
    "IORecommendation",
]


class IODependencyError(ImportError):
    """Raised when I/O optimization functionality is used but dependencies are not available."""


@dataclass
class IORecommendation:
    """Container for I/O optimization recommendations."""

    format: str
    chunks: dict[str, int]
    compression: dict[str, Any]
    storage_options: dict[str, Any]
    access_pattern: str
    estimated_throughput_mb_s: float
    warnings: list[str]

    def __repr__(self) -> str:
        return (
            f"IORecommendation(format='{self.format}', chunks={self.chunks}, "
            f"compression={self.compression['codec']}, "
            f"throughput~{self.estimated_throughput_mb_s:.1f}MB/s)"
        )


class IOOptimizer(ABC):
    """Abstract base class for format-specific I/O optimizers."""

    def __init__(self, client: Client | None = None):
        self.client = client

    @abstractmethod
    def detect_format(self, path_or_url: str, ds: xr.Dataset | xr.DataArray | None = None) -> bool:
        """Detect if this optimizer applies to the given path/dataset."""
        pass

    @abstractmethod
    def optimize_chunks(
        self,
        ds: xr.Dataset | xr.DataArray,
        target_chunk_mb: tuple[float, float] = (128, 512),
        access_pattern: str = "auto",
    ) -> dict[str, int]:
        """Recommend optimal chunk sizes for this format."""
        pass

    @abstractmethod
    def optimize_compression(
        self, ds: xr.Dataset | xr.DataArray, storage_location: str = "local"
    ) -> dict[str, Any]:
        """Recommend compression settings for this format."""
        pass

    @abstractmethod
    def optimize_storage_options(
        self, path_or_url: str, access_pattern: str = "auto"
    ) -> dict[str, Any]:
        """Recommend storage options for this format and location."""
        pass

    def estimate_throughput(
        self, chunk_mb: float, storage_location: str = "local", access_pattern: str = "sequential"
    ) -> float:
        """Estimate I/O throughput in MB/s based on chunk size and access pattern."""
        # Base throughput estimates (conservative)
        base_throughput = {
            "local": 500,  # Local SSD
            "network": 100,  # Network storage
            "cloud": 50,  # Cloud storage
        }

        # Get base throughput
        if "s3://" in storage_location or "gs://" in storage_location:
            location_type = "cloud"
        elif storage_location.startswith(("http://", "https://", "ftp://")):
            location_type = "network"
        else:
            location_type = "local"

        base = base_throughput[location_type]

        # Adjust for chunk size (optimal around 64-256 MB for cloud)
        if location_type == "cloud":
            if chunk_mb < 32:
                chunk_penalty = 0.5  # Too many small requests
            elif chunk_mb > 512:
                chunk_penalty = 0.7  # Too much memory pressure
            else:
                chunk_penalty = 1.0
        else:
            chunk_penalty = 1.0

        # Adjust for access pattern
        pattern_multiplier = {
            "sequential": 1.0,
            "random": 0.3,
            "streaming": 0.8,
            "compute": 0.9,
        }.get(access_pattern, 0.8)

        return base * chunk_penalty * pattern_multiplier


class ZarrOptimizer(IOOptimizer):
    """Optimizer for Zarr format storage."""

    def detect_format(self, path_or_url: str, ds: xr.Dataset | xr.DataArray | None = None) -> bool:
        """Detect Zarr format from path or xarray backend."""
        # Check file path/URL
        if ".zarr" in path_or_url.lower() or path_or_url.endswith((".zarr", ".zarr/")):
            return True

        # Check xarray backend if available
        if ds is not None and hasattr(ds, "encoding"):
            if ds.encoding.get("source", "").endswith(".zarr"):
                return True
            # Check if it's a zarr-backed dataset
            if (
                hasattr(ds, "_file_obj")
                and hasattr(ds._file_obj, "ds")
                and "zarr" in str(type(ds._file_obj.ds)).lower()
            ):
                return True

        return False

    def optimize_chunks(
        self,
        ds: xr.Dataset | xr.DataArray,
        target_chunk_mb: tuple[float, float] = (128, 512),
        access_pattern: str = "auto",
    ) -> dict[str, int]:
        """Optimize chunks for Zarr format."""
        # Zarr works best with larger chunks for cloud storage
        # Adjust target size based on storage location

        # Get dataset info - handle cases when xarray is not available or mocked
        if xr is not None and isinstance(ds, xr.DataArray):
            main_var = ds
            dims = dict(zip(ds.dims, ds.shape, strict=False))
        elif hasattr(ds, "sizes"):  # xarray Dataset
            # Find largest variable
            try:
                main_var = max(
                    ds.data_vars.values(), key=lambda v: np.prod(v.shape) if np else v.size
                )
                dims = dict(ds.sizes)
            except (AttributeError, TypeError):
                # Fallback for mocked datasets
                dims = getattr(ds, "sizes", {})
                main_var = ds
        else:
            # Fallback for mocked objects
            dims = getattr(ds, "sizes", {})
            main_var = ds

        # Zarr-specific chunking strategy
        recommended_chunks = {}
        dtype_size = np.dtype(main_var.dtype).itemsize

        # For Zarr, prefer larger chunks (up to 512MB) for better compression and cloud access
        target_max_mb = target_chunk_mb[1]
        target_max_bytes = int(target_max_mb * 1024 * 1024)

        # Start with full dimensions and chunk down
        working_chunks = dict(dims)

        # Estimate current chunk size
        def estimate_bytes():
            return dtype_size * np.prod(list(working_chunks.values()))

        # Reduce chunks if needed, prioritizing spatial dimensions for time-series data
        time_like_dims = [d for d in dims if any(t in d.lower() for t in ["time", "date", "t"])]
        spatial_dims = [d for d in dims if d not in time_like_dims]

        current_bytes = estimate_bytes()
        while current_bytes > target_max_bytes and any(working_chunks[d] > 1 for d in dims):
            # For Zarr, prefer to keep time dimensions large and chunk spatial dims
            candidate_dims = spatial_dims if spatial_dims else list(dims.keys())
            candidate_dims = [d for d in candidate_dims if working_chunks[d] > 1]

            if not candidate_dims:
                candidate_dims = [d for d in dims if working_chunks[d] > 1]

            if not candidate_dims:
                break

            # Chunk the largest dimension
            largest_dim = max(candidate_dims, key=lambda d: working_chunks[d])
            working_chunks[largest_dim] = max(1, working_chunks[largest_dim] // 2)
            current_bytes = estimate_bytes()

        # Only return dimensions that are actually chunked
        for dim, chunk_size in working_chunks.items():
            if chunk_size < dims[dim]:
                recommended_chunks[dim] = chunk_size

        return recommended_chunks

    def optimize_compression(
        self, ds: xr.Dataset | xr.DataArray, storage_location: str = "local"
    ) -> dict[str, Any]:
        """Recommend compression for Zarr."""
        # Analyze data characteristics
        if xr is not None and isinstance(ds, xr.DataArray):
            main_var = ds
        elif hasattr(ds, "data_vars"):
            main_var = max(ds.data_vars.values(), key=lambda v: np.prod(v.shape))
        else:
            # Fallback for when xarray isn't available or dataset is mocked
            main_var = ds

        dtype = main_var.dtype

        # Choose compressor based on data type and storage location
        if np.issubdtype(dtype, np.integer):
            # Integer data - use fast compression
            if "cloud" in storage_location.lower() or any(
                x in storage_location for x in ["s3://", "gs://", "azure://"]
            ):
                # Cloud storage - prioritize compression ratio
                codec = "zstd"
                level = 3
            else:
                # Local storage - prioritize speed
                codec = "lz4"
                level = 1
        else:
            # Float data - balance compression and speed
            if "cloud" in storage_location.lower() or any(
                x in storage_location for x in ["s3://", "gs://", "azure://"]
            ):
                codec = "zstd"
                level = 2
            else:
                codec = "blosc"
                level = 5

        # Additional filters for floating point data
        filters = []
        if np.issubdtype(dtype, np.floating):
            # Add bit rounding for better compression of floating point data
            try:
                import numcodecs

                # Round to ~6 significant digits for float32, ~12 for float64
                if dtype == np.float32:
                    filters.append(numcodecs.FixedScaleOffset(dtype=dtype, scale=1e6))
                elif dtype == np.float64:
                    filters.append(numcodecs.FixedScaleOffset(dtype=dtype, scale=1e12))
            except ImportError:
                pass  # numcodecs not available

        return {
            "codec": codec,
            "level": level,
            "filters": filters,
            "shuffle": bool(np.issubdtype(dtype, np.number)),
        }

    def optimize_storage_options(
        self, path_or_url: str, access_pattern: str = "auto"
    ) -> dict[str, Any]:
        """Optimize storage options for Zarr."""
        options = {}

        # Cloud storage optimizations
        if path_or_url.startswith("s3://"):
            options.update(
                {
                    "anon": False,
                    "default_cache_type": "readahead",
                    "default_block_size": 64 * 1024 * 1024,  # 64MB blocks
                }
            )

            # Pattern-specific optimizations
            if access_pattern in ["streaming", "sequential"]:
                options["default_cache_type"] = "readahead"
            elif access_pattern == "random":
                options["default_cache_type"] = "mmap"

        elif path_or_url.startswith("gs://"):
            options.update(
                {
                    "token": "anon",
                    "default_cache_type": "readahead",
                    "default_block_size": 64 * 1024 * 1024,
                }
            )

        elif path_or_url.startswith(("http://", "https://")):
            options.update(
                {
                    "default_cache_type": "readahead",
                    "default_block_size": 32 * 1024 * 1024,  # Smaller blocks for HTTP
                }
            )

        # Zarr-specific options
        options.update(
            {
                "consolidated": True,  # Use consolidated metadata when possible
                "overwrite": False,
            }
        )

        return options


class NetCDFOptimizer(IOOptimizer):
    """Optimizer for NetCDF format storage."""

    def detect_format(self, path_or_url: str, ds: xr.Dataset | xr.DataArray | None = None) -> bool:
        """Detect NetCDF format from path or xarray backend."""
        # Check file extension
        path_lower = path_or_url.lower()
        if any(ext in path_lower for ext in [".nc", ".nc4", ".netcdf", ".cdf"]):
            return True

        # Check xarray backend
        if ds is not None and hasattr(ds, "encoding"):
            source = ds.encoding.get("source", "")
            if any(ext in source.lower() for ext in [".nc", ".nc4", ".netcdf"]):
                return True

        return False

    def optimize_chunks(
        self,
        ds: xr.Dataset | xr.DataArray,
        target_chunk_mb: tuple[float, float] = (64, 256),
        access_pattern: str = "auto",
    ) -> dict[str, int]:
        """Optimize chunks for NetCDF format."""
        # NetCDF works better with moderate chunk sizes due to HDF5 backend

        if xr is not None and isinstance(ds, xr.DataArray):
            main_var = ds
            dims = dict(zip(ds.dims, ds.shape, strict=False))
        elif hasattr(ds, "data_vars") and hasattr(ds, "sizes"):
            main_var = max(ds.data_vars.values(), key=lambda v: np.prod(v.shape))
            dims = dict(ds.sizes)
        else:
            # Fallback for mocked or non-xarray objects
            main_var = ds
            dims = getattr(ds, "sizes", {})

        recommended_chunks = {}
        dtype_size = np.dtype(main_var.dtype).itemsize

        # NetCDF prefers moderate chunk sizes (64-256MB typically optimal)
        target_max_mb = target_chunk_mb[1]
        target_max_bytes = int(target_max_mb * 1024 * 1024)

        working_chunks = dict(dims)

        def estimate_bytes():
            return dtype_size * np.prod(list(working_chunks.values()))

        # Identify time/record dimensions (unlimited dimensions in NetCDF)
        unlimited_dims = []
        time_like_dims = [
            d for d in dims if any(t in d.lower() for t in ["time", "date", "record"])
        ]
        unlimited_dims.extend(time_like_dims)

        # For NetCDF, be more conservative with chunking unlimited dimensions
        current_bytes = estimate_bytes()
        while current_bytes > target_max_bytes and any(working_chunks[d] > 1 for d in dims):
            # Prefer to chunk non-unlimited dimensions first
            non_unlimited = [d for d in dims if d not in unlimited_dims and working_chunks[d] > 1]

            if non_unlimited:
                # Chunk largest non-unlimited dimension
                largest_dim = max(non_unlimited, key=lambda d: working_chunks[d])
            else:
                # Fall back to any chunkable dimension
                chunkable = [d for d in dims if working_chunks[d] > 1]
                if not chunkable:
                    break
                largest_dim = max(chunkable, key=lambda d: working_chunks[d])

            working_chunks[largest_dim] = max(1, working_chunks[largest_dim] // 2)
            current_bytes = estimate_bytes()

        # Return only chunked dimensions
        for dim, chunk_size in working_chunks.items():
            if chunk_size < dims[dim]:
                recommended_chunks[dim] = chunk_size

        return recommended_chunks

    def optimize_compression(
        self, ds: xr.Dataset | xr.DataArray, storage_location: str = "local"
    ) -> dict[str, Any]:
        """Recommend compression for NetCDF."""
        if xr is not None and isinstance(ds, xr.DataArray):
            main_var = ds
        elif hasattr(ds, "data_vars"):
            main_var = max(ds.data_vars.values(), key=lambda v: np.prod(v.shape))
        else:
            # Fallback for when xarray isn't available or dataset is mocked
            main_var = ds

        dtype = main_var.dtype

        # NetCDF4/HDF5 compression options
        compression_opts = {
            "codec": "zlib",  # Standard in NetCDF4
            "level": 4,  # Balanced compression
            "shuffle": True,  # Byte shuffling for better compression
            "fletcher32": False,  # Checksum (adds overhead)
        }

        # Adjust based on data type
        if np.issubdtype(dtype, np.integer):
            # Integer data compresses well
            compression_opts["level"] = 6
        elif np.issubdtype(dtype, np.floating):
            # Floating point - moderate compression
            compression_opts["level"] = 4
            # Enable least significant digit if supported
            try:
                # Estimate appropriate precision
                if dtype == np.float32:
                    compression_opts["least_significant_digit"] = 3
                elif dtype == np.float64:
                    compression_opts["least_significant_digit"] = 6
            except Exception as e:
                import logging

                logging.debug(f"NetCDF precision setting not supported: {e}")

        # For cloud storage, increase compression
        if "cloud" in storage_location.lower() or any(
            x in storage_location for x in ["s3://", "gs://", "http"]
        ):
            compression_opts["level"] = min(9, compression_opts["level"] + 2)

        return compression_opts

    def optimize_storage_options(
        self, path_or_url: str, access_pattern: str = "auto"
    ) -> dict[str, Any]:
        """Optimize storage options for NetCDF."""
        options = {}

        # NetCDF4 format options
        options.update(
            {
                "format": "NETCDF4",
                "engine": "netcdf4",
            }
        )

        # Cloud/remote access optimizations
        if path_or_url.startswith(("http://", "https://", "s3://", "gs://")):
            options.update(
                {
                    "cache": True,
                    "decode_times": True,
                    "use_cftime": True,
                }
            )

        # Access pattern specific options
        if access_pattern == "streaming":
            options["decode_coords"] = False  # Faster loading

        return options


def detect_storage_format(path_or_url: str, ds: xr.Dataset | xr.DataArray | None = None) -> str:
    """Detect storage format from path/URL and optional xarray object.

    Args:
        path_or_url: File path or URL to analyze
        ds: Optional xarray Dataset/DataArray to check for format hints

    Returns:
        Format name: "zarr", "netcdf", or "unknown"
    """
    # Create optimizers to test detection
    optimizers = [
        ("zarr", ZarrOptimizer()),
        ("netcdf", NetCDFOptimizer()),
    ]

    for format_name, optimizer in optimizers:
        if optimizer.detect_format(path_or_url, ds):
            return format_name

    return "unknown"


def recommend_io_chunks(
    ds: xr.Dataset | xr.DataArray,
    path_or_url: str | None = None,
    client: Client | None = None,
    format_hint: str | None = None,
    access_pattern: str = "auto",
    target_chunk_mb: tuple[float, float] = (128, 512),
    storage_location: str = "auto",
    verbose: bool = False,
) -> IORecommendation | dict[str, int]:
    """Recommend I/O-optimized chunks for xarray datasets.

    Args:
        ds: xarray Dataset or DataArray to optimize
        path_or_url: Optional path/URL to determine storage format and location
        client: Optional Dask client for cluster info
        format_hint: Optional format override ("zarr", "netcdf", etc.)
        access_pattern: Access pattern hint ("sequential", "random", "streaming", "compute", "auto")
        target_chunk_mb: Target chunk size range in MiB
        storage_location: Storage location hint ("local", "cloud", "network", "auto")
        verbose: If True, return full IORecommendation object

    Returns:
        IORecommendation object if verbose=True, else dict of chunk recommendations
    """
    # Detect format
    if format_hint:
        detected_format = format_hint
    elif path_or_url:
        detected_format = detect_storage_format(path_or_url, ds)
    else:
        detected_format = "unknown"

    # Auto-detect storage location if needed
    if storage_location == "auto" and path_or_url:
        if any(
            path_or_url.startswith(prefix) for prefix in ["s3://", "gs://", "azure://", "abfs://"]
        ):
            storage_location = "cloud"
        elif any(path_or_url.startswith(prefix) for prefix in ["http://", "https://", "ftp://"]):
            storage_location = "network"
        else:
            storage_location = "local"
    elif storage_location == "auto":
        storage_location = "local"

    # Select optimizer
    # Preserve original format for warnings
    original_format = detected_format

    if detected_format == "zarr":
        optimizer = ZarrOptimizer(client)
    elif detected_format == "netcdf":
        optimizer = NetCDFOptimizer(client)
    else:
        # Default to Zarr optimizer for unknown formats
        optimizer = ZarrOptimizer(client)
        detected_format = "zarr"

    # Get recommendations
    chunks = optimizer.optimize_chunks(ds, target_chunk_mb, access_pattern)
    compression = optimizer.optimize_compression(ds, storage_location)

    if path_or_url:
        storage_options = optimizer.optimize_storage_options(path_or_url, access_pattern)
    else:
        storage_options = {}

    # Estimate throughput
    if chunks:
        # Estimate chunk size
        if xr is not None:
            try:
                if isinstance(ds, xr.DataArray):
                    dims = dict(zip(ds.dims, ds.shape, strict=False))
                elif hasattr(ds, "sizes"):  # xarray Dataset
                    dims = dict(ds.sizes)
                else:
                    dims = {}
            except (TypeError, AttributeError):
                dims = {}
        elif hasattr(ds, "sizes"):  # For mock objects with sizes attribute
            try:
                dims = dict(ds.sizes)
            except (TypeError, AttributeError):
                dims = {}
        else:
            # Fallback for mock objects or when xarray is not available
            dims = {}
            # Try to get dimensions from the object if possible
            if hasattr(ds, "dims") and hasattr(ds, "shape"):
                dims = dict(zip(ds.dims, ds.shape, strict=False))

        chunk_elements = 1
        for _dim, chunk_size in chunks.items():
            chunk_elements *= chunk_size
        for _dim, size in dims.items():
            if _dim not in chunks:
                chunk_elements *= size

        # Get dtype safely
        if xr is not None:
            try:
                if isinstance(ds, xr.DataArray):
                    dtype_size = np.dtype(ds.dtype).itemsize
                elif hasattr(ds, "dtypes") and hasattr(ds, "data_vars"):
                    first_var_name = list(ds.data_vars.keys())[0]
                    dtype_size = np.dtype(ds.dtypes[first_var_name]).itemsize
                else:
                    dtype_size = np.dtype(np.float64).itemsize
            except (TypeError, AttributeError):
                dtype_size = np.dtype(np.float64).itemsize
        elif hasattr(ds, "dtypes") and hasattr(ds, "data_vars"):
            try:
                first_var_name = list(ds.data_vars.keys())[0]
                dtype_size = np.dtype(ds.dtypes[first_var_name]).itemsize
            except (TypeError, AttributeError):
                dtype_size = np.dtype(np.float64).itemsize
        else:
            # Fallback for mocked objects - assume float64
            dtype_size = np.dtype(np.float64).itemsize
        chunk_mb = (chunk_elements * dtype_size) / (1024 * 1024)
    else:
        chunk_mb = 100  # Default estimate

    throughput = optimizer.estimate_throughput(chunk_mb, storage_location, access_pattern)

    # Generate warnings
    warnings_list = []
    if original_format == "unknown":
        warnings_list.append("Could not detect storage format - using Zarr defaults")

    if storage_location == "cloud" and chunk_mb < 64:
        warnings_list.append(
            f"Small chunks ({chunk_mb:.1f}MB) may be inefficient for cloud storage"
        )
    elif storage_location == "local" and chunk_mb > 512:
        warnings_list.append(f"Large chunks ({chunk_mb:.1f}MB) may cause memory pressure")

    # Create recommendation
    recommendation = IORecommendation(
        format=detected_format,
        chunks=chunks,
        compression=compression,
        storage_options=storage_options,
        access_pattern=access_pattern,
        estimated_throughput_mb_s=throughput,
        warnings=warnings_list,
    )

    if verbose:
        # Print detailed report
        print("🗄️  I/O Optimization Recommendations")
        print("=" * 40)
        print(f"📁 Format: {detected_format.upper()}")
        print(f"📍 Location: {storage_location}")
        print(f"🔄 Access pattern: {access_pattern}")

        if chunks:
            print(f"📦 Recommended chunks: {chunks}")
            print(f"📏 Estimated chunk size: {chunk_mb:.1f} MiB")
        else:
            print("📦 No chunking recommended")

        print(
            f"🗜️  Compression: {compression.get('codec', 'none')} (level {compression.get('level', 0)})"
        )
        print(f"⚡ Estimated throughput: {throughput:.1f} MB/s")

        if warnings_list:
            print("\n⚠️  Warnings:")
            for warning in warnings_list:
                print(f"  • {warning}")

        if chunks:
            print("\n💡 Usage:")
            print(f"   ds_chunked = ds.chunk({chunks})")

        return recommendation
    else:
        return chunks


def _ensure_dependencies() -> None:
    """Check that required dependencies are available."""
    if xr is None:
        raise DependencyError(missing_package="xarray", feature="I/O optimization")
    if np is None:
        raise DependencyError(missing_package="numpy", feature="I/O optimization")
