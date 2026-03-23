"""HPC-tuned Dask helpers for single-node runs on NCI Gadi.

A drop-in convenience wrapper around dask.distributed.LocalCluster + Client that:
- Auto-detects CPU cores and memory from PBS/SLURM environment variables
- Routes all temp/spill files to $PBS_JOBFS for performance
- Configures aggressive memory spilling to prevent OOM crashes
- Chooses optimal process/thread topology based on workload type
- Provides SSH tunnel commands for dashboard access on HPC systems
"""

from .client import DaskClientContext, setup_dask_client
from .config import DaskSetupConfig
from .config_manager import ConfigManager
from .environment import get_environment_type, is_jupyter
from .logging import configure_logging, get_logger
from .reporting import ClusterReport, cluster_report

# Xarray integration (optional — requires xarray + numpy)
try:
    from .xarray import ChunkRecommendation, recommend_chunks, validate_chunks

    _xarray_available = True
except ImportError:
    _xarray_available = False

    def recommend_chunks(*args, **kwargs):
        raise ImportError(
            "recommend_chunks requires xarray and numpy. "
            "Install with: pip install xarray numpy"
        )

    def validate_chunks(*args, **kwargs):
        raise ImportError(
            "validate_chunks requires xarray and numpy. "
            "Install with: pip install xarray numpy"
        )

    class ChunkRecommendation:  # type: ignore[no-redef]
        """Placeholder — requires xarray and numpy."""

        def __new__(cls, *args, **kwargs):
            raise ImportError(
                "ChunkRecommendation requires xarray and numpy. "
                "Install with: pip install xarray numpy"
            )


# Rechunking helper (optional — requires rechunker + zarr)
try:
    from .rechunk import rechunk_dataset

    _rechunker_available = True
except ImportError:
    _rechunker_available = False

    def rechunk_dataset(*args, **kwargs):
        raise ImportError(
            "rechunk_dataset requires rechunker and zarr. "
            "Install with: pip install rechunker zarr"
        )


# I/O optimization (optional — requires zarr / netcdf4 for full functionality)
try:
    from .io_patterns import (
        IORecommendation,
        NetCDFOptimizer,
        ZarrOptimizer,
        detect_storage_format,
        recommend_io_chunks,
    )

    _io_patterns_available = True
except ImportError:
    _io_patterns_available = False

    def recommend_io_chunks(*args, **kwargs):
        raise ImportError(
            "recommend_io_chunks requires zarr and/or netcdf4. "
            "Install with: pip install zarr netcdf4"
        )

    def detect_storage_format(*args, **kwargs):
        raise ImportError(
            "detect_storage_format requires zarr and/or netcdf4. "
            "Install with: pip install zarr netcdf4"
        )

    class _MissingIOClass:
        """Placeholder raised when the io_patterns optional dependency is missing."""

        def __init_subclass__(cls, **kwargs):
            pass

        def __new__(cls, *args, **kwargs):
            raise ImportError(
                f"{cls.__name__} requires zarr and/or netcdf4. "
                "Install with: pip install zarr netcdf4"
            )

    class IORecommendation(_MissingIOClass):  # type: ignore[no-redef]
        pass

    class ZarrOptimizer(_MissingIOClass):  # type: ignore[no-redef]
        pass

    class NetCDFOptimizer(_MissingIOClass):  # type: ignore[no-redef]
        pass


# Enhanced error handling (optional, but strongly recommended — always available in practice)
try:
    from .error_handling import (
        ClusterSetupError,
        ConfigurationValidationError,
        DependencyError,
        EnhancedDaskSetupError,
        ErrorContext,
        ResourceConstraintError,
        StorageConfigurationError,
        create_user_friendly_error,
        format_exception_chain,
    )

    _error_handling_available = True
except ImportError:
    _error_handling_available = False

    class _MissingErrorClass(Exception):  # type: ignore[no-redef]
        """Placeholder raised when error_handling cannot be imported."""

        def __init__(self, *args, **kwargs):
            raise ImportError(
                f"{type(self).__name__} is part of dask_setup's error handling module. "
                "This import should never fail — please check your installation."
            )

    class EnhancedDaskSetupError(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    class ConfigurationValidationError(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    class ResourceConstraintError(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    class DependencyError(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    class StorageConfigurationError(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    class ClusterSetupError(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    class ErrorContext(_MissingErrorClass):  # type: ignore[no-redef]
        pass

    def create_user_friendly_error(*args, **kwargs):
        raise ImportError(
            "create_user_friendly_error is part of dask_setup's error handling module. "
            "This import should never fail — please check your installation."
        )

    def format_exception_chain(*args, **kwargs):
        raise ImportError(
            "format_exception_chain is part of dask_setup's error handling module. "
            "This import should never fail — please check your installation."
        )


__version__ = "1.4.0"

__all__ = [
    # Core API — always available
    "setup_dask_client",
    "DaskClientContext",
    "DaskSetupConfig",
    "ConfigManager",
    # Environment detection
    "is_jupyter",
    "get_environment_type",
    # Logging helpers
    "configure_logging",
    "get_logger",
    # Post-run reporting
    "ClusterReport",
    "cluster_report",
    # Xarray helpers — require xarray + numpy
    "recommend_chunks",
    "validate_chunks",
    "ChunkRecommendation",
    # Rechunking helper — requires rechunker + zarr
    "rechunk_dataset",
    # I/O pattern helpers — require zarr / netcdf4
    "recommend_io_chunks",
    "detect_storage_format",
    "IORecommendation",
    "ZarrOptimizer",
    "NetCDFOptimizer",
    # Enhanced error types
    "ErrorContext",
    "EnhancedDaskSetupError",
    "ConfigurationValidationError",
    "ResourceConstraintError",
    "DependencyError",
    "StorageConfigurationError",
    "ClusterSetupError",
    "create_user_friendly_error",
    "format_exception_chain",
]
