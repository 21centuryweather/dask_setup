"""HPC-tuned Dask helpers for single-node runs on NCI Gadi.

A drop-in convenience wrapper around dask.distributed.LocalCluster + Client that:
- Auto-detects CPU cores and memory from PBS/SLURM environment variables
- Routes all temp/spill files to $PBS_JOBFS for performance
- Configures aggressive memory spilling to prevent OOM crashes
- Chooses optimal process/thread topology based on workload type
- Provides SSH tunnel commands for dashboard access on HPC systems
"""

from .client import setup_dask_client

# Xarray integration (optional)
try:
    from .xarray import recommend_chunks

    _xarray_available = True
except ImportError:
    _xarray_available = False
    recommend_chunks = None

# I/O optimization (optional)
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
    IORecommendation = None
    NetCDFOptimizer = None
    ZarrOptimizer = None
    detect_storage_format = None
    recommend_io_chunks = None

# Enhanced error handling (optional)
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
    ClusterSetupError = None
    ConfigurationValidationError = None
    DependencyError = None
    EnhancedDaskSetupError = None
    ErrorContext = None
    ResourceConstraintError = None
    StorageConfigurationError = None
    create_user_friendly_error = None
    format_exception_chain = None

__version__ = "1.0.0"
__all__ = ["setup_dask_client"]

# Add xarray functions to exports if available
if _xarray_available:
    __all__.append("recommend_chunks")

# Add I/O optimization functions to exports if available
if _io_patterns_available:
    __all__.extend(
        [
            "recommend_io_chunks",
            "detect_storage_format",
            "IORecommendation",
            "ZarrOptimizer",
            "NetCDFOptimizer",
        ]
    )

# Add enhanced error handling to exports if available
if _error_handling_available:
    __all__.extend(
        [
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
    )
