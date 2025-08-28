"""Enhanced error handling framework for dask_setup.

This module provides user-friendly error classes with contextual information,
actionable suggestions, and comprehensive diagnostic capabilities to help users
quickly resolve issues.
"""

from __future__ import annotations

import os
import platform
import sys
from dataclasses import dataclass, field
from typing import Any

import psutil

from .exceptions import DaskSetupError


@dataclass
class ErrorContext:
    """Container for error context information."""

    # System information
    platform_info: str = field(default_factory=lambda: platform.platform())
    python_version: str = field(
        default_factory=lambda: f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    total_memory_gb: float = field(
        default_factory=lambda: psutil.virtual_memory().total / (1024**3)
    )
    cpu_count: int = field(default_factory=lambda: psutil.cpu_count(logical=True))

    # Environment information
    hpc_environment: str | None = field(default=None)
    pbs_jobfs: str | None = field(default_factory=lambda: os.getenv("PBS_JOBFS"))
    slurm_job_id: str | None = field(default_factory=lambda: os.getenv("SLURM_JOB_ID"))

    # Dependency information
    available_dependencies: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Auto-detect HPC environment and dependencies."""
        # Detect HPC environment
        if self.slurm_job_id:
            self.hpc_environment = "SLURM"
        elif self.pbs_jobfs:
            self.hpc_environment = "PBS"
        elif os.getenv("NCPUS"):
            self.hpc_environment = "PBS (legacy)"
        else:
            self.hpc_environment = "Local"

        # Check for key dependencies
        self._check_dependency("dask", "dask")
        self._check_dependency("xarray", "xarray")
        self._check_dependency("numpy", "numpy")
        self._check_dependency("zarr", "zarr")
        self._check_dependency("netcdf4", "netCDF4")

    def _check_dependency(self, name: str, import_name: str):
        """Check if a dependency is available and get version."""
        try:
            module = __import__(import_name)
            version = getattr(module, "__version__", "unknown")
            self.available_dependencies[name] = version
        except ImportError:
            pass

    def get_environment_summary(self) -> str:
        """Get a summary of the current environment."""
        lines = [
            f" Environment: {self.hpc_environment}",
            f" Python: {self.python_version}",
            f" Memory: {self.total_memory_gb:.1f} GB",
            f" CPUs: {self.cpu_count}",
        ]

        if self.available_dependencies:
            lines.append("Dependencies:")
            for name, version in sorted(self.available_dependencies.items()):
                lines.append(f"   • {name}: {version}")

        return "\n".join(lines)


class EnhancedDaskSetupError(DaskSetupError):
    """Base class for enhanced dask_setup errors with rich context."""

    def __init__(
        self,
        message: str,
        suggestions: list[str] | None = None,
        context: ErrorContext | None = None,
        documentation_url: str | None = None,
        error_code: str | None = None,
    ):
        self.suggestions = suggestions or []
        self.context = context or ErrorContext()
        self.documentation_url = documentation_url
        self.error_code = error_code

        # Format the complete error message
        formatted_message = self._format_error_message(message)
        super().__init__(formatted_message)

    def _format_error_message(self, message: str) -> str:
        """Format a comprehensive error message with context and suggestions."""
        lines = []

        # Error header with emoji and code
        if self.error_code:
            lines.append(f" [{self.error_code}] {message}")
        else:
            lines.append(f" {message}")

        # Add suggestions if available
        if self.suggestions:
            lines.append("\n Suggestions:")
            for i, suggestion in enumerate(self.suggestions, 1):
                lines.append(f"   {i}. {suggestion}")

        # Add documentation link if available
        if self.documentation_url:
            lines.append(f"\n Documentation: {self.documentation_url}")

        return "\n".join(lines)

    def get_diagnostic_info(self) -> str:
        """Get diagnostic information for error reporting."""
        lines = [
            f"Error Code: {self.error_code or 'N/A'}",
            f"Error Type: {type(self).__name__}",
            "",
            self.context.get_environment_summary(),
        ]
        return "\n".join(lines)


class ConfigurationValidationError(EnhancedDaskSetupError):
    """Enhanced configuration validation error with specific field guidance."""

    def __init__(
        self,
        field_errors: dict[str, str],
        invalid_config: dict[str, Any] | None = None,
        context: ErrorContext | None = None,
    ):
        self.field_errors = field_errors
        self.invalid_config = invalid_config or {}

        # Generate field-specific suggestions
        suggestions = self._generate_field_suggestions()

        # Create main error message
        error_details = []
        for field_name, error_msg in field_errors.items():
            current_value = self.invalid_config.get(field_name, "undefined")
            error_details.append(f"   • {field_name}: {error_msg} (current: {current_value})")

        message = "Configuration validation failed:\n" + "\n".join(error_details)

        super().__init__(
            message=message,
            suggestions=suggestions,
            context=context,
            error_code="CONFIG_VALIDATION",
            documentation_url="https://github.com/dask-contrib/dask_setup#configuration",
        )

    def _generate_field_suggestions(self) -> list[str]:
        """Generate field-specific suggestions based on validation errors."""
        suggestions = []

        for field_name, _error_msg in self.field_errors.items():
            if field_name == "workload_type":
                suggestions.append(
                    "Use workload_type='io' for I/O intensive tasks, 'cpu' for compute, or 'mixed'"
                )
            elif field_name == "max_workers":
                suggestions.append(f"Set max_workers to a value between 1 and {psutil.cpu_count()}")
            elif field_name == "reserve_mem_gb":
                total_gb = psutil.virtual_memory().total / (1024**3)
                reasonable = min(total_gb * 0.2, 10)
                suggestions.append(
                    f"Try reserve_mem_gb={reasonable:.0f} (20% of total memory, capped at 10 GB)"
                )
            elif field_name.startswith("memory_"):
                suggestions.append(
                    "Memory thresholds should be in order: 0 < target < spill < pause < terminate <= 1"
                )
            elif field_name == "dashboard_port":
                suggestions.append(
                    "Use a port between 1024-65535, or leave unset for automatic assignment"
                )
            elif field_name.startswith("io_"):
                suggestions.append(
                    "Check I/O optimization settings - see documentation for valid options"
                )

        if not suggestions:
            suggestions.append("Check the configuration documentation for valid parameter values")

        return suggestions


class ResourceConstraintError(EnhancedDaskSetupError):
    """Enhanced resource constraint error with HPC-aware suggestions."""

    def __init__(
        self,
        resource_type: str,
        required: float,
        available: float,
        units: str = "GB",
        context: ErrorContext | None = None,
    ):
        self.resource_type = resource_type
        self.required = required
        self.available = available
        self.units = units

        # Generate environment-specific suggestions
        suggestions = self._generate_resource_suggestions(context or ErrorContext())

        message = (
            f"Insufficient {resource_type} for configuration:\n"
            f"   Required: {required:.1f} {units}\n"
            f"   Available: {available:.1f} {units}\n"
            f"   Shortfall: {required - available:.1f} {units}"
        )

        super().__init__(
            message=message,
            suggestions=suggestions,
            context=context,
            error_code="RESOURCE_CONSTRAINT",
            documentation_url="https://github.com/dask-contrib/dask_setup#memory-configuration",
        )

    def _generate_resource_suggestions(self, context: ErrorContext) -> list[str]:
        """Generate resource-specific suggestions based on environment."""
        suggestions = []

        if self.resource_type.lower() == "memory":
            # Memory-specific suggestions
            if context.hpc_environment in ["SLURM", "PBS", "PBS (legacy)"]:
                suggestions.extend(
                    [
                        "Request more memory in your job submission script (e.g., #PBS -l mem=32gb)",
                        f"Reduce reserve_mem_gb to {max(1, self.available * 0.1):.0f} GB",
                        "Use fewer workers (reduce max_workers)",
                        "Consider using adaptive scaling with lower min_workers",
                    ]
                )
            else:
                suggestions.extend(
                    [
                        f"Reduce reserve_mem_gb to {max(1, self.available * 0.1):.0f} GB",
                        "Close other applications to free up memory",
                        "Use fewer workers (reduce max_workers)",
                        "Consider running on a machine with more memory",
                    ]
                )

        elif self.resource_type.lower() == "cpu":
            # CPU-specific suggestions
            suggestions.extend(
                [
                    f"Reduce max_workers to {max(1, int(self.available)):.0f}",
                    "Use workload_type='io' for I/O bound tasks (uses fewer cores)",
                    "Request more CPUs in your job submission if on HPC",
                ]
            )

        return suggestions


class DependencyError(EnhancedDaskSetupError):
    """Enhanced dependency error with installation guidance."""

    def __init__(
        self,
        missing_package: str,
        feature: str,
        context: ErrorContext | None = None,
        alternative_packages: list[str] | None = None,
    ):
        self.missing_package = missing_package
        self.feature = feature
        self.alternative_packages = alternative_packages or []

        # Generate installation suggestions
        suggestions = self._generate_installation_suggestions(context or ErrorContext())

        message = f"Missing dependency '{missing_package}' required for {feature}"

        super().__init__(
            message=message,
            suggestions=suggestions,
            context=context,
            error_code="MISSING_DEPENDENCY",
            documentation_url="https://github.com/dask-contrib/dask_setup#installation",
        )

    def _generate_installation_suggestions(self, context: ErrorContext) -> list[str]:
        """Generate installation suggestions based on environment."""
        suggestions = []

        # Primary installation command
        suggestions.append(f"Install with: pip install {self.missing_package}")

        # Conda alternative if available
        conda_packages = {
            "xarray": "conda install -c conda-forge xarray",
            "zarr": "conda install -c conda-forge zarr",
            "netcdf4": "conda install -c conda-forge netcdf4",
            "dask": "conda install -c conda-forge dask",
            "numpy": "conda install -c conda-forge numpy",
        }

        if self.missing_package in conda_packages:
            suggestions.append(f"Or with conda: {conda_packages[self.missing_package]}")

        # Feature-specific suggestions
        if self.feature == "xarray chunking recommendations":
            suggestions.append("For full I/O optimization: pip install 'xarray zarr netcdf4'")
        elif self.feature == "I/O optimization":
            suggestions.append("For Zarr support: pip install zarr")
            suggestions.append("For NetCDF support: pip install netcdf4")

        # Environment-specific notes
        if context.hpc_environment in ["SLURM", "PBS", "PBS (legacy)"]:
            suggestions.append(
                "On HPC systems, consider using a conda environment or virtual environment"
            )
            suggestions.append("Check if the package is available in your HPC's module system")

        return suggestions


class StorageConfigurationError(EnhancedDaskSetupError):
    """Enhanced storage configuration error with format-specific guidance."""

    def __init__(
        self,
        storage_issue: str,
        storage_path: str | None = None,
        detected_format: str | None = None,
        context: ErrorContext | None = None,
    ):
        self.storage_issue = storage_issue
        self.storage_path = storage_path
        self.detected_format = detected_format

        suggestions = self._generate_storage_suggestions()

        message_parts = [f"Storage configuration error: {storage_issue}"]
        if storage_path:
            message_parts.append(f"Path: {storage_path}")
        if detected_format:
            message_parts.append(f"Detected format: {detected_format}")

        message = "\n".join(message_parts)

        super().__init__(
            message=message,
            suggestions=suggestions,
            context=context,
            error_code="STORAGE_CONFIG",
            documentation_url="https://github.com/dask-contrib/dask_setup#io-optimization",
        )

    def _generate_storage_suggestions(self) -> list[str]:
        """Generate storage-specific suggestions."""
        suggestions = []

        if "zarr" in self.storage_issue.lower():
            suggestions.extend(
                [
                    "Ensure zarr is installed: pip install zarr",
                    "For cloud storage, check your credentials are configured",
                    "Zarr paths should end with .zarr (e.g., 'data.zarr' or 's3://bucket/data.zarr')",
                ]
            )

        if "netcdf" in self.storage_issue.lower():
            suggestions.extend(
                [
                    "Ensure netcdf4 is installed: pip install netcdf4",
                    "NetCDF files should have extensions like .nc, .nc4, .netcdf",
                    "For remote NetCDF files, ensure network connectivity",
                ]
            )

        if self.storage_path:
            if self.storage_path.startswith(("s3://", "gs://", "azure://")):
                suggestions.extend(
                    [
                        "For cloud storage, ensure your credentials are configured",
                        "Check that the bucket/container exists and is accessible",
                        "Consider using appropriate storage options for your cloud provider",
                    ]
                )
            elif self.storage_path.startswith(("http://", "https://")):
                suggestions.extend(
                    [
                        "Verify the URL is accessible",
                        "Check if authentication is required for the remote resource",
                    ]
                )
            else:
                suggestions.extend(
                    [
                        "Verify the file path exists and is readable",
                        "Check file permissions and directory access",
                    ]
                )

        if not suggestions:
            suggestions.append(
                "Check storage configuration and ensure all required dependencies are installed"
            )

        return suggestions


class ClusterSetupError(EnhancedDaskSetupError):
    """Enhanced cluster setup error with diagnostic information."""

    def __init__(
        self,
        setup_issue: str,
        cluster_config: dict[str, Any] | None = None,
        context: ErrorContext | None = None,
    ):
        self.setup_issue = setup_issue
        self.cluster_config = cluster_config or {}

        suggestions = self._generate_cluster_suggestions(context or ErrorContext())

        message = f"Cluster setup failed: {setup_issue}"
        if cluster_config:
            message += f"\nConfiguration: {cluster_config}"

        super().__init__(
            message=message,
            suggestions=suggestions,
            context=context,
            error_code="CLUSTER_SETUP",
            documentation_url="https://github.com/dask-contrib/dask_setup#cluster-setup",
        )

    def _generate_cluster_suggestions(self, context: ErrorContext) -> list[str]:
        """Generate cluster setup suggestions based on context."""
        suggestions = []

        if "port" in self.setup_issue.lower():
            suggestions.extend(
                [
                    "Try a different dashboard port or leave unset for automatic assignment",
                    "Check if the port is already in use: netstat -tlnp | grep <port>",
                    "Ensure the port is within the allowed range (1024-65535)",
                ]
            )

        if "memory" in self.setup_issue.lower():
            suggestions.extend(
                [
                    "Reduce the number of workers (max_workers)",
                    "Increase reserve_mem_gb to leave more memory for the system",
                    "Use adaptive scaling to allow memory-based worker scaling",
                ]
            )

        if "worker" in self.setup_issue.lower():
            suggestions.extend(
                [
                    f"Reduce max_workers to {max(1, context.cpu_count // 2)}",
                    "Check system resource availability",
                    "Consider using workload_type='io' for I/O bound tasks",
                ]
            )

        if "dashboard" in self.setup_issue.lower():
            suggestions.extend(
                [
                    "Try setting dashboard=False to disable the dashboard",
                    "Check if another process is using the dashboard port",
                    "Ensure you have permissions to bind to the specified port",
                ]
            )

        # Environment-specific suggestions
        if context.hpc_environment in ["SLURM", "PBS", "PBS (legacy)"]:
            suggestions.extend(
                [
                    "Check that your job allocation has sufficient resources",
                    "Verify PBS_JOBFS or SLURM temp directories are accessible",
                    "Ensure you're not exceeding job limits for memory or CPU",
                ]
            )

        if not suggestions:
            suggestions.append("Check system resources and configuration parameters")

        return suggestions


def create_user_friendly_error(error_type: str, message: str, **kwargs) -> EnhancedDaskSetupError:
    """Factory function to create user-friendly errors."""
    context = ErrorContext()

    error_classes = {
        "configuration": ConfigurationValidationError,
        "resource": ResourceConstraintError,
        "dependency": DependencyError,
        "storage": StorageConfigurationError,
        "cluster": ClusterSetupError,
    }

    error_class = error_classes.get(error_type, EnhancedDaskSetupError)

    if error_type == "configuration" and "field_errors" in kwargs:
        return error_class(
            field_errors=kwargs["field_errors"],
            invalid_config=kwargs.get("invalid_config", {}),
            context=context,
        )
    elif error_type == "resource":
        return error_class(
            resource_type=kwargs.get("resource_type", "memory"),
            required=kwargs.get("required", 0),
            available=kwargs.get("available", 0),
            context=context,
        )
    elif error_type == "dependency":
        return error_class(
            missing_package=kwargs.get("missing_package", "unknown"),
            feature=kwargs.get("feature", "functionality"),
            context=context,
        )
    elif error_type == "storage":
        return error_class(
            storage_issue=message,
            storage_path=kwargs.get("storage_path"),
            detected_format=kwargs.get("detected_format"),
            context=context,
        )
    elif error_type == "cluster":
        return error_class(
            setup_issue=message, cluster_config=kwargs.get("cluster_config"), context=context
        )
    else:
        return EnhancedDaskSetupError(
            message=message,
            suggestions=kwargs.get("suggestions"),
            context=context,
            error_code=kwargs.get("error_code"),
            documentation_url=kwargs.get("documentation_url"),
        )


def format_exception_chain(exc: Exception) -> str:
    """Format an exception chain with context for debugging."""
    lines = []

    current = exc
    level = 0

    while current is not None:
        indent = "  " * level
        lines.append(f"{indent}{type(current).__name__}: {current}")

        if hasattr(current, "get_diagnostic_info"):
            diagnostic = current.get_diagnostic_info()
            for line in diagnostic.split("\n"):
                lines.append(f"{indent}  {line}")

        current = getattr(current, "__cause__", None)
        level += 1

        if level > 5:  # Prevent infinite loops
            lines.append("  ... (truncated)")
            break

    return "\n".join(lines)
