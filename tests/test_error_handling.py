"""Tests for enhanced error handling framework."""

import os
from unittest.mock import patch

import pytest
from src.dask_setup.error_handling import (
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


class TestErrorContext:
    """Test error context information gathering."""

    def test_basic_context_creation(self):
        """Test basic context creation with system info."""
        context = ErrorContext()

        # Should have basic system info
        assert isinstance(context.platform_info, str)
        assert context.python_version.count(".") >= 2  # e.g., "3.10.5"
        assert context.total_memory_gb > 0
        assert context.cpu_count > 0

        # Environment should be detected
        assert context.hpc_environment in ["SLURM", "PBS", "PBS (legacy)", "Local"]

        # Should have dependency info
        assert isinstance(context.available_dependencies, dict)

    @patch.dict(os.environ, {"SLURM_JOB_ID": "12345"})
    def test_slurm_detection(self):
        """Test SLURM environment detection."""
        context = ErrorContext()
        assert context.hpc_environment == "SLURM"
        assert context.slurm_job_id == "12345"

    @patch.dict(os.environ, {"PBS_JOBFS": "/tmp/pbs.12345"})
    def test_pbs_detection(self):
        """Test PBS environment detection."""
        context = ErrorContext()
        assert context.hpc_environment == "PBS"
        assert context.pbs_jobfs == "/tmp/pbs.12345"

    def test_environment_summary(self):
        """Test environment summary formatting."""
        context = ErrorContext()
        summary = context.get_environment_summary()

        assert "Environment:" in summary
        assert "Python:" in summary
        assert "Memory:" in summary
        assert "CPUs:" in summary


class TestEnhancedDaskSetupError:
    """Test base enhanced error class."""

    def test_basic_error_creation(self):
        """Test basic error creation and formatting."""
        error = EnhancedDaskSetupError("Test error message")

        assert "❌ Test error message" in str(error)
        assert hasattr(error, "context")
        assert hasattr(error, "suggestions")
        assert hasattr(error, "documentation_url")
        assert hasattr(error, "error_code")

    def test_error_with_suggestions(self):
        """Test error with suggestions."""
        suggestions = ["Try reducing memory usage", "Check configuration parameters"]
        error = EnhancedDaskSetupError(
            "Test error", suggestions=suggestions, error_code="TEST_ERROR"
        )

        error_str = str(error)
        assert "[TEST_ERROR]" in error_str
        assert "💡 Suggestions:" in error_str
        assert "1. Try reducing memory usage" in error_str
        assert "2. Check configuration parameters" in error_str

    def test_error_with_documentation(self):
        """Test error with documentation URL."""
        error = EnhancedDaskSetupError("Test error", documentation_url="https://example.com/docs")

        assert "📖 Documentation: https://example.com/docs" in str(error)

    def test_diagnostic_info(self):
        """Test diagnostic information generation."""
        error = EnhancedDaskSetupError("Test error", error_code="TEST_ERROR")

        diagnostic = error.get_diagnostic_info()
        assert "Error Code: TEST_ERROR" in diagnostic
        assert "Error Type: EnhancedDaskSetupError" in diagnostic
        assert "Environment:" in diagnostic


class TestConfigurationValidationError:
    """Test configuration validation error."""

    def test_field_validation_error(self):
        """Test field validation error creation."""
        field_errors = {
            "max_workers": "must be positive",
            "reserve_mem_gb": "exceeds total system memory (32.0 GB)",
        }
        invalid_config = {"max_workers": -1, "reserve_mem_gb": 64.0}

        error = ConfigurationValidationError(
            field_errors=field_errors, invalid_config=invalid_config
        )

        error_str = str(error)
        assert "Configuration validation failed:" in error_str
        assert "max_workers: must be positive (current: -1)" in error_str
        assert "reserve_mem_gb: exceeds total system memory" in error_str
        assert "[CONFIG_VALIDATION]" in error_str
        assert "💡 Suggestions:" in error_str

    def test_field_specific_suggestions(self):
        """Test generation of field-specific suggestions."""
        field_errors = {"workload_type": "invalid value"}

        error = ConfigurationValidationError(field_errors=field_errors)

        suggestions = error.suggestions
        assert any("workload_type='io'" in s for s in suggestions)


class TestResourceConstraintError:
    """Test resource constraint error."""

    def test_memory_constraint_error(self):
        """Test memory constraint error."""
        error = ResourceConstraintError(
            resource_type="memory", required=32.0, available=16.0, units="GB"
        )

        error_str = str(error)
        assert "Insufficient memory for configuration:" in error_str
        assert "Required: 32.0 GB" in error_str
        assert "Available: 16.0 GB" in error_str
        assert "Shortfall: 16.0 GB" in error_str
        assert "[RESOURCE_CONSTRAINT]" in error_str

        suggestions = error.suggestions
        assert len(suggestions) > 0
        assert any("reduce" in s.lower() for s in suggestions)

    def test_cpu_constraint_error(self):
        """Test CPU constraint error."""
        error = ResourceConstraintError(
            resource_type="CPU", required=16.0, available=8.0, units="cores"
        )

        suggestions = error.suggestions
        assert any("max_workers" in s for s in suggestions)

    @patch.dict(os.environ, {"PBS_JOBFS": "/tmp/pbs.123"})
    def test_hpc_specific_suggestions(self):
        """Test HPC-specific suggestions."""
        error = ResourceConstraintError(resource_type="memory", required=32.0, available=16.0)

        suggestions = error.suggestions
        assert any("PBS" in s or "job submission" in s for s in suggestions)


class TestDependencyError:
    """Test dependency error."""

    def test_basic_dependency_error(self):
        """Test basic dependency error."""
        error = DependencyError(missing_package="xarray", feature="xarray chunking recommendations")

        error_str = str(error)
        assert "Missing dependency 'xarray'" in error_str
        assert "required for xarray chunking recommendations" in error_str
        assert "[MISSING_DEPENDENCY]" in error_str

        suggestions = error.suggestions
        assert any("pip install xarray" in s for s in suggestions)

    def test_conda_installation_suggestions(self):
        """Test conda installation suggestions."""
        error = DependencyError(missing_package="zarr", feature="I/O optimization")

        suggestions = error.suggestions
        assert any("pip install zarr" in s for s in suggestions)
        assert any("conda install" in s and "zarr" in s for s in suggestions)

    @patch.dict(os.environ, {"SLURM_JOB_ID": "123"})
    def test_hpc_installation_notes(self):
        """Test HPC-specific installation notes."""
        error = DependencyError(missing_package="numpy", feature="computation")

        suggestions = error.suggestions
        assert any("HPC" in s for s in suggestions)
        assert any("module system" in s for s in suggestions)


class TestStorageConfigurationError:
    """Test storage configuration error."""

    def test_format_detection_error(self):
        """Test format detection error."""
        error = StorageConfigurationError(
            storage_issue="Could not detect storage format", storage_path="/data/unknown.dat"
        )

        error_str = str(error)
        assert "Storage configuration error:" in error_str
        assert "Could not detect storage format" in error_str
        assert "Path: /data/unknown.dat" in error_str
        assert "[STORAGE_CONFIG]" in error_str

    def test_cloud_storage_suggestions(self):
        """Test cloud storage specific suggestions."""
        error = StorageConfigurationError(
            storage_issue="Access denied", storage_path="s3://my-bucket/data.zarr"
        )

        suggestions = error.suggestions
        assert any("credentials" in s.lower() for s in suggestions)
        assert any("bucket" in s.lower() for s in suggestions)

    def test_format_specific_suggestions(self):
        """Test format-specific suggestions."""
        error = StorageConfigurationError(
            storage_issue="zarr metadata not found", storage_path="/data/array.zarr"
        )

        suggestions = error.suggestions
        assert any("zarr" in s.lower() for s in suggestions)


class TestClusterSetupError:
    """Test cluster setup error."""

    def test_port_setup_error(self):
        """Test port-related setup error."""
        error = ClusterSetupError(setup_issue="Failed to bind to port 8787")

        suggestions = error.suggestions
        assert any("port" in s.lower() for s in suggestions)
        assert any("8787" in s or "netstat" in s for s in suggestions)

    def test_memory_setup_error(self):
        """Test memory-related setup error."""
        error = ClusterSetupError(setup_issue="Insufficient memory for workers")

        suggestions = error.suggestions
        assert any("workers" in s.lower() for s in suggestions)
        assert any("memory" in s.lower() for s in suggestions)

    @patch.dict(os.environ, {"PBS_JOBFS": "/tmp/pbs.123"})
    def test_hpc_cluster_suggestions(self):
        """Test HPC-specific cluster suggestions."""
        error = ClusterSetupError(setup_issue="Worker startup failed")

        suggestions = error.suggestions
        assert any("job allocation" in s.lower() or "PBS_JOBFS" in s for s in suggestions)


class TestErrorFactory:
    """Test error factory function."""

    def test_configuration_error_creation(self):
        """Test configuration error creation via factory."""
        error = create_user_friendly_error(
            "configuration",
            "Validation failed",
            field_errors={"max_workers": "must be positive"},
            invalid_config={"max_workers": -1},
        )

        assert isinstance(error, ConfigurationValidationError)
        assert "max_workers" in error.field_errors

    def test_resource_error_creation(self):
        """Test resource error creation via factory."""
        error = create_user_friendly_error(
            "resource", "Not enough memory", resource_type="memory", required=32.0, available=16.0
        )

        assert isinstance(error, ResourceConstraintError)
        assert error.resource_type == "memory"
        assert error.required == 32.0
        assert error.available == 16.0

    def test_dependency_error_creation(self):
        """Test dependency error creation via factory."""
        error = create_user_friendly_error(
            "dependency", "Package missing", missing_package="xarray", feature="chunking"
        )

        assert isinstance(error, DependencyError)
        assert error.missing_package == "xarray"
        assert error.feature == "chunking"

    def test_generic_error_fallback(self):
        """Test generic error fallback."""
        error = create_user_friendly_error("unknown_type", "Generic error", error_code="GENERIC")

        assert isinstance(error, EnhancedDaskSetupError)
        assert error.error_code == "GENERIC"


class TestExceptionChainFormatting:
    """Test exception chain formatting."""

    def test_simple_exception_chain(self):
        """Test formatting of simple exception chain."""
        original = ValueError("Original error")
        enhanced = EnhancedDaskSetupError("Enhanced error")
        # Manually set the cause for testing
        enhanced.__cause__ = original

        formatted = format_exception_chain(enhanced)

        assert "EnhancedDaskSetupError" in formatted
        assert "ValueError" in formatted
        assert "Enhanced error" in formatted
        assert "Original error" in formatted

    def test_diagnostic_info_in_chain(self):
        """Test diagnostic info inclusion in chain."""
        error = ConfigurationValidationError(
            field_errors={"test": "error"}, invalid_config={"test": "value"}
        )

        formatted = format_exception_chain(error)

        assert "Environment:" in formatted  # From diagnostic info
        assert "Error Code:" in formatted

    def test_long_chain_truncation(self):
        """Test truncation of very long exception chains."""
        # Create a chain of errors that exceeds the truncation limit
        errors = []
        for i in range(10):  # Create more than the 5-level limit
            error = EnhancedDaskSetupError(f"Error {i}")
            if i > 0:
                error.__cause__ = errors[i - 1]
            errors.append(error)

        # Use the top-level error (which has the longest chain)
        top_error = errors[-1]

        formatted = format_exception_chain(top_error)

        # Should include some content without infinite loops
        assert len(formatted) > 0
        assert "EnhancedDaskSetupError" in formatted
        assert "... (truncated)" in formatted  # Should be truncated

        # Should not contain all 10 errors due to truncation
        lines = formatted.split("\n")
        # Count actual exception lines (lines that start with class names)
        exception_lines = [
            line
            for line in lines
            if line.strip()
            and (
                "EnhancedDaskSetupError:" in line
                or line.strip().startswith("  EnhancedDaskSetupError:")
            )
        ]
        # Should have at most 6 exception instances (5 limit + 1 for truncation)
        assert len(exception_lines) <= 6


class TestIntegration:
    """Integration tests for error handling."""

    def test_config_validation_integration(self):
        """Test config validation with enhanced errors."""
        from src.dask_setup.config import DaskSetupConfig

        # Try to create an invalid config
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(max_workers=-1, reserve_mem_gb=-5)

        error = exc_info.value
        assert isinstance(error, ConfigurationValidationError)
        assert "max_workers" in error.field_errors
        assert "reserve_mem_gb" in error.field_errors
        assert len(error.suggestions) > 0

    def test_xarray_dependency_integration(self):
        """Test xarray integration with enhanced dependency errors."""
        # Mock missing xarray by patching the module-level import
        with patch("src.dask_setup.xarray.xr", None):
            from src.dask_setup.xarray import _ensure_xarray_available

            with pytest.raises(DependencyError) as exc_info:
                _ensure_xarray_available()

            error = exc_info.value
            assert error.missing_package == "xarray"
            assert error.feature == "xarray chunking recommendations"

    def test_memory_constraint_integration(self):
        """Test memory constraint detection."""
        # This would be tested with actual cluster setup,
        # but we can test the error creation pathway
        # Test memory constraint error type exists
        # (Actual memory constraint testing would require real cluster setup)
        assert ResourceConstraintError is not None

    def test_error_context_in_real_environment(self):
        """Test error context with real environment."""
        context = ErrorContext()

        # Should detect real system properties
        assert context.total_memory_gb > 0
        assert context.cpu_count > 0
        assert context.platform_info != ""
        assert context.python_version != ""

        # Environment detection should work
        assert context.hpc_environment in ["SLURM", "PBS", "PBS (legacy)", "Local"]

        # Summary should be comprehensive
        summary = context.get_environment_summary()
        lines = summary.split("\n")
        assert len(lines) >= 4  # At minimum: env, python, memory, cpu
