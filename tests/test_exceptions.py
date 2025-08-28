"""Unit tests for dask_setup.exceptions module."""

import pytest

from dask_setup.exceptions import (
    DaskSetupError,
    InsufficientResourcesError,
    InvalidConfigurationError,
    ResourceDetectionError,
)


class TestDaskSetupError:
    """Test base DaskSetupError exception."""

    @pytest.mark.unit
    def test_basic_exception(self):
        """Test creating a basic DaskSetupError."""
        error = DaskSetupError("test error")
        assert str(error) == "test error"
        assert isinstance(error, Exception)

    @pytest.mark.unit
    def test_inheritance(self):
        """Test DaskSetupError inheritance."""
        error = DaskSetupError("test error")
        assert isinstance(error, Exception)

    @pytest.mark.unit
    def test_empty_message(self):
        """Test DaskSetupError with empty message."""
        error = DaskSetupError("")
        assert str(error) == ""

    @pytest.mark.unit
    def test_with_cause(self):
        """Test DaskSetupError with underlying cause."""
        try:
            raise ValueError("original error")
        except ValueError as e:
            try:
                raise DaskSetupError("wrapped error") from e
            except DaskSetupError as wrapped_error:
                assert str(wrapped_error) == "wrapped error"
                assert wrapped_error.__cause__ is e


class TestResourceDetectionError:
    """Test ResourceDetectionError exception."""

    @pytest.mark.unit
    def test_basic_exception(self):
        """Test creating a basic ResourceDetectionError."""
        error = ResourceDetectionError("Failed to detect PBS resources")
        assert str(error) == "Failed to detect PBS resources"
        assert isinstance(error, DaskSetupError)

    @pytest.mark.unit
    def test_inheritance_chain(self):
        """Test ResourceDetectionError inheritance chain."""
        error = ResourceDetectionError("test error")
        assert isinstance(error, ResourceDetectionError)
        assert isinstance(error, DaskSetupError)
        assert isinstance(error, Exception)

    @pytest.mark.unit
    def test_detection_scenarios(self):
        """Test ResourceDetectionError for various detection scenarios."""
        # PBS detection failure
        pbs_error = ResourceDetectionError("PBS_NODEFILE not found")
        assert "PBS_NODEFILE" in str(pbs_error)

        # SLURM detection failure
        slurm_error = ResourceDetectionError("SLURM_CPUS_PER_TASK not set")
        assert "SLURM_CPUS_PER_TASK" in str(slurm_error)

        # psutil detection failure
        psutil_error = ResourceDetectionError("Cannot access system resources")
        assert "system resources" in str(psutil_error)


class TestInvalidConfigurationError:
    """Test InvalidConfigurationError exception."""

    @pytest.mark.unit
    def test_basic_exception(self):
        """Test creating a basic InvalidConfigurationError."""
        error = InvalidConfigurationError("Invalid configuration parameter")
        assert str(error) == "Invalid configuration parameter"
        assert isinstance(error, DaskSetupError)

    @pytest.mark.unit
    def test_inheritance_chain(self):
        """Test InvalidConfigurationError inheritance chain."""
        error = InvalidConfigurationError("test error")
        assert isinstance(error, InvalidConfigurationError)
        assert isinstance(error, DaskSetupError)
        assert isinstance(error, Exception)

    @pytest.mark.unit
    def test_validation_scenarios(self):
        """Test InvalidConfigurationError for validation scenarios."""
        # Worker count validation
        worker_error = InvalidConfigurationError("n_workers must be positive, got -1")
        assert "n_workers must be positive" in str(worker_error)
        assert "got -1" in str(worker_error)

        # Profile validation
        profile_error = InvalidConfigurationError("Profile 'nonexistent' not found")
        assert "Profile 'nonexistent'" in str(profile_error)
        assert "not found" in str(profile_error)

        # Configuration file error
        file_error = InvalidConfigurationError("Cannot read config file: /path/to/config.yaml")
        assert "Cannot read config file" in str(file_error)
        assert "/path/to/config.yaml" in str(file_error)


class TestInsufficientResourcesError:
    """Test InsufficientResourcesError exception."""

    @pytest.mark.unit
    def test_basic_exception(self):
        """Test creating a basic InsufficientResourcesError."""
        error = InsufficientResourcesError(16.0, 8.0)

        # Check inheritance
        assert isinstance(error, InsufficientResourcesError)
        assert isinstance(error, DaskSetupError)
        assert isinstance(error, Exception)

        # Check attributes
        assert error.required_mem == 16.0
        assert error.available_mem == 8.0
        assert error.suggested_actions == []

    @pytest.mark.unit
    def test_detailed_message(self):
        """Test InsufficientResourcesError creates detailed message."""
        error = InsufficientResourcesError(16.0, 8.0)
        message = str(error)

        # Check message contains key information
        assert "❌ Insufficient memory for configuration" in message
        assert "Required: 16.0 GB" in message
        assert "Available: 8.0 GB" in message
        assert "Shortfall: 8.0 GB" in message

    @pytest.mark.unit
    def test_with_suggestions(self):
        """Test InsufficientResourcesError with suggested actions."""
        suggestions = [
            "Reduce number of workers",
            "Use a compute node with more memory",
            "Reduce memory_limit_per_worker",
        ]
        error = InsufficientResourcesError(32.0, 16.0, suggestions)
        message = str(error)

        # Check suggestions are included
        assert "💡 Suggestions:" in message
        assert "1. Reduce number of workers" in message
        assert "2. Use a compute node with more memory" in message
        assert "3. Reduce memory_limit_per_worker" in message

        # Check attributes
        assert error.suggested_actions == suggestions

    @pytest.mark.unit
    def test_zero_shortfall(self):
        """Test InsufficientResourcesError with zero shortfall."""
        error = InsufficientResourcesError(8.0, 8.0)
        message = str(error)

        assert "Shortfall: 0.0 GB" in message

    @pytest.mark.unit
    def test_fractional_memory(self):
        """Test InsufficientResourcesError with fractional memory values."""
        error = InsufficientResourcesError(7.5, 4.2)
        message = str(error)

        assert "Required: 7.5 GB" in message
        assert "Available: 4.2 GB" in message
        assert "Shortfall: 3.3 GB" in message

    @pytest.mark.unit
    def test_empty_suggestions_list(self):
        """Test InsufficientResourcesError with empty suggestions."""
        error = InsufficientResourcesError(16.0, 8.0, [])
        message = str(error)

        # Should not contain suggestions section
        assert "💡 Suggestions:" not in message
        assert error.suggested_actions == []

    @pytest.mark.unit
    def test_default_suggestions(self):
        """Test InsufficientResourcesError with default suggestions."""
        error = InsufficientResourcesError(16.0, 8.0)
        assert error.suggested_actions == []


class TestExceptionHierarchy:
    """Test exception hierarchy and relationships."""

    @pytest.mark.unit
    def test_all_inherit_from_base(self):
        """Test all custom exceptions inherit from DaskSetupError."""
        exceptions = [
            ResourceDetectionError("test"),
            InvalidConfigurationError("test"),
            InsufficientResourcesError(8.0, 4.0),
        ]

        for exc in exceptions:
            assert isinstance(exc, DaskSetupError)
            assert isinstance(exc, Exception)

    @pytest.mark.unit
    def test_exception_types_are_distinct(self):
        """Test all exception types are distinct classes."""
        exceptions = [
            ResourceDetectionError,
            InvalidConfigurationError,
            InsufficientResourcesError,
        ]

        # Each should be a different class
        for i, exc1 in enumerate(exceptions):
            for j, exc2 in enumerate(exceptions):
                if i != j:
                    assert exc1 is not exc2

    @pytest.mark.unit
    def test_catching_base_exception(self):
        """Test that DaskSetupError can catch all custom exceptions."""
        exceptions_to_test = [
            ResourceDetectionError("test"),
            InvalidConfigurationError("test"),
            InsufficientResourcesError(16.0, 8.0),
        ]

        for exc in exceptions_to_test:
            try:
                raise exc
            except DaskSetupError:
                # Should catch successfully
                pass
            except Exception:
                pytest.fail(f"DaskSetupError should have caught {type(exc).__name__}")

    @pytest.mark.unit
    def test_specific_exception_catching(self):
        """Test catching specific exception types."""
        # Test ResourceDetectionError specifically
        try:
            raise ResourceDetectionError("PBS detection failed")
        except ResourceDetectionError as e:
            assert "PBS detection failed" in str(e)

        # Test InvalidConfigurationError specifically
        try:
            raise InvalidConfigurationError("Invalid profile")
        except InvalidConfigurationError as e:
            assert "Invalid profile" in str(e)

        # Test InsufficientResourcesError specifically
        try:
            raise InsufficientResourcesError(16.0, 8.0)
        except InsufficientResourcesError as e:
            assert e.required_mem == 16.0
            assert e.available_mem == 8.0

        # Test that wrong exception type isn't caught
        with pytest.raises(InvalidConfigurationError):
            try:
                raise InvalidConfigurationError("config error")
            except ResourceDetectionError:
                pytest.fail(
                    "Should not catch InvalidConfigurationError with ResourceDetectionError handler"
                )

    @pytest.mark.unit
    def test_exception_chaining(self):
        """Test exception chaining with actual exceptions."""
        # Test chaining with ResourceDetectionError
        try:
            raise FileNotFoundError("PBS_NODEFILE not found")
        except FileNotFoundError as e:
            try:
                raise ResourceDetectionError("Failed to detect PBS resources") from e
            except ResourceDetectionError as wrapped:
                assert wrapped.__cause__ is e
                assert isinstance(wrapped.__cause__, FileNotFoundError)

        # Test chaining with InsufficientResourcesError
        try:
            raise MemoryError("Not enough memory")
        except MemoryError as e:
            try:
                raise InsufficientResourcesError(32.0, 16.0) from e
            except InsufficientResourcesError as wrapped:
                assert wrapped.__cause__ is e
                assert isinstance(wrapped.__cause__, MemoryError)
