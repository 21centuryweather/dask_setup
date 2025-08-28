"""Unit tests for dask_setup.config module."""

import os
from unittest.mock import patch

import pytest

from dask_setup.config import ConfigProfile, DaskSetupConfig
from dask_setup.error_handling import ConfigurationValidationError


class TestDaskSetupConfig:
    """Test DaskSetupConfig class."""

    @pytest.mark.unit
    def test_default_config_creation(self):
        """Test creating config with default values."""
        config = DaskSetupConfig()

        # Check default values
        assert config.workload_type == "io"
        assert config.max_workers is None
        assert config.reserve_mem_gb == 50.0
        assert config.max_mem_gb is None
        assert config.dashboard is True
        assert config.adaptive is False
        assert config.min_workers is None
        assert config.temp_base_dir is None
        assert config.dashboard_port is None
        assert config.silence_logs is False
        assert config.memory_target == 0.75
        assert config.memory_spill == 0.85
        assert config.memory_pause == 0.92
        assert config.memory_terminate == 0.98
        assert config.name == ""
        assert config.description == ""
        assert config.tags == []

    @pytest.mark.unit
    def test_custom_config_creation(self):
        """Test creating config with custom values."""
        config = DaskSetupConfig(
            workload_type="cpu",
            max_workers=16,
            reserve_mem_gb=32.0,
            max_mem_gb=128.0,
            dashboard=False,
            adaptive=True,
            min_workers=4,
            temp_base_dir="/scratch/user",
            dashboard_port=8889,
            silence_logs=True,
            memory_target=0.8,
            memory_spill=0.9,
            memory_pause=0.95,
            memory_terminate=0.99,
            name="test-config",
            description="Test configuration",
            tags=["test", "cpu"],
        )

        assert config.workload_type == "cpu"
        assert config.max_workers == 16
        assert config.reserve_mem_gb == 32.0
        assert config.max_mem_gb == 128.0
        assert config.dashboard is False
        assert config.adaptive is True
        assert config.min_workers == 4
        assert config.temp_base_dir == "/scratch/user"
        assert config.dashboard_port == 8889
        assert config.silence_logs is True
        assert config.memory_target == 0.8
        assert config.memory_spill == 0.9
        assert config.memory_pause == 0.95
        assert config.memory_terminate == 0.99
        assert config.name == "test-config"
        assert config.description == "Test configuration"
        assert config.tags == ["test", "cpu"]

    @pytest.mark.unit
    def test_invalid_workload_type(self):
        """Test validation of invalid workload type."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(workload_type="invalid")

        assert "must be one of" in str(exc_info.value)
        assert "invalid" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize("workload_type", ["cpu", "io", "mixed"])
    def test_valid_workload_types(self, workload_type):
        """Test all valid workload types."""
        config = DaskSetupConfig(workload_type=workload_type)
        assert config.workload_type == workload_type

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_workers", [0, -1, -10])
    def test_invalid_max_workers(self, invalid_workers):
        """Test validation of invalid max_workers."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(max_workers=invalid_workers)

        assert "must be positive" in str(exc_info.value)
        assert str(invalid_workers) in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_workers", [0, -1, -5])
    def test_invalid_min_workers(self, invalid_workers):
        """Test validation of invalid min_workers."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(min_workers=invalid_workers)

        assert "must be positive" in str(exc_info.value)
        assert str(invalid_workers) in str(exc_info.value)

    @pytest.mark.unit
    def test_min_workers_greater_than_max_workers(self):
        """Test validation when min_workers > max_workers."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(max_workers=4, min_workers=8)

        assert "cannot be greater than max_workers" in str(exc_info.value)

    @pytest.mark.unit
    def test_valid_worker_combinations(self):
        """Test valid worker combinations."""
        # Equal min and max
        config1 = DaskSetupConfig(max_workers=8, min_workers=8)
        assert config1.max_workers == 8
        assert config1.min_workers == 8

        # Min less than max
        config2 = DaskSetupConfig(max_workers=16, min_workers=4)
        assert config2.max_workers == 16
        assert config2.min_workers == 4

        # Only max specified
        config3 = DaskSetupConfig(max_workers=10)
        assert config3.max_workers == 10
        assert config3.min_workers is None

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_reserve", [0.5, 1001.0, -1.0])
    def test_invalid_reserve_mem_gb(self, invalid_reserve):
        """Test validation of invalid reserve_mem_gb."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(reserve_mem_gb=invalid_reserve)

        assert f"must be between {DaskSetupConfig.MIN_RESERVE_MEM}" in str(exc_info.value)
        assert str(invalid_reserve) in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize("valid_reserve", [1.0, 50.0, 100.0, 500.0, 1000.0])
    def test_valid_reserve_mem_gb(self, valid_reserve):
        """Test valid reserve_mem_gb values."""
        config = DaskSetupConfig(reserve_mem_gb=valid_reserve)
        assert config.reserve_mem_gb == valid_reserve

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_max_mem", [0, -10.5])
    def test_invalid_max_mem_gb(self, invalid_max_mem):
        """Test validation of invalid max_mem_gb."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(max_mem_gb=invalid_max_mem)

        assert "must be positive" in str(exc_info.value)
        assert str(invalid_max_mem) in str(exc_info.value)

    @pytest.mark.unit
    def test_valid_max_mem_gb(self):
        """Test valid max_mem_gb values."""
        config = DaskSetupConfig(max_mem_gb=128.0)
        assert config.max_mem_gb == 128.0

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_threshold", [0.0, 1.1, -0.5, 2.0])
    def test_invalid_memory_thresholds(self, invalid_threshold):
        """Test validation of invalid memory thresholds."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(memory_target=invalid_threshold)

        assert "must be between 0.0 and 1.0" in str(exc_info.value)

    @pytest.mark.unit
    def test_memory_threshold_ordering_violation(self):
        """Test validation of memory threshold ordering."""
        # target >= spill should fail
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(memory_target=0.9, memory_spill=0.8)

        assert "must be in increasing order" in str(exc_info.value)

        # spill >= pause should fail
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(memory_spill=0.95, memory_pause=0.90)

        assert "must be in increasing order" in str(exc_info.value)

        # pause >= terminate should fail
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(memory_pause=0.99, memory_terminate=0.95)

        assert "must be in increasing order" in str(exc_info.value)

    @pytest.mark.unit
    def test_valid_memory_thresholds(self):
        """Test valid memory threshold configuration."""
        config = DaskSetupConfig(
            memory_target=0.6, memory_spill=0.7, memory_pause=0.8, memory_terminate=0.9
        )

        assert config.memory_target == 0.6
        assert config.memory_spill == 0.7
        assert config.memory_pause == 0.8
        assert config.memory_terminate == 0.9

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_port", [1023, 65536, 0, -1])
    def test_invalid_dashboard_port(self, invalid_port):
        """Test validation of invalid dashboard port."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(dashboard_port=invalid_port)

        assert "must be between 1024 and 65535" in str(exc_info.value)
        assert str(invalid_port) in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize("valid_port", [1024, 8787, 9999, 65535])
    def test_valid_dashboard_port(self, valid_port):
        """Test valid dashboard port values."""
        config = DaskSetupConfig(dashboard_port=valid_port)
        assert config.dashboard_port == valid_port

    @pytest.mark.unit
    def test_multiple_validation_errors(self):
        """Test that multiple validation errors are reported together."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(
                workload_type="invalid", max_workers=-1, reserve_mem_gb=0.5, dashboard_port=100
            )

        error_msg = str(exc_info.value)
        assert "must be one of" in error_msg
        assert "must be positive" in error_msg
        assert "must be between" in error_msg
        # Dashboard port error should be in the message (but format may vary)
        assert "dashboard_port" in error_msg and "between" in error_msg

    @pytest.mark.unit
    def test_to_dict(self):
        """Test conversion to dictionary."""
        config = DaskSetupConfig(
            workload_type="cpu",
            max_workers=8,
            reserve_mem_gb=32.0,
            name="test-config",
            description="Test description",
            tags=["test", "cpu"],
        )

        result = config.to_dict()

        # Check all expected keys are present
        expected_keys = {
            "workload_type",
            "max_workers",
            "reserve_mem_gb",
            "max_mem_gb",
            "dashboard",
            "adaptive",
            "min_workers",
            "temp_base_dir",
            "dashboard_port",
            "silence_logs",
            "memory_target",
            "memory_spill",
            "memory_pause",
            "memory_terminate",
            "spill_compression",
            "comm_compression",
            "spill_threads",
            "suggest_chunks",
            "io_format",
            "io_target_chunk_mb",
            "io_access_pattern",
            "io_storage_location",
            "io_compression_level",
            "name",
            "description",
            "tags",
        }
        assert set(result.keys()) == expected_keys

        # Check specific values
        assert result["workload_type"] == "cpu"
        assert result["max_workers"] == 8
        assert result["reserve_mem_gb"] == 32.0
        assert result["name"] == "test-config"
        assert result["description"] == "Test description"
        assert result["tags"] == ["test", "cpu"]

        # Check that tags is a copy (not the same object)
        assert result["tags"] is not config.tags

    @pytest.mark.unit
    def test_from_dict(self):
        """Test creation from dictionary."""
        data = {
            "workload_type": "mixed",
            "max_workers": 16,
            "reserve_mem_gb": 64.0,
            "adaptive": True,
            "name": "from-dict-test",
            "description": "Created from dict",
            "tags": ["mixed", "adaptive"],
            "unknown_field": "should be ignored",  # Unknown fields should be filtered
        }

        config = DaskSetupConfig.from_dict(data)

        assert config.workload_type == "mixed"
        assert config.max_workers == 16
        assert config.reserve_mem_gb == 64.0
        assert config.adaptive is True
        assert config.name == "from-dict-test"
        assert config.description == "Created from dict"
        assert config.tags == ["mixed", "adaptive"]

        # Verify unknown fields are not present
        assert not hasattr(config, "unknown_field")

    @pytest.mark.unit
    def test_from_dict_with_none_values(self):
        """Test creation from dictionary with None values (should be filtered)."""
        data = {
            "workload_type": "io",
            "max_workers": None,  # Should be filtered out
            "reserve_mem_gb": 25.0,
            "max_mem_gb": None,  # Should be filtered out
            "name": None,  # Should be filtered out
        }

        config = DaskSetupConfig.from_dict(data)

        assert config.workload_type == "io"
        assert config.max_workers is None  # Default value
        assert config.reserve_mem_gb == 25.0
        assert config.max_mem_gb is None  # Default value
        assert config.name == ""  # Default value

    @pytest.mark.unit
    def test_from_dict_skip_validation(self):
        """Test creation from dict with validation still applied (current implementation)."""
        # Note: Current implementation still validates during creation
        # The skip_validation flag affects internal behavior but doesn't prevent initial validation
        valid_data_with_skip = {
            "workload_type": "cpu",
            "max_workers": 8,
        }

        # Should work with skip_validation=True
        config = DaskSetupConfig.from_dict(valid_data_with_skip, skip_validation=True)
        assert config.workload_type == "cpu"
        assert config.max_workers == 8

        # The _skip_validation flag should be set
        assert config._skip_validation is True

    @pytest.mark.unit
    def test_merge_with_none(self):
        """Test merging with None (should return self)."""
        config = DaskSetupConfig(workload_type="cpu", max_workers=8)
        result = config.merge_with(None)

        assert result.workload_type == "cpu"
        assert result.max_workers == 8

    @pytest.mark.unit
    def test_merge_with_other_config(self):
        """Test merging with another configuration."""
        base_config = DaskSetupConfig(
            workload_type="cpu",
            max_workers=8,
            reserve_mem_gb=32.0,
            adaptive=False,
            name="base",
            tags=["base", "cpu"],
        )

        override_config = DaskSetupConfig(
            workload_type="io",  # Should override
            reserve_mem_gb=64.0,  # Should override
            adaptive=True,  # Should override
            dashboard_port=9999,  # Should override (new field)
            name="override",  # Should override
            tags=["override", "io"],  # Should be merged
        )

        result = base_config.merge_with(override_config)

        # Check overridden values
        assert result.workload_type == "io"
        assert result.reserve_mem_gb == 64.0
        assert result.adaptive is True
        assert result.dashboard_port == 9999
        assert result.name == "override"

        # Check preserved values
        assert result.max_workers == 8  # From base

        # Check merged tags (should be combined and deduplicated)
        expected_tags = {"base", "cpu", "override", "io"}
        assert set(result.tags) == expected_tags

    @pytest.mark.unit
    def test_merge_with_validation(self):
        """Test that merge result is validated."""
        DaskSetupConfig(workload_type="cpu")

        # Create an invalid override config that passes initial validation but creates invalid merge
        # Use a valid config that becomes invalid when merged with base
        invalid_override = DaskSetupConfig(
            min_workers=16  # This will create invalid state when merged with base max_workers=None
        )

        # Set max_workers on base to create the conflict
        base_config_with_max = DaskSetupConfig(workload_type="cpu", max_workers=8)

        # Merge should fail validation because min_workers (16) > max_workers (8)
        with pytest.raises(ConfigurationValidationError) as exc_info:
            base_config_with_max.merge_with(invalid_override)

        assert "cannot be greater than max_workers" in str(exc_info.value)

    @pytest.mark.unit
    def test_get_setup_client_kwargs(self):
        """Test getting parameters for setup_dask_client."""
        config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=12,
            reserve_mem_gb=48.0,
            max_mem_gb=256.0,
            dashboard=False,
            adaptive=True,
            min_workers=4,
            # These should NOT be in the result
            temp_base_dir="/scratch",
            dashboard_port=8888,
            silence_logs=True,
            name="test",
        )

        kwargs = config.get_setup_client_kwargs()

        expected_kwargs = {
            "workload_type": "mixed",
            "max_workers": 12,
            "reserve_mem_gb": 48.0,
            "max_mem_gb": 256.0,
            "dashboard": False,
            "adaptive": True,
            "min_workers": 4,
        }

        assert kwargs == expected_kwargs

    @pytest.mark.unit
    @patch.dict(os.environ, {}, clear=True)
    def test_validate_against_environment_high_memory_reservation(self):
        """Test environment validation with high memory reservation."""
        config = DaskSetupConfig(reserve_mem_gb=150.0)
        warnings = config.validate_against_environment()

        assert len(warnings) >= 1
        assert any("High memory reservation" in w for w in warnings)

    @pytest.mark.unit
    def test_validate_against_environment_adaptive_single_worker(self):
        """Test environment validation with adaptive scaling and single worker."""
        config = DaskSetupConfig(adaptive=True, max_workers=1)
        warnings = config.validate_against_environment()

        assert any("Adaptive scaling with max_workers=1 has no effect" in w for w in warnings)

    @pytest.mark.unit
    @patch.dict(os.environ, {"PBS_JOBFS": "/scratch/123"})
    def test_validate_against_environment_pbs_io_workload(self):
        """Test environment validation with PBS and I/O workload (good combo)."""
        config = DaskSetupConfig(workload_type="io", reserve_mem_gb=30.0)
        warnings = config.validate_against_environment()

        # Should not warn about I/O + PBS combination
        assert not any("PBS_JOBFS" in w for w in warnings)

    @pytest.mark.unit
    @patch.dict(os.environ, {}, clear=True)
    def test_validate_against_environment_non_hpc_high_memory(self):
        """Test environment validation without PBS but high memory reservation."""
        config = DaskSetupConfig(reserve_mem_gb=50.0)  # High for non-HPC
        warnings = config.validate_against_environment()

        assert any("non-HPC environments" in w for w in warnings)

    @pytest.mark.unit
    @patch.dict(os.environ, {}, clear=True)
    def test_validate_against_environment_no_warnings(self):
        """Test environment validation with no warnings."""
        config = DaskSetupConfig(
            workload_type="cpu",
            adaptive=False,
            max_workers=4,
            reserve_mem_gb=16.0,  # Reasonable for non-HPC
        )
        warnings = config.validate_against_environment()

        assert len(warnings) == 0


class TestConfigProfile:
    """Test ConfigProfile class."""

    @pytest.mark.unit
    def test_basic_profile_creation(self):
        """Test creating a basic profile."""
        config = DaskSetupConfig(
            workload_type="cpu", max_workers=8, name="test-config", description="Test configuration"
        )

        profile = ConfigProfile(name="cpu-profile", config=config)

        assert profile.name == "cpu-profile"
        assert profile.config.workload_type == "cpu"
        assert profile.config.max_workers == 8
        assert profile.builtin is False
        assert profile.created_at is None
        assert profile.modified_at is None

    @pytest.mark.unit
    def test_profile_sets_config_name(self):
        """Test that profile sets config name if not already set."""
        config = DaskSetupConfig(workload_type="io")  # No name set
        profile = ConfigProfile(name="io-profile", config=config)

        # Profile should set the config name
        assert profile.config.name == "io-profile"

    @pytest.mark.unit
    def test_profile_preserves_existing_config_name(self):
        """Test that profile preserves existing config name."""
        config = DaskSetupConfig(workload_type="io", name="existing-name")
        profile = ConfigProfile(name="io-profile", config=config)

        # Should preserve existing config name
        assert profile.config.name == "existing-name"

    @pytest.mark.unit
    def test_profile_with_timestamps(self):
        """Test profile creation with timestamps."""
        config = DaskSetupConfig(workload_type="mixed")

        profile = ConfigProfile(
            name="timestamped-profile",
            config=config,
            builtin=True,
            created_at="2023-01-01T00:00:00Z",
            modified_at="2023-01-02T00:00:00Z",
        )

        assert profile.builtin is True
        assert profile.created_at == "2023-01-01T00:00:00Z"
        assert profile.modified_at == "2023-01-02T00:00:00Z"

    @pytest.mark.unit
    def test_profile_description_property(self):
        """Test profile description property."""
        # With explicit description
        config1 = DaskSetupConfig(description="Explicit description")
        profile1 = ConfigProfile(name="test1", config=config1)
        assert profile1.description == "Explicit description"

        # Without explicit description (should use default)
        config2 = DaskSetupConfig()  # No description
        profile2 = ConfigProfile(name="test2", config=config2)
        assert profile2.description == "Configuration profile: test2"

    @pytest.mark.unit
    def test_profile_tags_property(self):
        """Test profile tags property."""
        config = DaskSetupConfig(tags=["cpu", "high-mem", "testing"])
        profile = ConfigProfile(name="tagged-profile", config=config)

        tags = profile.tags
        assert set(tags) == {"cpu", "high-mem", "testing"}

        # Should return a copy, not the original
        assert tags is not config.tags

    @pytest.mark.unit
    def test_profile_to_dict(self):
        """Test profile serialization to dictionary."""
        config = DaskSetupConfig(
            workload_type="cpu",
            max_workers=16,
            name="profile-config",
            description="Profile description",
            tags=["cpu", "production"],
        )

        profile = ConfigProfile(
            name="prod-cpu",
            config=config,
            builtin=True,
            created_at="2023-01-01T00:00:00Z",
            modified_at="2023-06-01T00:00:00Z",
        )

        result = profile.to_dict()

        expected_keys = {"name", "config", "builtin", "created_at", "modified_at"}
        assert set(result.keys()) == expected_keys

        assert result["name"] == "prod-cpu"
        assert result["builtin"] is True
        assert result["created_at"] == "2023-01-01T00:00:00Z"
        assert result["modified_at"] == "2023-06-01T00:00:00Z"

        # Check nested config
        config_dict = result["config"]
        assert config_dict["workload_type"] == "cpu"
        assert config_dict["max_workers"] == 16
        assert config_dict["name"] == "profile-config"
        assert config_dict["tags"] == ["cpu", "production"]

    @pytest.mark.unit
    def test_profile_from_dict(self):
        """Test profile creation from dictionary."""
        data = {
            "name": "from-dict-profile",
            "config": {
                "workload_type": "io",
                "max_workers": 4,
                "adaptive": True,
                "name": "io-config",
                "description": "I/O workload configuration",
                "tags": ["io", "adaptive"],
            },
            "builtin": False,
            "created_at": "2023-03-01T12:00:00Z",
            "modified_at": "2023-03-15T14:30:00Z",
        }

        profile = ConfigProfile.from_dict(data)

        assert profile.name == "from-dict-profile"
        assert profile.builtin is False
        assert profile.created_at == "2023-03-01T12:00:00Z"
        assert profile.modified_at == "2023-03-15T14:30:00Z"

        # Check nested config
        assert profile.config.workload_type == "io"
        assert profile.config.max_workers == 4
        assert profile.config.adaptive is True
        assert profile.config.name == "io-config"
        assert profile.config.description == "I/O workload configuration"
        assert profile.config.tags == ["io", "adaptive"]

    @pytest.mark.unit
    def test_profile_from_dict_minimal(self):
        """Test profile creation from minimal dictionary."""
        data = {"name": "minimal-profile", "config": {"workload_type": "mixed"}}

        profile = ConfigProfile.from_dict(data)

        assert profile.name == "minimal-profile"
        assert profile.builtin is False  # Default
        assert profile.created_at is None  # Default
        assert profile.modified_at is None  # Default

        # Config should have defaults
        assert profile.config.workload_type == "mixed"
        assert profile.config.reserve_mem_gb == 50.0  # Default
        assert profile.config.dashboard is True  # Default

    @pytest.mark.unit
    def test_profile_from_dict_empty_config(self):
        """Test profile creation with empty config dictionary."""
        data = {
            "name": "empty-config-profile",
            # No config provided
        }

        profile = ConfigProfile.from_dict(data)

        assert profile.name == "empty-config-profile"
        # Should create config with all defaults
        assert profile.config.workload_type == "io"  # Default
        assert profile.config.reserve_mem_gb == 50.0  # Default


class TestConfigIntegration:
    """Integration tests for config classes working together."""

    @pytest.mark.unit
    def test_config_profile_roundtrip_serialization(self):
        """Test complete serialization roundtrip for profiles."""
        # Create complex config
        original_config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=12,
            min_workers=4,
            reserve_mem_gb=64.0,
            max_mem_gb=512.0,
            dashboard=False,
            adaptive=True,
            temp_base_dir="/scratch/user",
            dashboard_port=8888,
            silence_logs=True,
            memory_target=0.6,
            memory_spill=0.75,
            memory_pause=0.85,
            memory_terminate=0.95,
            name="complex-config",
            description="Complex test configuration",
            tags=["mixed", "adaptive", "high-mem"],
        )

        # Create profile
        original_profile = ConfigProfile(
            name="complex-profile",
            config=original_config,
            builtin=False,
            created_at="2023-01-01T00:00:00Z",
            modified_at="2023-06-01T00:00:00Z",
        )

        # Serialize to dict
        profile_dict = original_profile.to_dict()

        # Deserialize back
        restored_profile = ConfigProfile.from_dict(profile_dict)

        # Verify profile fields
        assert restored_profile.name == original_profile.name
        assert restored_profile.builtin == original_profile.builtin
        assert restored_profile.created_at == original_profile.created_at
        assert restored_profile.modified_at == original_profile.modified_at

        # Verify config fields
        restored_config = restored_profile.config
        original_config_dict = original_config.to_dict()
        restored_config_dict = restored_config.to_dict()

        assert restored_config_dict == original_config_dict

    @pytest.mark.unit
    def test_config_merge_and_validation_integration(self):
        """Test integration between config merging and validation."""
        base_config = DaskSetupConfig(workload_type="cpu", max_workers=8, reserve_mem_gb=32.0)

        # Create valid override
        valid_override = DaskSetupConfig(workload_type="io", adaptive=True, min_workers=2)

        # Merge should succeed
        merged = base_config.merge_with(valid_override)
        assert merged.workload_type == "io"
        assert merged.max_workers == 8  # From base
        assert merged.adaptive is True  # From override
        assert merged.min_workers == 2  # From override

        # Create invalid override (min > max workers)
        invalid_override = DaskSetupConfig.from_dict(
            {
                "min_workers": 16,  # Greater than base max_workers=8
            },
            skip_validation=True,
        )

        # Merge should fail validation
        with pytest.raises(ConfigurationValidationError) as exc_info:
            base_config.merge_with(invalid_override)

        assert "cannot be greater than max_workers" in str(exc_info.value)

    @pytest.mark.unit
    def test_complex_validation_scenarios(self):
        """Test complex validation scenarios."""
        # Test valid complex configuration
        valid_config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=16,
            min_workers=4,
            reserve_mem_gb=128.0,
            max_mem_gb=1024.0,
            dashboard=True,
            dashboard_port=8889,
            adaptive=True,
            memory_target=0.65,
            memory_spill=0.8,
            memory_pause=0.9,
            memory_terminate=0.98,
            temp_base_dir="/fast/scratch",
            silence_logs=False,
            name="production-mixed",
            description="Production mixed workload configuration",
            tags=["production", "mixed", "adaptive", "high-mem"],
        )

        # Should not raise any exceptions
        assert valid_config.workload_type == "mixed"

        # Test environment warnings for this config
        warnings = valid_config.validate_against_environment()
        # High memory reservation should generate warning
        assert any("High memory reservation" in w for w in warnings)

    @pytest.mark.unit
    def test_profile_config_name_synchronization(self):
        """Test that profile and config names stay synchronized."""
        # Create config without name
        config = DaskSetupConfig(workload_type="cpu")
        assert config.name == ""

        # Create profile - should set config name
        profile = ConfigProfile(name="sync-test", config=config)
        assert profile.config.name == "sync-test"

        # Serialize and deserialize
        profile_dict = profile.to_dict()
        restored_profile = ConfigProfile.from_dict(profile_dict)

        # Names should still be synchronized
        assert restored_profile.name == "sync-test"
        assert restored_profile.config.name == "sync-test"
