"""Unit tests for dask_setup.client module."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from dask_setup.client import (
    _compute_smart_reserve_default,
    _resolve_configuration,
    setup_dask_client,
)
from dask_setup.config import ConfigProfile, DaskSetupConfig
from dask_setup.exceptions import InsufficientResourcesError
from dask_setup.types import MemorySpec, ResourceSpec, TopologySpec


class TestResolveConfiguration:
    """Test configuration resolution logic."""

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_resolve_configuration_defaults_only(self, mock_config_manager):
        """Test configuration resolution with only defaults."""
        # Don't use any profile
        config = _resolve_configuration()

        # Should return default configuration
        assert config.workload_type == "io"
        assert config.max_workers is None
        # The reserve default scales with the machine (20% of RAM, clamped to
        # [4, 50]); a flat 50.0 reserved more than the whole of a 16 GiB laptop.
        assert config.reserve_mem_gb == _compute_smart_reserve_default()
        assert 4.0 <= config.reserve_mem_gb <= 50.0
        assert config.max_mem_gb is None
        assert config.dashboard is True
        assert config.adaptive is False
        assert config.min_workers is None

        # ConfigManager should not be called since no profile was specified
        mock_config_manager.assert_not_called()

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_resolve_configuration_explicit_params_only(self, mock_config_manager):
        """Test configuration resolution with explicit parameters."""
        config = _resolve_configuration(
            workload_type="cpu",
            max_workers=8,
            reserve_mem_gb=32.0,
            max_mem_gb=64.0,
            dashboard=False,
            adaptive=True,
            min_workers=2,
        )

        # Should use explicit parameters
        assert config.workload_type == "cpu"
        assert config.max_workers == 8
        assert config.reserve_mem_gb == 32.0
        assert config.max_mem_gb == 64.0
        assert config.dashboard is False
        assert config.adaptive is True
        assert config.min_workers == 2

        # ConfigManager should not be called since no profile was specified
        mock_config_manager.assert_not_called()

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_resolve_configuration_profile_only(self, mock_config_manager):
        """Test configuration resolution with profile only."""
        # Setup mock profile
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        profile_config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=6,
            reserve_mem_gb=40.0,
            dashboard=False,
            adaptive=True,
            min_workers=1,
        )
        profile_obj = ConfigProfile(name="test_profile", config=profile_config, builtin=True)
        mock_manager.get_profile.return_value = profile_obj

        config = _resolve_configuration(profile="test_profile")

        # Should use profile configuration
        assert config.workload_type == "mixed"
        assert config.max_workers == 6
        assert config.reserve_mem_gb == 40.0
        assert config.dashboard is False
        assert config.adaptive is True
        assert config.min_workers == 1

        mock_manager.get_profile.assert_called_once_with("test_profile")

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_resolve_configuration_explicit_overrides_profile(self, mock_config_manager):
        """Test that explicit parameters override profile settings."""
        # Setup mock profile
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        profile_config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=6,
            reserve_mem_gb=40.0,
            dashboard=False,
            adaptive=True,
            min_workers=1,
        )
        profile_obj = ConfigProfile(name="base_profile", config=profile_config, builtin=True)
        mock_manager.get_profile.return_value = profile_obj

        # Override some profile settings. Merge order is
        # defaults < profile < explicit, and only parameters that were
        # actually supplied count as explicit.
        config = _resolve_configuration(
            profile="base_profile",
            workload_type="cpu",
            max_workers=10,
            dashboard=False,
            adaptive=True,
        )

        # Supplied parameters win over the profile
        assert config.workload_type == "cpu"
        assert config.max_workers == 10
        assert config.dashboard is False
        assert config.adaptive is True

        # Parameters that were NOT supplied keep the profile's values rather
        # than falling back to the library defaults. This is the regression
        # guard for the merge bug where a throwaway DaskSetupConfig built from
        # the explicit kwargs carried its own defaults over the profile.
        assert config.reserve_mem_gb == 40.0  # from the profile, not the 50.0 default
        assert config.min_workers == 1  # from the profile

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_resolve_configuration_profile_not_found(self, mock_config_manager):
        """Test error handling when profile is not found."""
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        mock_manager.get_profile.return_value = None
        mock_manager.list_profiles.return_value = {
            "available1": MagicMock(),
            "available2": MagicMock(),
        }

        with pytest.raises(ValueError) as exc_info:
            _resolve_configuration(profile="nonexistent")

        assert "Profile 'nonexistent' not found" in str(exc_info.value)
        assert "available1" in str(exc_info.value)
        assert "available2" in str(exc_info.value)

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_resolve_configuration_default_detection(self, mock_config_manager):
        """Test that default values are properly detected and not treated as explicit."""
        # Use default values - should not trigger explicit config creation
        config = _resolve_configuration(
            workload_type="io",  # Default
            reserve_mem_gb=50.0,  # Default
            dashboard=True,  # Default
            adaptive=False,  # Default
        )

        # Should result in default configuration since all values match defaults
        expected_defaults = DaskSetupConfig()
        assert config.workload_type == expected_defaults.workload_type
        assert config.reserve_mem_gb == expected_defaults.reserve_mem_gb
        assert config.dashboard == expected_defaults.dashboard
        assert config.adaptive == expected_defaults.adaptive


class TestSetupDaskClient:
    """Test main setup_dask_client function."""

    def setup_method(self):
        """Set up common test fixtures."""
        # Standard resource spec for testing
        self.test_resources = ResourceSpec(
            total_cores=8,
            total_mem_bytes=32 * (1024**3),  # 32 GB
            detection_method="test",
        )

        # Standard topology spec for testing
        self.test_topology = TopologySpec(
            n_workers=4, threads_per_worker=2, processes=True, workload_type="io"
        )

        # Standard memory spec for testing
        self.test_memory_spec = MemorySpec(
            total_mem_gib=32.0,
            usable_mem_gb=30.0,
            mem_per_worker_bytes=7 * (1024**3),  # 7 GB per worker
            reserved_mem_gb=2.0,
        )

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_basic_success(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test successful basic setup with default parameters."""
        # Setup mocks
        config = DaskSetupConfig(workload_type="io", dashboard=True)
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Call setup_dask_client
        client, cluster, temp_dir = setup_dask_client()

        # Verify return values
        assert client is mock_client
        assert cluster is mock_cluster
        assert temp_dir == "/tmp/dask-temp"

        # Verify function calls
        # Nothing was supplied, so every parameter forwards as the None
        # sentinel — _resolve_configuration supplies the defaults.
        mock_resolve_config.assert_called_once_with(
            profile=None,
            workload_type=None,
            max_workers=None,
            reserve_mem_gb=None,
            max_mem_gb=None,
            dashboard=None,
            adaptive=None,
            min_workers=None,
            suggest_chunks=None,
            input_config=None,
        )
        mock_detect_resources.assert_called_once()
        mock_create_temp_dir.assert_called_once_with(base_dir=config.temp_base_dir)
        mock_decide_topology.assert_called_once_with(
            workload_type="io",
            total_cores=8,
            max_workers=None,
        )
        mock_validate_topology.assert_called_once_with(self.test_topology, 8)
        mock_calculate_memory.assert_called_once_with(
            total_mem_bytes=self.test_resources.total_mem_bytes,
            n_workers=4,
            reserve_mem_gb=50.0,
            max_mem_gb=None,
        )
        mock_create_cluster.assert_called_once_with(
            topology=self.test_topology,
            memory_spec=self.test_memory_spec,
            temp_dir="/tmp/dask-temp",
            dashboard_address=":0",
            silence_logs=logging.WARNING,
            adaptive=False,
            min_workers=None,
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )
        mock_client_class.assert_called_once_with(mock_cluster)
        mock_print_dashboard.assert_called_once_with(mock_client, silent=False)

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_with_profile(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test setup with profile configuration."""
        # Setup config with profile-specific settings
        config = DaskSetupConfig(
            workload_type="cpu",
            max_workers=6,
            reserve_mem_gb=40.0,
            dashboard=False,
            adaptive=True,
            min_workers=2,
            silence_logs=True,
        )
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Call with profile
        client, cluster, temp_dir = setup_dask_client(
            profile="cpu_profile",
            max_workers=8,  # Should override profile setting
        )

        # Verify configuration resolution was called with profile
        # Only max_workers was supplied; every other parameter forwards as
        # None so the profile's values survive.
        mock_resolve_config.assert_called_once_with(
            profile="cpu_profile",
            workload_type=None,
            max_workers=8,  # Explicit override
            reserve_mem_gb=None,
            max_mem_gb=None,
            dashboard=None,
            adaptive=None,
            min_workers=None,
            suggest_chunks=None,
            input_config=None,
        )

        # Verify topology uses resolved config
        mock_decide_topology.assert_called_once_with(
            workload_type="cpu",  # From resolved config
            total_cores=8,
            max_workers=6,  # From resolved config (profile won, explicit didn't override due to default detection)
        )

        # Verify dashboard is not printed when disabled
        assert not mock_print_dashboard.called

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_custom_dashboard_port(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test setup with custom dashboard port."""
        config = DaskSetupConfig(dashboard=True, dashboard_port=8787)
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        setup_dask_client()

        # Verify cluster created with specific dashboard port
        mock_create_cluster.assert_called_once_with(
            topology=self.test_topology,
            memory_spec=self.test_memory_spec,
            temp_dir="/tmp/dask-temp",
            dashboard_address=":8787",  # Custom port
            silence_logs=logging.WARNING,
            adaptive=False,
            min_workers=None,
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )

    @pytest.mark.unit
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    def test_setup_dask_client_insufficient_resources_error(
        self,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
    ):
        """Test error handling for insufficient resources."""
        config = DaskSetupConfig(workload_type="cpu", max_workers=8, reserve_mem_gb=20.0)
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = self.test_topology

        # Mock memory calculation failure
        mock_calculate_memory.side_effect = ValueError("Insufficient memory")

        with pytest.raises(InsufficientResourcesError) as exc_info:
            setup_dask_client()

        # Verify error details
        error = exc_info.value
        assert error.required_mem > 0
        assert error.available_mem > 0
        assert len(error.suggested_actions) > 0

        # Check for reasonable suggestions
        suggestions_text = " ".join(error.suggested_actions)
        assert (
            "Reduce reserve_mem_gb" in suggestions_text
            or "Limit max_workers" in suggestions_text
            or "larger memory allocation" in suggestions_text
        )

    @pytest.mark.unit
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    def test_setup_dask_client_memory_suggestions(
        self,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
    ):
        """Test specific memory error suggestions."""
        # High reserve_mem_gb should trigger reduction suggestion
        config = DaskSetupConfig(
            reserve_mem_gb=100.0,  # Very high
            max_workers=1,
        )
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"

        topology = TopologySpec(
            n_workers=1, threads_per_worker=8, processes=True, workload_type="cpu"
        )
        mock_decide_topology.return_value = topology
        mock_calculate_memory.side_effect = ValueError("Insufficient memory")

        with pytest.raises(InsufficientResourcesError) as exc_info:
            setup_dask_client()

        suggestions = exc_info.value.suggested_actions
        # Should suggest reducing reserve_mem_gb
        assert any("Reduce reserve_mem_gb from 100" in s for s in suggestions)

    @pytest.mark.unit
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    def test_setup_dask_client_worker_count_suggestions(
        self,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
    ):
        """Test worker count reduction suggestions."""
        config = DaskSetupConfig(
            reserve_mem_gb=5.0,  # Low, won't trigger memory reduction
            max_workers=8,
        )
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"

        topology = TopologySpec(
            n_workers=8, threads_per_worker=1, processes=True, workload_type="cpu"
        )
        mock_decide_topology.return_value = topology
        mock_calculate_memory.side_effect = ValueError("Insufficient memory")

        with pytest.raises(InsufficientResourcesError) as exc_info:
            setup_dask_client()

        suggestions = exc_info.value.suggested_actions
        # Should suggest limiting workers since reserve_mem_gb is low but n_workers > 1
        assert any("Limit max_workers" in s for s in suggestions)

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_adaptive_scaling(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test setup with adaptive scaling enabled."""
        config = DaskSetupConfig(adaptive=True, min_workers=2, max_workers=8, dashboard=False)
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        setup_dask_client()

        # Verify adaptive parameters passed to cluster creation
        mock_create_cluster.assert_called_once_with(
            topology=self.test_topology,
            memory_spec=self.test_memory_spec,
            temp_dir="/tmp/dask-temp",
            dashboard_address=None,  # Dashboard disabled
            silence_logs=logging.WARNING,
            adaptive=True,
            min_workers=2,
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )

        # Dashboard should not be printed when disabled
        mock_print_dashboard.assert_not_called()

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_custom_temp_base_dir(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test setup with custom temporary directory base."""
        config = DaskSetupConfig(temp_base_dir="/custom/temp")
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/custom/temp/dask-xyz"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        setup_dask_client()

        # Verify temp directory creation uses custom base
        mock_create_temp_dir.assert_called_once_with(base_dir="/custom/temp")

        # Verify cluster uses the custom temp directory
        mock_create_cluster.assert_called_once_with(
            topology=self.test_topology,
            memory_spec=self.test_memory_spec,
            temp_dir="/custom/temp/dask-xyz",
            dashboard_address=":0",
            silence_logs=logging.WARNING,
            adaptive=False,
            min_workers=None,
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_silence_logs(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test setup with silent mode enabled."""
        config = DaskSetupConfig(dashboard=True, silence_logs=True)
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        setup_dask_client()

        # Verify dashboard info is called with silent=True
        mock_print_dashboard.assert_called_once_with(mock_client, silent=True)

        # Should still print summary info (separate from dashboard logging)
        assert mock_print.called
        summary_calls = list(mock_print.call_args_list)
        summary_text = "".join(str(call) for call in summary_calls)
        assert "setup_dask_client" in summary_text
        assert "temp/spill dir" in summary_text

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client._resolve_configuration")
    @patch("builtins.print")
    def test_setup_dask_client_output_formatting(
        self,
        mock_print,
        mock_resolve_config,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test that summary output contains expected information."""
        config = DaskSetupConfig(dashboard=False)  # Disable dashboard for cleaner output test
        mock_resolve_config.return_value = config
        mock_detect_resources.return_value = self.test_resources
        mock_create_temp_dir.return_value = "/tmp/dask-test-dir"
        mock_decide_topology.return_value = self.test_topology
        mock_calculate_memory.return_value = self.test_memory_spec

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        setup_dask_client()

        # Check that summary information was printed
        print_calls = [call[0][0] for call in mock_print.call_args_list]
        summary = "".join(print_calls)

        # Should contain key information
        assert "[setup_dask_client]" in summary
        assert "temp/spill dir: /tmp/dask-test-dir" in summary
        assert "Workers: 4" in summary
        assert "threads/worker: 2" in summary
        assert "processes: True" in summary  # TopologySpec.processes is a bool
        assert "total ~32.0 GiB" in summary
        assert "usable ~30.0 GiB" in summary
        assert "per-worker ~7.0 GiB" in summary


class TestClientIntegration:
    """Integration tests for client functionality."""

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client.ConfigManager")
    @patch("builtins.print")
    def test_end_to_end_with_profile(
        self,
        mock_print,
        mock_config_manager,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """Test complete end-to-end workflow with profile and parameter overrides."""
        # Setup profile
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager

        profile_config = DaskSetupConfig(
            workload_type="mixed",
            max_workers=6,
            reserve_mem_gb=30.0,
            dashboard=False,
            adaptive=True,
            min_workers=1,
        )
        profile_obj = ConfigProfile(name="mixed_profile", config=profile_config, builtin=False)
        mock_manager.get_profile.return_value = profile_obj

        # Setup system resources
        resources = ResourceSpec(
            total_cores=12,
            total_mem_bytes=64 * (1024**3),  # 64 GB
            detection_method="test",
        )
        mock_detect_resources.return_value = resources

        # Setup topology and memory
        topology = TopologySpec(
            n_workers=8, threads_per_worker=1, processes=True, workload_type="mixed"
        )
        mock_decide_topology.return_value = topology

        memory_spec = MemorySpec(
            total_mem_gib=64.0,
            usable_mem_gb=60.0,
            mem_per_worker_bytes=7 * (1024**3),
            reserved_mem_gb=4.0,
        )
        mock_calculate_memory.return_value = memory_spec

        mock_create_temp_dir.return_value = "/scratch/dask-temp"

        mock_cluster = MagicMock()
        mock_create_cluster.return_value = mock_cluster
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        # Call with profile and some parameter overrides
        client, cluster, temp_dir = setup_dask_client(
            profile="mixed_profile",
            workload_type="cpu",  # Override profile
            dashboard=True,  # Override profile
            max_workers=10,  # Override profile
            adaptive=True,  # Explicitly pass to ensure override
        )

        # Verify the configuration resolution worked correctly
        # topology decision should use the resolved (overridden) config
        mock_decide_topology.assert_called_once_with(
            workload_type="cpu",  # Overridden from profile
            total_cores=12,
            max_workers=10,  # Overridden from profile
        )

        # Verify cluster creation uses the final resolved configuration
        mock_create_cluster.assert_called_once_with(
            topology=topology,
            memory_spec=memory_spec,
            temp_dir="/scratch/dask-temp",
            dashboard_address=":0",  # Dashboard was overridden to True
            silence_logs=logging.WARNING,
            adaptive=True,  # From profile (not overridden)
            min_workers=1,  # From profile (not overridden)
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )

        # Verify return values
        assert client is mock_client
        assert cluster is mock_cluster
        assert temp_dir == "/scratch/dask-temp"

        # Dashboard should be printed since it was overridden to True
        mock_print_dashboard.assert_called_once_with(mock_client, silent=False)

    @pytest.mark.unit
    @patch("dask_setup.client.detect_resources")
    @patch("dask_setup.client.ConfigManager")
    def test_error_propagation_from_dependencies(self, mock_config_manager, mock_detect_resources):
        """Test that errors from dependency modules are properly propagated."""
        # Test resource detection error
        from dask_setup.exceptions import ResourceDetectionError

        mock_detect_resources.side_effect = ResourceDetectionError("Failed to detect resources")

        with pytest.raises(ResourceDetectionError):
            setup_dask_client()

        # Test profile loading error (already covered in resolve_configuration tests)
        mock_detect_resources.side_effect = None
        mock_detect_resources.return_value = ResourceSpec(
            total_cores=4, total_mem_bytes=8 * (1024**3), detection_method="test"
        )

        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager
        mock_manager.get_profile.return_value = None
        mock_manager.list_profiles.return_value = {}

        with pytest.raises(ValueError, match="Profile .* not found"):
            setup_dask_client(profile="nonexistent")

    @pytest.mark.unit
    def test_parameter_validation_coverage(self):
        """Test that all documented parameters are handled by the function signature."""
        import inspect

        sig = inspect.signature(setup_dask_client)
        param_names = set(sig.parameters.keys())

        expected_params = {
            "workload_type",
            "max_workers",
            "reserve_mem_gb",
            "max_mem_gb",
            "dashboard",
            "adaptive",
            "min_workers",
            "profile",
        }

        # All expected parameters should be in the signature
        assert expected_params.issubset(param_names)

        # Every configuration parameter uses None as its "not supplied"
        # sentinel. This is load-bearing: _resolve_configuration treats any
        # non-None value as an explicit override, so a concrete default here
        # would make that parameter permanently override the profile.
        for name in expected_params:
            assert sig.parameters[name].default is None, (
                f"{name} must default to None so 'not supplied' is distinguishable "
                f"from 'supplied with the default value'"
            )

        # The effective defaults — what you get when nothing is supplied — are
        # the documented ones.
        resolved = _resolve_configuration()
        assert resolved.workload_type == "io"
        assert resolved.max_workers is None
        assert resolved.reserve_mem_gb == _compute_smart_reserve_default()
        assert resolved.max_mem_gb is None
        assert resolved.dashboard is True
        assert resolved.adaptive is False
        assert resolved.min_workers is None


class TestConfigurationReachesTheCluster:
    """End-to-end guards that a resolved configuration survives to cluster creation.

    These deliberately do NOT mock ``_resolve_configuration`` — the merge chain
    is the code under test. Everything downstream of it is mocked so no real
    cluster is started.
    """

    @staticmethod
    def _resources():
        return ResourceSpec(total_cores=8, total_mem_bytes=256 * (1024**3), detection_method="test")

    @staticmethod
    def _memory_spec():
        return MemorySpec(
            total_mem_gib=256.0,
            usable_mem_gb=196.0,
            mem_per_worker_bytes=49 * (1024**3),
            reserved_mem_gb=60.0,
        )

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("builtins.print")
    def test_builtin_profile_survives_to_topology_and_memory(
        self,
        mock_print,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """profile='climate_analysis' must actually produce a cpu/60 GiB cluster.

        Regression guard: the explicit-keyword layer used to be merged as a
        fully-defaulted DaskSetupConfig, which reset every field the caller had
        not named — so every profile silently resolved to the library defaults.
        """
        mock_detect_resources.return_value = self._resources()
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = TopologySpec(
            n_workers=8, threads_per_worker=1, processes=True, workload_type="cpu"
        )
        mock_calculate_memory.return_value = self._memory_spec()
        mock_create_cluster.return_value = MagicMock()
        mock_client_class.return_value = MagicMock()

        setup_dask_client(profile="climate_analysis", dashboard=False)

        # The profile's workload_type reaches topology selection
        assert mock_decide_topology.call_args.kwargs["workload_type"] == "cpu"
        # ...and its reserve_mem_gb reaches the memory calculation
        assert mock_calculate_memory.call_args.kwargs["reserve_mem_gb"] == 60.0

    @pytest.mark.unit
    @patch("dask_setup.client.print_dashboard_info")
    @patch("dask_setup.client.Client")
    @patch("dask_setup.client.create_cluster")
    @patch("dask_setup.client.calculate_memory_spec")
    @patch("dask_setup.client.validate_topology")
    @patch("dask_setup.client.decide_topology")
    @patch("dask_setup.client.create_dask_temp_dir")
    @patch("dask_setup.client.detect_resources")
    @patch("builtins.print")
    def test_config_object_survives_including_advanced_fields(
        self,
        mock_print,
        mock_detect_resources,
        mock_create_temp_dir,
        mock_decide_topology,
        mock_validate_topology,
        mock_calculate_memory,
        mock_create_cluster,
        mock_client_class,
        mock_print_dashboard,
    ):
        """config= must be honoured, including fields with no keyword equivalent."""
        mock_detect_resources.return_value = self._resources()
        mock_create_temp_dir.return_value = "/tmp/dask-temp"
        mock_decide_topology.return_value = TopologySpec(
            n_workers=4, threads_per_worker=1, processes=True, workload_type="cpu"
        )
        mock_calculate_memory.return_value = self._memory_spec()
        mock_create_cluster.return_value = MagicMock()
        mock_client_class.return_value = MagicMock()

        cfg = DaskSetupConfig(
            workload_type="cpu",
            reserve_mem_gb=12.0,
            memory_target=0.60,
            memory_spill=0.70,
            spill_compression="zstd",
        )
        setup_dask_client(config=cfg, dashboard=False, max_workers=4)

        assert mock_decide_topology.call_args.kwargs["workload_type"] == "cpu"
        assert mock_calculate_memory.call_args.kwargs["reserve_mem_gb"] == 12.0

        # Advanced fields reach create_cluster; max_workers was the only
        # explicit override and must not have reset anything else.
        cluster_kwargs = mock_create_cluster.call_args.kwargs
        assert cluster_kwargs["memory_target"] == 0.60
        assert cluster_kwargs["memory_spill"] == 0.70
        assert cluster_kwargs["spill_compression"] == "zstd"
        assert mock_decide_topology.call_args.kwargs["max_workers"] == 4

    @pytest.mark.unit
    def test_distinct_configs_resolve_distinctly(self):
        """Two different configs must not collapse to the same resolution.

        benchmark_config() A/B-tests configurations this way; when the merge
        was broken every entry produced an identical cluster.
        """
        io_cfg = DaskSetupConfig(workload_type="io")
        cpu_cfg = DaskSetupConfig(workload_type="cpu")

        resolved_io = _resolve_configuration(input_config=io_cfg, dashboard=False)
        resolved_cpu = _resolve_configuration(input_config=cpu_cfg, dashboard=False)

        assert resolved_io.workload_type == "io"
        assert resolved_cpu.workload_type == "cpu"


class TestModeDispatchHonoursConfiguration:
    """Non-local modes must honour ds= and the resolved configuration.

    Regression guards for two bugs in the same early-return block:
    it returned a 3-tuple even when ds= was given (breaking the documented
    4-tuple contract exactly on Gadi, where mode auto-resolves to pbs or
    interactive), and it forwarded only workload_type, dropping profile=,
    config= and every other setting before any merge happened.
    """

    _RET = ("client", "cluster", "/scratch/tmp")

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("mode", "target"),
        [
            ("interactive", "setup_interactive_cluster"),
            ("pbs", "setup_pbs_cluster"),
            ("slurm", "setup_slurm_cluster"),
        ],
    )
    def test_ds_yields_a_four_tuple_on_every_mode(self, mode, target):
        with (
            patch(f"dask_setup.client.{target}", return_value=self._RET),
            patch(
                "dask_setup.client._chunk_recommendations_for", return_value={"time": 240}
            ) as mock_chunks,
        ):
            result = setup_dask_client(ds=MagicMock(), mode=mode)

        # Unpacking this way is what the README documents; it used to raise
        # ValueError: not enough values to unpack (expected 4, got 3)
        client, cluster, tmp, chunks = result
        assert chunks == {"time": 240}
        assert tmp == "/scratch/tmp"
        mock_chunks.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("mode", "target"),
        [
            ("interactive", "setup_interactive_cluster"),
            ("pbs", "setup_pbs_cluster"),
            ("slurm", "setup_slurm_cluster"),
        ],
    )
    def test_no_ds_still_yields_a_three_tuple(self, mode, target):
        with patch(f"dask_setup.client.{target}", return_value=self._RET):
            result = setup_dask_client(mode=mode)

        assert len(result) == 3

    @pytest.mark.unit
    def test_profile_reaches_the_pbs_backend(self):
        captured = {}

        def fake(cfg):
            captured["cfg"] = cfg
            return self._RET

        with patch("dask_setup.client.setup_pbs_cluster", side_effect=fake):
            setup_dask_client(profile="climate_analysis", mode="pbs")

        # climate_analysis is a cpu profile; this used to arrive as "io"
        assert captured["cfg"].workload_type == "cpu"

    @pytest.mark.unit
    def test_config_object_reaches_the_pbs_backend(self):
        captured = {}

        def fake(cfg):
            captured["cfg"] = cfg
            return self._RET

        cfg = DaskSetupConfig(workload_type="gpu", max_workers=4, adaptive=True, min_workers=2)
        with patch("dask_setup.client.setup_pbs_cluster", side_effect=fake):
            setup_dask_client(config=cfg, mode="pbs")

        built = captured["cfg"]
        assert built.workload_type == "gpu"
        assert built.workers_per_node == 4
        assert built.adaptive is True
        assert built.min_jobs == 2

    @pytest.mark.unit
    def test_explicit_multi_node_config_still_wins(self):
        from dask_setup.multinode import MultiNodeConfig

        captured = {}

        def fake(cfg):
            captured["cfg"] = cfg
            return self._RET

        mine = MultiNodeConfig(workload_type="cpu", cores_per_worker=12, workers_per_node=4)
        with patch("dask_setup.client.setup_pbs_cluster", side_effect=fake):
            setup_dask_client(profile="climate_analysis", mode="pbs", multi_node_config=mine)

        assert captured["cfg"] is mine

    @pytest.mark.unit
    def test_profile_reaches_the_interactive_backend(self):
        captured = {}

        def fake(**kwargs):
            captured.update(kwargs)
            return self._RET

        with patch("dask_setup.client.setup_interactive_cluster", side_effect=fake):
            setup_dask_client(profile="development", mode="interactive")

        assert captured["workload_type"] == "mixed"
        assert captured["workers_per_node"] == 2
        # The full resolved config goes through, so the LocalCluster path
        # inside setup_interactive_cluster honours reserve_mem_gb too
        assert captured["config"].reserve_mem_gb == 8.0

    @pytest.mark.unit
    def test_warns_about_settings_the_batch_backends_cannot_apply(self, caplog):
        import logging

        with (
            patch("dask_setup.client.setup_pbs_cluster", return_value=self._RET),
            caplog.at_level(logging.WARNING, logger="dask_setup.client"),
        ):
            setup_dask_client(reserve_mem_gb=99.0, mode="pbs")

        assert any("do not apply to this cluster mode" in r.message for r in caplog.records)

    @pytest.mark.unit
    def test_no_spurious_warning_for_interactive(self, caplog):
        """Interactive on one node goes through the local path and honours these."""
        import logging

        with (
            patch("dask_setup.client.setup_interactive_cluster", return_value=self._RET),
            caplog.at_level(logging.WARNING, logger="dask_setup.client"),
        ):
            setup_dask_client(reserve_mem_gb=99.0, mode="interactive")

        assert not [r for r in caplog.records if "do not apply" in r.message]


class TestSmartReserveDefault:
    """reserve_mem_gb's default is machine-aware rather than a flat 50 GiB.

    _compute_smart_reserve_default() existed but nothing called it, so every
    machine got 50.0 -- which on a 16 GiB laptop reserves more than the whole
    machine and leaves nothing for workers.
    """

    @pytest.mark.unit
    def test_bare_call_uses_the_machine_aware_default(self):
        assert _resolve_configuration().reserve_mem_gb == _compute_smart_reserve_default()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "total_ram_gb,expected",
        [
            (1.0, 1.0),  # validation floor beats the half-machine cap
            (4.0, 2.0),  # half-machine cap beats the 4 GiB floor
            (8.0, 4.0),  # clamped to the minimum
            (16.0, 4.0),  # 3.2 -> clamped to the minimum
            (64.0, 12.8),
            (128.0, 25.6),
            (300.0, 50.0),  # clamped to the maximum
        ],
    )
    def test_formula_is_twenty_percent_clamped(self, total_ram_gb, expected):
        with patch("dask_setup.client.psutil.virtual_memory") as mock_vm:
            mock_vm.return_value.total = int(total_ram_gb * 1024**3)
            assert _compute_smart_reserve_default() == pytest.approx(expected)

    @pytest.mark.unit
    def test_never_reserves_the_whole_of_a_small_machine(self):
        for total_ram_gb in (4.0, 8.0, 16.0, 32.0):
            with patch("dask_setup.client.psutil.virtual_memory") as mock_vm:
                mock_vm.return_value.total = int(total_ram_gb * 1024**3)
                assert _compute_smart_reserve_default() < total_ram_gb

    @pytest.mark.unit
    def test_explicit_argument_still_wins(self):
        assert _resolve_configuration(reserve_mem_gb=12.5).reserve_mem_gb == 12.5

    @pytest.mark.unit
    @patch("dask_setup.client.ConfigManager")
    def test_a_profile_still_wins(self, mock_config_manager):
        mock_manager = MagicMock()
        mock_config_manager.return_value = mock_manager
        mock_manager.get_profile.return_value = ConfigProfile(
            name="p", config=DaskSetupConfig(reserve_mem_gb=37.0)
        )

        assert _resolve_configuration(profile="p").reserve_mem_gb == 37.0

    @pytest.mark.unit
    def test_a_config_object_still_wins(self):
        cfg = DaskSetupConfig(reserve_mem_gb=21.0)
        assert _resolve_configuration(input_config=cfg).reserve_mem_gb == 21.0

    @pytest.mark.unit
    def test_result_always_passes_config_validation(self):
        """The default must never be a value DaskSetupConfig would reject."""
        for total_ram_gb in (0.5, 1.0, 2.0, 4.0, 16.0, 64.0, 300.0, 2000.0):
            with patch("dask_setup.client.psutil.virtual_memory") as mock_vm:
                mock_vm.return_value.total = int(total_ram_gb * 1024**3)
                reserve = _compute_smart_reserve_default()
            # Raises ConfigurationValidationError if out of range.
            DaskSetupConfig(reserve_mem_gb=reserve)

    @pytest.mark.unit
    def test_falls_back_to_fifty_if_psutil_fails(self):
        with patch("dask_setup.client.psutil.virtual_memory", side_effect=OSError("boom")):
            assert _compute_smart_reserve_default() == 50.0


class TestSilenceLogsReachesTheCluster:
    """config.silence_logs was collected and validated but never passed on.

    create_cluster kept its logging.ERROR default on every run, so the setting
    did nothing and distributed's own warnings were always suppressed.
    """

    @staticmethod
    def _create_cluster_kwargs(config):
        with (
            patch(
                "dask_setup.client.detect_resources",
                return_value=ResourceSpec(8, 32 * 1024**3, "test"),
            ),
            patch("dask_setup.client.decide_topology", return_value=TopologySpec(2, 2, True, "io")),
            patch("dask_setup.client.validate_topology"),
            patch("dask_setup.client.create_dask_temp_dir", return_value="/tmp/x"),
            patch("dask_setup.client.compute_usable_mem_gb", return_value=30.0),
            patch(
                "dask_setup.client.calculate_memory_spec",
                return_value=MemorySpec(32.0, 30.0, 15 * 1024**3, 2.0),
            ),
            patch("dask_setup.client.create_cluster") as mock_create,
            patch("dask_setup.client.Client"),
            patch("dask_setup.client.print_dashboard_info"),
        ):
            setup_dask_client(config=config)
        return mock_create.call_args.kwargs

    @pytest.mark.unit
    def test_silence_logs_true_suppresses_worker_output(self):
        kwargs = self._create_cluster_kwargs(DaskSetupConfig(silence_logs=True))
        assert kwargs["silence_logs"] == logging.ERROR

    @pytest.mark.unit
    def test_silence_logs_false_leaves_warnings_visible(self):
        kwargs = self._create_cluster_kwargs(DaskSetupConfig(silence_logs=False))
        assert kwargs["silence_logs"] == logging.WARNING

    @pytest.mark.unit
    def test_the_two_settings_actually_differ(self):
        quiet = self._create_cluster_kwargs(DaskSetupConfig(silence_logs=True))
        loud = self._create_cluster_kwargs(DaskSetupConfig(silence_logs=False))
        assert quiet["silence_logs"] != loud["silence_logs"]
