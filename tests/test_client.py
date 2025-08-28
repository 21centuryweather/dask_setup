"""Unit tests for dask_setup.client module."""

from unittest.mock import MagicMock, patch

import pytest

from dask_setup.client import _resolve_configuration, setup_dask_client
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
        assert config.reserve_mem_gb == 50.0
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

        # Override some profile settings - test the actual function behavior
        # The function creates an explicit config with non-default values only,
        # then merges: defaults < profile < explicit
        config = _resolve_configuration(
            profile="base_profile",
            workload_type="cpu",  # Override (differs from default "io")
            max_workers=10,  # Override (differs from default None)
            dashboard=False,  # Override (differs from default True)
            adaptive=True,  # Explicit override (differs from default False)
        )

        # Should use explicit overrides where they differ from function defaults
        assert config.workload_type == "cpu"  # Explicit override (cpu != "io")
        assert config.max_workers == 10  # Explicit override (10 != None)
        assert config.dashboard is False  # Explicit override (False != True)
        assert config.adaptive is True  # Explicit override (True != False)

        # For parameters where defaults are used (not overridden):
        # - reserve_mem_gb: function gets default 50.0, which becomes default so is not explicit
        #   but the merge is defaults < profile < explicit, so defaults win over profile
        # - min_workers: function gets default None, profile has 1, so profile wins
        assert config.reserve_mem_gb == 50.0  # Default wins (50.0 is the function default)
        assert config.min_workers == 1  # From profile (None is default, 1 from profile)

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
        mock_resolve_config.assert_called_once_with(
            profile=None,
            workload_type="io",
            max_workers=None,
            reserve_mem_gb=50.0,
            max_mem_gb=None,
            dashboard=True,
            adaptive=False,
            min_workers=None,
            suggest_chunks=False,
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
        mock_resolve_config.assert_called_once_with(
            profile="cpu_profile",
            workload_type="io",
            max_workers=8,  # Explicit override
            reserve_mem_gb=50.0,
            max_mem_gb=None,
            dashboard=True,
            adaptive=False,
            min_workers=None,
            suggest_chunks=False,
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

        # Check parameter defaults match documentation
        assert sig.parameters["workload_type"].default == "io"
        assert sig.parameters["max_workers"].default is None
        assert sig.parameters["reserve_mem_gb"].default == 50.0
        assert sig.parameters["max_mem_gb"].default is None
        assert sig.parameters["dashboard"].default is True
        assert sig.parameters["adaptive"].default is False
        assert sig.parameters["min_workers"].default is None
        assert sig.parameters["profile"].default is None
