"""Unit tests for dask_setup.cluster module."""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dask.distributed import LocalCluster

from dask_setup.cluster import (
    calculate_memory_spec,
    configure_dask_settings,
    create_cluster,
)
from dask_setup.types import MemorySpec, TopologySpec


class TestCalculateMemorySpec:
    """Test memory specification calculation function."""

    @pytest.mark.unit
    def test_basic_calculation(self):
        """Test basic memory calculation."""
        # 100 GiB total, 4 workers, 50 GiB reserved
        total_mem_bytes = 100 * (1024**3)

        result = calculate_memory_spec(total_mem_bytes, n_workers=4)

        assert isinstance(result, MemorySpec)
        assert result.total_mem_gib == 100.0
        assert result.reserved_mem_gb == 50.0
        assert result.usable_mem_gb == 50.0  # 100 - 50 = 50
        assert result.mem_per_worker_bytes == int(12.5 * (1024**3))  # 50/4 = 12.5 GiB per worker

    @pytest.mark.unit
    def test_memory_calculation_with_custom_reserve(self):
        """Test memory calculation with custom reserve amount."""
        total_mem_bytes = 64 * (1024**3)  # 64 GiB

        result = calculate_memory_spec(total_mem_bytes, n_workers=2, reserve_mem_gb=20.0)

        assert result.total_mem_gib == 64.0
        assert result.reserved_mem_gb == 20.0
        assert result.usable_mem_gb == 44.0  # 64 - 20 = 44
        assert result.mem_per_worker_bytes == int(22.0 * (1024**3))  # 44/2 = 22 GiB per worker

    @pytest.mark.unit
    def test_memory_calculation_with_max_mem_cap(self):
        """Test memory calculation with max_mem_gb cap."""
        total_mem_bytes = 128 * (1024**3)  # 128 GiB available

        result = calculate_memory_spec(
            total_mem_bytes, n_workers=4, reserve_mem_gb=10.0, max_mem_gb=80.0
        )

        assert result.total_mem_gib == 128.0  # Still shows total available
        assert result.reserved_mem_gb == 10.0
        assert result.usable_mem_gb == 70.0  # 80 - 10 = 70 (capped by max_mem_gb)
        assert result.mem_per_worker_bytes == int(17.5 * (1024**3))  # 70/4 = 17.5 GiB per worker

    @pytest.mark.unit
    def test_minimum_memory_per_worker(self):
        """Test that each worker gets at least 1 GiB."""
        total_mem_bytes = 8 * (1024**3)  # 8 GiB total

        # With many workers and small usable memory, should ensure 1 GiB minimum per worker
        result = calculate_memory_spec(total_mem_bytes, n_workers=16, reserve_mem_gb=2.0)

        assert result.usable_mem_gb == 6.0  # 8 - 2 = 6
        # Even though 6/16 = 0.375 GiB per worker, should be capped at minimum 1 GiB
        assert result.mem_per_worker_bytes == int(1.0 * (1024**3))

    @pytest.mark.unit
    def test_insufficient_memory_error(self):
        """Test error when insufficient memory after reservation."""
        total_mem_bytes = 32 * (1024**3)  # 32 GiB total

        # Reserve more than total memory
        with pytest.raises(ValueError) as exc_info:
            calculate_memory_spec(total_mem_bytes, n_workers=4, reserve_mem_gb=40.0)

        assert "Not enough memory after reserving 40.0 GiB" in str(exc_info.value)
        assert "from 32.0 GiB total" in str(exc_info.value)

    @pytest.mark.unit
    def test_exactly_reserved_memory_error(self):
        """Test error when reservation equals total memory."""
        total_mem_bytes = 50 * (1024**3)  # 50 GiB total

        with pytest.raises(ValueError) as exc_info:
            calculate_memory_spec(total_mem_bytes, n_workers=2, reserve_mem_gb=50.0)

        assert "Not enough memory after reserving 50.0 GiB" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "total_gb,n_workers,reserve_gb,expected_per_worker",
        [
            # Various realistic scenarios
            (16, 2, 4.0, 6.0),  # 16GB total, 2 workers, 4GB reserved = 6GB per worker
            (32, 4, 8.0, 6.0),  # 32GB total, 4 workers, 8GB reserved = 6GB per worker
            (64, 8, 16.0, 6.0),  # 64GB total, 8 workers, 16GB reserved = 6GB per worker
            (128, 16, 32.0, 6.0),  # 128GB total, 16 workers, 32GB reserved = 6GB per worker
            # Edge case: very few workers
            (64, 1, 16.0, 48.0),  # 64GB total, 1 worker, 16GB reserved = 48GB per worker
            # Edge case: minimum memory per worker
            (8, 10, 2.0, 1.0),  # 8GB total, 10 workers, 2GB reserved = 1GB per worker (minimum)
        ],
    )
    def test_memory_calculation_scenarios(
        self, total_gb, n_workers, reserve_gb, expected_per_worker
    ):
        """Test various memory calculation scenarios."""
        total_mem_bytes = int(total_gb * (1024**3))

        result = calculate_memory_spec(
            total_mem_bytes, n_workers=n_workers, reserve_mem_gb=reserve_gb
        )

        expected_bytes = int(expected_per_worker * (1024**3))
        assert result.mem_per_worker_bytes == expected_bytes
        assert result.total_mem_gib == total_gb
        assert result.reserved_mem_gb == reserve_gb

    @pytest.mark.unit
    def test_max_mem_gb_smaller_than_reserve(self):
        """Test edge case where max_mem_gb is smaller than or near reserve."""
        total_mem_bytes = 128 * (1024**3)

        # max_mem_gb is only slightly larger than reserve
        result = calculate_memory_spec(
            total_mem_bytes, n_workers=2, reserve_mem_gb=30.0, max_mem_gb=32.0
        )

        assert result.usable_mem_gb == 2.0  # 32 - 30 = 2
        assert result.mem_per_worker_bytes == int(1.0 * (1024**3))  # minimum per worker

    @pytest.mark.unit
    def test_max_mem_gb_equal_to_reserve(self):
        """Test edge case where max_mem_gb equals reserve."""
        total_mem_bytes = 100 * (1024**3)

        with pytest.raises(ValueError):
            calculate_memory_spec(
                total_mem_bytes, n_workers=4, reserve_mem_gb=50.0, max_mem_gb=50.0
            )

    @pytest.mark.unit
    def test_zero_workers_error(self):
        """Test that zero workers raises appropriate error."""
        total_mem_bytes = 100 * (1024**3)  # Enough memory to pass initial check

        # This should cause division by zero when calculating per-worker memory
        with pytest.raises(ZeroDivisionError):
            calculate_memory_spec(total_mem_bytes, n_workers=0, reserve_mem_gb=20.0)

    @pytest.mark.unit
    def test_large_memory_values(self):
        """Test calculation with very large memory values."""
        # 1 TB total memory
        total_mem_bytes = 1024 * (1024**3)

        result = calculate_memory_spec(total_mem_bytes, n_workers=64, reserve_mem_gb=100.0)

        assert result.total_mem_gib == 1024.0
        assert result.usable_mem_gb == 924.0  # 1024 - 100 = 924
        expected_per_worker = 924.0 / 64  # ~14.44 GB per worker
        expected_bytes = int(expected_per_worker * (1024**3))
        assert result.mem_per_worker_bytes == expected_bytes


class TestConfigureDaskSettings:
    """Test Dask configuration function."""

    @patch("dask.config.set")
    @pytest.mark.unit
    def test_configure_dask_settings(self, mock_dask_set):
        """Test that Dask settings are configured correctly."""
        temp_dir = Path("/tmp/test_dask")

        configure_dask_settings(temp_dir)

        # Check that dask.config.set was called once
        mock_dask_set.assert_called_once()

        # Get the configuration dict that was passed
        config_dict = mock_dask_set.call_args[0][0]

        # Verify expected configuration keys and values
        expected_config = {
            "temporary-directory": "/tmp/test_dask",
            "distributed.worker.local-directory": "/tmp/test_dask",
            "distributed.worker.memory.target": 0.75,
            "distributed.worker.memory.spill": 0.85,
            "distributed.worker.memory.pause": 0.92,
            "distributed.worker.memory.terminate": 0.98,
            "distributed.worker.multiprocessing-method": "spawn",
            "array.slicing.split_large_chunks": True,
            "distributed.comm.compression": False,
            "distributed.worker.memory.spill-compression": "auto",
        }

        assert config_dict == expected_config

    @patch("dask.config.set")
    @pytest.mark.unit
    def test_configure_dask_settings_with_different_path(self, mock_dask_set):
        """Test Dask configuration with different temporary directory path."""
        temp_dir = Path("/scratch/user/dask_workspace")

        configure_dask_settings(temp_dir)

        mock_dask_set.assert_called_once()
        config_dict = mock_dask_set.call_args[0][0]

        # Check that the path was converted to string correctly
        assert config_dict["temporary-directory"] == "/scratch/user/dask_workspace"
        assert config_dict["distributed.worker.local-directory"] == "/scratch/user/dask_workspace"


class TestCreateCluster:
    """Test cluster creation function."""

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_basic(self, mock_local_cluster, mock_configure_dask):
        """Test basic cluster creation."""
        # Mock the LocalCluster instance
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        # Setup test data
        topology = TopologySpec(
            n_workers=4, threads_per_worker=2, processes=True, workload_type="mixed"
        )
        memory_spec = MemorySpec(
            total_mem_gib=64.0,
            usable_mem_gb=48.0,
            mem_per_worker_bytes=int(12.0 * (1024**3)),
            reserved_mem_gb=16.0,
        )
        temp_dir = Path("/tmp/dask_test")

        result = create_cluster(topology, memory_spec, temp_dir)

        # Verify Dask settings were configured with default parameters
        mock_configure_dask.assert_called_once_with(
            temp_dir=temp_dir,
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )

        # Verify LocalCluster was created with correct parameters
        mock_local_cluster.assert_called_once_with(
            n_workers=4,
            threads_per_worker=2,
            processes=True,
            memory_limit=int(12.0 * (1024**3)),
            dashboard_address=":0",
            local_directory="/tmp/dask_test",
            silence_logs=logging.ERROR,
        )

        # Verify cluster instance is returned
        assert result is mock_cluster_instance

        # Verify adaptive scaling was not enabled
        mock_cluster_instance.adapt.assert_not_called()

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_with_adaptive_scaling(self, mock_local_cluster, mock_configure_dask):
        """Test cluster creation with adaptive scaling enabled."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        topology = TopologySpec(
            n_workers=8, threads_per_worker=1, processes=True, workload_type="cpu"
        )
        memory_spec = MemorySpec(
            total_mem_gib=32.0,
            usable_mem_gb=24.0,
            mem_per_worker_bytes=int(3.0 * (1024**3)),
            reserved_mem_gb=8.0,
        )
        temp_dir = Path("/scratch/dask")

        result = create_cluster(topology, memory_spec, temp_dir, adaptive=True, min_workers=2)

        # Verify LocalCluster creation
        mock_local_cluster.assert_called_once_with(
            n_workers=8,
            threads_per_worker=1,
            processes=True,
            memory_limit=int(3.0 * (1024**3)),
            dashboard_address=":0",
            local_directory="/scratch/dask",
            silence_logs=logging.ERROR,
        )

        # Verify adaptive scaling was configured
        mock_cluster_instance.adapt.assert_called_once_with(minimum=2, maximum=8, wait_count=2)

        assert result is mock_cluster_instance

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_adaptive_default_min_workers(
        self, mock_local_cluster, mock_configure_dask
    ):
        """Test adaptive scaling with default minimum workers calculation."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        topology = TopologySpec(
            n_workers=16, threads_per_worker=1, processes=True, workload_type="cpu"
        )
        memory_spec = MemorySpec(
            total_mem_gib=64.0,
            usable_mem_gb=48.0,
            mem_per_worker_bytes=int(3.0 * (1024**3)),
            reserved_mem_gb=16.0,
        )
        temp_dir = Path("/tmp/dask")

        create_cluster(topology, memory_spec, temp_dir, adaptive=True)

        # min_workers should default to max(1, n_workers // 2) = max(1, 16 // 2) = 8
        mock_cluster_instance.adapt.assert_called_once_with(minimum=8, maximum=16, wait_count=2)

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_adaptive_min_workers_edge_case(
        self, mock_local_cluster, mock_configure_dask
    ):
        """Test adaptive scaling with very few workers (minimum should be 1)."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        topology = TopologySpec(
            n_workers=1, threads_per_worker=4, processes=False, workload_type="io"
        )
        memory_spec = MemorySpec(
            total_mem_gib=16.0,
            usable_mem_gb=12.0,
            mem_per_worker_bytes=int(12.0 * (1024**3)),
            reserved_mem_gb=4.0,
        )
        temp_dir = Path("/tmp/dask")

        create_cluster(topology, memory_spec, temp_dir, adaptive=True)

        # min_workers should be max(1, 1 // 2) = max(1, 0) = 1
        mock_cluster_instance.adapt.assert_called_once_with(minimum=1, maximum=1, wait_count=2)

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_custom_parameters(self, mock_local_cluster, mock_configure_dask):
        """Test cluster creation with custom parameters."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        topology = TopologySpec(
            n_workers=2, threads_per_worker=4, processes=False, workload_type="io"
        )
        memory_spec = MemorySpec(
            total_mem_gib=32.0,
            usable_mem_gb=24.0,
            mem_per_worker_bytes=int(12.0 * (1024**3)),
            reserved_mem_gb=8.0,
        )
        temp_dir = Path("/scratch/user/dask")

        result = create_cluster(
            topology,
            memory_spec,
            temp_dir,
            dashboard_address="127.0.0.1:8787",
            silence_logs=logging.WARNING,
        )

        # Verify LocalCluster was created with custom parameters
        mock_local_cluster.assert_called_once_with(
            n_workers=2,
            threads_per_worker=4,
            processes=False,
            memory_limit=int(12.0 * (1024**3)),
            dashboard_address="127.0.0.1:8787",
            local_directory="/scratch/user/dask",
            silence_logs=logging.WARNING,
        )

        assert result is mock_cluster_instance

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_disabled_dashboard(self, mock_local_cluster, mock_configure_dask):
        """Test cluster creation with disabled dashboard."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        topology = TopologySpec(
            n_workers=4, threads_per_worker=1, processes=True, workload_type="cpu"
        )
        memory_spec = MemorySpec(
            total_mem_gib=16.0,
            usable_mem_gb=12.0,
            mem_per_worker_bytes=int(3.0 * (1024**3)),
            reserved_mem_gb=4.0,
        )
        temp_dir = Path("/tmp/dask")

        create_cluster(topology, memory_spec, temp_dir, dashboard_address=None)

        # Verify dashboard is disabled
        mock_local_cluster.assert_called_once_with(
            n_workers=4,
            threads_per_worker=1,
            processes=True,
            memory_limit=int(3.0 * (1024**3)),
            dashboard_address=None,
            local_directory="/tmp/dask",
            silence_logs=logging.ERROR,
        )

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_create_cluster_io_workload_configuration(
        self, mock_local_cluster, mock_configure_dask
    ):
        """Test cluster creation specifically for I/O workload."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        # I/O workload: single worker, many threads, no processes
        topology = TopologySpec(
            n_workers=1, threads_per_worker=8, processes=False, workload_type="io"
        )
        memory_spec = MemorySpec(
            total_mem_gib=64.0,
            usable_mem_gb=48.0,
            mem_per_worker_bytes=int(48.0 * (1024**3)),  # All memory to single worker
            reserved_mem_gb=16.0,
        )
        temp_dir = Path("/fast_ssd/dask")

        result = create_cluster(topology, memory_spec, temp_dir)

        mock_local_cluster.assert_called_once_with(
            n_workers=1,
            threads_per_worker=8,
            processes=False,  # Key difference for I/O workload
            memory_limit=int(48.0 * (1024**3)),
            dashboard_address=":0",
            local_directory="/fast_ssd/dask",
            silence_logs=logging.ERROR,
        )

        assert result is mock_cluster_instance


class TestClusterIntegration:
    """Integration tests for cluster functions working together."""

    @patch("dask_setup.cluster.configure_dask_settings")
    @patch("dask_setup.cluster.LocalCluster")
    @pytest.mark.unit
    def test_full_cluster_setup_workflow(self, mock_local_cluster, mock_configure_dask):
        """Test complete workflow from memory calculation to cluster creation."""
        mock_cluster_instance = MagicMock(spec=LocalCluster)
        mock_local_cluster.return_value = mock_cluster_instance

        # Step 1: Calculate memory specification
        total_mem_bytes = 64 * (1024**3)  # 64 GiB
        memory_spec = calculate_memory_spec(total_mem_bytes, n_workers=8, reserve_mem_gb=16.0)

        # Verify memory calculation
        assert memory_spec.total_mem_gib == 64.0
        assert memory_spec.usable_mem_gb == 48.0
        assert memory_spec.mem_per_worker_bytes == int(6.0 * (1024**3))

        # Step 2: Create topology
        topology = TopologySpec(
            n_workers=8, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        # Step 3: Create cluster
        temp_dir = Path("/tmp/dask_integration_test")
        cluster = create_cluster(topology, memory_spec, temp_dir)

        # Verify cluster creation with default parameters
        mock_configure_dask.assert_called_once_with(
            temp_dir=temp_dir,
            memory_target=0.75,
            memory_spill=0.85,
            memory_pause=0.92,
            memory_terminate=0.98,
            spill_compression="auto",
            comm_compression=False,
            spill_threads=None,
        )
        mock_local_cluster.assert_called_once_with(
            n_workers=8,
            threads_per_worker=2,
            processes=True,
            memory_limit=int(6.0 * (1024**3)),
            dashboard_address=":0",
            local_directory="/tmp/dask_integration_test",
            silence_logs=logging.ERROR,
        )

        assert cluster is mock_cluster_instance

    @pytest.mark.unit
    def test_memory_calculation_edge_cases(self):
        """Test memory calculation with various edge cases."""
        # Test cases: (total_gb, n_workers, reserve_gb, should_succeed)
        test_cases = [
            # Normal cases
            (32, 4, 8, True),
            (64, 8, 16, True),
            (128, 16, 32, True),
            # Edge cases that should work
            (16, 1, 4, True),  # Single worker
            (8, 8, 2, True),  # Minimal memory per worker (should get 1GB each)
            # Edge cases that should fail
            (16, 4, 16, False),  # Reserve equals total
            (32, 8, 40, False),  # Reserve exceeds total
        ]

        for total_gb, n_workers, reserve_gb, should_succeed in test_cases:
            total_bytes = int(total_gb * (1024**3))

            if should_succeed:
                result = calculate_memory_spec(total_bytes, n_workers, reserve_gb)
                assert result.total_mem_gib == total_gb
                assert result.reserved_mem_gb == reserve_gb
                assert result.mem_per_worker_bytes >= int(
                    1.0 * (1024**3)
                )  # At least 1GB per worker
            else:
                with pytest.raises(ValueError):
                    calculate_memory_spec(total_bytes, n_workers, reserve_gb)

    @pytest.mark.unit
    def test_realistic_hpc_scenarios(self):
        """Test realistic HPC cluster configurations."""
        scenarios = [
            # Small job: 16 GB RAM, 4 cores
            {
                "name": "Small Job",
                "total_gb": 16,
                "n_workers": 4,
                "threads_per_worker": 1,
                "processes": True,
                "reserve_gb": 4.0,
                "expected_per_worker_gb": 3.0,
            },
            # Medium job: 64 GB RAM, 16 cores
            {
                "name": "Medium Job",
                "total_gb": 64,
                "n_workers": 8,
                "threads_per_worker": 2,
                "processes": True,
                "reserve_gb": 16.0,
                "expected_per_worker_gb": 6.0,
            },
            # Large job: 256 GB RAM, 32 cores
            {
                "name": "Large Job",
                "total_gb": 256,
                "n_workers": 16,
                "threads_per_worker": 2,
                "processes": True,
                "reserve_gb": 64.0,
                "expected_per_worker_gb": 12.0,
            },
            # I/O intensive: all memory to single threaded worker
            {
                "name": "I/O Intensive",
                "total_gb": 128,
                "n_workers": 1,
                "threads_per_worker": 16,
                "processes": False,
                "reserve_gb": 32.0,
                "expected_per_worker_gb": 96.0,
            },
        ]

        for scenario in scenarios:
            total_bytes = int(scenario["total_gb"] * (1024**3))

            memory_spec = calculate_memory_spec(
                total_bytes, scenario["n_workers"], scenario["reserve_gb"]
            )

            expected_bytes = int(scenario["expected_per_worker_gb"] * (1024**3))

            assert memory_spec.total_mem_gib == scenario["total_gb"]
            assert memory_spec.reserved_mem_gb == scenario["reserve_gb"]
            assert memory_spec.mem_per_worker_bytes == expected_bytes, (
                f"Failed for scenario {scenario['name']}: "
                f"expected {expected_bytes} bytes per worker, "
                f"got {memory_spec.mem_per_worker_bytes}"
            )
