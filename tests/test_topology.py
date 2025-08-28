"""Unit tests for dask_setup.topology module."""

import pytest

from dask_setup.exceptions import InvalidConfigurationError
from dask_setup.topology import decide_topology, validate_topology
from dask_setup.types import TopologySpec


class TestDecideTopology:
    """Test topology decision function."""

    @pytest.mark.unit
    def test_cpu_workload_basic(self):
        """Test CPU workload topology decision."""
        result = decide_topology("cpu", total_cores=8)

        assert isinstance(result, TopologySpec)
        assert result.workload_type == "cpu"
        assert result.processes is True
        assert result.threads_per_worker == 1
        assert result.n_workers == 8  # Should use all cores

    @pytest.mark.unit
    def test_io_workload_basic(self):
        """Test I/O workload topology decision."""
        result = decide_topology("io", total_cores=8)

        assert isinstance(result, TopologySpec)
        assert result.workload_type == "io"
        assert result.processes is False
        assert result.n_workers == 1  # Single worker for I/O
        assert result.threads_per_worker == 4  # Based on total_cores/2

    @pytest.mark.unit
    def test_mixed_workload_basic(self):
        """Test mixed workload topology decision."""
        result = decide_topology("mixed", total_cores=8)

        assert isinstance(result, TopologySpec)
        assert result.workload_type == "mixed"
        assert result.processes is True
        assert result.threads_per_worker == 2
        assert result.n_workers == 4  # 8 cores / 2 threads per worker

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "workload_type,total_cores,expected_workers,expected_threads,expected_processes",
        [
            # CPU workload tests
            ("cpu", 1, 1, 1, True),
            ("cpu", 4, 4, 1, True),
            ("cpu", 16, 16, 1, True),
            ("cpu", 32, 32, 1, True),
            # I/O workload tests - threads based on ceil(cores/2) but clamped 4-16
            ("io", 1, 1, 4, False),  # min 4 threads
            ("io", 4, 1, 4, False),  # ceil(4/2) = 2, but min is 4
            ("io", 8, 1, 4, False),  # ceil(8/2) = 4
            ("io", 16, 1, 8, False),  # ceil(16/2) = 8
            ("io", 32, 1, 16, False),  # ceil(32/2) = 16, max is 16
            ("io", 48, 1, 16, False),  # ceil(48/2) = 24, but max is 16
            # Mixed workload tests
            ("mixed", 1, 1, 2, True),  # At least 1 worker
            ("mixed", 2, 1, 2, True),  # 2/2 = 1 worker
            ("mixed", 4, 2, 2, True),  # 4/2 = 2 workers
            ("mixed", 8, 4, 2, True),  # 8/2 = 4 workers
            ("mixed", 16, 8, 2, True),  # 16/2 = 8 workers
        ],
    )
    def test_workload_types_parametrized(
        self, workload_type, total_cores, expected_workers, expected_threads, expected_processes
    ):
        """Test various workload type configurations."""
        result = decide_topology(workload_type, total_cores)

        assert result.n_workers == expected_workers
        assert result.threads_per_worker == expected_threads
        assert result.processes == expected_processes
        assert result.workload_type == workload_type

    @pytest.mark.unit
    def test_max_workers_constraint_cpu(self):
        """Test max_workers constraint with CPU workload."""
        result = decide_topology("cpu", total_cores=16, max_workers=8)

        assert result.n_workers == 8  # Limited by max_workers
        assert result.threads_per_worker == 1
        assert result.processes is True

    @pytest.mark.unit
    def test_max_workers_constraint_mixed(self):
        """Test max_workers constraint with mixed workload."""
        result = decide_topology("mixed", total_cores=16, max_workers=2)

        assert result.n_workers == 2  # Limited by max_workers
        assert result.threads_per_worker == 2
        assert result.processes is True

    @pytest.mark.unit
    def test_max_workers_larger_than_cores(self):
        """Test max_workers larger than available cores."""
        result = decide_topology("cpu", total_cores=4, max_workers=10)

        # CPU workload should be limited by cores, not max_workers
        assert result.n_workers == 4

    @pytest.mark.unit
    def test_max_workers_none_defaults_to_cores(self):
        """Test that max_workers=None uses total_cores as default."""
        result1 = decide_topology("cpu", total_cores=8, max_workers=None)
        result2 = decide_topology("cpu", total_cores=8, max_workers=8)

        assert result1.n_workers == result2.n_workers
        assert result1.threads_per_worker == result2.threads_per_worker

    @pytest.mark.unit
    def test_io_workload_thread_calculation(self):
        """Test I/O workload thread count calculation edge cases."""
        # Test minimum threads (4)
        result_low = decide_topology("io", total_cores=2)
        assert result_low.threads_per_worker == 4

        # Test maximum threads (16)
        result_high = decide_topology("io", total_cores=64)
        assert result_high.threads_per_worker == 16

        # Test mid-range calculation
        result_mid = decide_topology("io", total_cores=20)
        assert result_mid.threads_per_worker == 10  # ceil(20/2) = 10

    @pytest.mark.unit
    def test_mixed_workload_minimum_worker(self):
        """Test mixed workload ensures at least 1 worker."""
        # With 1 core, mixed would calculate 1//2 = 0 workers
        result = decide_topology("mixed", total_cores=1)

        assert result.n_workers == 1  # Should be forced to minimum 1
        assert result.threads_per_worker == 2

    @pytest.mark.unit
    def test_invalid_workload_type(self):
        """Test invalid workload type raises error."""
        with pytest.raises(InvalidConfigurationError) as exc_info:
            decide_topology("invalid", total_cores=8)

        assert "workload_type must be 'cpu', 'io', or 'mixed'" in str(exc_info.value)
        assert "invalid" in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "invalid_workload",
        [
            "CPU",  # Wrong case
            "IO",  # Wrong case
            "Mixed",  # Wrong case
            "compute",  # Different word
            "network",  # Different word
            "",  # Empty string
            "cpu_bound",  # Close but not exact
        ],
    )
    def test_invalid_workload_types(self, invalid_workload):
        """Test various invalid workload type strings."""
        with pytest.raises(InvalidConfigurationError):
            decide_topology(invalid_workload, total_cores=4)

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_cores", [0, -1, -10])
    def test_invalid_total_cores(self, invalid_cores):
        """Test invalid total_cores values."""
        with pytest.raises(InvalidConfigurationError) as exc_info:
            decide_topology("cpu", total_cores=invalid_cores)

        assert "total_cores must be positive" in str(exc_info.value)
        assert str(invalid_cores) in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_max_workers", [0, -1, -5])
    def test_invalid_max_workers(self, invalid_max_workers):
        """Test invalid max_workers values."""
        with pytest.raises(InvalidConfigurationError) as exc_info:
            decide_topology("cpu", total_cores=8, max_workers=invalid_max_workers)

        assert "max_workers must be positive" in str(exc_info.value)
        assert str(invalid_max_workers) in str(exc_info.value)

    @pytest.mark.unit
    def test_edge_case_single_core(self):
        """Test topology decisions with single core."""
        cpu_result = decide_topology("cpu", total_cores=1)
        assert cpu_result.n_workers == 1
        assert cpu_result.threads_per_worker == 1

        io_result = decide_topology("io", total_cores=1)
        assert io_result.n_workers == 1
        assert io_result.threads_per_worker == 4  # Minimum threads for I/O

        mixed_result = decide_topology("mixed", total_cores=1)
        assert mixed_result.n_workers == 1
        assert mixed_result.threads_per_worker == 2

    @pytest.mark.unit
    def test_large_core_counts(self):
        """Test topology decisions with large core counts."""
        # Test with 128 cores
        cpu_result = decide_topology("cpu", total_cores=128)
        assert cpu_result.n_workers == 128
        assert cpu_result.threads_per_worker == 1

        io_result = decide_topology("io", total_cores=128)
        assert io_result.n_workers == 1
        assert io_result.threads_per_worker == 16  # Capped at maximum

        mixed_result = decide_topology("mixed", total_cores=128)
        assert mixed_result.n_workers == 64
        assert mixed_result.threads_per_worker == 2

    @pytest.mark.unit
    def test_total_thread_calculation(self):
        """Test that total threads calculation is correct."""
        test_cases = [
            ("cpu", 8, 8 * 1),  # 8 workers * 1 thread = 8 total
            ("io", 16, 1 * 8),  # 1 worker * 8 threads = 8 total
            ("mixed", 16, 8 * 2),  # 8 workers * 2 threads = 16 total
        ]

        for workload_type, cores, expected_total in test_cases:
            result = decide_topology(workload_type, cores)
            total_threads = result.n_workers * result.threads_per_worker
            assert total_threads == expected_total


class TestValidateTopology:
    """Test topology validation function."""

    @pytest.mark.unit
    def test_valid_topology_cpu(self):
        """Test validation of valid CPU topology."""
        topology = TopologySpec(
            n_workers=8, threads_per_worker=1, processes=True, workload_type="cpu"
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=8)

    @pytest.mark.unit
    def test_valid_topology_io(self):
        """Test validation of valid I/O topology."""
        topology = TopologySpec(
            n_workers=1, threads_per_worker=8, processes=False, workload_type="io"
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=16)

    @pytest.mark.unit
    def test_valid_topology_mixed(self):
        """Test validation of valid mixed topology."""
        topology = TopologySpec(
            n_workers=4, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=8)

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_workers", [0, -1, -5])
    def test_invalid_n_workers(self, invalid_workers):
        """Test validation with invalid n_workers."""
        topology = TopologySpec(
            n_workers=invalid_workers, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        with pytest.raises(InvalidConfigurationError) as exc_info:
            validate_topology(topology, total_cores=8)

        assert "n_workers must be positive" in str(exc_info.value)
        assert str(invalid_workers) in str(exc_info.value)

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_threads", [0, -1, -3])
    def test_invalid_threads_per_worker(self, invalid_threads):
        """Test validation with invalid threads_per_worker."""
        topology = TopologySpec(
            n_workers=4, threads_per_worker=invalid_threads, processes=True, workload_type="mixed"
        )

        with pytest.raises(InvalidConfigurationError) as exc_info:
            validate_topology(topology, total_cores=8)

        assert "threads_per_worker must be positive" in str(exc_info.value)
        assert str(invalid_threads) in str(exc_info.value)

    @pytest.mark.unit
    def test_severe_oversubscription(self):
        """Test validation catches severe CPU oversubscription."""
        # 8 workers * 8 threads = 64 total threads, but only 8 cores
        # This is 8x oversubscription, which exceeds the 2x limit
        topology = TopologySpec(
            n_workers=8, threads_per_worker=8, processes=True, workload_type="mixed"
        )

        with pytest.raises(InvalidConfigurationError) as exc_info:
            validate_topology(topology, total_cores=8)

        assert "requests 64 total threads but only 8 cores available" in str(exc_info.value)
        assert "severe oversubscription" in str(exc_info.value)

    @pytest.mark.unit
    def test_acceptable_oversubscription(self):
        """Test that 2x oversubscription is acceptable."""
        # 8 workers * 2 threads = 16 total threads with 8 cores (2x oversubscription)
        topology = TopologySpec(
            n_workers=8, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=8)

    @pytest.mark.unit
    def test_exact_core_match(self):
        """Test topology that exactly matches available cores."""
        # 4 workers * 2 threads = 8 total threads with 8 cores (1x, no oversubscription)
        topology = TopologySpec(
            n_workers=4, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=8)

    @pytest.mark.unit
    def test_undersubscription(self):
        """Test topology that uses fewer threads than cores."""
        # 2 workers * 2 threads = 4 total threads with 8 cores (undersubscribed)
        topology = TopologySpec(
            n_workers=2, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=8)

    @pytest.mark.unit
    def test_multiple_workers_without_processes(self):
        """Test that multiple workers without processes is invalid."""
        topology = TopologySpec(
            n_workers=4,  # Multiple workers
            threads_per_worker=2,
            processes=False,  # But no processes
            workload_type="io",
        )

        with pytest.raises(InvalidConfigurationError) as exc_info:
            validate_topology(topology, total_cores=8)

        assert "Cannot have multiple workers (n_workers > 1) when processes=False" in str(
            exc_info.value
        )

    @pytest.mark.unit
    def test_single_worker_without_processes_valid(self):
        """Test that single worker without processes is valid."""
        topology = TopologySpec(
            n_workers=1,  # Single worker
            threads_per_worker=8,
            processes=False,  # No processes is OK for single worker
            workload_type="io",
        )

        # Should not raise any exceptions
        validate_topology(topology, total_cores=8)

    @pytest.mark.unit
    def test_oversubscription_boundary_conditions(self):
        """Test oversubscription validation boundary conditions."""
        # Test exactly at the 2x limit (should pass)
        topology_limit = TopologySpec(
            n_workers=4, threads_per_worker=4, processes=True, workload_type="mixed"
        )
        # 4 * 4 = 16 threads with 8 cores = exactly 2x
        validate_topology(topology_limit, total_cores=8)

        # Test just over the 2x limit (should fail)
        topology_over = TopologySpec(
            n_workers=4,
            threads_per_worker=5,  # 4 * 5 = 20 threads with 8 cores = 2.5x
            processes=True,
            workload_type="mixed",
        )
        with pytest.raises(InvalidConfigurationError):
            validate_topology(topology_over, total_cores=8)

    @pytest.mark.unit
    def test_validation_with_large_numbers(self):
        """Test validation with large core and worker counts."""
        # Large but valid configuration
        topology = TopologySpec(
            n_workers=64, threads_per_worker=2, processes=True, workload_type="mixed"
        )
        # 64 * 2 = 128 threads with 128 cores = 1x (no oversubscription)
        validate_topology(topology, total_cores=128)

        # Large invalid configuration
        topology_invalid = TopologySpec(
            n_workers=64,
            threads_per_worker=8,  # 64 * 8 = 512 threads
            processes=True,
            workload_type="mixed",
        )
        # 512 threads with 64 cores = 8x oversubscription (too much)
        with pytest.raises(InvalidConfigurationError):
            validate_topology(topology_invalid, total_cores=64)


class TestTopologyIntegration:
    """Integration tests combining topology decision and validation."""

    @pytest.mark.unit
    def test_decide_and_validate_workflow(self):
        """Test that decided topologies pass validation for reasonable configurations."""
        test_cases = [
            ("cpu", 8),
            ("io", 8),
            ("mixed", 8),
            ("cpu", 1),
            # Note: I/O with 1 core causes oversubscription due to minimum 4 threads
            ("mixed", 1),
            ("cpu", 32),
            ("io", 32),
            ("mixed", 32),
        ]

        for workload_type, cores in test_cases:
            topology = decide_topology(workload_type, cores)
            # Should not raise any exceptions
            validate_topology(topology, cores)

    @pytest.mark.unit
    def test_io_single_core_oversubscription_edge_case(self):
        """Test that I/O workload with single core causes expected oversubscription."""
        # I/O workload with 1 core creates 1 worker with 4 threads (4x oversubscription)
        topology = decide_topology("io", total_cores=1)

        # This should fail validation due to oversubscription
        with pytest.raises(InvalidConfigurationError) as exc_info:
            validate_topology(topology, total_cores=1)

        assert "severe oversubscription" in str(exc_info.value)
        assert "4 total threads but only 1 cores" in str(exc_info.value)

    @pytest.mark.unit
    def test_decide_with_max_workers_validation(self):
        """Test that decided topologies with max_workers constraints pass validation."""
        test_cases = [
            ("cpu", 16, 8),
            ("mixed", 16, 4),
            ("cpu", 32, 16),
            ("mixed", 32, 8),
        ]

        for workload_type, cores, max_workers in test_cases:
            topology = decide_topology(workload_type, cores, max_workers)
            # Should not raise any exceptions
            validate_topology(topology, cores)

    @pytest.mark.unit
    def test_realistic_hpc_scenarios(self):
        """Test realistic HPC scenarios with various core counts."""
        # Small job: 4 cores
        topology_small = decide_topology("mixed", 4)
        validate_topology(topology_small, 4)
        assert topology_small.n_workers == 2
        assert topology_small.threads_per_worker == 2

        # Medium job: 28 cores (typical node)
        topology_medium = decide_topology("cpu", 28)
        validate_topology(topology_medium, 28)
        assert topology_medium.n_workers == 28
        assert topology_medium.threads_per_worker == 1

        # Large job: 128 cores (multi-node)
        topology_large = decide_topology("mixed", 128, max_workers=32)
        validate_topology(topology_large, 128)
        assert topology_large.n_workers == 32  # Limited by max_workers
        assert topology_large.threads_per_worker == 2

    @pytest.mark.unit
    def test_workload_type_characteristics(self):
        """Test that each workload type has expected characteristics."""
        cores = 16

        # CPU workload should maximize workers, minimize threads per worker
        cpu_topo = decide_topology("cpu", cores)
        validate_topology(cpu_topo, cores)
        assert cpu_topo.processes is True
        assert cpu_topo.threads_per_worker == 1
        assert cpu_topo.n_workers == cores

        # I/O workload should minimize workers, maximize threads per worker
        io_topo = decide_topology("io", cores)
        validate_topology(io_topo, cores)
        assert io_topo.processes is False
        assert io_topo.n_workers == 1
        assert io_topo.threads_per_worker > 1

        # Mixed should balance workers and threads
        mixed_topo = decide_topology("mixed", cores)
        validate_topology(mixed_topo, cores)
        assert mixed_topo.processes is True
        assert mixed_topo.threads_per_worker == 2
        assert mixed_topo.n_workers == cores // 2

    @pytest.mark.unit
    def test_memory_efficiency_implications(self):
        """Test topology decisions that have memory efficiency implications."""
        # Many workers (CPU workload) - higher memory overhead
        cpu_topo = decide_topology("cpu", 32)
        total_workers_cpu = cpu_topo.n_workers

        # Fewer workers (mixed workload) - lower memory overhead
        mixed_topo = decide_topology("mixed", 32)
        total_workers_mixed = mixed_topo.n_workers

        # CPU workload should have more workers than mixed
        assert total_workers_cpu > total_workers_mixed
        assert cpu_topo.n_workers == 32
        assert mixed_topo.n_workers == 16

    @pytest.mark.unit
    def test_thread_scaling_patterns(self):
        """Test thread scaling patterns across different core counts."""
        core_counts = [1, 2, 4, 8, 16, 32, 64]

        for cores in core_counts:
            for workload_type in ["cpu", "io", "mixed"]:
                topology = decide_topology(workload_type, cores)

                # I/O workload with 1 core causes oversubscription due to minimum 4 threads
                if workload_type == "io" and cores == 1:
                    # This case should fail validation due to oversubscription (4 threads / 1 core = 4x)
                    with pytest.raises(InvalidConfigurationError):
                        validate_topology(topology, cores)
                else:
                    # All other cases should pass validation
                    validate_topology(topology, cores)

                total_threads = topology.n_workers * topology.threads_per_worker

                # Should always have at least 1 worker
                assert topology.n_workers >= 1

                # Should always have at least 1 thread per worker
                assert topology.threads_per_worker >= 1

                # For non-problematic cases, total threads should be reasonable
                if not (workload_type == "io" and cores == 1):
                    # Total threads should never exceed cores by more than 2x
                    assert total_threads <= cores * 2
