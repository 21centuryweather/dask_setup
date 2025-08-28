"""Unit tests for dask_setup.types module."""

import pytest

from dask_setup.types import MemorySpec, ResourceSpec, TopologySpec


class TestResourceSpec:
    """Test ResourceSpec NamedTuple."""

    @pytest.mark.unit
    def test_creation(self):
        """Test ResourceSpec creation with valid parameters."""
        spec = ResourceSpec(
            total_cores=16,
            total_mem_bytes=32 * (1024**3),  # 32 GB
            detection_method="PBS",
        )

        assert spec.total_cores == 16
        assert spec.total_mem_bytes == 32 * (1024**3)
        assert spec.detection_method == "PBS"

    @pytest.mark.unit
    def test_immutability(self):
        """Test that ResourceSpec is immutable."""
        spec = ResourceSpec(8, 16 * (1024**3), "SLURM")

        # Should not be able to modify fields
        with pytest.raises(AttributeError):
            spec.total_cores = 16

    @pytest.mark.unit
    def test_equality_and_hashing(self):
        """Test ResourceSpec equality and hashability."""
        spec1 = ResourceSpec(8, 16 * (1024**3), "psutil")
        spec2 = ResourceSpec(8, 16 * (1024**3), "psutil")
        spec3 = ResourceSpec(16, 16 * (1024**3), "psutil")

        # Test equality
        assert spec1 == spec2
        assert spec1 != spec3

        # Test hashability
        specs_set = {spec1, spec2, spec3}
        assert len(specs_set) == 2  # spec1 and spec2 are equal

    @pytest.mark.unit
    def test_string_representation(self):
        """Test ResourceSpec string representation."""
        spec = ResourceSpec(4, 8 * (1024**3), "PBS")
        repr_str = repr(spec)

        assert "ResourceSpec" in repr_str
        assert "total_cores=4" in repr_str
        assert "detection_method='PBS'" in repr_str

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "cores,mem_bytes,method",
        [
            (1, 1024**3, "PBS"),
            (128, 1024 * (1024**3), "SLURM"),
            (0, 0, "psutil"),  # Edge case - zero resources
        ],
    )
    def test_various_valid_inputs(self, cores, mem_bytes, method):
        """Test ResourceSpec with various valid inputs."""
        spec = ResourceSpec(cores, mem_bytes, method)
        assert spec.total_cores == cores
        assert spec.total_mem_bytes == mem_bytes
        assert spec.detection_method == method


class TestTopologySpec:
    """Test TopologySpec NamedTuple."""

    @pytest.mark.unit
    def test_creation(self):
        """Test TopologySpec creation."""
        spec = TopologySpec(
            n_workers=4, threads_per_worker=2, processes=True, workload_type="mixed"
        )

        assert spec.n_workers == 4
        assert spec.threads_per_worker == 2
        assert spec.processes is True
        assert spec.workload_type == "mixed"

    @pytest.mark.unit
    def test_workload_types(self):
        """Test TopologySpec with different workload types."""
        cpu_spec = TopologySpec(8, 1, True, "cpu")
        io_spec = TopologySpec(1, 16, False, "io")
        mixed_spec = TopologySpec(4, 2, True, "mixed")

        assert cpu_spec.workload_type == "cpu"
        assert io_spec.workload_type == "io"
        assert mixed_spec.workload_type == "mixed"

        # Verify typical patterns
        assert cpu_spec.threads_per_worker == 1
        assert io_spec.n_workers == 1
        assert mixed_spec.threads_per_worker == 2

    @pytest.mark.unit
    def test_total_threads_calculation(self):
        """Test that we can calculate total threads from topology."""
        spec = TopologySpec(4, 3, True, "mixed")
        total_threads = spec.n_workers * spec.threads_per_worker
        assert total_threads == 12

    @pytest.mark.unit
    def test_immutability_and_equality(self):
        """Test TopologySpec immutability and equality."""
        spec1 = TopologySpec(2, 4, False, "io")
        spec2 = TopologySpec(2, 4, False, "io")

        # Test immutability
        with pytest.raises(AttributeError):
            spec1.n_workers = 4

        # Test equality
        assert spec1 == spec2
        assert hash(spec1) == hash(spec2)


class TestMemorySpec:
    """Test MemorySpec NamedTuple."""

    @pytest.mark.unit
    def test_creation(self):
        """Test MemorySpec creation."""
        spec = MemorySpec(
            total_mem_gib=64.0,
            usable_mem_gb=54.0,
            mem_per_worker_bytes=27 * (1024**3),
            reserved_mem_gb=10.0,
        )

        assert spec.total_mem_gib == 64.0
        assert spec.usable_mem_gb == 54.0
        assert spec.mem_per_worker_bytes == 27 * (1024**3)
        assert spec.reserved_mem_gb == 10.0

    @pytest.mark.unit
    def test_memory_consistency(self):
        """Test memory calculations are consistent."""
        total = 32.0
        reserved = 8.0
        usable = total - reserved
        n_workers = 4
        per_worker_gb = usable / n_workers
        per_worker_bytes = int(per_worker_gb * (1024**3))

        spec = MemorySpec(
            total_mem_gib=total,
            usable_mem_gb=usable,
            mem_per_worker_bytes=per_worker_bytes,
            reserved_mem_gb=reserved,
        )

        # Verify consistency
        assert spec.total_mem_gib - spec.reserved_mem_gb == spec.usable_mem_gb
        assert spec.mem_per_worker_bytes == per_worker_bytes

    @pytest.mark.unit
    def test_memory_units(self):
        """Test memory unit conversions are consistent."""
        mem_gb = 16.0
        mem_bytes = int(mem_gb * (1024**3))

        spec = MemorySpec(
            total_mem_gib=32.0,
            usable_mem_gb=mem_gb,
            mem_per_worker_bytes=mem_bytes,
            reserved_mem_gb=16.0,
        )

        # Convert back and verify
        converted_gb = spec.mem_per_worker_bytes / (1024**3)
        assert abs(converted_gb - mem_gb) < 0.001  # Allow for floating point precision

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "total,reserved,workers",
        [
            (16.0, 4.0, 1),
            (64.0, 16.0, 8),
            (128.0, 32.0, 16),
            (1.0, 0.5, 1),  # Small memory system
        ],
    )
    def test_various_memory_configurations(self, total, reserved, workers):
        """Test MemorySpec with various realistic configurations."""
        usable = total - reserved
        per_worker_gb = usable / workers
        per_worker_bytes = int(per_worker_gb * (1024**3))

        spec = MemorySpec(
            total_mem_gib=total,
            usable_mem_gb=usable,
            mem_per_worker_bytes=per_worker_bytes,
            reserved_mem_gb=reserved,
        )

        # Basic consistency checks
        assert spec.total_mem_gib >= spec.usable_mem_gb
        assert spec.usable_mem_gb >= 0
        assert spec.mem_per_worker_bytes >= 0
        assert spec.reserved_mem_gb >= 0

    @pytest.mark.unit
    def test_string_representation(self):
        """Test MemorySpec string representation."""
        spec = MemorySpec(16.0, 12.0, 6 * (1024**3), 4.0)
        repr_str = repr(spec)

        assert "MemorySpec" in repr_str
        assert "total_mem_gib=16.0" in repr_str
        assert "usable_mem_gb=12.0" in repr_str
