"""Tests for spill compression functionality in dask_setup."""

import tempfile
from pathlib import Path

import dask
import pytest

from dask_setup.cluster import configure_dask_settings
from dask_setup.config import DaskSetupConfig
from dask_setup.error_handling import ConfigurationValidationError


class TestCompressionConfiguration:
    """Test compression configuration validation and functionality."""

    def test_default_compression_settings(self):
        """Test default compression settings."""
        config = DaskSetupConfig()
        assert config.spill_compression == "auto"
        assert not config.comm_compression
        assert config.spill_threads is None

    @pytest.mark.parametrize(
        "algorithm", ["auto", "lz4", "zstd", "snappy", "gzip", "blosc", "zlib", "bz2", "lzma"]
    )
    def test_valid_compression_algorithms(self, algorithm):
        """Test that valid compression algorithms are accepted."""
        config = DaskSetupConfig(spill_compression=algorithm)
        assert config.spill_compression == algorithm

    def test_invalid_compression_algorithm(self):
        """Test that invalid compression algorithms are rejected."""
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(spill_compression="invalid_algorithm")

        error_str = str(exc_info.value)
        assert "spill_compression" in error_str
        assert "must be one of" in error_str
        assert "invalid_algorithm" in error_str

    def test_compression_in_serialization(self):
        """Test that compression settings are included in serialization."""
        config = DaskSetupConfig(spill_compression="lz4", comm_compression=True)
        config_dict = config.to_dict()

        assert config_dict["spill_compression"] == "lz4"
        assert config_dict["comm_compression"]

    def test_compression_deserialization(self):
        """Test compression settings can be restored from dictionary."""
        data = {"spill_compression": "zstd", "comm_compression": True, "workload_type": "cpu"}

        config = DaskSetupConfig.from_dict(data)
        assert config.spill_compression == "zstd"
        assert config.comm_compression

    def test_compression_merge_precedence(self):
        """Test that compression settings merge correctly with precedence."""
        base_config = DaskSetupConfig(spill_compression="auto", comm_compression=False)
        override_config = DaskSetupConfig(spill_compression="lz4", comm_compression=True)

        merged = base_config.merge_with(override_config)

        assert merged.spill_compression == "lz4"
        assert merged.comm_compression


class TestDaskConfigurationIntegration:
    """Test integration with Dask configuration."""

    def test_configure_dask_settings_compression(self):
        """Test that compression settings are applied to Dask configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            configure_dask_settings(
                temp_dir=temp_path, spill_compression="lz4", comm_compression=True
            )

            # Verify Dask configuration was updated
            assert dask.config.get("distributed.worker.memory.spill-compression") == "lz4"
            assert dask.config.get("distributed.comm.compression")

    @pytest.mark.parametrize(
        "spill_comp,comm_comp",
        [
            ("auto", False),
            ("lz4", True),
            ("zstd", False),
            ("snappy", True),
        ],
    )
    def test_multiple_compression_configurations(self, spill_comp, comm_comp):
        """Test various combinations of compression settings."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            configure_dask_settings(
                temp_dir=temp_path, spill_compression=spill_comp, comm_compression=comm_comp
            )

            assert dask.config.get("distributed.worker.memory.spill-compression") == spill_comp
            assert dask.config.get("distributed.comm.compression") == comm_comp

    def test_configure_dask_settings_with_memory_thresholds(self):
        """Test that compression works alongside memory threshold configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            configure_dask_settings(
                temp_dir=temp_path,
                memory_target=0.70,
                memory_spill=0.80,
                memory_pause=0.90,
                memory_terminate=0.95,
                spill_compression="zstd",
                comm_compression=True,
            )

            # Verify all settings were applied
            assert dask.config.get("distributed.worker.memory.target") == 0.70
            assert dask.config.get("distributed.worker.memory.spill") == 0.80
            assert dask.config.get("distributed.worker.memory.pause") == 0.90
            assert dask.config.get("distributed.worker.memory.terminate") == 0.95
            assert dask.config.get("distributed.worker.memory.spill-compression") == "zstd"
            assert dask.config.get("distributed.comm.compression")


class TestCompressionRecommendations:
    """Test compression algorithm recommendations for different workloads."""

    def test_cpu_workload_compression_recommendations(self):
        """Test that CPU workloads can use any compression algorithm."""
        # CPU workloads benefit from compression since they tend to spill more
        algorithms = ["lz4", "zstd", "snappy"]

        for alg in algorithms:
            config = DaskSetupConfig(workload_type="cpu", spill_compression=alg)
            # Should not raise any validation errors
            assert config.workload_type == "cpu"
            assert config.spill_compression == alg

    def test_io_workload_compression_recommendations(self):
        """Test that I/O workloads can benefit from fast compression."""
        # I/O workloads might benefit from faster compression (less CPU overhead)
        config = DaskSetupConfig(workload_type="io", spill_compression="lz4")
        assert config.workload_type == "io"
        assert config.spill_compression == "lz4"

    def test_mixed_workload_compression_recommendations(self):
        """Test that mixed workloads can use balanced compression."""
        config = DaskSetupConfig(workload_type="mixed", spill_compression="auto")
        assert config.workload_type == "mixed"
        assert config.spill_compression == "auto"


class TestCompressionProfileIntegration:
    """Test compression settings work with configuration profiles."""

    def test_compression_in_profile_serialization(self):
        """Test that profiles correctly serialize compression settings."""
        from dask_setup.config import ConfigProfile

        config = DaskSetupConfig(
            spill_compression="zstd",
            comm_compression=True,
            workload_type="cpu",
            description="HPC workload with compression",
        )

        profile = ConfigProfile(name="hpc_compressed", config=config)
        profile_dict = profile.to_dict()

        assert profile_dict["config"]["spill_compression"] == "zstd"
        assert profile_dict["config"]["comm_compression"]

    def test_compression_profile_restoration(self):
        """Test that profiles correctly restore compression settings."""
        from dask_setup.config import ConfigProfile

        profile_data = {
            "name": "test_profile",
            "config": {
                "workload_type": "cpu",
                "spill_compression": "lz4",
                "comm_compression": True,
                "reserve_mem_gb": 30.0,
            },
        }

        profile = ConfigProfile.from_dict(profile_data)

        assert profile.config.spill_compression == "lz4"
        assert profile.config.comm_compression
        assert profile.config.workload_type == "cpu"
        assert profile.config.reserve_mem_gb == 30.0


@pytest.mark.integration
class TestCompressionEndToEnd:
    """End-to-end tests for compression functionality."""

    def test_compression_settings_end_to_end(self):
        """Test compression settings work through the complete setup process."""
        # This would require a full cluster setup, which is more complex
        # For now, we verify that the configuration flows correctly

        config = DaskSetupConfig(
            workload_type="cpu",
            spill_compression="lz4",
            comm_compression=True,
            max_workers=1,
            reserve_mem_gb=2.0,
        )

        # Verify configuration is valid
        config.validate()

        # Verify it can be serialized and restored
        config_dict = config.to_dict()
        restored_config = DaskSetupConfig.from_dict(config_dict)

        assert restored_config.spill_compression == config.spill_compression
        assert restored_config.comm_compression == config.comm_compression
        assert restored_config.workload_type == config.workload_type


class TestSpillThreadsConfiguration:
    """Test spill_threads configuration validation and functionality."""

    def test_default_spill_threads_setting(self):
        """Test default spill_threads setting is None."""
        config = DaskSetupConfig()
        assert config.spill_threads is None

    @pytest.mark.parametrize("threads", [1, 2, 4, 8, 16])
    def test_valid_spill_threads_values(self, threads):
        """Test that valid spill_threads values are accepted."""
        config = DaskSetupConfig(spill_threads=threads)
        assert config.spill_threads == threads

    def test_invalid_spill_threads_values(self):
        """Test that invalid spill_threads values are rejected."""
        # Test zero
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(spill_threads=0)
        error_str = str(exc_info.value)
        assert "spill_threads" in error_str
        assert "positive integer" in error_str

        # Test negative
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(spill_threads=-1)
        error_str = str(exc_info.value)
        assert "spill_threads" in error_str
        assert "positive integer" in error_str

        # Test too large
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(spill_threads=32)
        error_str = str(exc_info.value)
        assert "spill_threads" in error_str
        assert "16" in error_str

        # Test non-integer
        with pytest.raises(ConfigurationValidationError) as exc_info:
            DaskSetupConfig(spill_threads=4.5)
        error_str = str(exc_info.value)
        assert "spill_threads" in error_str
        assert "positive integer" in error_str

    def test_spill_threads_serialization(self):
        """Test spill_threads is included in serialization."""
        config = DaskSetupConfig(spill_threads=4)
        config_dict = config.to_dict()
        assert config_dict["spill_threads"] == 4

        # Test None serialization
        config_none = DaskSetupConfig(spill_threads=None)
        config_dict_none = config_none.to_dict()
        assert config_dict_none["spill_threads"] is None

    def test_spill_threads_deserialization(self):
        """Test spill_threads can be restored from dictionary."""
        data = {"workload_type": "cpu", "spill_threads": 8, "spill_compression": "lz4"}

        config = DaskSetupConfig.from_dict(data)
        assert config.spill_threads == 8
        assert config.workload_type == "cpu"
        assert config.spill_compression == "lz4"

    def test_spill_threads_merge_precedence(self):
        """Test that spill_threads merges correctly with precedence."""
        base_config = DaskSetupConfig(spill_threads=None)
        override_config = DaskSetupConfig(spill_threads=4)

        merged = base_config.merge_with(override_config)
        assert merged.spill_threads == 4

        # Test reverse merge
        merged_reverse = override_config.merge_with(base_config)
        assert merged_reverse.spill_threads == 4  # None doesn't override existing value

    def test_spill_threads_with_dask_config(self):
        """Test spill_threads is applied to Dask configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Test with threads specified
            configure_dask_settings(temp_dir=temp_path, spill_threads=6)

            # Verify Dask configuration was updated
            assert dask.config.get("distributed.p2p.threads") == 6

            # Test with None (should not set the config)
            configure_dask_settings(temp_dir=temp_path, spill_threads=None)

            # Should not have the key set or should be the default None
            p2p_threads = dask.config.get("distributed.p2p.threads")
            assert p2p_threads is None or p2p_threads == 6  # Previous value might persist

    @pytest.mark.parametrize(
        "threads,expected",
        [
            (1, 1),
            (4, 4),
            (8, 8),
            (16, 16),
            (None, None),
        ],
    )
    def test_spill_threads_dask_integration(self, threads, expected):
        """Test various spill_threads values integrate correctly with Dask."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            configure_dask_settings(
                temp_dir=temp_path,
                spill_threads=threads,
                spill_compression="lz4",
                comm_compression=True,
            )

            # Check spill threads configuration
            if expected is not None:
                assert dask.config.get("distributed.p2p.threads") == expected

            # Verify other settings are still applied correctly
            assert dask.config.get("distributed.worker.memory.spill-compression") == "lz4"
            assert dask.config.get("distributed.comm.compression")


class TestSpillThreadsProfileIntegration:
    """Test spill_threads settings work with configuration profiles."""

    def test_spill_threads_in_profile_serialization(self):
        """Test that profiles correctly serialize spill_threads settings."""
        from dask_setup.config import ConfigProfile

        config = DaskSetupConfig(
            workload_type="io",
            spill_threads=6,
            spill_compression="lz4",
            description="I/O workload with parallel spill",
        )

        profile = ConfigProfile(name="io_parallel_spill", config=config)
        profile_dict = profile.to_dict()

        assert profile_dict["config"]["spill_threads"] == 6
        assert profile_dict["config"]["workload_type"] == "io"
        assert profile_dict["config"]["spill_compression"] == "lz4"

    def test_spill_threads_profile_restoration(self):
        """Test that profiles correctly restore spill_threads settings."""
        from dask_setup.config import ConfigProfile

        profile_data = {
            "name": "parallel_spill_profile",
            "config": {
                "workload_type": "mixed",
                "spill_threads": 4,
                "spill_compression": "zstd",
                "comm_compression": False,
                "reserve_mem_gb": 20.0,
            },
        }

        profile = ConfigProfile.from_dict(profile_data)

        assert profile.config.spill_threads == 4
        assert profile.config.workload_type == "mixed"
        assert profile.config.spill_compression == "zstd"
        assert not profile.config.comm_compression
        assert profile.config.reserve_mem_gb == 20.0


class TestSpillThreadsEndToEnd:
    """End-to-end tests for spill_threads functionality."""

    def test_spill_threads_end_to_end_integration(self):
        """Test spill_threads works through complete configuration process."""
        config = DaskSetupConfig(
            workload_type="cpu",
            spill_threads=8,
            spill_compression="zstd",
            comm_compression=True,
            max_workers=2,
            reserve_mem_gb=4.0,
        )

        # Verify configuration is valid
        config.validate()

        # Test serialization round-trip
        config_dict = config.to_dict()
        restored_config = DaskSetupConfig.from_dict(config_dict)

        assert restored_config.spill_threads == config.spill_threads
        assert restored_config.spill_compression == config.spill_compression
        assert restored_config.comm_compression == config.comm_compression
        assert restored_config.workload_type == config.workload_type

        # Test Dask configuration application
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            configure_dask_settings(
                temp_dir=temp_path,
                spill_threads=config.spill_threads,
                spill_compression=config.spill_compression,
                comm_compression=config.comm_compression,
            )

            # Verify all settings applied correctly
            assert dask.config.get("distributed.p2p.threads") == 8
            assert dask.config.get("distributed.worker.memory.spill-compression") == "zstd"
            assert dask.config.get("distributed.comm.compression")
