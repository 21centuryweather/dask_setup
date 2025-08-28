"""Unit tests for dask_setup.resources module."""

import os
from unittest.mock import patch

import pytest

from dask_setup.exceptions import ResourceDetectionError
from dask_setup.resources import (
    _detect_pbs_resources,
    _detect_psutil_resources,
    _detect_slurm_resources,
    _parse_mem_bytes,
    detect_resources,
)
from dask_setup.types import ResourceSpec


class TestParseMemBytes:
    """Test memory string parsing function."""

    @pytest.mark.unit
    def test_none_input(self):
        """Test parsing None input."""
        assert _parse_mem_bytes(None) is None

    @pytest.mark.unit
    def test_empty_string(self):
        """Test parsing empty string."""
        assert _parse_mem_bytes("") is None
        assert _parse_mem_bytes("   ") is None

    @pytest.mark.unit
    def test_pure_numbers_as_mb(self):
        """Test parsing pure numbers (treated as MB)."""
        assert _parse_mem_bytes("1024") == 1024 * 1024 * 1024  # 1 GB in bytes
        assert _parse_mem_bytes("512") == 512 * 1024 * 1024  # 512 MB in bytes
        assert _parse_mem_bytes("0") == 0

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_str,expected_bytes",
        [
            # Bytes
            ("100b", 100),
            ("100", 100 * 1024 * 1024),  # Pure number as MB
            # Kilobytes (decimal)
            ("1k", 1000),
            ("1kb", 1000),
            ("10K", 10000),
            ("10KB", 10000),
            # Kilobytes (binary)
            ("1ki", 1024),
            ("1kib", 1024),
            ("10KI", 10240),
            ("10KIB", 10240),
            # Megabytes (decimal)
            ("1m", 1000**2),
            ("1mb", 1000**2),
            ("100M", 100 * 1000**2),
            ("100MB", 100 * 1000**2),
            # Megabytes (binary)
            ("1mi", 1024**2),
            ("1mib", 1024**2),
            ("100MI", 100 * 1024**2),
            ("100MIB", 100 * 1024**2),
            # Gigabytes (decimal)
            ("1g", 1000**3),
            ("1gb", 1000**3),
            ("4G", 4 * 1000**3),
            ("4GB", 4 * 1000**3),
            # Gigabytes (binary)
            ("1gi", 1024**3),
            ("1gib", 1024**3),
            ("4GI", 4 * 1024**3),
            ("4GIB", 4 * 1024**3),
            # Terabytes
            ("1t", 1000**4),
            ("1tb", 1000**4),
            ("1ti", 1024**4),
            ("1tib", 1024**4),
            # Petabytes
            ("1p", 1000**5),
            ("1pb", 1000**5),
            ("1pi", 1024**5),
            ("1pib", 1024**5),
            # Exabytes
            ("1e", 1000**6),
            ("1eb", 1000**6),
            ("1ei", 1024**6),
            ("1eib", 1024**6),
        ],
    )
    def test_valid_memory_formats(self, input_str, expected_bytes):
        """Test parsing various valid memory format strings."""
        assert _parse_mem_bytes(input_str) == expected_bytes

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_str",
        [
            "1.5gb",  # Decimal sizes
            "2.25GB",
            "0.5gi",
            "10.75mib",
        ],
    )
    def test_decimal_sizes(self, input_str):
        """Test parsing decimal memory sizes."""
        result = _parse_mem_bytes(input_str)
        assert result is not None
        assert isinstance(result, int)
        assert result > 0

    @pytest.mark.unit
    def test_whitespace_handling(self):
        """Test parsing with various whitespace."""
        assert _parse_mem_bytes("  1gb  ") == 1000**3
        assert _parse_mem_bytes("1 gb") == 1000**3
        assert _parse_mem_bytes("1  gb") == 1000**3
        assert _parse_mem_bytes("\t1gb\n") == 1000**3

    @pytest.mark.unit
    def test_enhanced_whitespace_handling(self):
        """Test enhanced whitespace handling including tabs and mixed combinations."""
        # Various space combinations
        assert _parse_mem_bytes("16 GB") == 16 * 1000**3
        assert _parse_mem_bytes("1.5  TiB") == int(1.5 * 1024**4)
        assert _parse_mem_bytes("\t\t512 MB\n\n") == 512 * 1000**2
        assert _parse_mem_bytes("  \t  32\t GiB  \n") == 32 * 1024**3

        # Multiple internal spaces should be normalized
        assert _parse_mem_bytes("64    GB") == 64 * 1000**3
        assert _parse_mem_bytes("8\t\t\tGiB") == 8 * 1024**3

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "invalid_input",
        [
            "invalid",
            "1xb",
            "gb1",
            "1.2.3gb",
            "-1gb",
            "1 2gb",
            "abc123",
            "1tb2gb",  # Multiple units
        ],
    )
    def test_invalid_formats(self, invalid_input):
        """Test parsing invalid memory format strings."""
        assert _parse_mem_bytes(invalid_input) is None

    @pytest.mark.unit
    def test_case_insensitive(self):
        """Test case-insensitive parsing."""
        assert _parse_mem_bytes("1GB") == _parse_mem_bytes("1gb")
        assert _parse_mem_bytes("1GiB") == _parse_mem_bytes("1gib")
        assert _parse_mem_bytes("1MB") == _parse_mem_bytes("1mb")

    @pytest.mark.unit
    def test_overflow_protection(self):
        """Test handling of potential overflow values."""
        # Very large numbers should return None if they cause overflow
        result = _parse_mem_bytes("999999999999999999999999eb")
        # Should either return None or a very large int, but not crash
        assert result is None or isinstance(result, int)

    @pytest.mark.unit
    def test_zero_values(self):
        """Test parsing zero values."""
        assert _parse_mem_bytes("0") == 0
        assert _parse_mem_bytes("0gb") == 0
        assert _parse_mem_bytes("0.0gb") == 0


class TestDetectSlurmResources:
    """Test SLURM resource detection."""

    def setup_method(self):
        """Clear environment before each test."""
        env_vars = ["SLURM_CPUS_ON_NODE", "SLURM_MEM_PER_NODE", "SLURM_MEM_PER_CPU"]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]

    @pytest.mark.unit
    def test_no_slurm_environment(self):
        """Test detection when no SLURM environment variables are set."""
        result = _detect_slurm_resources()
        assert result is None

    @pytest.mark.unit
    def test_valid_slurm_with_total_memory(self):
        """Test SLURM detection with total memory per node."""
        os.environ["SLURM_CPUS_ON_NODE"] = "8"
        os.environ["SLURM_MEM_PER_NODE"] = "32768"  # 32 GB in MB

        result = _detect_slurm_resources()

        assert result is not None
        assert isinstance(result, ResourceSpec)
        assert result.total_cores == 8
        assert result.total_mem_bytes == 32768 * 1024 * 1024  # 32 GB in bytes
        assert result.detection_method == "SLURM"

    @pytest.mark.unit
    def test_valid_slurm_with_per_cpu_memory(self):
        """Test SLURM detection with memory per CPU."""
        os.environ["SLURM_CPUS_ON_NODE"] = "4"
        os.environ["SLURM_MEM_PER_CPU"] = "4096"  # 4 GB per CPU in MB

        result = _detect_slurm_resources()

        assert result is not None
        assert result.total_cores == 4
        assert result.total_mem_bytes == 4 * 4096 * 1024 * 1024  # 16 GB total
        assert result.detection_method == "SLURM"

    @pytest.mark.unit
    def test_slurm_priority_total_over_per_cpu(self):
        """Test that total memory takes priority over per-CPU memory."""
        os.environ["SLURM_CPUS_ON_NODE"] = "4"
        os.environ["SLURM_MEM_PER_NODE"] = "16384"  # 16 GB total
        os.environ["SLURM_MEM_PER_CPU"] = "8192"  # Would be 32 GB total (4 * 8GB)

        result = _detect_slurm_resources()

        assert result is not None
        assert result.total_mem_bytes == 16384 * 1024 * 1024  # Uses total, not per-CPU

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.virtual_memory")
    def test_slurm_fallback_to_psutil_memory(self, mock_virtual_memory):
        """Test fallback to psutil when SLURM memory info is unavailable."""
        mock_virtual_memory.return_value.total = 16 * 1024**3  # 16 GB

        os.environ["SLURM_CPUS_ON_NODE"] = "8"
        # No memory environment variables set

        result = _detect_slurm_resources()

        assert result is not None
        assert result.total_cores == 8
        assert result.total_mem_bytes == 16 * 1024**3
        assert result.detection_method == "SLURM"
        mock_virtual_memory.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_cpus", ["", "abc", "1.5"])
    def test_invalid_slurm_cpus(self, invalid_cpus):
        """Test handling of invalid SLURM CPU values."""
        os.environ["SLURM_CPUS_ON_NODE"] = invalid_cpus
        os.environ["SLURM_MEM_PER_NODE"] = "8192"

        result = _detect_slurm_resources()
        assert result is None

    @pytest.mark.unit
    @pytest.mark.parametrize("edge_case_cpus,expected_cores", [("0", 0), ("-1", -1)])
    def test_slurm_edge_case_cpus(self, edge_case_cpus, expected_cores):
        """Test SLURM detection with edge case CPU values that parse as integers."""
        os.environ["SLURM_CPUS_ON_NODE"] = edge_case_cpus
        os.environ["SLURM_MEM_PER_NODE"] = "8192"

        result = _detect_slurm_resources()
        assert result is not None
        assert result.total_cores == expected_cores

    @pytest.mark.unit
    def test_slurm_with_non_digit_memory(self):
        """Test SLURM with non-digit memory values (should parse successfully)."""
        os.environ["SLURM_CPUS_ON_NODE"] = "4"
        os.environ["SLURM_MEM_PER_NODE"] = "8gb"  # Non-digit format (decimal GB)

        result = _detect_slurm_resources()

        assert result is not None
        assert result.total_cores == 4
        assert result.total_mem_bytes == 8000000000  # 8gb = 8 * 10^9 bytes (decimal GB)


class TestDetectPbsResources:
    """Test PBS resource detection."""

    def setup_method(self):
        """Clear environment before each test."""
        env_vars = ["NCPUS", "PBS_NCPUS", "PBS_VMEM", "PBS_MEM"]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]

    @pytest.mark.unit
    def test_no_pbs_environment(self):
        """Test detection when no PBS environment variables are set."""
        result = _detect_pbs_resources()
        assert result is None

    @pytest.mark.unit
    def test_valid_pbs_with_ncpus(self):
        """Test PBS detection with NCPUS variable."""
        os.environ["NCPUS"] = "16"
        os.environ["PBS_VMEM"] = "64gb"

        result = _detect_pbs_resources()

        assert result is not None
        assert isinstance(result, ResourceSpec)
        assert result.total_cores == 16
        assert result.total_mem_bytes == 64 * 1000**3  # 64 GB in bytes (decimal)
        assert result.detection_method == "PBS"

    @pytest.mark.unit
    def test_valid_pbs_with_pbs_ncpus(self):
        """Test PBS detection with PBS_NCPUS variable."""
        os.environ["PBS_NCPUS"] = "8"
        os.environ["PBS_MEM"] = "32gib"  # Binary gigabytes

        result = _detect_pbs_resources()

        assert result is not None
        assert result.total_cores == 8
        assert result.total_mem_bytes == 32 * 1024**3  # 32 GiB in bytes
        assert result.detection_method == "PBS"

    @pytest.mark.unit
    def test_pbs_ncpus_priority(self):
        """Test that NCPUS takes priority over PBS_NCPUS."""
        os.environ["NCPUS"] = "8"
        os.environ["PBS_NCPUS"] = "16"  # Should be ignored
        os.environ["PBS_VMEM"] = "32gb"

        result = _detect_pbs_resources()

        assert result is not None
        assert result.total_cores == 8  # Uses NCPUS, not PBS_NCPUS

    @pytest.mark.unit
    def test_pbs_vmem_priority(self):
        """Test that PBS_VMEM takes priority over PBS_MEM."""
        os.environ["NCPUS"] = "4"
        os.environ["PBS_VMEM"] = "16gb"
        os.environ["PBS_MEM"] = "32gb"  # Should be ignored

        result = _detect_pbs_resources()

        assert result is not None
        assert result.total_mem_bytes == 16 * 1000**3  # Uses PBS_VMEM

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.virtual_memory")
    def test_pbs_fallback_to_psutil_memory(self, mock_virtual_memory):
        """Test fallback to psutil when PBS memory parsing fails."""
        mock_virtual_memory.return_value.total = 24 * 1024**3  # 24 GB

        os.environ["NCPUS"] = "6"
        os.environ["PBS_VMEM"] = "invalid_memory_format"

        result = _detect_pbs_resources()

        assert result is not None
        assert result.total_cores == 6
        assert result.total_mem_bytes == 24 * 1024**3
        assert result.detection_method == "PBS"
        mock_virtual_memory.assert_called_once()

    @pytest.mark.unit
    @pytest.mark.parametrize("invalid_cpus", ["", "abc", "-1", "1.5"])
    def test_invalid_pbs_cpus(self, invalid_cpus):
        """Test handling of invalid PBS CPU values."""
        os.environ["NCPUS"] = invalid_cpus
        os.environ["PBS_VMEM"] = "16gb"

        result = _detect_pbs_resources()
        assert result is None

    @pytest.mark.unit
    def test_pbs_zero_cpus_edge_case(self):
        """Test PBS detection with zero CPUs (valid digit but edge case)."""
        os.environ["NCPUS"] = "0"
        os.environ["PBS_VMEM"] = "16gb"

        result = _detect_pbs_resources()
        assert result is not None
        assert result.total_cores == 0

    @pytest.mark.unit
    def test_pbs_with_various_memory_formats(self):
        """Test PBS with various memory format strings."""
        test_cases = [
            ("16gb", 16 * 1000**3),
            ("16384", 16384 * 1024 * 1024),  # Pure number as MB
            ("8gib", 8 * 1024**3),
            ("32000mb", 32000 * 1000**2),
        ]

        for mem_str, expected_bytes in test_cases:
            # Clear and set environment
            for var in ["NCPUS", "PBS_NCPUS", "PBS_VMEM", "PBS_MEM"]:
                if var in os.environ:
                    del os.environ[var]

            os.environ["NCPUS"] = "4"
            os.environ["PBS_VMEM"] = mem_str

            result = _detect_pbs_resources()
            assert result is not None
            assert result.total_mem_bytes == expected_bytes


class TestDetectPsutilResources:
    """Test psutil resource detection."""

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.cpu_count")
    @patch("dask_setup.resources.psutil.virtual_memory")
    def test_valid_psutil_detection(self, mock_virtual_memory, mock_cpu_count):
        """Test successful psutil resource detection."""
        mock_cpu_count.return_value = 12
        mock_virtual_memory.return_value.total = 48 * 1024**3  # 48 GB

        result = _detect_psutil_resources()

        assert isinstance(result, ResourceSpec)
        assert result.total_cores == 12
        assert result.total_mem_bytes == 48 * 1024**3
        assert result.detection_method == "psutil"

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.cpu_count")
    @patch("dask_setup.resources.psutil.virtual_memory")
    def test_psutil_none_cpu_count(self, mock_virtual_memory, mock_cpu_count):
        """Test handling of None CPU count from psutil."""
        mock_cpu_count.return_value = None
        mock_virtual_memory.return_value.total = 16 * 1024**3

        with pytest.raises(ResourceDetectionError) as exc_info:
            _detect_psutil_resources()

        assert "Could not detect CPU cores from psutil" in str(exc_info.value)

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.cpu_count")
    @patch("dask_setup.resources.psutil.virtual_memory")
    def test_psutil_zero_cpu_count(self, mock_virtual_memory, mock_cpu_count):
        """Test handling of zero CPU count from psutil."""
        mock_cpu_count.return_value = 0
        mock_virtual_memory.return_value.total = 16 * 1024**3

        with pytest.raises(ResourceDetectionError) as exc_info:
            _detect_psutil_resources()

        assert "Could not detect CPU cores from psutil" in str(exc_info.value)

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.cpu_count")
    @patch("dask_setup.resources.psutil.virtual_memory")
    def test_psutil_zero_memory(self, mock_virtual_memory, mock_cpu_count):
        """Test handling of zero memory from psutil."""
        mock_cpu_count.return_value = 8
        mock_virtual_memory.return_value.total = 0

        with pytest.raises(ResourceDetectionError) as exc_info:
            _detect_psutil_resources()

        assert "Could not detect memory from psutil" in str(exc_info.value)

    @pytest.mark.unit
    @patch("dask_setup.resources.psutil.cpu_count")
    def test_psutil_exception_handling(self, mock_cpu_count):
        """Test handling of exceptions during psutil detection."""
        mock_cpu_count.side_effect = OSError("System error")

        with pytest.raises(ResourceDetectionError) as exc_info:
            _detect_psutil_resources()

        assert "psutil resource detection failed" in str(exc_info.value)
        assert "System error" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, OSError)


class TestDetectResources:
    """Test main resource detection function."""

    def setup_method(self):
        """Clear environment before each test."""
        env_vars = [
            "SLURM_CPUS_ON_NODE",
            "SLURM_MEM_PER_NODE",
            "SLURM_MEM_PER_CPU",
            "NCPUS",
            "PBS_NCPUS",
            "PBS_VMEM",
            "PBS_MEM",
        ]
        for var in env_vars:
            if var in os.environ:
                del os.environ[var]

    @pytest.mark.unit
    @patch("dask_setup.resources._detect_slurm_resources")
    def test_slurm_detection_priority(self, mock_slurm):
        """Test that SLURM detection has highest priority."""
        slurm_spec = ResourceSpec(16, 64 * 1024**3, "SLURM")
        mock_slurm.return_value = slurm_spec

        result = detect_resources()

        assert result is slurm_spec
        mock_slurm.assert_called_once()

    @pytest.mark.unit
    @patch("dask_setup.resources._detect_slurm_resources")
    @patch("dask_setup.resources._detect_pbs_resources")
    def test_pbs_detection_fallback(self, mock_pbs, mock_slurm):
        """Test that PBS detection is used when SLURM fails."""
        mock_slurm.return_value = None
        pbs_spec = ResourceSpec(8, 32 * 1024**3, "PBS")
        mock_pbs.return_value = pbs_spec

        result = detect_resources()

        assert result is pbs_spec
        mock_slurm.assert_called_once()
        mock_pbs.assert_called_once()

    @pytest.mark.unit
    @patch("dask_setup.resources._detect_slurm_resources")
    @patch("dask_setup.resources._detect_pbs_resources")
    @patch("dask_setup.resources._detect_psutil_resources")
    def test_psutil_final_fallback(self, mock_psutil, mock_pbs, mock_slurm):
        """Test that psutil is used when both SLURM and PBS fail."""
        mock_slurm.return_value = None
        mock_pbs.return_value = None
        psutil_spec = ResourceSpec(4, 16 * 1024**3, "psutil")
        mock_psutil.return_value = psutil_spec

        result = detect_resources()

        assert result is psutil_spec
        mock_slurm.assert_called_once()
        mock_pbs.assert_called_once()
        mock_psutil.assert_called_once()

    @pytest.mark.unit
    def test_real_environment_detection(self):
        """Test detection with real environment (should fallback to psutil)."""
        # This test uses the real environment, so should get psutil detection
        result = detect_resources()

        assert isinstance(result, ResourceSpec)
        assert result.total_cores > 0
        assert result.total_mem_bytes > 0
        assert result.detection_method == "psutil"

    @pytest.mark.unit
    def test_slurm_environment_integration(self):
        """Test full SLURM environment integration."""
        os.environ["SLURM_CPUS_ON_NODE"] = "24"
        os.environ["SLURM_MEM_PER_NODE"] = "98304"  # 96 GB in MB

        result = detect_resources()

        assert result.total_cores == 24
        assert result.total_mem_bytes == 98304 * 1024 * 1024
        assert result.detection_method == "SLURM"

    @pytest.mark.unit
    def test_pbs_environment_integration(self):
        """Test full PBS environment integration."""
        os.environ["NCPUS"] = "20"
        os.environ["PBS_VMEM"] = "128gb"

        result = detect_resources()

        assert result.total_cores == 20
        assert result.total_mem_bytes == 128 * 1000**3
        assert result.detection_method == "PBS"

    @pytest.mark.unit
    @patch("dask_setup.resources._detect_slurm_resources")
    @patch("dask_setup.resources._detect_pbs_resources")
    @patch("dask_setup.resources._detect_psutil_resources")
    def test_all_detection_methods_fail(self, mock_psutil, mock_pbs, mock_slurm):
        """Test when all detection methods fail."""
        mock_slurm.return_value = None
        mock_pbs.return_value = None
        mock_psutil.side_effect = ResourceDetectionError("All methods failed")

        with pytest.raises(ResourceDetectionError) as exc_info:
            detect_resources()

        assert "All methods failed" in str(exc_info.value)


class TestResourceDetectionIntegration:
    """Integration tests for resource detection."""

    @pytest.mark.unit
    def test_edge_case_memory_values(self):
        """Test with edge case memory values."""
        test_cases = [
            "1b",  # 1 byte
            "1kb",  # 1 kilobyte
            "0.5gb",  # Fractional gigabyte
            "999tb",  # Very large value
        ]

        for mem_str in test_cases:
            result = _parse_mem_bytes(mem_str)
            if result is not None:
                assert isinstance(result, int)
                assert result >= 0

    @pytest.mark.unit
    def test_mixed_case_environment_variables(self):
        """Test environment variables with different cases."""
        # Clear environment
        for var in ["NCPUS", "PBS_NCPUS", "PBS_VMEM", "PBS_MEM"]:
            if var in os.environ:
                del os.environ[var]

        # Set mixed case values
        os.environ["NCPUS"] = "8"
        os.environ["PBS_VMEM"] = "16GB"  # Uppercase units

        result = _detect_pbs_resources()

        assert result is not None
        assert result.total_cores == 8
        assert result.total_mem_bytes == 16 * 1000**3
        assert result.detection_method == "PBS"


class TestEnhancedMemoryParsingFeatures:
    """Test new enhanced memory parsing features."""

    @pytest.mark.unit
    def test_overflow_protection_with_logging(self, caplog):
        """Test overflow protection logs warnings appropriately."""
        import logging

        caplog.set_level(logging.WARNING)

        # Test very large value that should trigger overflow protection
        result = _parse_mem_bytes("999999999eb")  # Way beyond 8 EiB limit
        assert result is None

        # Check that warning was logged
        warning_messages = [
            record.message for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert any("exceeds maximum supported" in msg for msg in warning_messages)

    @pytest.mark.unit
    def test_negative_values_handling(self):
        """Test that negative values are handled correctly."""
        test_cases = ["-1gb", "-5.5mb", "-0.1tb"]

        for test_case in test_cases:
            result = _parse_mem_bytes(test_case)
            assert result is None  # Should reject negative values

    @pytest.mark.unit
    def test_extreme_whitespace_combinations(self):
        """Test parsing with extreme whitespace combinations."""
        test_cases = [
            "\n\r\t  16   \t\n GB \r\n",  # Mixed line endings and tabs
            "   \u00a0\u2000\u2001 8 \u2002\u2003 GiB   ",  # Unicode spaces
            "\f\v 32 \f\v MB \f\v",  # Form feed and vertical tab
        ]

        expected_values = [
            16 * 1000**3,  # 16 GB
            8 * 1024**3,  # 8 GiB
            32 * 1000**2,  # 32 MB
        ]

        for test_case, expected in zip(test_cases, expected_values, strict=False):
            result = _parse_mem_bytes(test_case)
            assert result == expected

    @pytest.mark.unit
    def test_decimal_precision_edge_cases(self):
        """Test decimal precision in parsing."""
        test_cases = [
            ("1.0gb", 1 * 1000**3),
            ("1.5gb", int(1.5 * 1000**3)),
            ("0.5gib", int(0.5 * 1024**3)),
            ("2.25tb", int(2.25 * 1000**4)),
            ("0.125mib", int(0.125 * 1024**2)),
        ]

        for input_str, expected in test_cases:
            result = _parse_mem_bytes(input_str)
            assert result == expected

    @pytest.mark.unit
    def test_maximum_supported_memory_boundary(self):
        """Test values at the 8 EiB boundary."""
        # Just under the limit should work
        max_bytes = 8 * 1024**6  # 8 EiB
        just_under = "7.99ei"
        result = _parse_mem_bytes(just_under)
        assert result is not None
        assert result < max_bytes

        # At the limit should work (exactly 8ei equals the limit)
        at_limit = "8ei"
        result = _parse_mem_bytes(at_limit)
        assert result == max_bytes  # Should equal the maximum limit

        # Over the limit should be rejected
        over_limit = "8.01ei"
        result = _parse_mem_bytes(over_limit)
        assert result is None  # Should be rejected due to > limit check

    @pytest.mark.unit
    def test_enhanced_slurm_memory_parsing(self):
        """Test enhanced SLURM memory parsing with mixed formats."""
        # Clear environment
        for var in ["SLURM_CPUS_ON_NODE", "SLURM_MEM_PER_NODE", "SLURM_MEM_PER_CPU"]:
            if var in os.environ:
                del os.environ[var]

        test_cases = [
            ("96 GB", 96 * 1000**3),  # Space-separated
            ("128gib", 128 * 1024**3),  # Binary units
            ("98304", 98304 * 1024 * 1024),  # Pure digits (MB)
        ]

        for mem_str, expected_bytes in test_cases:
            os.environ["SLURM_CPUS_ON_NODE"] = "24"
            os.environ["SLURM_MEM_PER_NODE"] = mem_str

            result = _detect_slurm_resources()

            assert result is not None
            assert result.total_cores == 24
            assert result.total_mem_bytes == expected_bytes
            assert result.detection_method == "SLURM"

            # Clean up for next iteration
            del os.environ["SLURM_MEM_PER_NODE"]

    @pytest.mark.unit
    def test_enhanced_slurm_per_cpu_parsing(self):
        """Test enhanced SLURM per-CPU memory parsing."""
        # Clear environment
        for var in ["SLURM_CPUS_ON_NODE", "SLURM_MEM_PER_NODE", "SLURM_MEM_PER_CPU"]:
            if var in os.environ:
                del os.environ[var]

        os.environ["SLURM_CPUS_ON_NODE"] = "8"
        os.environ["SLURM_MEM_PER_CPU"] = "16 GB"  # Space-separated format

        result = _detect_slurm_resources()

        assert result is not None
        assert result.total_cores == 8
        assert result.total_mem_bytes == 8 * 16 * 1000**3  # 8 CPUs * 16 GB each
        assert result.detection_method == "SLURM"


class TestMemoryValidationFeatures:
    """Test memory validation and feedback features."""

    @pytest.mark.unit
    def test_validate_memory_value_small_warning(self, caplog):
        """Test validation warnings for small memory values."""
        import logging

        from dask_setup.resources import validate_memory_value

        caplog.set_level(logging.WARNING)

        # Test very small memory (< 32 MiB)
        small_mem = 16 * 1024 * 1024  # 16 MiB
        validate_memory_value(small_mem, "test memory")

        # Check that warning was logged
        warning_messages = [
            record.message for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert any("Very small test memory value" in msg for msg in warning_messages)
        assert any("16.0 MiB" in msg for msg in warning_messages)

    @pytest.mark.unit
    def test_validate_memory_value_large_warning(self, caplog):
        """Test validation warnings for large memory values."""
        import logging

        from dask_setup.resources import validate_memory_value

        caplog.set_level(logging.WARNING)

        # Test very large memory (> 4 TiB)
        large_mem = 5 * 1024**4  # 5 TiB
        validate_memory_value(large_mem, "system memory")

        # Check that warning was logged
        warning_messages = [
            record.message for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert any("Very large system memory value" in msg for msg in warning_messages)
        assert any("5.0 TiB" in msg for msg in warning_messages)

    @pytest.mark.unit
    def test_validate_memory_value_normal_range(self, caplog):
        """Test that normal memory values don't trigger warnings."""
        import logging

        from dask_setup.resources import validate_memory_value

        caplog.set_level(logging.WARNING)

        # Test normal memory values
        normal_values = [
            64 * 1024 * 1024,  # 64 MiB
            1 * 1024**3,  # 1 GiB
            2 * 1024**4,  # 2 TiB
        ]

        for mem_val in normal_values:
            validate_memory_value(mem_val, "normal memory")

        # Should have no warnings
        warning_messages = [
            record.message for record in caplog.records if record.levelno == logging.WARNING
        ]
        assert len(warning_messages) == 0

    @pytest.mark.unit
    def test_parse_mem_bytes_with_feedback_error(self):
        """Test _parse_mem_bytes_with_feedback raises helpful errors."""
        from dask_setup.resources import _parse_mem_bytes_with_feedback

        with pytest.raises(ResourceDetectionError) as exc_info:
            _parse_mem_bytes_with_feedback("invalid_format", "PBS memory")

        error_msg = str(exc_info.value)
        assert "Could not parse PBS memory value 'invalid_format'" in error_msg
        assert "Use formats like '64GB', '1.5gib', '32768MB'" in error_msg

    @pytest.mark.unit
    def test_parse_mem_bytes_with_feedback_success(self):
        """Test _parse_mem_bytes_with_feedback returns correct values."""
        from dask_setup.resources import _parse_mem_bytes_with_feedback

        result = _parse_mem_bytes_with_feedback("64GB", "test memory")
        assert result == 64 * 1000**3

        # None input should return None without error
        result = _parse_mem_bytes_with_feedback(None, "test memory")
        assert result is None


class TestResourceCoverageEdgeCases:
    """Tests for edge cases to achieve better code coverage."""

    @pytest.mark.unit
    def test_parse_mem_bytes_unknown_unit_coverage(self):
        """Test unknown unit coverage for line 72."""
        # Test unknown unit - should return None
        result = _parse_mem_bytes("100xyz")
        assert result is None

    @pytest.mark.unit
    def test_parse_mem_bytes_exception_coverage(self):
        """Test exception handling coverage for lines 76-77."""
        # Mock float() or int() to raise an exception to test exception handling
        with patch("builtins.float", side_effect=ValueError("Mock exception")):
            result = _parse_mem_bytes("1.5gb")
            assert result is None

        # Also test AttributeError path
        with patch("builtins.float", side_effect=AttributeError("Mock exception")):
            result = _parse_mem_bytes("2.0tb")
            assert result is None

    @pytest.mark.unit
    @patch("dask_setup.resources.os.getenv")
    def test_pbs_resources_value_error_coverage(self, mock_getenv):
        """Test ValueError coverage for PBS resources lines 128-129."""

        # Mock environment where isdigit passes but int() fails
        def side_effect(key):
            if key == "PBS_NCPUS" or key == "NCPUS":
                # This scenario is rare but possible with certain string objects
                return "8"
            return None

        mock_getenv.side_effect = side_effect

        # Force int() to raise ValueError
        with patch("builtins.int", side_effect=ValueError("test")):
            result = _detect_pbs_resources()
            assert result is None
