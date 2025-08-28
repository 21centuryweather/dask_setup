"""Unit tests for dask_setup.tempdir module."""

import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from dask_setup.tempdir import cleanup_temp_dir, create_dask_temp_dir, get_temp_dir_info


class TestCreateDaskTempDir:
    """Test temporary directory creation function."""

    def setup_method(self):
        """Clear temp-related environment variables before each test."""
        env_vars = ["PBS_JOBFS", "TMPDIR", "DASK_TEMPORARY_DIRECTORY"]
        self.original_env = {}
        for var in env_vars:
            self.original_env[var] = os.environ.get(var)
            if var in os.environ:
                del os.environ[var]

    def teardown_method(self):
        """Restore environment variables after each test."""
        for var, value in self.original_env.items():
            if value is not None:
                os.environ[var] = value
            elif var in os.environ:
                del os.environ[var]

    @pytest.mark.unit
    def test_create_with_explicit_base_dir(self):
        """Test creating temp dir with explicit base directory."""
        with tempfile.TemporaryDirectory() as temp_base:
            result_dir = create_dask_temp_dir(base_dir=temp_base)

            # Check directory was created
            assert result_dir.exists()
            assert result_dir.is_dir()

            # Check it's in the expected location
            assert result_dir.parent == Path(temp_base)

            # Check naming pattern (dask-{pid})
            assert result_dir.name.startswith("dask-")
            assert str(os.getpid()) in result_dir.name

            # Check environment variables were set
            assert os.environ["TMPDIR"] == str(result_dir)
            assert os.environ["DASK_TEMPORARY_DIRECTORY"] == str(result_dir)

            # Clean up
            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_create_with_pbs_jobfs(self):
        """Test temp dir creation using PBS_JOBFS environment variable."""
        with tempfile.TemporaryDirectory() as pbs_dir:
            os.environ["PBS_JOBFS"] = pbs_dir

            result_dir = create_dask_temp_dir()

            # Check it used PBS_JOBFS as base
            assert result_dir.parent == Path(pbs_dir)
            assert result_dir.exists()

            # Check environment variables
            assert os.environ["TMPDIR"] == str(result_dir)
            assert os.environ["DASK_TEMPORARY_DIRECTORY"] == str(result_dir)

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_create_with_tmpdir_fallback(self):
        """Test temp dir creation falling back to TMPDIR."""
        with tempfile.TemporaryDirectory() as tmpdir:
            os.environ["TMPDIR"] = tmpdir
            # Ensure PBS_JOBFS is not set

            result_dir = create_dask_temp_dir()

            # Check it used TMPDIR as base
            assert result_dir.parent == Path(tmpdir)
            assert result_dir.exists()

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_create_with_tmp_final_fallback(self):
        """Test temp dir creation with final /tmp fallback."""
        # No environment variables set, should use /tmp

        result_dir = create_dask_temp_dir()

        # Check it used /tmp as base
        assert result_dir.parent == Path("/tmp")
        assert result_dir.exists()

        # Verify directory naming
        assert result_dir.name == f"dask-{os.getpid()}"

        cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_create_priority_order(self):
        """Test that PBS_JOBFS has priority over TMPDIR."""
        with tempfile.TemporaryDirectory() as pbs_dir, tempfile.TemporaryDirectory() as tmpdir:
            os.environ["PBS_JOBFS"] = pbs_dir
            os.environ["TMPDIR"] = tmpdir

            result_dir = create_dask_temp_dir()

            # Should use PBS_JOBFS, not TMPDIR
            assert result_dir.parent == Path(pbs_dir)
            assert result_dir.parent != Path(tmpdir)

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_explicit_base_dir_overrides_env(self):
        """Test that explicit base_dir parameter overrides environment variables."""
        with (
            tempfile.TemporaryDirectory() as pbs_dir,
            tempfile.TemporaryDirectory() as explicit_dir,
        ):
            os.environ["PBS_JOBFS"] = pbs_dir

            result_dir = create_dask_temp_dir(base_dir=explicit_dir)

            # Should use explicit dir, not PBS_JOBFS
            assert result_dir.parent == Path(explicit_dir)
            assert result_dir.parent != Path(pbs_dir)

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_directory_already_exists(self):
        """Test behavior when directory already exists."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Create directory first time
            result_dir1 = create_dask_temp_dir(base_dir=temp_base)

            # Create same directory again (should not fail due to exist_ok=True)
            result_dir2 = create_dask_temp_dir(base_dir=temp_base)

            # Should be the same directory
            assert result_dir1 == result_dir2
            assert result_dir1.exists()

            cleanup_temp_dir(result_dir1, force=True)

    @pytest.mark.unit
    def test_environment_variable_setting(self):
        """Test that environment variables are properly set."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Store original values
            os.environ.get("TMPDIR")
            os.environ.get("DASK_TEMPORARY_DIRECTORY")

            result_dir = create_dask_temp_dir(base_dir=temp_base)

            # Check both environment variables are set correctly
            assert os.environ["TMPDIR"] == str(result_dir)
            assert os.environ["DASK_TEMPORARY_DIRECTORY"] == str(result_dir)

            # Check they point to absolute paths
            assert Path(os.environ["TMPDIR"]).is_absolute()
            assert Path(os.environ["DASK_TEMPORARY_DIRECTORY"]).is_absolute()

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_path_object_returned(self):
        """Test that function returns a Path object."""
        with tempfile.TemporaryDirectory() as temp_base:
            result_dir = create_dask_temp_dir(base_dir=temp_base)

            assert isinstance(result_dir, Path)

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_nested_directory_creation(self):
        """Test creating temp dir in non-existent base directory."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Create a nested path that doesn't exist
            nested_base = Path(temp_base) / "nested" / "path"

            result_dir = create_dask_temp_dir(base_dir=str(nested_base))

            # Should create parent directories and temp dir
            assert result_dir.exists()
            assert result_dir.parent == nested_base
            assert nested_base.exists()

            cleanup_temp_dir(result_dir, force=True)

    @pytest.mark.unit
    def test_process_id_in_directory_name(self):
        """Test that process ID is included in directory name."""
        with tempfile.TemporaryDirectory() as temp_base:
            result_dir = create_dask_temp_dir(base_dir=temp_base)

            # Check that PID is in the directory name
            pid_str = str(os.getpid())
            assert pid_str in result_dir.name
            assert result_dir.name == f"dask-{pid_str}"

            cleanup_temp_dir(result_dir, force=True)


class TestCleanupTempDir:
    """Test temporary directory cleanup function."""

    @pytest.mark.unit
    def test_cleanup_existing_directory(self):
        """Test cleaning up an existing directory."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Create a temp directory with some files
            temp_dir = Path(temp_base) / "test_cleanup"
            temp_dir.mkdir()

            # Add some files
            (temp_dir / "file1.txt").write_text("content1")
            (temp_dir / "file2.txt").write_text("content2")

            # Create subdirectory with file
            sub_dir = temp_dir / "subdir"
            sub_dir.mkdir()
            (sub_dir / "file3.txt").write_text("content3")

            assert temp_dir.exists()
            assert len(list(temp_dir.rglob("*"))) > 0  # Has content

            # Clean up
            cleanup_temp_dir(temp_dir)

            # Should be gone
            assert not temp_dir.exists()

    @pytest.mark.unit
    def test_cleanup_non_existent_directory(self):
        """Test cleaning up a directory that doesn't exist."""
        non_existent = Path("/tmp/definitely_does_not_exist_12345")

        # Should not raise an error
        cleanup_temp_dir(non_existent)

        # Still shouldn't exist
        assert not non_existent.exists()

    @pytest.mark.unit
    def test_cleanup_with_string_path(self):
        """Test cleanup with string path instead of Path object."""
        with tempfile.TemporaryDirectory() as temp_base:
            temp_dir = Path(temp_base) / "test_string_cleanup"
            temp_dir.mkdir()
            (temp_dir / "file.txt").write_text("content")

            assert temp_dir.exists()

            # Clean up using string path
            cleanup_temp_dir(str(temp_dir))

            assert not temp_dir.exists()

    @pytest.mark.unit
    def test_cleanup_force_mode(self):
        """Test cleanup with force=True ignores errors."""
        # Create a mock directory that will cause shutil.rmtree to fail
        with (
            patch("dask_setup.tempdir.shutil.rmtree") as mock_rmtree,
            patch("dask_setup.tempdir.Path.exists", return_value=True),
        ):
            mock_rmtree.side_effect = OSError("Permission denied")

            # Should not raise with force=True
            cleanup_temp_dir("/some/path", force=True)

            mock_rmtree.assert_called_once()

    @pytest.mark.unit
    def test_cleanup_without_force_raises_error(self):
        """Test cleanup without force=True raises errors."""
        with (
            patch("dask_setup.tempdir.shutil.rmtree") as mock_rmtree,
            patch("dask_setup.tempdir.Path.exists", return_value=True),
        ):
            mock_rmtree.side_effect = OSError("Permission denied")

            # Should raise without force=True
            with pytest.raises(OSError, match="Permission denied"):
                cleanup_temp_dir("/some/path", force=False)

            mock_rmtree.assert_called_once()

    @pytest.mark.unit
    def test_cleanup_path_conversion(self):
        """Test that various path types are handled correctly."""
        test_paths = [
            Path("/tmp/test"),
            "/tmp/test",
            "test_dir",
        ]

        for path in test_paths:
            # Should not raise an error even for non-existent paths
            cleanup_temp_dir(path)


class TestGetTempDirInfo:
    """Test temporary directory information function."""

    @pytest.mark.unit
    def test_info_non_existent_directory(self):
        """Test getting info for non-existent directory."""
        non_existent = Path("/tmp/definitely_does_not_exist_99999")

        info = get_temp_dir_info(non_existent)

        assert info["path"] == str(non_existent.absolute())
        assert info["exists"] is False
        assert info["size_bytes"] == -1
        assert info["file_count"] == -1

    @pytest.mark.unit
    def test_info_empty_directory(self):
        """Test getting info for empty directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            info = get_temp_dir_info(temp_dir)

            assert info["path"] == str(Path(temp_dir).absolute())
            assert info["exists"] is True
            assert info["size_bytes"] == 0
            assert info["file_count"] == 0

    @pytest.mark.unit
    def test_info_directory_with_files(self):
        """Test getting info for directory with files."""
        with tempfile.TemporaryDirectory() as temp_base:
            temp_dir = Path(temp_base) / "test_info"
            temp_dir.mkdir()

            # Create files with known sizes
            file1_content = "Hello, world!"  # 13 bytes
            file2_content = "Test content that is longer."  # 29 bytes

            (temp_dir / "file1.txt").write_text(file1_content)
            (temp_dir / "file2.txt").write_text(file2_content)

            info = get_temp_dir_info(temp_dir)

            assert info["exists"] is True
            assert info["file_count"] == 2
            assert info["size_bytes"] == len(file1_content.encode()) + len(file2_content.encode())

    @pytest.mark.unit
    def test_info_directory_with_subdirectories(self):
        """Test getting info for directory with subdirectories and nested files."""
        with tempfile.TemporaryDirectory() as temp_base:
            temp_dir = Path(temp_base) / "test_nested"
            temp_dir.mkdir()

            # Root level files
            (temp_dir / "root1.txt").write_text("root content 1")
            (temp_dir / "root2.txt").write_text("root content 2")

            # Subdirectory with files
            sub_dir = temp_dir / "subdir"
            sub_dir.mkdir()
            (sub_dir / "sub1.txt").write_text("sub content 1")
            (sub_dir / "sub2.txt").write_text("sub content 2")

            # Nested subdirectory
            nested_dir = sub_dir / "nested"
            nested_dir.mkdir()
            (nested_dir / "nested1.txt").write_text("nested content")

            info = get_temp_dir_info(temp_dir)

            assert info["exists"] is True
            assert info["file_count"] == 5  # All files across all directories
            assert info["size_bytes"] > 0

            # Calculate expected size
            expected_size = (
                len("root content 1")
                + len("root content 2")
                + len("sub content 1")
                + len("sub content 2")
                + len("nested content")
            )
            assert info["size_bytes"] == expected_size

    @pytest.mark.unit
    def test_info_with_string_path(self):
        """Test getting info using string path."""
        with tempfile.TemporaryDirectory() as temp_dir:
            (Path(temp_dir) / "test_file.txt").write_text("test content")

            # Use string path instead of Path object
            info = get_temp_dir_info(temp_dir)

            assert info["exists"] is True
            assert info["file_count"] == 1
            assert info["size_bytes"] == len("test content")

    @pytest.mark.unit
    def test_info_path_conversion(self):
        """Test that path is converted to absolute path string."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            relative_path = Path(temp_path.name)  # Just the directory name

            # Change to parent directory to make relative path work
            import os

            orig_cwd = os.getcwd()
            try:
                os.chdir(temp_path.parent)

                info = get_temp_dir_info(relative_path)

                # Should still give absolute path
                assert Path(info["path"]).is_absolute()
                # The paths should resolve to the same location (handles symlinks on macOS)
                assert Path(info["path"]).resolve() == temp_path.resolve()

            finally:
                os.chdir(orig_cwd)

    @pytest.mark.unit
    def test_info_with_inaccessible_files(self):
        """Test behavior when some files are inaccessible during size calculation."""
        with tempfile.TemporaryDirectory() as temp_base:
            temp_dir = Path(temp_base) / "test_access"
            temp_dir.mkdir()

            # Create a file
            test_file = temp_dir / "accessible.txt"
            test_file.write_text("accessible content")

            # Mock os.path.getsize to simulate inaccessible file
            with patch("dask_setup.tempdir.os.path.getsize") as mock_getsize:

                def getsize_side_effect(path):
                    if "accessible" in str(path):
                        return len("accessible content")
                    else:
                        raise FileNotFoundError("File disappeared")

                mock_getsize.side_effect = getsize_side_effect

                # Create another file that will be "inaccessible"
                (temp_dir / "inaccessible.txt").write_text("inaccessible")

                info = get_temp_dir_info(temp_dir)

            # Should still succeed, but may have different size/count
            assert info["exists"] is True
            assert isinstance(info["size_bytes"], int)
            assert isinstance(info["file_count"], int)

    @pytest.mark.unit
    def test_info_file_disappeared_during_scan(self):
        """Test coverage for lines 100-102 when files disappear during scan."""
        with tempfile.TemporaryDirectory() as temp_base:
            temp_dir = Path(temp_base) / "disappearing_files"
            temp_dir.mkdir()

            # Create files
            (temp_dir / "file1.txt").write_text("content1")
            (temp_dir / "file2.txt").write_text("content2")

            # Mock getsize to simulate file disappearing
            original_getsize = os.path.getsize

            def mock_getsize(path):
                if "file2.txt" in str(path):
                    raise FileNotFoundError("File disappeared")
                return original_getsize(path)

            with patch("dask_setup.tempdir.os.path.getsize", side_effect=mock_getsize):
                info = get_temp_dir_info(temp_dir)

                # Should handle FileNotFoundError gracefully and continue
                assert info["exists"] is True
                assert info["file_count"] == 2  # Both files are counted by os.walk
                assert info["size_bytes"] == len("content1")  # Only file1 size counted

    @pytest.mark.unit
    def test_info_permission_error_handling(self):
        """Test behavior when directory access raises PermissionError."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Mock os.walk to raise PermissionError
            with patch("dask_setup.tempdir.os.walk") as mock_walk:
                mock_walk.side_effect = PermissionError("Access denied")

                info = get_temp_dir_info(temp_path)

                # Should handle error gracefully
                assert info["exists"] is True  # Path exists
                assert info["size_bytes"] == -1  # But couldn't calculate size
                assert info["file_count"] == -1  # Or count files

    @pytest.mark.unit
    def test_info_return_types(self):
        """Test that returned info has correct types."""
        with tempfile.TemporaryDirectory() as temp_dir:
            info = get_temp_dir_info(temp_dir)

            assert isinstance(info, dict)
            assert isinstance(info["path"], str)
            assert isinstance(info["exists"], bool)
            assert isinstance(info["size_bytes"], int)
            assert isinstance(info["file_count"], int)

    @pytest.mark.unit
    def test_info_large_directory(self):
        """Test info calculation for directory with many files."""
        with tempfile.TemporaryDirectory() as temp_base:
            temp_dir = Path(temp_base) / "large_dir"
            temp_dir.mkdir()

            # Create many small files
            num_files = 50
            for i in range(num_files):
                (temp_dir / f"file_{i:03d}.txt").write_text(f"Content {i}")

            info = get_temp_dir_info(temp_dir)

            assert info["exists"] is True
            assert info["file_count"] == num_files
            assert info["size_bytes"] > 0


class TestTempDirIntegration:
    """Integration tests combining multiple tempdir functions."""

    @pytest.mark.unit
    def test_create_info_cleanup_workflow(self):
        """Test complete workflow: create -> get info -> cleanup."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Create temp directory
            temp_dir = create_dask_temp_dir(base_dir=temp_base)
            assert temp_dir.exists()

            # Add some content
            (temp_dir / "test_file.txt").write_text("test content")
            (temp_dir / "subdir").mkdir()
            (temp_dir / "subdir" / "nested_file.txt").write_text("nested content")

            # Get info
            info = get_temp_dir_info(temp_dir)
            assert info["exists"] is True
            assert info["file_count"] == 2
            assert info["size_bytes"] > 0

            # Cleanup
            cleanup_temp_dir(temp_dir)
            assert not temp_dir.exists()

            # Info after cleanup
            info_after = get_temp_dir_info(temp_dir)
            assert info_after["exists"] is False
            assert info_after["size_bytes"] == -1
            assert info_after["file_count"] == -1

    @pytest.mark.unit
    def test_environment_isolation(self):
        """Test that multiple temp directories don't interfere."""
        with tempfile.TemporaryDirectory() as temp_base:
            # Create first temp directory
            temp_dir1 = create_dask_temp_dir(base_dir=temp_base)
            env_after_first = os.environ["TMPDIR"]

            # Create second temp directory (should be same due to same PID)
            temp_dir2 = create_dask_temp_dir(base_dir=temp_base)
            env_after_second = os.environ["TMPDIR"]

            # Should be the same directory (same process)
            assert temp_dir1 == temp_dir2
            assert env_after_first == env_after_second

            cleanup_temp_dir(temp_dir1, force=True)
