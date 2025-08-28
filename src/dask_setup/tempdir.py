"""Temporary directory management for dask_setup."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def create_dask_temp_dir(base_dir: str | None = None) -> Path:
    """Create and configure temporary directory for Dask operations.

    Priority for base directory selection:
    1. base_dir parameter (if provided)
    2. $PBS_JOBFS environment variable (HPC job filesystem)
    3. $TMPDIR environment variable
    4. /tmp fallback

    Args:
        base_dir: Optional base directory path. If None, uses environment detection.

    Returns:
        Path to the created temporary directory

    Side effects:
        - Creates the temporary directory with process-specific name
        - Sets TMPDIR environment variable to point to the directory
        - Sets DASK_TEMPORARY_DIRECTORY environment variable
    """
    if base_dir is None:
        base_dir = os.environ.get("PBS_JOBFS") or os.environ.get("TMPDIR") or "/tmp"

    # Create unique directory name with process ID
    dask_temp_dir = Path(base_dir) / f"dask-{os.getpid()}"
    dask_temp_dir.mkdir(parents=True, exist_ok=True)

    # Set environment variables so other libraries use this directory
    dask_temp_str = str(dask_temp_dir)
    os.environ["TMPDIR"] = dask_temp_str
    os.environ["DASK_TEMPORARY_DIRECTORY"] = dask_temp_str

    return dask_temp_dir


def cleanup_temp_dir(temp_dir: Path | str, force: bool = False) -> None:
    """Clean up temporary directory created by create_dask_temp_dir.

    Args:
        temp_dir: Path to temporary directory to clean up
        force: If True, ignore errors during cleanup

    Note:
        This is primarily useful for testing. In production, temp directories
        are typically cleaned up automatically by the job scheduler or OS.
    """
    temp_path = Path(temp_dir)

    if temp_path.exists():
        try:
            shutil.rmtree(temp_path)
        except OSError:
            if not force:
                raise


def get_temp_dir_info(temp_dir: Path | str) -> dict[str, str | int]:
    """Get information about a temporary directory.

    Args:
        temp_dir: Path to temporary directory

    Returns:
        Dictionary with directory information:
        - path: Absolute path as string
        - exists: Whether directory exists
        - size_bytes: Total size of directory contents in bytes (-1 if error)
        - file_count: Number of files in directory (-1 if error)
    """
    temp_path = Path(temp_dir)

    info = {
        "path": str(temp_path.absolute()),
        "exists": temp_path.exists(),
        "size_bytes": -1,
        "file_count": -1,
    }

    if temp_path.exists():
        try:
            # Calculate total size and file count
            total_size = 0
            file_count = 0

            for root, _dirs, files in os.walk(temp_path):
                file_count += len(files)
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        total_size += os.path.getsize(file_path)
                    except (OSError, FileNotFoundError):
                        # Skip files that disappeared or are inaccessible
                        continue

            info["size_bytes"] = total_size
            info["file_count"] = file_count

        except (OSError, PermissionError):
            # Keep -1 values if we can't access the directory
            pass

    return info
