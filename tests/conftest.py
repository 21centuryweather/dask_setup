"""Pytest configuration and fixtures for dask_setup tests."""

import os
from unittest.mock import patch

import pytest


@pytest.fixture
def isolated_env():
    """
    Fixture that snapshots and restores os.environ.

    Yields a dict-like object that can be modified during tests,
    with automatic cleanup afterward.
    """
    original_env = os.environ.copy()
    try:
        yield os.environ
    finally:
        os.environ.clear()
        os.environ.update(original_env)


@pytest.fixture
def mock_psutil():
    """
    Fixture to mock psutil.cpu_count() and psutil.virtual_memory().

    Returns a dict with 'cpu_count' and 'virtual_memory' mock objects
    that can be configured per test.
    """
    with patch("psutil.cpu_count") as mock_cpu, patch("psutil.virtual_memory") as mock_mem:
        # Set sensible defaults
        mock_cpu.return_value = 8  # 8 logical cores

        # Mock memory object with total attribute
        mock_mem_obj = type("MockMemory", (), {"total": 16 * (1024**3)})()  # 16 GiB
        mock_mem.return_value = mock_mem_obj

        yield {"cpu_count": mock_cpu, "virtual_memory": mock_mem}


@pytest.fixture
def temp_dir(tmp_path):
    """
    Fixture providing a temporary directory path as string.

    Uses pytest's built-in tmp_path but returns as string for compatibility
    with dask_setup code that expects string paths.
    """
    return str(tmp_path)
