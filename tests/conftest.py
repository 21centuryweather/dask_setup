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
    # Need to mock psutil in the specific modules that import it
    patches = [
        patch("psutil.cpu_count"),
        patch("psutil.virtual_memory"),
        patch("dask_setup.resources.psutil.cpu_count"),
        patch("dask_setup.resources.psutil.virtual_memory"),
    ]

    with (
        patches[0] as mock_cpu1,
        patches[1] as mock_mem1,
        patches[2] as mock_cpu2,
        patches[3] as mock_mem2,
    ):
        # Set sensible defaults for all mocks
        mock_cpu1.return_value = 8  # 8 logical cores
        mock_cpu2.return_value = 8

        # Mock memory object with total attribute
        mock_mem_obj = type("MockMemory", (), {"total": 16 * (1024**3)})()  # 16 GiB
        mock_mem1.return_value = mock_mem_obj
        mock_mem2.return_value = mock_mem_obj

        # Store all mocks so we can synchronize them when values change
        all_cpu_mocks = [mock_cpu1, mock_cpu2]
        all_mem_mocks = [mock_mem1, mock_mem2]

        # Create wrapper class that synchronizes all mocks
        class SynchronizedMock:
            def __init__(self, primary_mock, all_mocks):
                self._primary = primary_mock
                self._all_mocks = all_mocks

            @property
            def return_value(self):
                return self._primary.return_value

            @return_value.setter
            def return_value(self, value):
                # Set the same value on all mocks
                for mock in self._all_mocks:
                    mock.return_value = value

            def __getattr__(self, name):
                return getattr(self._primary, name)

        class SynchronizedMemoryMock:
            def __init__(self, primary_mock, all_mocks):
                self._primary = primary_mock
                self._all_mocks = all_mocks

            @property
            def return_value(self):
                return self._primary.return_value

            @return_value.setter
            def return_value(self, value):
                # Set the same value on all mocks
                for mock in self._all_mocks:
                    mock.return_value = value

            def __getattr__(self, name):
                return getattr(self._primary, name)

        # Return synchronized mock references
        yield {
            "cpu_count": SynchronizedMock(mock_cpu1, all_cpu_mocks),
            "virtual_memory": SynchronizedMemoryMock(mock_mem1, all_mem_mocks),
        }


@pytest.fixture
def temp_dir(tmp_path):
    """
    Fixture providing a temporary directory path as string.

    Uses pytest's built-in tmp_path but returns as string for compatibility
    with dask_setup code that expects string paths.
    """
    return str(tmp_path)
