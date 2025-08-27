"""Tests for dask_setup.setup_dask_client function."""

import os
from pathlib import Path

import pytest

from dask_setup import setup_dask_client


@pytest.mark.parametrize(
    "workload_type,expected_processes,expected_threads",
    [
        ("cpu", True, 1),
        ("io", False, 8),  # Will be clamped based on logical cores
        ("mixed", True, 2),
    ],
)
def test_workload_types(
    isolated_env, mock_psutil, workload_type, expected_processes, expected_threads
):
    """Test that different workload types configure workers correctly."""
    # Use 8 logical cores for predictable results
    mock_psutil["cpu_count"].return_value = 8

    client, cluster, temp_dir = setup_dask_client(
        workload_type=workload_type, dashboard=False, max_workers=8
    )

    try:
        # Check cluster configuration
        assert len(cluster.workers) >= 1

        # For "io" workload, we expect 1 worker with multiple threads
        if workload_type == "io":
            assert len(cluster.workers) == 1
            # threads_per_worker should be between 4-16, clamped by logical_cores/2
            worker = list(cluster.workers.values())[0]
            assert worker.nthreads >= 4
        elif workload_type == "cpu":
            # CPU workload: processes=True, threads=1, workers≈cores
            assert len(cluster.workers) <= 8
            worker = list(cluster.workers.values())[0]
            assert worker.nthreads == 1
        elif workload_type == "mixed":
            # Mixed: processes=True, threads=2, workers=cores/2
            assert len(cluster.workers) <= 4  # 8 cores / 2 threads
            worker = list(cluster.workers.values())[0]
            assert worker.nthreads == 2

        # Verify temp directory is created and valid
        assert Path(temp_dir).exists()
        assert Path(temp_dir).is_dir()

    finally:
        client.close()
        cluster.close()


def test_pbs_environment_detection(isolated_env, mock_psutil):
    """Test PBS environment variable detection."""
    # Set PBS environment variables
    isolated_env["PBS_NCPUS"] = "16"
    isolated_env["PBS_MEM"] = "64gb"
    isolated_env["PBS_JOBFS"] = "/local/pbs/jobfs"

    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu", dashboard=False, max_workers=16
    )

    try:
        # Should have detected 16 cores from PBS
        assert len(cluster.workers) == 16

        # Temp directory should be under PBS_JOBFS
        assert temp_dir.startswith("/local/pbs/jobfs")

        # Environment should be updated
        assert os.environ.get("TMPDIR") == temp_dir
        assert os.environ.get("DASK_TEMPORARY_DIRECTORY") == temp_dir

    finally:
        client.close()
        cluster.close()


def test_slurm_environment_detection(isolated_env, mock_psutil):
    """Test SLURM environment variable detection."""
    # Set SLURM environment variables
    isolated_env["SLURM_CPUS_ON_NODE"] = "24"
    isolated_env["SLURM_MEM_PER_NODE"] = "98304"  # 96 GB in MB

    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu", dashboard=False, max_workers=24
    )

    try:
        # Should have detected 24 cores from SLURM
        assert len(cluster.workers) == 24

    finally:
        client.close()
        cluster.close()


def test_psutil_fallback(isolated_env, mock_psutil):
    """Test fallback to psutil when no HPC environment variables are set."""
    # Make sure no HPC env vars are set
    for key in [
        "PBS_NCPUS",
        "NCPUS",
        "PBS_MEM",
        "PBS_VMEM",
        "SLURM_CPUS_ON_NODE",
        "SLURM_MEM_PER_NODE",
    ]:
        isolated_env.pop(key, None)

    # Configure psutil mocks
    mock_psutil["cpu_count"].return_value = 4
    mock_psutil["virtual_memory"].return_value.total = 8 * (1024**3)  # 8 GiB

    client, cluster, temp_dir = setup_dask_client(workload_type="cpu", dashboard=False)

    try:
        # Should fall back to psutil values
        assert len(cluster.workers) == 4

    finally:
        client.close()
        cluster.close()


def test_memory_reservation_error(isolated_env, mock_psutil):
    """Test that excessive memory reservation raises ValueError."""
    mock_psutil["virtual_memory"].return_value.total = 4 * (1024**3)  # 4 GiB

    with pytest.raises(ValueError, match="Not enough memory after reserving"):
        setup_dask_client(
            workload_type="cpu",
            reserve_mem_gb=10.0,  # More than available
            dashboard=False,
        )


def test_temp_directory_routing(isolated_env, mock_psutil, temp_dir):
    """Test that temp directories are routed correctly."""
    # Test PBS_JOBFS priority
    pbs_jobfs = temp_dir + "/pbs_jobfs"
    os.makedirs(pbs_jobfs, exist_ok=True)
    isolated_env["PBS_JOBFS"] = pbs_jobfs
    isolated_env["TMPDIR"] = temp_dir + "/tmpdir"  # Should be ignored

    client, cluster, dask_temp_dir = setup_dask_client(
        workload_type="cpu", max_workers=1, dashboard=False
    )

    try:
        # Should use PBS_JOBFS
        assert dask_temp_dir.startswith(pbs_jobfs)
        assert os.environ["TMPDIR"] == dask_temp_dir
        assert os.environ["DASK_TEMPORARY_DIRECTORY"] == dask_temp_dir

    finally:
        client.close()
        cluster.close()


def test_temp_directory_tmpdir_fallback(isolated_env, mock_psutil, temp_dir):
    """Test fallback to TMPDIR when PBS_JOBFS not available."""
    # Remove PBS_JOBFS, set TMPDIR
    isolated_env.pop("PBS_JOBFS", None)
    tmpdir_path = temp_dir + "/custom_tmp"
    os.makedirs(tmpdir_path, exist_ok=True)
    isolated_env["TMPDIR"] = tmpdir_path

    client, cluster, dask_temp_dir = setup_dask_client(
        workload_type="cpu", max_workers=1, dashboard=False
    )

    try:
        # Should use custom TMPDIR
        assert dask_temp_dir.startswith(tmpdir_path)

    finally:
        client.close()
        cluster.close()


def test_dashboard_disabled(isolated_env, mock_psutil, capsys):
    """Test that dashboard=False suppresses dashboard output."""
    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu", max_workers=1, dashboard=False
    )

    try:
        captured = capsys.readouterr()
        # Should not contain dashboard links or SSH tunnel instructions
        assert "dashboard" not in captured.out.lower()
        assert "tunnel" not in captured.out.lower()
        assert "ssh" not in captured.out.lower()

        # Should still contain basic setup info
        assert "setup_dask_client" in captured.out

    finally:
        client.close()
        cluster.close()


def test_dashboard_enabled(isolated_env, mock_psutil, capsys):
    """Test that dashboard=True produces dashboard output."""
    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu", max_workers=1, dashboard=True
    )

    try:
        captured = capsys.readouterr()
        # Should contain dashboard and tunnel information
        assert "dashboard" in captured.out.lower()
        assert "tunnel" in captured.out.lower() or "ssh" in captured.out.lower()

    finally:
        client.close()
        cluster.close()


def test_invalid_workload_type(isolated_env, mock_psutil):
    """Test that invalid workload_type raises AssertionError."""
    with pytest.raises(AssertionError, match="Invalid workload_type"):
        setup_dask_client(workload_type="invalid", dashboard=False)


def test_memory_limits_per_worker(isolated_env, mock_psutil):
    """Test that memory limits are correctly set per worker."""
    # Set up 8 GB total memory
    mock_psutil["virtual_memory"].return_value.total = 8 * (1024**3)

    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu",
        max_workers=2,
        reserve_mem_gb=2.0,  # Reserve 2 GB
        dashboard=False,
    )

    try:
        # Should have 2 workers
        assert len(cluster.workers) == 2

        # Each worker should get roughly (8 - 2) / 2 = 3 GB
        for worker in cluster.workers.values():
            # Memory limit should be around 3 GB (allow some variance)
            memory_limit_gb = worker.memory_limit / (1024**3)
            assert 2.5 <= memory_limit_gb <= 3.5

    finally:
        client.close()
        cluster.close()


def test_adaptive_scaling(isolated_env, mock_psutil):
    """Test adaptive scaling configuration."""
    mock_psutil["cpu_count"].return_value = 8

    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu", adaptive=True, min_workers=2, max_workers=8, dashboard=False
    )

    try:
        # Should have adaptive scaling enabled
        assert hasattr(cluster, "adaptive")
        # Note: Testing actual adaptive behavior would require more complex setup

    finally:
        client.close()
        cluster.close()


def test_return_types(isolated_env, mock_psutil):
    """Test that function returns correct types."""
    from dask.distributed import Client, LocalCluster

    result = setup_dask_client(workload_type="cpu", max_workers=1, dashboard=False)

    client, cluster, temp_dir = result

    try:
        # Check return types
        assert isinstance(client, Client)
        assert isinstance(cluster, LocalCluster)
        assert isinstance(temp_dir, str)

        # Check temp_dir is a valid path
        assert Path(temp_dir).exists()

    finally:
        client.close()
        cluster.close()


def test_dask_configuration_applied(isolated_env, mock_psutil):
    """Test that Dask configuration is properly applied."""
    import dask

    client, cluster, temp_dir = setup_dask_client(
        workload_type="cpu", max_workers=1, dashboard=False
    )

    try:
        # Check that key Dask config values were set
        config = dask.config.config

        # Memory thresholds
        assert config.get("distributed.worker.memory.target") == 0.75
        assert config.get("distributed.worker.memory.spill") == 0.85
        assert config.get("distributed.worker.memory.pause") == 0.92
        assert config.get("distributed.worker.memory.terminate") == 0.98

        # Temp directory settings
        assert config.get("temporary-directory") == temp_dir
        assert config.get("distributed.worker.local-directory") == temp_dir

    finally:
        client.close()
        cluster.close()


# Integration-style test to ensure everything works together
def test_integration_smoke_test():
    """Smoke test that verifies basic functionality without mocking."""
    import tempfile

    # Use a temporary directory for this test
    with tempfile.TemporaryDirectory() as tmpdir:
        os.environ["TMPDIR"] = tmpdir

        try:
            client, cluster, dask_temp = setup_dask_client(
                workload_type="cpu", max_workers=1, dashboard=False
            )

            # Test that we can actually submit a simple computation
            import dask.array as da

            x = da.ones((100, 100), chunks=(50, 50))
            result = x.sum().compute()

            # Should get expected result
            assert result == 10000.0

            # Temp directory should exist and be under our tmpdir
            assert Path(dask_temp).exists()
            assert dask_temp.startswith(tmpdir)

        finally:
            if "client" in locals():
                client.close()
            if "cluster" in locals():
                cluster.close()
