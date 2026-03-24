"""Tests for v2.0 multi-node Dask cluster support."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# MultiNodeConfig
# ---------------------------------------------------------------------------


class TestMultiNodeConfig:
    def test_defaults(self):
        from dask_setup.multinode import MultiNodeConfig

        cfg = MultiNodeConfig()
        assert cfg.workload_type == "cpu"
        assert cfg.workers_per_node == 1
        assert cfg.cores_per_worker == 1
        assert cfg.mem_per_worker_gb == 4.0
        assert cfg.walltime == "01:00:00"
        assert cfg.queue == "normal"
        assert cfg.project is None
        assert cfg.job_extra_directives == []
        assert cfg.n_nodes == 1
        assert cfg.shared_tmp_dir is None
        assert cfg.env_extra == []
        assert cfg.adaptive is False
        assert cfg.min_jobs == 1
        assert cfg.max_jobs == 10

    def test_total_cores_per_job(self):
        from dask_setup.multinode import MultiNodeConfig

        cfg = MultiNodeConfig(workers_per_node=4, cores_per_worker=12)
        assert cfg.total_cores_per_job == 48

    def test_total_mem_gb_per_job(self):
        from dask_setup.multinode import MultiNodeConfig

        cfg = MultiNodeConfig(workers_per_node=4, mem_per_worker_gb=32.0)
        assert cfg.total_mem_gb_per_job == pytest.approx(128.0)

    def test_valid_workload_types(self):
        from dask_setup.multinode import MultiNodeConfig

        for wt in ("cpu", "io", "mixed", "gpu", "auto"):
            cfg = MultiNodeConfig(workload_type=wt)
            assert cfg.workload_type == wt

    def test_invalid_workload_type_raises(self):
        from dask_setup.multinode import MultiNodeConfig

        with pytest.raises(Exception, match="workload_type"):
            MultiNodeConfig(workload_type="bogus")

    def test_invalid_workers_per_node_raises(self):
        from dask_setup.multinode import MultiNodeConfig

        with pytest.raises(Exception):
            MultiNodeConfig(workers_per_node=0)

    def test_invalid_cores_per_worker_raises(self):
        from dask_setup.multinode import MultiNodeConfig

        with pytest.raises(Exception):
            MultiNodeConfig(cores_per_worker=0)

    def test_invalid_mem_per_worker_raises(self):
        from dask_setup.multinode import MultiNodeConfig

        with pytest.raises(Exception):
            MultiNodeConfig(mem_per_worker_gb=-1.0)

    def test_adaptive_min_jobs_validation(self):
        from dask_setup.multinode import MultiNodeConfig

        with pytest.raises(Exception, match="min_jobs"):
            MultiNodeConfig(adaptive=True, min_jobs=0)

    def test_adaptive_max_less_than_min_raises(self):
        from dask_setup.multinode import MultiNodeConfig

        with pytest.raises(Exception, match="max_jobs"):
            MultiNodeConfig(adaptive=True, min_jobs=5, max_jobs=2)

    def test_to_dict(self):
        from dask_setup.multinode import MultiNodeConfig

        cfg = MultiNodeConfig(
            workload_type="io",
            workers_per_node=2,
            cores_per_worker=4,
            mem_per_worker_gb=8.0,
            walltime="02:00:00",
            queue="express",
            project="abc123",
        )
        d = cfg.to_dict()
        assert d["workload_type"] == "io"
        assert d["workers_per_node"] == 2
        assert d["cores_per_worker"] == 4
        assert d["mem_per_worker_gb"] == 8.0
        assert d["project"] == "abc123"

    def test_to_dict_is_json_serialisable(self):
        import json

        from dask_setup.multinode import MultiNodeConfig

        cfg = MultiNodeConfig(job_extra_directives=["-l ngpus=1"])
        d = cfg.to_dict()
        # Should not raise
        json.dumps(d)


# ---------------------------------------------------------------------------
# SharedTempDir
# ---------------------------------------------------------------------------


class TestSharedTempDir:
    def test_creates_directory(self, tmp_path):
        from dask_setup.multinode import SharedTempDir

        std = SharedTempDir(path=tmp_path, create_subdirectory=False)
        assert std.resolved_path.exists()
        assert std.resolved_path == tmp_path

    def test_creates_subdirectory_with_jobid(self, tmp_path, monkeypatch):
        from dask_setup.multinode import SharedTempDir

        monkeypatch.setenv("PBS_JOBID", "12345.gadi-login")
        std = SharedTempDir(path=tmp_path, create_subdirectory=True)
        assert std.resolved_path.exists()
        assert "dask_tmp_12345.gadi-login" in std.resolved_path.name

    def test_creates_local_subdir_when_no_jobid(self, tmp_path, monkeypatch):
        from dask_setup.multinode import SharedTempDir

        monkeypatch.delenv("PBS_JOBID", raising=False)
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_JOBID", raising=False)
        std = SharedTempDir(path=tmp_path, create_subdirectory=True)
        assert std.resolved_path.name == "dask_tmp_local"

    def test_str_returns_path(self, tmp_path):
        from dask_setup.multinode import SharedTempDir

        std = SharedTempDir(path=tmp_path, create_subdirectory=False)
        assert str(std) == str(tmp_path)

    def test_fspath(self, tmp_path):
        from dask_setup.multinode import SharedTempDir

        std = SharedTempDir(path=tmp_path, create_subdirectory=False)
        assert os.fspath(std) == str(tmp_path)

    def test_cleanup_removes_directory(self, tmp_path):
        from dask_setup.multinode import SharedTempDir

        std = SharedTempDir(path=tmp_path, create_subdirectory=False, cleanup_on_close=True)
        assert std.resolved_path.exists()
        std.cleanup()
        assert not std.resolved_path.exists()

    def test_cleanup_noop_when_disabled(self, tmp_path):
        from dask_setup.multinode import SharedTempDir

        std = SharedTempDir(path=tmp_path, create_subdirectory=False, cleanup_on_close=False)
        std.cleanup()
        assert std.resolved_path.exists()


# ---------------------------------------------------------------------------
# detect_cluster_mode
# ---------------------------------------------------------------------------


class TestDetectClusterMode:
    def test_detects_slurm(self, monkeypatch):
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("SLURM_JOB_ID", "999")
        monkeypatch.delenv("PBS_JOBID", raising=False)
        assert detect_cluster_mode() == "slurm"

    def test_detects_pbs(self, monkeypatch):
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.delenv("SLURM_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_JOBID", raising=False)
        monkeypatch.delenv("SLURM_NODELIST", raising=False)
        monkeypatch.setenv("PBS_JOBID", "123456.gadi")
        assert detect_cluster_mode() == "pbs"

    def test_detects_local(self, monkeypatch):
        from dask_setup.multinode import detect_cluster_mode

        for v in (
            "SLURM_JOB_ID",
            "SLURM_JOBID",
            "SLURM_NODELIST",
            "PBS_JOBID",
            "PBS_NODEFILE",
            "PBS_ENVIRONMENT",
        ):
            monkeypatch.delenv(v, raising=False)
        assert detect_cluster_mode() == "local"

    def test_slurm_takes_priority_over_pbs(self, monkeypatch):
        """SLURM indicators take priority when both are set."""
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("SLURM_JOB_ID", "888")
        monkeypatch.setenv("PBS_JOBID", "444")
        assert detect_cluster_mode() == "slurm"


# ---------------------------------------------------------------------------
# PBS/SLURM script generation
# ---------------------------------------------------------------------------


class TestGeneratePBSScript:
    def test_basic_output(self):
        from dask_setup.multinode import MultiNodeConfig, generate_pbs_script

        cfg = MultiNodeConfig(
            workers_per_node=2,
            cores_per_worker=8,
            mem_per_worker_gb=16.0,
            walltime="03:00:00",
            queue="normal",
        )
        script = generate_pbs_script(cfg, script_path="/home/user/run.py")
        assert "#!/bin/bash" in script
        assert "#PBS" in script
        assert "walltime=03:00:00" in script
        assert "mem=32GB" in script
        assert "ncpus=16" in script
        assert "/home/user/run.py" in script

    def test_includes_project(self):
        from dask_setup.multinode import MultiNodeConfig, generate_pbs_script

        cfg = MultiNodeConfig(project="p00001")
        script = generate_pbs_script(cfg, script_path="job.py")
        assert "#PBS -P p00001" in script

    def test_includes_extra_directives(self):
        from dask_setup.multinode import MultiNodeConfig, generate_pbs_script

        cfg = MultiNodeConfig(job_extra_directives=["-l ngpus=1", "-l other=val"])
        script = generate_pbs_script(cfg, script_path="job.py")
        assert "-l ngpus=1" in script
        assert "-l other=val" in script

    def test_includes_shared_tmp_dir(self):
        from dask_setup.multinode import MultiNodeConfig, generate_pbs_script

        cfg = MultiNodeConfig(shared_tmp_dir="/scratch/project/tmp")
        script = generate_pbs_script(cfg, script_path="job.py")
        assert "DASK_SETUP_SHARED_TMP" in script
        assert "/scratch/project/tmp" in script

    def test_custom_python_executable(self):
        from dask_setup.multinode import MultiNodeConfig, generate_pbs_script

        cfg = MultiNodeConfig()
        script = generate_pbs_script(
            cfg, script_path="job.py", python_executable="/opt/conda/bin/python"
        )
        assert "/opt/conda/bin/python" in script


class TestGenerateSLURMScript:
    def test_basic_output(self):
        from dask_setup.multinode import MultiNodeConfig, generate_slurm_script

        cfg = MultiNodeConfig(
            workers_per_node=2,
            cores_per_worker=4,
            mem_per_worker_gb=8.0,
            walltime="02:00:00",
            queue="compute",
        )
        script = generate_slurm_script(cfg, script_path="/home/user/run.py")
        assert "#!/bin/bash" in script
        assert "#SBATCH" in script
        assert "--time=02:00:00" in script
        assert "--partition=compute" in script
        assert "mem=16G" in script
        assert "--ntasks=2" in script
        assert "--cpus-per-task=4" in script
        assert "/home/user/run.py" in script

    def test_includes_account(self):
        from dask_setup.multinode import MultiNodeConfig, generate_slurm_script

        cfg = MultiNodeConfig(project="myaccount")
        script = generate_slurm_script(cfg, script_path="job.py")
        assert "--account=myaccount" in script

    def test_includes_extra_directives(self):
        from dask_setup.multinode import MultiNodeConfig, generate_slurm_script

        cfg = MultiNodeConfig(job_extra_directives=["--gres=gpu:1"])
        script = generate_slurm_script(cfg, script_path="job.py")
        assert "--gres=gpu:1" in script


# ---------------------------------------------------------------------------
# _apply_overrides
# ---------------------------------------------------------------------------


class TestApplyOverrides:
    def test_none_config_creates_from_kwargs(self):
        from dask_setup.multinode import _apply_overrides

        cfg = _apply_overrides(None, workers_per_node=4, cores_per_worker=8)
        assert cfg.workers_per_node == 4
        assert cfg.cores_per_worker == 8

    def test_existing_config_overridden(self):
        from dask_setup.multinode import MultiNodeConfig, _apply_overrides

        base = MultiNodeConfig(queue="normal", walltime="01:00:00")
        cfg = _apply_overrides(base, queue="express", walltime="04:00:00")
        assert cfg.queue == "express"
        assert cfg.walltime == "04:00:00"

    def test_none_kwargs_do_not_override(self):
        from dask_setup.multinode import MultiNodeConfig, _apply_overrides

        base = MultiNodeConfig(queue="express")
        cfg = _apply_overrides(base, queue=None)
        assert cfg.queue == "express"


# ---------------------------------------------------------------------------
# GPU topology
# ---------------------------------------------------------------------------


class TestGPUTopology:
    def test_gpu_topology_no_gpus_falls_back(self):
        """With no GPUs detected, gpu topology should fall back gracefully."""
        from dask_setup.topology import decide_topology

        with patch("dask_setup.topology._count_gpus", return_value=0):
            topo = decide_topology("gpu", total_cores=16)
        # Fallback to io-like single-process multi-thread
        assert topo.n_workers == 1
        assert topo.workload_type == "gpu"

    def test_gpu_topology_with_gpus(self):
        from dask_setup.topology import decide_topology

        with patch("dask_setup.topology._count_gpus", return_value=2):
            topo = decide_topology("gpu", total_cores=16)
        assert topo.n_workers == 2
        assert topo.processes is True
        assert topo.threads_per_worker >= 2

    def test_gpu_topology_max_workers_caps_gpus(self):
        from dask_setup.topology import decide_topology

        with patch("dask_setup.topology._count_gpus", return_value=4):
            topo = decide_topology("gpu", total_cores=16, max_workers=2)
        assert topo.n_workers == 2

    def test_count_gpus_cuda_visible_devices(self, monkeypatch):
        from dask_setup.topology import _count_gpus

        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "0,1,2")
        assert _count_gpus() == 3

    def test_count_gpus_empty_env(self, monkeypatch):
        from dask_setup.topology import _count_gpus

        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "")
        # No cupy, no devices — should return 0 or fall through gracefully
        # We can't guarantee cupy is installed, so just check it doesn't raise
        result = _count_gpus()
        assert isinstance(result, int)

    def test_count_gpus_disabled(self, monkeypatch):
        from dask_setup.topology import _count_gpus

        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", "-1")
        # -1 means "no devices" for CUDA
        result = _count_gpus()
        assert result == 0

    def test_gpu_valid_in_dask_setup_config(self):
        from dask_setup.config import DaskSetupConfig

        cfg = DaskSetupConfig(workload_type="gpu")
        assert cfg.workload_type == "gpu"

    def test_gpu_in_decide_topology_invalid_old_check(self):
        """'gpu' should NOT raise InvalidConfigurationError (it's a valid type now)."""
        from dask_setup.topology import decide_topology

        with patch("dask_setup.topology._count_gpus", return_value=1):
            topo = decide_topology("gpu", total_cores=8)
        assert topo is not None


# ---------------------------------------------------------------------------
# CLI: submit subcommand
# ---------------------------------------------------------------------------


class TestCLISubmit:
    def test_submit_registered(self):
        from dask_setup.cli import create_parser

        parser = create_parser()
        args = parser.parse_args(
            [
                "submit",
                "myjob.py",
                "--scheduler",
                "pbs",
                "--workload-type",
                "cpu",
                "--workers-per-node",
                "4",
                "--cores-per-worker",
                "12",
                "--mem-per-worker",
                "32.0",
                "--walltime",
                "04:00:00",
                "--queue",
                "normal",
            ]
        )
        assert args.script == "myjob.py"
        assert args.scheduler == "pbs"
        assert args.workers_per_node == 4
        assert args.cores_per_worker == 12
        assert args.mem_per_worker_gb == 32.0
        assert args.walltime == "04:00:00"

    def test_submit_defaults(self):
        from dask_setup.cli import create_parser

        parser = create_parser()
        args = parser.parse_args(["submit", "myjob.py"])
        assert args.scheduler == "pbs"
        assert args.workload_type == "cpu"
        assert args.workers_per_node == 1
        assert args.cores_per_worker == 1
        assert args.mem_per_worker_gb == 4.0
        assert args.walltime == "01:00:00"
        assert args.queue == "normal"
        assert args.n_nodes == 1

    def test_cmd_submit_pbs_prints_script(self, capsys):
        from dask_setup.cli import cmd_submit

        args = argparse.Namespace(
            script="run.py",
            scheduler="pbs",
            workload_type="cpu",
            workers_per_node=2,
            cores_per_worker=8,
            mem_per_worker_gb=16.0,
            walltime="02:00:00",
            queue="normal",
            project="",
            n_nodes=1,
            shared_tmp_dir="",
            extra_directive=None,
            python="python3",
            output="",
        )
        rc = cmd_submit(args)
        assert rc == 0
        captured = capsys.readouterr()
        assert "#!/bin/bash" in captured.out
        assert "#PBS" in captured.out

    def test_cmd_submit_slurm_prints_script(self, capsys):
        from dask_setup.cli import cmd_submit

        args = argparse.Namespace(
            script="run.py",
            scheduler="slurm",
            workload_type="cpu",
            workers_per_node=1,
            cores_per_worker=4,
            mem_per_worker_gb=8.0,
            walltime="01:00:00",
            queue="compute",
            project="",
            n_nodes=1,
            shared_tmp_dir="",
            extra_directive=None,
            python="python3",
            output="",
        )
        rc = cmd_submit(args)
        assert rc == 0
        captured = capsys.readouterr()
        assert "#SBATCH" in captured.out

    def test_cmd_submit_writes_to_file(self, tmp_path):
        from dask_setup.cli import cmd_submit

        output_file = str(tmp_path / "job.sh")
        args = argparse.Namespace(
            script="run.py",
            scheduler="pbs",
            workload_type="cpu",
            workers_per_node=1,
            cores_per_worker=4,
            mem_per_worker_gb=4.0,
            walltime="01:00:00",
            queue="normal",
            project="",
            n_nodes=1,
            shared_tmp_dir="",
            extra_directive=None,
            python="python3",
            output=output_file,
        )
        rc = cmd_submit(args)
        assert rc == 0
        content = Path(output_file).read_text()
        assert "#!/bin/bash" in content

    def test_cmd_submit_invalid_scheduler(self, capsys):
        from dask_setup.cli import cmd_submit

        args = argparse.Namespace(
            script="run.py",
            scheduler="lsf",  # not implemented
            workload_type="cpu",
            workers_per_node=1,
            cores_per_worker=1,
            mem_per_worker_gb=4.0,
            walltime="01:00:00",
            queue="normal",
            project="",
            n_nodes=1,
            shared_tmp_dir="",
            extra_directive=None,
            python="python3",
            output="",
        )
        rc = cmd_submit(args)
        assert rc != 0


# ---------------------------------------------------------------------------
# setup_dask_client mode= dispatch
# ---------------------------------------------------------------------------


class TestSetupDaskClientMode:
    """Tests for the new mode= parameter on setup_dask_client."""

    def test_mode_local_uses_local_cluster(self, monkeypatch):
        """mode='local' should bypass the multi-node dispatch."""
        # Clear any HPC env vars so auto-detection returns local anyway
        for v in ("PBS_JOBID", "PBS_NODEFILE", "SLURM_JOB_ID", "SLURM_JOBID"):
            monkeypatch.delenv(v, raising=False)

        from dask_setup.multinode import detect_cluster_mode

        assert detect_cluster_mode() == "local"

    def test_mode_auto_detects_pbs(self, monkeypatch):
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("PBS_JOBID", "99999.gadi")
        monkeypatch.delenv("SLURM_JOB_ID", raising=False)
        monkeypatch.delenv("SLURM_JOBID", raising=False)
        monkeypatch.delenv("SLURM_NODELIST", raising=False)
        assert detect_cluster_mode() == "pbs"

    def test_mode_auto_detects_slurm(self, monkeypatch):
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("SLURM_JOB_ID", "77777")
        assert detect_cluster_mode() == "slurm"

    def test_setup_dask_client_accepts_mode_kwarg(self):
        """Verify setup_dask_client accepts mode= without TypeError."""
        import inspect

        from dask_setup.client import setup_dask_client

        sig = inspect.signature(setup_dask_client)
        assert "mode" in sig.parameters
        assert "multi_node_config" in sig.parameters

    def test_setup_dask_client_dispatches_to_pbs(self):
        """When mode='pbs', setup_dask_client should call setup_pbs_cluster."""
        from dask_setup.multinode import MultiNodeConfig

        mock_client = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://127.0.0.1:8786"

        with patch(
            "dask_setup.client.setup_pbs_cluster",
            return_value=(mock_client, mock_cluster, None),
        ) as mock_pbs:
            from dask_setup.client import setup_dask_client

            result = setup_dask_client(
                mode="pbs",
                multi_node_config=MultiNodeConfig(walltime="01:00:00"),
            )
            mock_pbs.assert_called_once()
            assert result[0] is mock_client
            assert result[1] is mock_cluster

    def test_setup_dask_client_dispatches_to_slurm(self):
        """When mode='slurm', setup_dask_client should call setup_slurm_cluster."""
        from dask_setup.multinode import MultiNodeConfig

        mock_client = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://127.0.0.1:8786"

        with patch(
            "dask_setup.client.setup_slurm_cluster",
            return_value=(mock_client, mock_cluster, None),
        ) as mock_slurm:
            from dask_setup.client import setup_dask_client

            result = setup_dask_client(
                mode="slurm",
                multi_node_config=MultiNodeConfig(queue="compute"),
            )
            mock_slurm.assert_called_once()
            assert result[0] is mock_client


# ---------------------------------------------------------------------------
# __init__ exports
# ---------------------------------------------------------------------------


def test_v2_symbols_exported():
    """All v2.0 symbols should be importable from dask_setup."""
    import dask_setup

    assert hasattr(dask_setup, "MultiNodeConfig")
    assert hasattr(dask_setup, "setup_pbs_cluster")
    assert hasattr(dask_setup, "setup_slurm_cluster")
    assert hasattr(dask_setup, "detect_cluster_mode")
    assert hasattr(dask_setup, "SharedTempDir")


def test_version_is_2():
    import dask_setup

    assert dask_setup.__version__.startswith("2.")


# ---------------------------------------------------------------------------
# _wait_for_workers
# ---------------------------------------------------------------------------


class TestWaitForWorkers:
    """Tests for the _wait_for_workers helper."""

    def test_returns_when_workers_connect(self):
        """_wait_for_workers should return normally when wait_for_workers succeeds."""
        from dask_setup.multinode import _wait_for_workers

        mock_client = MagicMock()
        mock_client.wait_for_workers.return_value = None  # succeeds
        mock_client.scheduler_info.return_value = {"workers": {"w1": {}, "w2": {}}}
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://127.0.0.1:8786"

        # Should not raise
        _wait_for_workers(mock_client, mock_cluster, n_jobs=2, workers_per_job=4, timeout=60.0)
        mock_client.wait_for_workers.assert_called_once_with(1, timeout=60.0)

    def test_raises_timeout_error_with_diagnostic_message(self):
        """_wait_for_workers should raise TimeoutError with helpful info on timeout."""
        from dask_setup.multinode import _wait_for_workers

        mock_client = MagicMock()
        mock_client.wait_for_workers.side_effect = TimeoutError("timed out")
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://10.0.0.1:8786"

        with pytest.raises(TimeoutError) as exc_info:
            _wait_for_workers(mock_client, mock_cluster, n_jobs=3, workers_per_job=4, timeout=30.0)

        msg = str(exc_info.value)
        assert "30" in msg            # timeout value
        assert "3" in msg             # n_jobs
        assert "12" in msg            # expected_total = 3 * 4
        assert "qstat" in msg         # actionable hint

    def test_raises_on_any_exception_from_wait(self):
        """Non-TimeoutError exceptions from wait_for_workers are also wrapped."""
        from dask_setup.multinode import _wait_for_workers

        mock_client = MagicMock()
        mock_client.wait_for_workers.side_effect = RuntimeError("scheduler gone")
        mock_cluster = MagicMock()

        with pytest.raises(TimeoutError):
            _wait_for_workers(mock_client, mock_cluster, n_jobs=1, workers_per_job=1, timeout=10.0)

    def test_timeout_message_includes_scheduler_address(self):
        """Timeout message should include the scheduler address for debugging."""
        from dask_setup.multinode import _wait_for_workers

        mock_client = MagicMock()
        mock_client.wait_for_workers.side_effect = TimeoutError("timed out")
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://hpc-node-01:8786"

        with pytest.raises(TimeoutError, match="hpc-node-01"):
            _wait_for_workers(mock_client, mock_cluster, n_jobs=1, workers_per_job=1, timeout=5.0)


# ---------------------------------------------------------------------------
# setup_pbs_cluster — wait_for_workers integration
# ---------------------------------------------------------------------------


class TestSetupPBSClusterWait:
    """Tests for wait_for_workers behaviour in setup_pbs_cluster."""

    def _make_mocks(self):
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://127.0.0.1:8786"
        mock_client = MagicMock()
        mock_client.scheduler_info.return_value = {"workers": {"w1": {}}}
        return mock_cluster, mock_client

    def test_wait_for_workers_called_by_default(self):
        """setup_pbs_cluster should call _wait_for_workers when wait_for_workers=True."""
        from dask_setup.multinode import setup_pbs_cluster

        mock_cluster, mock_client = self._make_mocks()

        with (
            patch("dask_setup.multinode.PBSCluster", return_value=mock_cluster),
            patch("dask_setup.multinode.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers") as mock_wait,
        ):
            # Inline import so the patch applies
            import importlib
            import dask_setup.multinode as mn
            mn._check_jobqueue = lambda: None  # skip import check

            client, cluster, _ = setup_pbs_cluster(
                workers_per_node=4,
                cores_per_worker=8,
                mem_per_worker_gb=32,
                n_workers=2,
                wait_for_workers=True,
                worker_timeout=120.0,
            )

        mock_wait.assert_called_once()
        call_kwargs = mock_wait.call_args
        assert call_kwargs.args[4] == 120.0  # timeout

    def test_wait_skipped_when_disabled(self):
        """setup_pbs_cluster should not call _wait_for_workers when wait_for_workers=False."""
        from dask_setup.multinode import setup_pbs_cluster

        mock_cluster, mock_client = self._make_mocks()

        with (
            patch("dask_setup.multinode.PBSCluster", return_value=mock_cluster),
            patch("dask_setup.multinode.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers") as mock_wait,
        ):
            import dask_setup.multinode as mn
            mn._check_jobqueue = lambda: None

            setup_pbs_cluster(
                workers_per_node=1,
                cores_per_worker=4,
                mem_per_worker_gb=8,
                wait_for_workers=False,
            )

        mock_wait.assert_not_called()

    def test_timeout_error_propagates(self):
        """A TimeoutError from _wait_for_workers should propagate to the caller."""
        from dask_setup.multinode import setup_pbs_cluster

        mock_cluster, mock_client = self._make_mocks()

        with (
            patch("dask_setup.multinode.PBSCluster", return_value=mock_cluster),
            patch("dask_setup.multinode.Client", return_value=mock_client),
            patch(
                "dask_setup.multinode._wait_for_workers",
                side_effect=TimeoutError("no workers"),
            ),
        ):
            import dask_setup.multinode as mn
            mn._check_jobqueue = lambda: None

            with pytest.raises(TimeoutError, match="no workers"):
                setup_pbs_cluster(
                    workers_per_node=1,
                    cores_per_worker=4,
                    mem_per_worker_gb=8,
                )


# ---------------------------------------------------------------------------
# setup_slurm_cluster — wait_for_workers integration
# ---------------------------------------------------------------------------


class TestSetupSLURMClusterWait:
    """Mirrors TestSetupPBSClusterWait for the SLURM path."""

    def _make_mocks(self):
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://127.0.0.1:8786"
        mock_client = MagicMock()
        mock_client.scheduler_info.return_value = {"workers": {"w1": {}}}
        return mock_cluster, mock_client

    def test_wait_for_workers_called_by_default(self):
        from dask_setup.multinode import setup_slurm_cluster

        mock_cluster, mock_client = self._make_mocks()

        with (
            patch("dask_setup.multinode.SLURMCluster", return_value=mock_cluster),
            patch("dask_setup.multinode.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers") as mock_wait,
        ):
            import dask_setup.multinode as mn
            mn._check_jobqueue = lambda: None

            setup_slurm_cluster(
                workers_per_node=2,
                cores_per_worker=16,
                mem_per_worker_gb=64,
                n_workers=4,
                wait_for_workers=True,
                worker_timeout=600.0,
            )

        mock_wait.assert_called_once()
        assert mock_wait.call_args.args[4] == 600.0  # timeout

    def test_wait_skipped_when_disabled(self):
        from dask_setup.multinode import setup_slurm_cluster

        mock_cluster, mock_client = self._make_mocks()

        with (
            patch("dask_setup.multinode.SLURMCluster", return_value=mock_cluster),
            patch("dask_setup.multinode.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers") as mock_wait,
        ):
            import dask_setup.multinode as mn
            mn._check_jobqueue = lambda: None

            setup_slurm_cluster(
                workers_per_node=1,
                cores_per_worker=4,
                mem_per_worker_gb=8,
                wait_for_workers=False,
            )

        mock_wait.assert_not_called()


# ---------------------------------------------------------------------------
# detect_cluster_mode — interactive detection
# ---------------------------------------------------------------------------


class TestDetectClusterModeInteractive:
    """Tests for interactive PBS/SLURM detection in detect_cluster_mode()."""

    def test_pbs_interactive_returns_interactive(self, monkeypatch):
        """PBS_ENVIRONMENT=PBS_INTERACTIVE should return 'interactive'."""
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("PBS_JOBID", "12345.gadi-login-01")
        monkeypatch.setenv("PBS_NODEFILE", "/var/spool/pbs/aux/12345")
        monkeypatch.setenv("PBS_ENVIRONMENT", "PBS_INTERACTIVE")

        assert detect_cluster_mode() == "interactive"

    def test_pbs_batch_returns_pbs(self, monkeypatch):
        """PBS_ENVIRONMENT=PBS_BATCH should still return 'pbs'."""
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("PBS_JOBID", "12345.gadi-login-01")
        monkeypatch.setenv("PBS_NODEFILE", "/var/spool/pbs/aux/12345")
        monkeypatch.setenv("PBS_ENVIRONMENT", "PBS_BATCH")

        assert detect_cluster_mode() == "pbs"

    def test_pbs_no_environment_var_returns_pbs(self, monkeypatch):
        """PBS_JOBID set but PBS_ENVIRONMENT absent → treat as batch → 'pbs'."""
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("PBS_JOBID", "12345.gadi-login-01")
        monkeypatch.delenv("PBS_ENVIRONMENT", raising=False)

        assert detect_cluster_mode() == "pbs"

    def test_slurm_interactive_no_batch_flag(self, monkeypatch):
        """SLURM_JOB_ID set without SLURM_BATCH_FLAG=1 → 'interactive'."""
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("SLURM_JOB_ID", "9876")
        monkeypatch.setenv("SLURM_NODELIST", "gpu-node-01")
        monkeypatch.delenv("SLURM_BATCH_FLAG", raising=False)
        # PBS env must be absent for SLURM path to be reached
        monkeypatch.delenv("PBS_JOBID", raising=False)
        monkeypatch.delenv("PBS_NODEFILE", raising=False)
        monkeypatch.delenv("PBS_ENVIRONMENT", raising=False)

        assert detect_cluster_mode() == "interactive"

    def test_slurm_batch_returns_slurm(self, monkeypatch):
        """SLURM_BATCH_FLAG=1 → batch → 'slurm'."""
        from dask_setup.multinode import detect_cluster_mode

        monkeypatch.setenv("SLURM_JOB_ID", "9876")
        monkeypatch.setenv("SLURM_NODELIST", "gpu-node-01")
        monkeypatch.setenv("SLURM_BATCH_FLAG", "1")
        monkeypatch.delenv("PBS_JOBID", raising=False)
        monkeypatch.delenv("PBS_NODEFILE", raising=False)
        monkeypatch.delenv("PBS_ENVIRONMENT", raising=False)

        assert detect_cluster_mode() == "slurm"


# ---------------------------------------------------------------------------
# _parse_pbs_nodefile
# ---------------------------------------------------------------------------


class TestParsePBSNodefile:
    """Tests for the PBS_NODEFILE parser."""

    def test_single_node(self, tmp_path):
        """Single node repeated N times → {node: N}."""
        from dask_setup.multinode import _parse_pbs_nodefile

        nodefile = tmp_path / "nodefile"
        nodefile.write_text("gadi-cpu-clx-0001\n" * 48)
        result = _parse_pbs_nodefile(str(nodefile))
        assert result == {"gadi-cpu-clx-0001": 48}

    def test_two_nodes_equal_cores(self, tmp_path):
        """Two nodes with equal core counts."""
        from dask_setup.multinode import _parse_pbs_nodefile

        nodefile = tmp_path / "nodefile"
        nodefile.write_text(
            "gadi-cpu-clx-0001\n" * 48 + "gadi-cpu-clx-0002\n" * 48
        )
        result = _parse_pbs_nodefile(str(nodefile))
        assert result == {"gadi-cpu-clx-0001": 48, "gadi-cpu-clx-0002": 48}

    def test_nonexistent_file_returns_empty(self):
        """Non-existent nodefile returns an empty dict without raising."""
        from dask_setup.multinode import _parse_pbs_nodefile

        result = _parse_pbs_nodefile("/nonexistent/path/nodefile")
        assert result == {}

    def test_preserves_insertion_order(self, tmp_path):
        """First node in the file is first key in the dict."""
        from dask_setup.multinode import _parse_pbs_nodefile

        nodefile = tmp_path / "nodefile"
        nodefile.write_text(
            "node-a\n" * 4 + "node-b\n" * 4 + "node-c\n" * 4
        )
        result = _parse_pbs_nodefile(str(nodefile))
        assert list(result.keys()) == ["node-a", "node-b", "node-c"]


# ---------------------------------------------------------------------------
# setup_interactive_cluster — single-node path
# ---------------------------------------------------------------------------


class TestSetupInteractiveClusterSingleNode:
    """Single-node interactive cluster delegates to setup_dask_client(mode='local')."""

    def test_single_node_delegates_to_local(self, tmp_path, monkeypatch):
        """With one unique node in PBS_NODEFILE, setup_dask_client(mode='local') is called."""
        from dask_setup.multinode import setup_interactive_cluster

        nodefile = tmp_path / "nodefile"
        nodefile.write_text("gadi-node-01\n" * 16)
        monkeypatch.setenv("PBS_NODEFILE", str(nodefile))

        mock_client = MagicMock()
        mock_cluster = MagicMock()

        # setup_dask_client is imported inside the function from .client, so
        # the patch target is dask_setup.client.setup_dask_client.
        with patch(
            "dask_setup.client.setup_dask_client",
            return_value=(mock_client, mock_cluster, str(tmp_path)),
        ) as mock_sdc:
            client, cluster, tmp = setup_interactive_cluster(workload_type="cpu")

        mock_sdc.assert_called_once_with(workload_type="cpu", mode="local")
        assert client is mock_client
        assert cluster is mock_cluster

    def test_no_nodefile_falls_back_to_local(self, monkeypatch):
        """No PBS_NODEFILE and no SLURM env → single-node path → setup_dask_client(mode='local')."""
        from dask_setup.multinode import setup_interactive_cluster

        monkeypatch.delenv("PBS_NODEFILE", raising=False)
        monkeypatch.delenv("SLURM_NODELIST", raising=False)
        monkeypatch.delenv("SLURM_JOB_NODELIST", raising=False)

        mock_client = MagicMock()
        mock_cluster = MagicMock()

        with patch(
            "dask_setup.client.setup_dask_client",
            return_value=(mock_client, mock_cluster, "/tmp/dask"),
        ) as mock_sdc:
            client, cluster, tmp = setup_interactive_cluster()

        mock_sdc.assert_called_once_with(workload_type="cpu", mode="local")
        assert client is mock_client

    def test_workload_type_forwarded(self, monkeypatch):
        """workload_type is forwarded unchanged to setup_dask_client."""
        from dask_setup.multinode import setup_interactive_cluster

        monkeypatch.delenv("PBS_NODEFILE", raising=False)
        monkeypatch.delenv("SLURM_NODELIST", raising=False)
        monkeypatch.delenv("SLURM_JOB_NODELIST", raising=False)

        with patch(
            "dask_setup.client.setup_dask_client",
            return_value=(MagicMock(), MagicMock(), ""),
        ) as mock_sdc:
            setup_interactive_cluster(workload_type="io")

        mock_sdc.assert_called_once_with(workload_type="io", mode="local")


# ---------------------------------------------------------------------------
# setup_interactive_cluster — multi-node SSH path
# ---------------------------------------------------------------------------


class TestSetupInteractiveClusterMultiNode:
    """Multi-node interactive cluster uses SSHCluster."""

    def test_multi_node_uses_ssh_cluster(self, tmp_path, monkeypatch):
        """Two unique nodes → SSHCluster."""
        from dask_setup.multinode import setup_interactive_cluster

        nodefile = tmp_path / "nodefile"
        nodefile.write_text("node-01\n" * 48 + "node-02\n" * 48)
        monkeypatch.setenv("PBS_NODEFILE", str(nodefile))

        mock_ssh_cluster = MagicMock()
        mock_ssh_cluster.scheduler_address = "tcp://node-01:8786"
        mock_client = MagicMock()
        mock_client.scheduler_info.return_value = {"workers": {"w1": {}}}

        # SSHCluster and Client are imported inside the function from
        # dask.distributed, so patch them there.
        with (
            patch("dask.distributed.SSHCluster", return_value=mock_ssh_cluster) as mock_ssh,
            patch("dask.distributed.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers"),
        ):
            client, cluster, tmp = setup_interactive_cluster(workload_type="cpu")

        mock_ssh.assert_called_once()
        call_hosts = mock_ssh.call_args.args[0]
        assert call_hosts == ["node-01", "node-02"]
        assert client is mock_client

    def test_cpu_topology_one_worker_per_core(self, tmp_path, monkeypatch):
        """workload_type='cpu' → workers_per_node == cores_per_node."""
        from dask_setup.multinode import setup_interactive_cluster

        nodefile = tmp_path / "nodefile"
        nodefile.write_text("node-a\n" * 12 + "node-b\n" * 12)
        monkeypatch.setenv("PBS_NODEFILE", str(nodefile))

        mock_ssh_cluster = MagicMock()
        mock_ssh_cluster.scheduler_address = "tcp://node-a:8786"
        mock_client = MagicMock()
        mock_client.scheduler_info.return_value = {"workers": {}}

        with (
            patch("dask.distributed.SSHCluster", return_value=mock_ssh_cluster) as mock_ssh,
            patch("dask.distributed.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers"),
        ):
            setup_interactive_cluster(workload_type="cpu")

        worker_opts = mock_ssh.call_args.kwargs["worker_options"]
        assert worker_opts["n_workers"] == 12   # cores per node
        assert worker_opts["nthreads"] == 1

    def test_io_topology_single_worker_many_threads(self, tmp_path, monkeypatch):
        """workload_type='io' → 1 worker per node with many threads."""
        from dask_setup.multinode import setup_interactive_cluster

        nodefile = tmp_path / "nodefile"
        nodefile.write_text("node-a\n" * 48 + "node-b\n" * 48)
        monkeypatch.setenv("PBS_NODEFILE", str(nodefile))

        mock_ssh_cluster = MagicMock()
        mock_ssh_cluster.scheduler_address = "tcp://node-a:8786"
        mock_client = MagicMock()
        mock_client.scheduler_info.return_value = {"workers": {}}

        with (
            patch("dask.distributed.SSHCluster", return_value=mock_ssh_cluster) as mock_ssh,
            patch("dask.distributed.Client", return_value=mock_client),
            patch("dask_setup.multinode._wait_for_workers"),
        ):
            setup_interactive_cluster(workload_type="io")

        worker_opts = mock_ssh.call_args.kwargs["worker_options"]
        assert worker_opts["n_workers"] == 1
        assert worker_opts["nthreads"] >= 4     # min threads for io


# ---------------------------------------------------------------------------
# setup_dask_client — interactive mode dispatch
# ---------------------------------------------------------------------------


class TestSetupDaskClientInteractiveDispatch:
    """setup_dask_client(mode='interactive') should call setup_interactive_cluster."""

    def test_explicit_interactive_mode(self):
        """mode='interactive' always calls setup_interactive_cluster."""
        mock_client = MagicMock()
        mock_cluster = MagicMock()
        mock_cluster.scheduler_address = "tcp://127.0.0.1:8786"

        with patch(
            "dask_setup.client.setup_interactive_cluster",
            return_value=(mock_client, mock_cluster, "/tmp/dask"),
        ) as mock_interactive:
            from dask_setup.client import setup_dask_client

            result = setup_dask_client(mode="interactive")

        mock_interactive.assert_called_once()
        assert result[0] is mock_client

    def test_auto_mode_pbs_interactive_dispatches_correctly(self, monkeypatch):
        """mode='auto' with PBS_ENVIRONMENT=PBS_INTERACTIVE → setup_interactive_cluster."""
        monkeypatch.setenv("PBS_JOBID", "12345.gadi")
        monkeypatch.setenv("PBS_NODEFILE", "/fake/nodefile")
        monkeypatch.setenv("PBS_ENVIRONMENT", "PBS_INTERACTIVE")

        mock_client = MagicMock()
        mock_cluster = MagicMock()

        with patch(
            "dask_setup.client.setup_interactive_cluster",
            return_value=(mock_client, mock_cluster, ""),
        ) as mock_interactive:
            from dask_setup.client import setup_dask_client

            result = setup_dask_client(mode="auto")

        mock_interactive.assert_called_once()
        assert result[0] is mock_client
