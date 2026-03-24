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
