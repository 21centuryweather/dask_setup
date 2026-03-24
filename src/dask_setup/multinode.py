"""Multi-node Dask cluster configuration and setup utilities.

This module provides the :class:`MultiNodeConfig` dataclass and the
:func:`setup_pbs_cluster` / :func:`setup_slurm_cluster` helpers for
launching ``dask-jobqueue`` clusters from the same ``DaskSetupConfig``-style
API used for single-node setups.

Example::

    from dask_setup.multinode import MultiNodeConfig, setup_pbs_cluster

    cfg = MultiNodeConfig(
        workload_type="cpu",
        workers_per_node=4,
        cores_per_worker=12,
        mem_per_worker_gb=32.0,
        walltime="04:00:00",
        queue="normal",
    )
    client, cluster, tmp = setup_pbs_cluster(cfg)
    try:
        ...
    finally:
        client.close()
        cluster.close()
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .logging import get_logger

logger = get_logger("multinode")

__all__ = [
    "MultiNodeConfig",
    "setup_pbs_cluster",
    "setup_slurm_cluster",
    "detect_cluster_mode",
    "SharedTempDir",
]


# ---------------------------------------------------------------------------
# SharedTempDir — Lustre/GPFS-safe temp directory for multi-node rechunking
# ---------------------------------------------------------------------------


@dataclass
class SharedTempDir:
    """A shared temporary directory visible to all nodes in the job.

    On multi-node PBS/SLURM jobs, ``$PBS_JOBFS`` is *per-node* and cannot be
    used as a shared store (e.g. for Rechunker targets).  This helper wraps a
    Lustre / GPFS / Scratch path that is visible cluster-wide.

    Parameters
    ----------
    path:
        Absolute path to the shared filesystem directory to use.
    create_subdirectory:
        If ``True`` (default), create a unique ``dask_tmp_<jobid>``
        subdirectory inside *path* to avoid collisions across concurrent jobs.
    cleanup_on_close:
        If ``True``, delete the directory tree when :meth:`cleanup` is called.
        Defaults to ``False`` — shared scratch is typically cleaned by the
        scheduler.

    Attributes
    ----------
    resolved_path : Path
        The actual directory that was created (may include the subdirectory).
    """

    path: str | Path
    create_subdirectory: bool = True
    cleanup_on_close: bool = False
    resolved_path: Path = field(init=False, default=None)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        base = Path(self.path)
        if self.create_subdirectory:
            job_id = (
                os.getenv("PBS_JOBID")
                or os.getenv("SLURM_JOB_ID")
                or os.getenv("SLURM_JOBID")
                or "local"
            )
            subdir = base / f"dask_tmp_{job_id}"
        else:
            subdir = base
        subdir.mkdir(parents=True, exist_ok=True)
        self.resolved_path = subdir
        logger.debug("SharedTempDir created", path=str(subdir))

    def cleanup(self) -> None:
        """Remove the temporary directory if *cleanup_on_close* is ``True``."""
        if self.cleanup_on_close and self.resolved_path.exists():
            import shutil

            shutil.rmtree(self.resolved_path, ignore_errors=True)
            logger.debug("SharedTempDir cleaned up", path=str(self.resolved_path))

    def __str__(self) -> str:
        return str(self.resolved_path)

    def __fspath__(self) -> str:
        return str(self.resolved_path)


# ---------------------------------------------------------------------------
# MultiNodeConfig
# ---------------------------------------------------------------------------


@dataclass
class MultiNodeConfig:
    """Configuration for a multi-node ``dask-jobqueue`` cluster.

    This is a companion to :class:`~dask_setup.config.DaskSetupConfig` that
    adds the fields needed to submit worker jobs to PBS or SLURM.  The
    single-node ``DaskSetupConfig`` fields (``workload_type``,
    ``reserve_mem_gb``, etc.) are accepted here too so callers can use one
    object for both paths.

    Parameters
    ----------
    workload_type:
        Worker topology — ``"cpu"``, ``"io"``, ``"mixed"``, or ``"gpu"``.
        Passed through to the single-node topology logic when deciding
        ``cores_per_worker`` and ``threads_per_worker``.
    workers_per_node:
        Number of Dask worker *processes* to start per submitted job.
        Typically 1 per GPU for GPU workloads, or ``ncpus / cores_per_worker``
        for CPU workloads.
    cores_per_worker:
        CPU cores allocated to each worker.  For PBS this maps to
        ``-l ncpus=cores_per_worker * workers_per_node``.
    mem_per_worker_gb:
        RAM in GiB to request per worker process.  The total node request is
        ``mem_per_worker_gb * workers_per_node``.
    walltime:
        Job walltime string in ``HH:MM:SS`` format.
    queue:
        Scheduler queue / partition to submit to.
    project:
        PBS/SLURM project / account string (maps to ``-P`` / ``--account``).
    job_extra_directives:
        Additional ``#PBS`` or ``#SBATCH`` directives to inject verbatim into
        the job script header.
    n_nodes:
        Number of nodes to request.  Defaults to ``1`` (scale up by adding
        more jobs, not more nodes per job).
    shared_tmp_dir:
        Path to a Lustre / GPFS shared directory for stores that must be
        visible to all workers (e.g. Rechunker targets).  When ``None``,
        per-node temp is used.
    env_extra:
        Extra environment variable exports to include in worker job scripts.
    scheduler_options:
        Options forwarded verbatim to ``dask-jobqueue``'s
        ``scheduler_options`` parameter.
    reserve_mem_gb:
        OS memory reservation per node (for compatibility with single-node
        config objects).  Defaults to ``4.0`` in multi-node mode (the OS
        footprint on a dedicated compute node is small).
    adaptive:
        Enable adaptive (elastic) scaling via
        ``cluster.adapt(minimum=min_jobs, maximum=max_jobs)``.
    min_jobs:
        Minimum active jobs when *adaptive* is ``True``.
    max_jobs:
        Maximum active jobs when *adaptive* is ``True``.
    """

    # Core topology
    workload_type: str = "cpu"
    workers_per_node: int = 1
    cores_per_worker: int = 1
    mem_per_worker_gb: float = 4.0
    walltime: str = "01:00:00"
    queue: str = "normal"

    # Optional
    project: str | None = None
    job_extra_directives: list[str] = field(default_factory=list)
    n_nodes: int = 1
    shared_tmp_dir: str | Path | None = None
    env_extra: list[str] = field(default_factory=list)
    scheduler_options: dict[str, Any] = field(default_factory=dict)
    reserve_mem_gb: float = 4.0

    # Adaptive scaling
    adaptive: bool = False
    min_jobs: int = 1
    max_jobs: int = 10

    # ---------------------------------------------------------------------------
    # Validation constants
    # ---------------------------------------------------------------------------
    VALID_WORKLOAD_TYPES: tuple[str, ...] = field(
        default=("cpu", "io", "mixed", "gpu", "auto"), init=False, repr=False
    )

    def __post_init__(self) -> None:
        errors: list[str] = []

        if self.workload_type not in {"cpu", "io", "mixed", "gpu", "auto"}:
            errors.append(
                f"workload_type must be one of 'cpu','io','mixed','gpu','auto', "
                f"got '{self.workload_type}'"
            )
        if self.workers_per_node < 1:
            errors.append(f"workers_per_node must be >= 1, got {self.workers_per_node}")
        if self.cores_per_worker < 1:
            errors.append(f"cores_per_worker must be >= 1, got {self.cores_per_worker}")
        if self.mem_per_worker_gb <= 0:
            errors.append(f"mem_per_worker_gb must be > 0, got {self.mem_per_worker_gb}")
        if self.n_nodes < 1:
            errors.append(f"n_nodes must be >= 1, got {self.n_nodes}")
        if self.adaptive:
            if self.min_jobs < 1:
                errors.append(f"min_jobs must be >= 1, got {self.min_jobs}")
            if self.max_jobs < self.min_jobs:
                errors.append(f"max_jobs ({self.max_jobs}) must be >= min_jobs ({self.min_jobs})")

        if errors:
            from .exceptions import InvalidConfigurationError

            raise InvalidConfigurationError(
                "MultiNodeConfig validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
            )

    @property
    def total_cores_per_job(self) -> int:
        """Total CPU cores requested per submitted job."""
        return self.workers_per_node * self.cores_per_worker

    @property
    def total_mem_gb_per_job(self) -> float:
        """Total RAM (GiB) requested per submitted job."""
        return self.workers_per_node * self.mem_per_worker_gb

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable representation."""
        return {
            "workload_type": self.workload_type,
            "workers_per_node": self.workers_per_node,
            "cores_per_worker": self.cores_per_worker,
            "mem_per_worker_gb": self.mem_per_worker_gb,
            "walltime": self.walltime,
            "queue": self.queue,
            "project": self.project,
            "job_extra_directives": list(self.job_extra_directives),
            "n_nodes": self.n_nodes,
            "shared_tmp_dir": str(self.shared_tmp_dir) if self.shared_tmp_dir else None,
            "reserve_mem_gb": self.reserve_mem_gb,
            "adaptive": self.adaptive,
            "min_jobs": self.min_jobs,
            "max_jobs": self.max_jobs,
        }


# ---------------------------------------------------------------------------
# detect_cluster_mode — auto-detect whether to use local / PBS / SLURM
# ---------------------------------------------------------------------------

_PBS_INDICATORS = ("PBS_JOBID", "PBS_NODEFILE", "PBS_ENVIRONMENT")
_SLURM_INDICATORS = ("SLURM_JOB_ID", "SLURM_JOBID", "SLURM_NODELIST")


def detect_cluster_mode() -> str:
    """Detect the appropriate cluster mode from the execution environment.

    Returns
    -------
    str
        One of ``"pbs"``, ``"slurm"``, or ``"local"``.
    """
    if any(os.getenv(v) for v in _SLURM_INDICATORS):
        logger.debug("Cluster mode detected: slurm")
        return "slurm"
    if any(os.getenv(v) for v in _PBS_INDICATORS):
        logger.debug("Cluster mode detected: pbs")
        return "pbs"
    logger.debug("Cluster mode detected: local")
    return "local"


# ---------------------------------------------------------------------------
# _build_pbs_cluster / _build_slurm_cluster — dask-jobqueue wrappers
# ---------------------------------------------------------------------------


def _check_jobqueue() -> None:
    """Raise a helpful ImportError if dask-jobqueue is not installed."""
    try:
        import dask_jobqueue  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "dask-jobqueue is required for multi-node cluster support.\n"
            "Install with:  pip install dask-jobqueue\n"
            "or:            conda install -c conda-forge dask-jobqueue"
        ) from exc


def _make_shared_tmpdir(cfg: MultiNodeConfig) -> SharedTempDir | None:
    """Create a SharedTempDir from the config's shared_tmp_dir path, if set."""
    if cfg.shared_tmp_dir is None:
        return None
    return SharedTempDir(path=cfg.shared_tmp_dir)


def _worker_extra_args(cfg: MultiNodeConfig) -> list[str]:
    """Build extra arguments forwarded to each dask-worker process."""
    args = []
    if cfg.shared_tmp_dir is not None:
        args += ["--local-directory", str(cfg.shared_tmp_dir)]
    return args


def setup_pbs_cluster(
    config: MultiNodeConfig | None = None,
    *,
    # Convenience kwargs (override config fields)
    workload_type: str | None = None,
    workers_per_node: int | None = None,
    cores_per_worker: int | None = None,
    mem_per_worker_gb: float | None = None,
    walltime: str | None = None,
    queue: str | None = None,
    project: str | None = None,
    job_extra_directives: list[str] | None = None,
    n_nodes: int | None = None,
    shared_tmp_dir: str | Path | None = None,
    env_extra: list[str] | None = None,
    adaptive: bool | None = None,
    min_jobs: int | None = None,
    max_jobs: int | None = None,
    n_workers: int = 1,
) -> tuple[Any, Any, SharedTempDir | None]:
    """Set up a multi-node Dask cluster backed by PBS/Torque.

    Uses ``dask-jobqueue.PBSCluster`` under the hood, applying
    ``MultiNodeConfig`` defaults and the same error-handling conventions as
    single-node :func:`~dask_setup.client.setup_dask_client`.

    Parameters
    ----------
    config:
        A :class:`MultiNodeConfig` instance.  If ``None``, a default one is
        constructed from the keyword arguments.
    workload_type, workers_per_node, …:
        Convenience overrides for any ``MultiNodeConfig`` field.
    n_workers:
        Number of worker *jobs* to submit immediately (before any adaptive
        scaling kicks in).

    Returns
    -------
    (client, cluster, shared_tmp)
        ``client`` — a connected ``dask.distributed.Client``.
        ``cluster`` — the live ``PBSCluster`` object.
        ``shared_tmp`` — a :class:`SharedTempDir` or ``None``.
    """
    _check_jobqueue()
    from dask.distributed import Client
    from dask_jobqueue import PBSCluster

    cfg = _apply_overrides(
        config,
        workload_type=workload_type,
        workers_per_node=workers_per_node,
        cores_per_worker=cores_per_worker,
        mem_per_worker_gb=mem_per_worker_gb,
        walltime=walltime,
        queue=queue,
        project=project,
        job_extra_directives=job_extra_directives,
        n_nodes=n_nodes,
        shared_tmp_dir=shared_tmp_dir,
        env_extra=env_extra,
        adaptive=adaptive,
        min_jobs=min_jobs,
        max_jobs=max_jobs,
    )

    shared_tmp = _make_shared_tmpdir(cfg)
    extra_worker_args = _worker_extra_args(cfg)

    mem_str = f"{cfg.mem_per_worker_gb:.0f}GB"

    extra_directives = list(cfg.job_extra_directives)

    cluster = PBSCluster(
        cores=cfg.cores_per_worker,
        memory=mem_str,
        processes=cfg.workers_per_node,
        walltime=cfg.walltime,
        queue=cfg.queue,
        project=cfg.project,
        resource_spec=(
            f"ncpus={cfg.total_cores_per_job},"
            f"mem={cfg.total_mem_gb_per_job:.0f}GB"
            + (f",nodes={cfg.n_nodes}" if cfg.n_nodes > 1 else "")
        ),
        job_extra_directives=extra_directives,
        env_extra=cfg.env_extra,
        worker_extra_args=extra_worker_args,
        scheduler_options=cfg.scheduler_options,
    )

    if cfg.adaptive:
        cluster.adapt(minimum=cfg.min_jobs, maximum=cfg.max_jobs)
        logger.debug(
            "PBSCluster adaptive scaling enabled",
            minimum=cfg.min_jobs,
            maximum=cfg.max_jobs,
        )
    else:
        cluster.scale(n_workers)
        logger.debug("PBSCluster scaled", n_workers=n_workers)

    client = Client(cluster)
    logger.debug("PBSCluster client connected", scheduler=str(cluster.scheduler_address))
    return client, cluster, shared_tmp


def setup_slurm_cluster(
    config: MultiNodeConfig | None = None,
    *,
    # Convenience kwargs (override config fields)
    workload_type: str | None = None,
    workers_per_node: int | None = None,
    cores_per_worker: int | None = None,
    mem_per_worker_gb: float | None = None,
    walltime: str | None = None,
    queue: str | None = None,
    project: str | None = None,
    job_extra_directives: list[str] | None = None,
    n_nodes: int | None = None,
    shared_tmp_dir: str | Path | None = None,
    env_extra: list[str] | None = None,
    adaptive: bool | None = None,
    min_jobs: int | None = None,
    max_jobs: int | None = None,
    n_workers: int = 1,
) -> tuple[Any, Any, SharedTempDir | None]:
    """Set up a multi-node Dask cluster backed by SLURM.

    Uses ``dask-jobqueue.SLURMCluster`` under the hood.  Mirrors the API of
    :func:`setup_pbs_cluster`.

    Parameters
    ----------
    config:
        A :class:`MultiNodeConfig` instance.
    n_workers:
        Number of worker jobs to submit immediately.

    Returns
    -------
    (client, cluster, shared_tmp)
    """
    _check_jobqueue()
    from dask.distributed import Client
    from dask_jobqueue import SLURMCluster

    cfg = _apply_overrides(
        config,
        workload_type=workload_type,
        workers_per_node=workers_per_node,
        cores_per_worker=cores_per_worker,
        mem_per_worker_gb=mem_per_worker_gb,
        walltime=walltime,
        queue=queue,
        project=project,
        job_extra_directives=job_extra_directives,
        n_nodes=n_nodes,
        shared_tmp_dir=shared_tmp_dir,
        env_extra=env_extra,
        adaptive=adaptive,
        min_jobs=min_jobs,
        max_jobs=max_jobs,
    )

    shared_tmp = _make_shared_tmpdir(cfg)
    extra_worker_args = _worker_extra_args(cfg)

    mem_str = f"{cfg.mem_per_worker_gb:.0f}GB"

    cluster = SLURMCluster(
        cores=cfg.cores_per_worker,
        memory=mem_str,
        processes=cfg.workers_per_node,
        walltime=cfg.walltime,
        queue=cfg.queue,
        account=cfg.project,
        job_extra_directives=list(cfg.job_extra_directives),
        env_extra=cfg.env_extra,
        worker_extra_args=extra_worker_args,
        scheduler_options=cfg.scheduler_options,
    )

    if cfg.adaptive:
        cluster.adapt(minimum=cfg.min_jobs, maximum=cfg.max_jobs)
        logger.debug(
            "SLURMCluster adaptive scaling enabled",
            minimum=cfg.min_jobs,
            maximum=cfg.max_jobs,
        )
    else:
        cluster.scale(n_workers)
        logger.debug("SLURMCluster scaled", n_workers=n_workers)

    client = Client(cluster)
    logger.debug("SLURMCluster client connected", scheduler=str(cluster.scheduler_address))
    return client, cluster, shared_tmp


# ---------------------------------------------------------------------------
# Job script generation (used by dask-setup submit)
# ---------------------------------------------------------------------------


def generate_pbs_script(
    cfg: MultiNodeConfig,
    script_path: str,
    python_executable: str = "python3",
    dask_setup_args: str = "",
) -> str:
    """Generate a PBS job script string that launches a dask_setup-based job.

    Parameters
    ----------
    cfg:
        Multi-node configuration.
    script_path:
        Path to the user's Python script to run.
    python_executable:
        Python interpreter to use (default ``python3``).
    dask_setup_args:
        Additional arguments to pass to the script.

    Returns
    -------
    str
        Complete ``#PBS`` job script content.
    """
    project_line = f"#PBS -P {cfg.project}" if cfg.project else ""
    extra_lines = "\n".join(f"#PBS {d}" for d in cfg.job_extra_directives)
    mem_total = f"{cfg.total_mem_gb_per_job:.0f}GB"
    env_lines = "\n".join(f"export {e}" for e in cfg.env_extra)
    shared_tmp_line = (
        f"export DASK_SETUP_SHARED_TMP={cfg.shared_tmp_dir}" if cfg.shared_tmp_dir else ""
    )

    lines = [
        "#!/bin/bash",
        f"#PBS -l ncpus={cfg.total_cores_per_job},mem={mem_total},walltime={cfg.walltime}",
        f"#PBS -q {cfg.queue}",
    ]
    if project_line:
        lines.append(project_line)
    if extra_lines:
        lines.append(extra_lines)
    lines += [
        "",
        "# --- Environment ---",
        "set -euo pipefail",
    ]
    if env_lines:
        lines.append(env_lines)
    if shared_tmp_line:
        lines.append(shared_tmp_line)
    lines += [
        "",
        "# --- Run ---",
        f"{python_executable} {script_path} {dask_setup_args}".rstrip(),
    ]
    return "\n".join(lines) + "\n"


def generate_slurm_script(
    cfg: MultiNodeConfig,
    script_path: str,
    python_executable: str = "python3",
    dask_setup_args: str = "",
) -> str:
    """Generate a SLURM job script string.

    Parameters
    ----------
    cfg:
        Multi-node configuration.
    script_path:
        Path to the user's Python script to run.
    python_executable:
        Python interpreter to use (default ``python3``).
    dask_setup_args:
        Additional arguments to pass to the script.

    Returns
    -------
    str
        Complete ``#SBATCH`` job script content.
    """
    account_line = f"#SBATCH --account={cfg.project}" if cfg.project else ""
    extra_lines = "\n".join(f"#SBATCH {d}" for d in cfg.job_extra_directives)
    mem_total = f"{cfg.total_mem_gb_per_job:.0f}G"
    env_lines = "\n".join(f"export {e}" for e in cfg.env_extra)
    shared_tmp_line = (
        f"export DASK_SETUP_SHARED_TMP={cfg.shared_tmp_dir}" if cfg.shared_tmp_dir else ""
    )

    lines = [
        "#!/bin/bash",
        f"#SBATCH --ntasks={cfg.workers_per_node}",
        f"#SBATCH --cpus-per-task={cfg.cores_per_worker}",
        f"#SBATCH --mem={mem_total}",
        f"#SBATCH --time={cfg.walltime}",
        f"#SBATCH --partition={cfg.queue}",
    ]
    if account_line:
        lines.append(account_line)
    if extra_lines:
        lines.append(extra_lines)
    lines += [
        "",
        "# --- Environment ---",
        "set -euo pipefail",
    ]
    if env_lines:
        lines.append(env_lines)
    if shared_tmp_line:
        lines.append(shared_tmp_line)
    lines += [
        "",
        "# --- Run ---",
        f"{python_executable} {script_path} {dask_setup_args}".rstrip(),
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# _apply_overrides — internal helper
# ---------------------------------------------------------------------------


def _apply_overrides(
    config: MultiNodeConfig | None,
    **kwargs: Any,
) -> MultiNodeConfig:
    """Merge explicit keyword overrides onto a base MultiNodeConfig.

    If *config* is ``None``, a new ``MultiNodeConfig`` is created from the
    keyword arguments (``None`` values are dropped so defaults apply).
    """
    if config is None:
        clean = {k: v for k, v in kwargs.items() if v is not None}
        return MultiNodeConfig(**clean)

    # Build an updated copy by replacing non-None kwargs
    d = config.to_dict()
    for k, v in kwargs.items():
        if v is not None and k in d:
            d[k] = v
    # to_dict() serialises shared_tmp_dir as str|None; restore list fields
    d.setdefault("job_extra_directives", [])
    d.setdefault("env_extra", [])
    d.setdefault("scheduler_options", {})
    return MultiNodeConfig(**{k: v for k, v in d.items() if k != "VALID_WORKLOAD_TYPES"})
