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
    "setup_interactive_cluster",
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

    Distinguishes between *batch* jobs (where submitting child worker jobs via
    dask-jobqueue is the right approach) and *interactive* jobs (where the
    resources are already allocated and workers should be started directly).

    PBS sets ``PBS_ENVIRONMENT=PBS_INTERACTIVE`` for interactive jobs (``qsub -I``).
    SLURM sets ``SLURM_JOB_NAME=interactive`` or the job was submitted via
    ``salloc`` (``SLURM_JOB_ID`` is set but ``SLURM_BATCH_FLAG`` is absent or ``0``).

    Returns
    -------
    str
        One of ``"pbs"``, ``"slurm"``, ``"interactive"``, or ``"local"``.

        ``"interactive"`` is returned when inside a PBS interactive job
        (``PBS_ENVIRONMENT=PBS_INTERACTIVE``) or a SLURM interactive allocation
        (``salloc`` / ``SLURM_BATCH_FLAG`` not set).  In this mode,
        :func:`setup_interactive_cluster` should be used instead of
        :func:`setup_pbs_cluster` / :func:`setup_slurm_cluster`.
    """
    # --- PBS ---
    if any(os.getenv(v) for v in _PBS_INDICATORS):
        if os.getenv("PBS_ENVIRONMENT") == "PBS_INTERACTIVE":
            logger.debug("Cluster mode detected: interactive (PBS)")
            return "interactive"
        logger.debug("Cluster mode detected: pbs")
        return "pbs"

    # --- SLURM ---
    if any(os.getenv(v) for v in _SLURM_INDICATORS):
        # SLURM_BATCH_FLAG=1 in batch jobs, absent or 0 in interactive (salloc)
        batch_flag = os.getenv("SLURM_BATCH_FLAG", "0")
        if batch_flag != "1":
            logger.debug("Cluster mode detected: interactive (SLURM)")
            return "interactive"
        logger.debug("Cluster mode detected: slurm")
        return "slurm"

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


def _wait_for_workers(
    client: Any,
    cluster: Any,
    n_jobs: int,
    workers_per_job: int,
    timeout: float,
) -> None:
    """Block until at least one worker connects, or raise ``TimeoutError``.

    ``cluster.scale()`` with dask-jobqueue merely *submits* jobs to the HPC
    scheduler — it does not wait for those jobs to be allocated and started.
    ``Client(cluster)`` connects to the *scheduler* process (which starts
    immediately), but the scheduler has 0 workers until PBS/SLURM actually
    launches the submitted jobs.

    This helper calls ``client.wait_for_workers(1, timeout=timeout)`` so that
    callers always receive a client that has at least one live worker.

    Parameters
    ----------
    client:
        The connected ``dask.distributed.Client``.
    cluster:
        The ``PBSCluster`` / ``SLURMCluster`` (used for diagnostic info only).
    n_jobs:
        Number of jobs that were submitted (used in the timeout message).
    workers_per_job:
        Workers per job (``MultiNodeConfig.workers_per_node``), used to
        compute the expected total worker count in the timeout message.
    timeout:
        Maximum seconds to wait before raising ``TimeoutError``.
    """
    expected_total = n_jobs * workers_per_job
    logger.debug(
        "Waiting for workers",
        expected_total=expected_total,
        timeout=timeout,
    )
    try:
        client.wait_for_workers(1, timeout=timeout)
    except Exception as exc:
        # dask raises TimeoutError or asyncio.TimeoutError depending on version
        scheduler_addr = getattr(cluster, "scheduler_address", "unknown")
        raise TimeoutError(
            f"No Dask workers connected after {timeout:.0f} s.\n"
            f"\n"
            f"  Jobs submitted : {n_jobs}\n"
            f"  Expected workers: {expected_total} ({workers_per_job} per job)\n"
            f"  Scheduler address: {scheduler_addr}\n"
            f"\n"
            f"Common causes:\n"
            f"  - Jobs are still queued — raise worker_timeout or check the queue "
            f"with 'qstat' / 'squeue'\n"
            f"  - Workers started but couldn't reach the scheduler — check that "
            f"the scheduler port is reachable from compute nodes\n"
            f"  - Environment or import errors inside the worker — check the "
            f"worker job logs (usually in ~/dask-worker-space/ or the PBS log dir)\n"
        ) from exc

    n_connected = len(client.scheduler_info().get("workers", {}))
    logger.debug("Workers connected", n_connected=n_connected, expected_total=expected_total)


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
    wait_for_workers: bool = True,
    worker_timeout: float = 300.0,
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
    wait_for_workers:
        If ``True`` (default), block until at least one worker has connected
        to the scheduler before returning.  This prevents the client from
        appearing to have 0 workers when PBS has not yet started the submitted
        jobs.  Set to ``False`` to return immediately and poll
        ``client.wait_for_workers()`` yourself.
    worker_timeout:
        Seconds to wait for workers to come online when *wait_for_workers* is
        ``True``.  Defaults to 300 s (5 minutes).  Raise this on queues with
        long wait times.  A ``TimeoutError`` is raised (with a diagnostic
        message) if no workers connect within this window.

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

    if wait_for_workers:
        _wait_for_workers(client, cluster, n_workers, cfg.workers_per_node, worker_timeout)

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
    wait_for_workers: bool = True,
    worker_timeout: float = 300.0,
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
    wait_for_workers:
        If ``True`` (default), block until at least one worker has connected
        to the scheduler before returning.  Set to ``False`` to return
        immediately.
    worker_timeout:
        Seconds to wait for workers to come online.  Defaults to 300 s.

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

    if wait_for_workers:
        _wait_for_workers(client, cluster, n_workers, cfg.workers_per_node, worker_timeout)

    return client, cluster, shared_tmp


# ---------------------------------------------------------------------------
# Interactive cluster — use already-allocated PBS/SLURM resources
# ---------------------------------------------------------------------------


def _parse_pbs_nodefile(nodefile: str) -> dict[str, int]:
    """Return ``{hostname: core_count}`` from a ``PBS_NODEFILE``.

    NCI Gadi (and most PBS systems) write one line *per CPU* in the nodefile,
    so the same hostname appears ``ncpus`` times.  Deduplication gives the
    unique nodes; the count gives cores per node.
    """
    from collections import Counter

    try:
        with open(nodefile) as f:
            lines = [ln.strip() for ln in f if ln.strip()]
        return dict(Counter(lines))
    except OSError:
        return {}


def _parse_slurm_nodelist(nodelist: str, cpus_per_node: int = 1) -> dict[str, int]:
    """Expand a SLURM compressed nodelist into ``{hostname: core_count}``.

    Uses ``scontrol show hostnames`` when available; falls back to treating
    *nodelist* as a comma-separated list of plain hostnames.
    """
    import subprocess

    try:
        result = subprocess.run(
            ["scontrol", "show", "hostnames", nodelist],
            capture_output=True,
            text=True,
            timeout=5,
        )
        hosts = [h for h in result.stdout.splitlines() if h.strip()]
    except Exception:
        # Fallback: treat as comma-separated (works for simple cases)
        hosts = [h.strip() for h in nodelist.split(",") if h.strip()]

    return {h: cpus_per_node for h in hosts}


def setup_interactive_cluster(
    workload_type: str = "cpu",
    workers_per_node: int | None = None,
    threads_per_worker: int | None = None,
    wait_for_workers: bool = True,
    worker_timeout: float = 60.0,
    scheduler_port: int = 8786,
) -> tuple[Any, Any, str | None]:
    """Set up a Dask cluster using resources already allocated in an interactive job.

    This is the right function to call from a Jupyter notebook running inside a
    ``qsub -I`` (PBS) or ``salloc`` (SLURM) interactive session.  Unlike
    :func:`setup_pbs_cluster` / :func:`setup_slurm_cluster`, it does **not**
    submit new batch jobs — it creates workers directly on the nodes that PBS or
    SLURM has already assigned to the current session.

    Behaviour
    ---------
    - **Single allocated node** — creates a ``LocalCluster`` using all cores and
      memory available in the allocation (reads ``PBS_NCPUS`` / ``PBS_MEM`` or
      SLURM equivalents via :func:`~dask_setup.resources.detect_resources`).
    - **Multiple allocated nodes** — creates a ``dask.distributed.SSHCluster``
      across all nodes listed in ``PBS_NODEFILE`` / ``$SLURM_NODELIST``.
      Passwordless SSH between nodes is required (standard on most HPC systems).

    Parameters
    ----------
    workload_type:
        Worker topology — ``"cpu"``, ``"io"``, ``"mixed"``, or ``"auto"``.
        Determines threads per worker and workers per node using the same
        logic as single-node :func:`~dask_setup.client.setup_dask_client`.
    workers_per_node:
        Override the number of worker processes to start per node.  When
        ``None`` (default), derived from *workload_type* and the core count.
    threads_per_worker:
        Override threads per worker process.  When ``None``, derived from
        *workload_type*.
    wait_for_workers:
        Block until at least one worker has connected.  Defaults to ``True``.
    worker_timeout:
        Seconds to wait for workers.  Defaults to 60 s (workers start almost
        immediately because no job submission is involved).
    scheduler_port:
        Port for the Dask scheduler (multi-node SSH path only).

    Returns
    -------
    (client, cluster, tmp_path)
        ``client`` — a connected ``dask.distributed.Client``.
        ``cluster`` — ``LocalCluster`` or ``SSHCluster``.
        ``tmp_path`` — path to the temporary directory, or ``""`` when not set.
    """
    from dask.distributed import Client

    # --- Discover allocated nodes ------------------------------------------
    node_cores: dict[str, int] = {}

    nodefile = os.getenv("PBS_NODEFILE", "")
    slurm_nodelist = os.getenv("SLURM_NODELIST") or os.getenv("SLURM_JOB_NODELIST", "")

    if nodefile and Path(nodefile).exists():
        node_cores = _parse_pbs_nodefile(nodefile)
        logger.debug("PBS_NODEFILE parsed", nodes=list(node_cores.keys()))
    elif slurm_nodelist:
        # SLURM_CPUS_ON_NODE can be a comma-separated list (one per node)
        cpus_str = os.getenv("SLURM_CPUS_ON_NODE", "1")
        try:
            cpus_per_node = int(cpus_str.split(",")[0])
        except ValueError:
            cpus_per_node = 1
        node_cores = _parse_slurm_nodelist(slurm_nodelist, cpus_per_node)
        logger.debug("SLURM nodelist parsed", nodes=list(node_cores.keys()))

    unique_nodes = list(node_cores.keys())

    # --- Single-node path (LocalCluster) ------------------------------------
    if len(unique_nodes) <= 1:
        logger.info(
            "Interactive cluster: single node, using LocalCluster",
            node=unique_nodes[0] if unique_nodes else "unknown",
        )
        from .resources import detect_resources
        from .topology import decide_topology
        from .cluster import create_cluster, configure_dask_settings
        from .tempdir import create_dask_temp_dir

        resources = detect_resources(fallback=True)
        resolved_wl = workload_type
        if workload_type == "auto":
            try:
                from .workload import infer_workload_type
                resolved_wl = infer_workload_type(None, resources)
            except Exception:
                resolved_wl = "cpu"

        from .config import DaskSetupConfig
        cfg_obj = DaskSetupConfig(workload_type=resolved_wl)
        topology = decide_topology(cfg_obj, resources)
        configure_dask_settings(resources, topology)
        dask_tmp = create_dask_temp_dir()
        cluster = create_cluster(topology, resources, str(dask_tmp))
        client = Client(cluster)
        if wait_for_workers:
            _wait_for_workers(
                client, cluster,
                n_jobs=1,
                workers_per_job=topology.n_workers,
                timeout=worker_timeout,
            )
        return client, cluster, str(dask_tmp)

    # --- Multi-node path (SSHCluster) ---------------------------------------
    logger.info(
        "Interactive cluster: multi-node, using SSHCluster",
        nodes=unique_nodes,
    )
    from dask.distributed import SSHCluster

    # Compute topology for the SSH workers
    cores_per_node = node_cores[unique_nodes[0]]
    resolved_wl = workload_type if workload_type != "auto" else "cpu"

    if workers_per_node is None:
        if resolved_wl == "io":
            _wpn = 1
            _tpw = min(16, max(4, cores_per_node))
        elif resolved_wl == "mixed":
            import math
            _wpn = max(1, math.ceil(cores_per_node / 2))
            _tpw = 2
        else:  # cpu / auto
            _wpn = cores_per_node
            _tpw = 1
    else:
        _wpn = workers_per_node
        _tpw = threads_per_worker if threads_per_worker is not None else max(1, cores_per_node // _wpn)

    if threads_per_worker is not None:
        _tpw = threads_per_worker

    logger.debug(
        "SSHCluster topology",
        workers_per_node=_wpn,
        threads_per_worker=_tpw,
        cores_per_node=cores_per_node,
    )

    cluster = SSHCluster(
        unique_nodes,  # first host is the scheduler
        connect_options={"known_hosts": None},
        worker_options={"nthreads": _tpw, "n_workers": _wpn},
        scheduler_options={"port": scheduler_port, "dashboard_address": ":8787"},
    )

    client = Client(cluster)
    logger.debug("SSHCluster client connected", scheduler=str(cluster.scheduler_address))

    if wait_for_workers:
        _wait_for_workers(
            client, cluster,
            n_jobs=len(unique_nodes),
            workers_per_job=_wpn,
            timeout=worker_timeout,
        )

    return client, cluster, ""


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
