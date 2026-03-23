"""Worker health callbacks for Dask cluster lifecycle events.

Provides :func:`register_worker_callbacks`, which installs a lightweight
scheduler plugin that fires user-supplied functions when workers are added
to or removed from the cluster.

Typical use-cases:

- Logging worker deaths to a file or monitoring system.
- Setting a :class:`threading.Event` so the main thread can detect failures.
- Checkpointing state when a worker dies mid-job.

.. warning::
   Callbacks run inside the Dask **scheduler's** event loop.  Keep them
   short and non-blocking.  Performing heavy computation or blocking I/O
   inside a callback will stall the scheduler and can cause cascading
   failures.  For heavy work, enqueue a task from within the callback
   instead (e.g. put a message on a :class:`queue.Queue` and process it
   in a separate thread).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from .logging import get_logger

if TYPE_CHECKING:
    from dask.distributed import Client

logger = get_logger("callbacks")


def register_worker_callbacks(
    client: "Client",
    on_worker_death: Callable[[str], None] | None = None,
    on_worker_added: Callable[[str], None] | None = None,
) -> None:
    """Register callbacks for worker lifecycle events on a Dask cluster.

    Installs a :class:`~distributed.diagnostics.plugin.SchedulerPlugin`
    that calls user-supplied functions when workers join or leave the
    cluster.

    Parameters
    ----------
    client : dask.distributed.Client
        A connected Dask client.
    on_worker_death : callable(worker_addr: str) | None
        Called when a worker is removed from the scheduler — whether it
        crashed, was explicitly closed, or was scaled down.  ``worker_addr``
        is the worker's TCP address string (e.g.
        ``"tcp://127.0.0.1:12345"``).
    on_worker_added : callable(worker_addr: str) | None
        Called when a new worker connects to the scheduler.

    Notes
    -----
    - If neither callback is provided this function is a no-op.
    - The plugin is registered with ``name="dask_setup_worker_health"``.
      Calling this function a second time replaces the existing plugin
      (idempotent where supported by the installed Dask version).
    - Exceptions raised inside callbacks are caught and logged as warnings
      so that a buggy callback never crashes the scheduler.

    Examples
    --------
    ::

        import threading
        from dask_setup import setup_dask_client, register_worker_callbacks

        dead: list[str] = []
        _lock = threading.Lock()

        def on_death(addr: str) -> None:
            with _lock:
                dead.append(addr)

        client, cluster, tmp = setup_dask_client()
        register_worker_callbacks(client, on_worker_death=on_death)

        # ... run tasks ...

        if dead:
            print(f"Workers that died during the run: {dead}")
    """
    if on_worker_death is None and on_worker_added is None:
        logger.debug(
            "register_worker_callbacks: no callbacks provided — skipping plugin install"
        )
        return

    try:
        from distributed.diagnostics.plugin import SchedulerPlugin
    except ImportError:
        logger.warning(
            "Could not import distributed.diagnostics.plugin.SchedulerPlugin; "
            "worker health callbacks will not be installed.  "
            "Ensure dask[distributed] is installed."
        )
        return

    # ------------------------------------------------------------------
    # Inner plugin class — defined here so it closes over the callbacks
    # without needing them as instance state on the scheduler side.
    # ------------------------------------------------------------------

    class _WorkerHealthPlugin(SchedulerPlugin):
        """Fires user callbacks on worker add/remove events."""

        name = "dask_setup_worker_health"

        def __init__(
            self,
            death_cb: Callable[[str], None] | None,
            added_cb: Callable[[str], None] | None,
        ) -> None:
            self._death_cb = death_cb
            self._added_cb = added_cb

        # SchedulerPlugin interface — method signatures differ slightly
        # across Dask versions; accept **kwargs for forward-compatibility.

        def add_worker(self, scheduler=None, worker: str = "", **kwargs) -> None:
            if self._added_cb is not None:
                try:
                    self._added_cb(worker)
                except Exception as exc:
                    logger.warning(
                        "on_worker_added callback raised an exception",
                        worker=worker,
                        error=str(exc),
                    )

        def remove_worker(self, scheduler=None, worker: str = "", **kwargs) -> None:
            if self._death_cb is not None:
                try:
                    self._death_cb(worker)
                except Exception as exc:
                    logger.warning(
                        "on_worker_death callback raised an exception",
                        worker=worker,
                        error=str(exc),
                    )

    plugin = _WorkerHealthPlugin(
        death_cb=on_worker_death,
        added_cb=on_worker_added,
    )

    # Register with idempotent=True where supported (Dask ≥ 2023.x)
    try:
        client.register_scheduler_plugin(plugin, idempotent=True)
    except TypeError:
        # Older Dask versions do not accept idempotent=
        client.register_scheduler_plugin(plugin)

    logger.debug(
        "Worker health plugin registered",
        has_death_cb=on_worker_death is not None,
        has_added_cb=on_worker_added is not None,
    )
