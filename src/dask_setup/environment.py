"""Runtime environment detection for dask_setup.

Detects the host runtime so other modules can adapt their behaviour — for
example, rendering a clickable dashboard URL instead of an SSH tunnel hint
when running inside a Jupyter notebook.

Detection results are cached after the first call so repeated checks are free.
"""

from __future__ import annotations

__all__ = ["is_jupyter", "get_environment_type"]

# Module-level cache so detection only runs once per process
_env_type: str | None = None


def _detect_environment() -> str:
    """Run environment detection and return a canonical string.

    Returns one of:
    - ``"jupyter"``  — ZMQInteractiveShell (JupyterLab, classic notebook,
                       VSCode notebook, Google Colab, etc.)
    - ``"ipython"``  — TerminalInteractiveShell (IPython in a terminal)
    - ``"script"``   — plain Python script / REPL (no IPython)
    """
    try:
        from IPython import get_ipython  # type: ignore[import-untyped]

        shell = get_ipython()
        if shell is None:
            return "script"

        shell_class = type(shell).__name__
        if shell_class == "ZMQInteractiveShell":
            return "jupyter"
        # TerminalInteractiveShell or any other IPython-based shell
        return "ipython"

    except ImportError:
        return "script"


def get_environment_type() -> str:
    """Return a string describing the detected runtime environment.

    Returns
    -------
    str
        One of ``"jupyter"``, ``"ipython"``, or ``"script"``.

    The result is cached after the first call.
    """
    global _env_type
    if _env_type is None:
        _env_type = _detect_environment()
    return _env_type


def is_jupyter() -> bool:
    """Return ``True`` if running inside a Jupyter kernel.

    Covers JupyterLab, classic Jupyter Notebook, VSCode notebooks,
    Google Colab, and any other environment that uses a ZMQ kernel.

    The result is cached after the first call so it is safe to call
    repeatedly inside hot paths.

    Returns
    -------
    bool

    Examples
    --------
    ::

        from dask_setup.environment import is_jupyter

        if is_jupyter():
            print("Running in Jupyter")
    """
    return get_environment_type() == "jupyter"
