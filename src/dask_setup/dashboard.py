"""Dashboard utilities for dask_setup."""

from __future__ import annotations

import socket
from urllib.parse import urlparse

from dask.distributed import Client


def get_dashboard_info(client: Client) -> dict[str, str]:
    """Extract dashboard connection information from client.

    Args:
        client: Connected Dask client

    Returns:
        Dictionary with dashboard connection details:
        - link: Full dashboard URL
        - host: Dashboard hostname
        - port: Dashboard port
        - local_host: Local hostname for SSH tunneling
    """
    dashboard_link = client.dashboard_link

    if not dashboard_link:
        return {"link": "", "host": "", "port": "", "local_host": socket.gethostname()}

    # Parse the dashboard URL
    parsed = urlparse(dashboard_link)
    host = parsed.hostname or "127.0.0.1"
    port = str(parsed.port or 8787)

    return {"link": dashboard_link, "host": host, "port": port, "local_host": socket.gethostname()}


def format_dashboard_message(client: Client) -> str:
    """Format dashboard access message with SSH tunnel instructions.

    Args:
        client: Connected Dask client

    Returns:
        Formatted message string for dashboard access
    """
    info = get_dashboard_info(client)

    if not info["link"]:
        return "Dashboard is disabled."

    local_host = info["local_host"]
    port = info["port"]

    return (
        f"Dask dashboard: {info['link']}\n"
        f"Tunnel from your laptop (run locally):\n"
        f"  ssh -N -L 8787:{local_host}:{port} gadi.nci.org.au\n"
        f"Then open: http://localhost:8787"
    )


def display_jupyter_dashboard(client: Client) -> None:
    """Render a clickable Dask dashboard link in a Jupyter notebook output cell.

    Uses ``IPython.display.HTML`` to produce an anchor tag that opens the
    dashboard in a new browser tab.  The link is styled to be clearly visible
    against both light and dark notebook themes.

    Args:
        client: Connected Dask client

    Raises:
        ImportError: If IPython is not installed (should not normally occur
            when running inside a Jupyter kernel).
    """
    from IPython.display import HTML, display  # type: ignore[import-untyped]

    info = get_dashboard_info(client)
    if not info["link"]:
        return

    html = (
        '<div style="'
        "font-family: monospace; "
        "padding: 6px 10px; "
        "border-left: 3px solid #4CAF50; "
        "margin: 4px 0;"
        '">'
        "<b>Dask Dashboard →</b> "
        f'<a href="{info["link"]}" target="_blank" rel="noopener noreferrer">'
        f"{info['link']}"
        "</a>"
        "</div>"
    )
    display(HTML(html))


def print_dashboard_info(client: Client, silent: bool = False) -> None:
    """Print dashboard information, adapting to the runtime environment.

    - Inside a **Jupyter notebook**: renders a clickable HTML link via
      ``IPython.display`` (falls back to plain text if IPython is unavailable).
    - Everywhere else: prints the standard SSH tunnel hint to stdout.

    Args:
        client: Connected Dask client
        silent: If True, do nothing (suppresses all output).
    """
    if silent:
        return

    from .environment import is_jupyter

    if is_jupyter():
        try:
            display_jupyter_dashboard(client)
            return
        except ImportError:
            # IPython.display unavailable despite being in a Jupyter environment —
            # fall through to the plain-text path below
            pass

    print(format_dashboard_message(client))
