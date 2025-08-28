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


def print_dashboard_info(client: Client, silent: bool = False) -> None:
    """Print dashboard information to stdout.

    Args:
        client: Connected Dask client
        silent: If True, don't print anything (useful for testing)
    """
    if not silent:
        message = format_dashboard_message(client)
        print(message)
