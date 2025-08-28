"""Unit tests for dask_setup.dashboard module."""

from unittest.mock import MagicMock, patch

import pytest
from dask.distributed import Client

from dask_setup.dashboard import (
    format_dashboard_message,
    get_dashboard_info,
    print_dashboard_info,
)


class TestGetDashboardInfo:
    """Test dashboard information extraction function."""

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_basic(self, mock_gethostname):
        """Test basic dashboard info extraction."""
        mock_gethostname.return_value = "compute-node-01"

        # Mock client with dashboard link
        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://192.168.1.10:8787/status"

        result = get_dashboard_info(mock_client)

        assert result["link"] == "http://192.168.1.10:8787/status"
        assert result["host"] == "192.168.1.10"
        assert result["port"] == "8787"
        assert result["local_host"] == "compute-node-01"

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_custom_port(self, mock_gethostname):
        """Test dashboard info extraction with custom port."""
        mock_gethostname.return_value = "hpc-node-05"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://10.0.0.5:9999/status"

        result = get_dashboard_info(mock_client)

        assert result["link"] == "http://10.0.0.5:9999/status"
        assert result["host"] == "10.0.0.5"
        assert result["port"] == "9999"
        assert result["local_host"] == "hpc-node-05"

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_localhost(self, mock_gethostname):
        """Test dashboard info extraction with localhost."""
        mock_gethostname.return_value = "workstation"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://127.0.0.1:8888/status"

        result = get_dashboard_info(mock_client)

        assert result["link"] == "http://127.0.0.1:8888/status"
        assert result["host"] == "127.0.0.1"
        assert result["port"] == "8888"
        assert result["local_host"] == "workstation"

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_no_dashboard(self, mock_gethostname):
        """Test dashboard info extraction when dashboard is disabled."""
        mock_gethostname.return_value = "server-01"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = None

        result = get_dashboard_info(mock_client)

        assert result["link"] == ""
        assert result["host"] == ""
        assert result["port"] == ""
        assert result["local_host"] == "server-01"

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_empty_dashboard_link(self, mock_gethostname):
        """Test dashboard info extraction with empty dashboard link."""
        mock_gethostname.return_value = "node-test"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = ""

        result = get_dashboard_info(mock_client)

        assert result["link"] == ""
        assert result["host"] == ""
        assert result["port"] == ""
        assert result["local_host"] == "node-test"

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_default_port(self, mock_gethostname):
        """Test dashboard info extraction with default port fallback."""
        mock_gethostname.return_value = "cluster-head"

        # URL without explicit port should default to 8787
        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://192.168.1.100/status"

        result = get_dashboard_info(mock_client)

        assert result["link"] == "http://192.168.1.100/status"
        assert result["host"] == "192.168.1.100"
        assert result["port"] == "8787"  # Default port
        assert result["local_host"] == "cluster-head"

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_get_dashboard_info_no_hostname_in_url(self, mock_gethostname):
        """Test dashboard info extraction with malformed URL (no hostname)."""
        mock_gethostname.return_value = "fallback-host"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://:8787/status"

        result = get_dashboard_info(mock_client)

        assert result["link"] == "http://:8787/status"
        assert result["host"] == "127.0.0.1"  # Default fallback
        assert result["port"] == "8787"
        assert result["local_host"] == "fallback-host"

    @patch("socket.gethostname")
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "dashboard_url,expected_host,expected_port",
        [
            ("http://node01:8787/status", "node01", "8787"),
            ("http://192.168.0.100:9000/status", "192.168.0.100", "9000"),
            ("https://secure-cluster:8443/dashboard", "secure-cluster", "8443"),
            ("http://cluster.example.com:8080/", "cluster.example.com", "8080"),
            ("http://localhost:8788", "localhost", "8788"),
            ("http://[::1]:8787/status", "::1", "8787"),  # IPv6 localhost
        ],
    )
    def test_get_dashboard_info_various_urls(
        self, mock_gethostname, dashboard_url, expected_host, expected_port
    ):
        """Test dashboard info extraction with various URL formats."""
        mock_gethostname.return_value = "test-host"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = dashboard_url

        result = get_dashboard_info(mock_client)

        assert result["link"] == dashboard_url
        assert result["host"] == expected_host
        assert result["port"] == expected_port
        assert result["local_host"] == "test-host"


class TestFormatDashboardMessage:
    """Test dashboard message formatting function."""

    @patch("dask_setup.dashboard.get_dashboard_info")
    @pytest.mark.unit
    def test_format_dashboard_message_basic(self, mock_get_info):
        """Test basic dashboard message formatting."""
        mock_get_info.return_value = {
            "link": "http://compute-node-01:8787/status",
            "host": "compute-node-01",
            "port": "8787",
            "local_host": "compute-node-01",
        }

        mock_client = MagicMock(spec=Client)
        result = format_dashboard_message(mock_client)

        expected = (
            "Dask dashboard: http://compute-node-01:8787/status\n"
            "Tunnel from your laptop (run locally):\n"
            "  ssh -N -L 8787:compute-node-01:8787 gadi.nci.org.au\n"
            "Then open: http://localhost:8787"
        )

        assert result == expected
        mock_get_info.assert_called_once_with(mock_client)

    @patch("dask_setup.dashboard.get_dashboard_info")
    @pytest.mark.unit
    def test_format_dashboard_message_custom_port(self, mock_get_info):
        """Test dashboard message formatting with custom port."""
        mock_get_info.return_value = {
            "link": "http://worker-node-05:9999/status",
            "host": "worker-node-05",
            "port": "9999",
            "local_host": "worker-node-05",
        }

        mock_client = MagicMock(spec=Client)
        result = format_dashboard_message(mock_client)

        expected = (
            "Dask dashboard: http://worker-node-05:9999/status\n"
            "Tunnel from your laptop (run locally):\n"
            "  ssh -N -L 8787:worker-node-05:9999 gadi.nci.org.au\n"
            "Then open: http://localhost:8787"
        )

        assert result == expected

    @patch("dask_setup.dashboard.get_dashboard_info")
    @pytest.mark.unit
    def test_format_dashboard_message_disabled(self, mock_get_info):
        """Test dashboard message formatting when dashboard is disabled."""
        mock_get_info.return_value = {"link": "", "host": "", "port": "", "local_host": "some-host"}

        mock_client = MagicMock(spec=Client)
        result = format_dashboard_message(mock_client)

        assert result == "Dashboard is disabled."
        mock_get_info.assert_called_once_with(mock_client)

    @patch("dask_setup.dashboard.get_dashboard_info")
    @pytest.mark.unit
    def test_format_dashboard_message_different_hosts(self, mock_get_info):
        """Test dashboard message formatting with different hostnames."""
        test_cases = [
            {
                "link": "http://hpc-gpu-01:8888/status",
                "host": "hpc-gpu-01",
                "port": "8888",
                "local_host": "hpc-gpu-01",
                "expected_ssh": "ssh -N -L 8787:hpc-gpu-01:8888 gadi.nci.org.au",
            },
            {
                "link": "http://cluster-head:8080/dashboard",
                "host": "cluster-head",
                "port": "8080",
                "local_host": "cluster-head",
                "expected_ssh": "ssh -N -L 8787:cluster-head:8080 gadi.nci.org.au",
            },
        ]

        mock_client = MagicMock(spec=Client)

        for case in test_cases:
            mock_get_info.return_value = {
                "link": case["link"],
                "host": case["host"],
                "port": case["port"],
                "local_host": case["local_host"],
            }

            result = format_dashboard_message(mock_client)

            assert case["expected_ssh"] in result
            assert f"Dask dashboard: {case['link']}" in result
            assert "Then open: http://localhost:8787" in result


class TestPrintDashboardInfo:
    """Test dashboard info printing function."""

    @patch("dask_setup.dashboard.format_dashboard_message")
    @patch("builtins.print")
    @pytest.mark.unit
    def test_print_dashboard_info_normal(self, mock_print, mock_format):
        """Test normal dashboard info printing."""
        mock_format.return_value = "Test dashboard message"

        mock_client = MagicMock(spec=Client)
        print_dashboard_info(mock_client)

        mock_format.assert_called_once_with(mock_client)
        mock_print.assert_called_once_with("Test dashboard message")

    @patch("dask_setup.dashboard.format_dashboard_message")
    @patch("builtins.print")
    @pytest.mark.unit
    def test_print_dashboard_info_silent(self, mock_print, mock_format):
        """Test dashboard info printing in silent mode."""
        mock_format.return_value = "Test dashboard message"

        mock_client = MagicMock(spec=Client)
        print_dashboard_info(mock_client, silent=True)

        # format_dashboard_message should not be called in silent mode
        mock_format.assert_not_called()
        mock_print.assert_not_called()

    @patch("dask_setup.dashboard.format_dashboard_message")
    @patch("builtins.print")
    @pytest.mark.unit
    def test_print_dashboard_info_silent_false_explicit(self, mock_print, mock_format):
        """Test dashboard info printing with explicit silent=False."""
        mock_format.return_value = "Another test message"

        mock_client = MagicMock(spec=Client)
        print_dashboard_info(mock_client, silent=False)

        mock_format.assert_called_once_with(mock_client)
        mock_print.assert_called_once_with("Another test message")

    @patch("dask_setup.dashboard.format_dashboard_message")
    @patch("builtins.print")
    @pytest.mark.unit
    def test_print_dashboard_info_disabled_dashboard(self, mock_print, mock_format):
        """Test printing info for disabled dashboard."""
        mock_format.return_value = "Dashboard is disabled."

        mock_client = MagicMock(spec=Client)
        print_dashboard_info(mock_client)

        mock_format.assert_called_once_with(mock_client)
        mock_print.assert_called_once_with("Dashboard is disabled.")


class TestDashboardIntegration:
    """Integration tests for dashboard functions working together."""

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_full_dashboard_workflow(self, mock_gethostname):
        """Test complete workflow from info extraction to message formatting."""
        mock_gethostname.return_value = "gpu-node-03"

        # Create mock client
        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://gpu-node-03:8889/status"

        # Test info extraction
        info = get_dashboard_info(mock_client)
        assert info["link"] == "http://gpu-node-03:8889/status"
        assert info["host"] == "gpu-node-03"
        assert info["port"] == "8889"
        assert info["local_host"] == "gpu-node-03"

        # Test message formatting
        message = format_dashboard_message(mock_client)
        expected_lines = [
            "Dask dashboard: http://gpu-node-03:8889/status",
            "Tunnel from your laptop (run locally):",
            "  ssh -N -L 8787:gpu-node-03:8889 gadi.nci.org.au",
            "Then open: http://localhost:8787",
        ]

        for line in expected_lines:
            assert line in message

    @patch("socket.gethostname")
    @patch("builtins.print")
    @pytest.mark.unit
    def test_full_workflow_with_printing(self, mock_print, mock_gethostname):
        """Test complete workflow including printing."""
        mock_gethostname.return_value = "test-node-42"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://test-node-42:8787/status"

        # Test the complete workflow
        print_dashboard_info(mock_client, silent=False)

        # Verify print was called with the formatted message
        mock_print.assert_called_once()
        printed_message = mock_print.call_args[0][0]

        assert "Dask dashboard: http://test-node-42:8787/status" in printed_message
        assert "ssh -N -L 8787:test-node-42:8787 gadi.nci.org.au" in printed_message
        assert "Then open: http://localhost:8787" in printed_message

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_disabled_dashboard_workflow(self, mock_gethostname):
        """Test workflow when dashboard is disabled."""
        mock_gethostname.return_value = "disabled-node"

        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = None

        # Test info extraction
        info = get_dashboard_info(mock_client)
        assert info["link"] == ""
        assert info["host"] == ""
        assert info["port"] == ""
        assert info["local_host"] == "disabled-node"

        # Test message formatting
        message = format_dashboard_message(mock_client)
        assert message == "Dashboard is disabled."

    @patch("socket.gethostname")
    @pytest.mark.unit
    def test_edge_cases_and_malformed_urls(self, mock_gethostname):
        """Test edge cases with malformed or unusual URLs."""
        mock_gethostname.return_value = "edge-case-node"

        edge_cases = [
            # URL without port
            {
                "url": "http://no-port-host/status",
                "expected_host": "no-port-host",
                "expected_port": "8787",  # Should default to 8787
            },
            # URL with unusual port
            {
                "url": "http://weird-port-host:12345/dashboard",
                "expected_host": "weird-port-host",
                "expected_port": "12345",
            },
            # HTTPS URL
            {
                "url": "https://secure-host:8443/status",
                "expected_host": "secure-host",
                "expected_port": "8443",
            },
        ]

        mock_client = MagicMock(spec=Client)

        for case in edge_cases:
            mock_client.dashboard_link = case["url"]

            info = get_dashboard_info(mock_client)
            assert info["host"] == case["expected_host"]
            assert info["port"] == case["expected_port"]
            assert info["local_host"] == "edge-case-node"

            # Verify message formatting works
            message = format_dashboard_message(mock_client)
            assert info["link"] in message
            assert f"ssh -N -L 8787:{info['local_host']}:{info['port']} gadi.nci.org.au" in message

    @pytest.mark.unit
    def test_real_socket_hostname_integration(self):
        """Test with real socket.gethostname() call (no mocking)."""
        mock_client = MagicMock(spec=Client)
        mock_client.dashboard_link = "http://127.0.0.1:8787/status"

        # This will use the real hostname from the system
        info = get_dashboard_info(mock_client)

        # Verify the structure is correct
        assert "link" in info
        assert "host" in info
        assert "port" in info
        assert "local_host" in info

        # Verify specific values
        assert info["link"] == "http://127.0.0.1:8787/status"
        assert info["host"] == "127.0.0.1"
        assert info["port"] == "8787"

        # local_host should be a non-empty string (actual hostname)
        assert isinstance(info["local_host"], str)
        assert len(info["local_host"]) > 0

        # Verify message formatting works with real hostname
        message = format_dashboard_message(mock_client)
        assert "Dask dashboard: http://127.0.0.1:8787/status" in message
        assert f"ssh -N -L 8787:{info['local_host']}:8787 gadi.nci.org.au" in message
