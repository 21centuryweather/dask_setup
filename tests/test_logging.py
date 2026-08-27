"""Tests for dask_setup.logging configuration."""

from __future__ import annotations

import logging

import pytest

from dask_setup.logging import DaskSetupLogger, configure_from_env, configure_logging, get_logger


@pytest.fixture(autouse=True)
def _restore_logging_state():
    """Logging config is process-global; put it back after each test."""
    root = logging.getLogger("dask_setup")
    saved_level, saved_handlers = root.level, root.handlers[:]
    saved_configured = DaskSetupLogger._configured
    yield
    root.handlers[:] = saved_handlers
    root.setLevel(saved_level)
    DaskSetupLogger._configured = saved_configured


class TestConfigureIsNotANoOp:
    """configure() returned early once anything had configured logging.

    get_logger() auto-configures on first use, and every dask_setup module
    creates a logger at import time -- so by the time user code could call
    configure_logging(), the early return made it do nothing.  There was no
    way to turn on debug logging.
    """

    @pytest.mark.unit
    def test_explicit_configure_changes_the_level(self):
        get_logger("client")  # trigger the implicit first-use configuration
        configure_logging(level="DEBUG")
        assert logging.getLogger("dask_setup").isEnabledFor(logging.DEBUG)

    @pytest.mark.unit
    def test_level_can_be_changed_repeatedly(self):
        get_logger("client")
        for level in (logging.WARNING, logging.DEBUG, logging.ERROR, logging.INFO):
            configure_logging(level=level)
            assert logging.getLogger("dask_setup").level == level

    @pytest.mark.unit
    def test_existing_loggers_see_the_new_level(self):
        log = get_logger("resources")
        configure_logging(level="ERROR")
        assert not log.logger.isEnabledFor(logging.INFO)
        configure_logging(level="DEBUG")
        assert log.logger.isEnabledFor(logging.DEBUG)

    @pytest.mark.unit
    def test_reconfiguring_does_not_stack_handlers(self):
        for _ in range(5):
            configure_logging(level="INFO")
        assert len(logging.getLogger("dask_setup").handlers) == 1

    @pytest.mark.unit
    def test_unknown_level_name_is_rejected(self):
        with pytest.raises(ValueError, match="Unknown logging level"):
            configure_logging(level="LOUD")

    @pytest.mark.unit
    def test_a_non_level_logging_attribute_is_rejected(self):
        """getattr(logging, "GETLOGGER") used to raise AttributeError here."""
        with pytest.raises(ValueError, match="Unknown logging level"):
            configure_logging(level="getLogger")


class TestConfigureFromEnv:
    """DASK_SETUP_LOG_LEVEL was documented but nothing ever read it."""

    @pytest.mark.unit
    def test_env_level_is_honoured_on_first_use(self, monkeypatch):
        monkeypatch.setenv("DASK_SETUP_LOG_LEVEL", "DEBUG")
        DaskSetupLogger._configured = False
        log = get_logger("client")
        assert log.logger.isEnabledFor(logging.DEBUG)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "value,expected",
        [("DEBUG", logging.DEBUG), ("warning", logging.WARNING), ("ERROR", logging.ERROR)],
    )
    def test_env_level_values(self, monkeypatch, value, expected):
        monkeypatch.setenv("DASK_SETUP_LOG_LEVEL", value)
        configure_from_env()
        assert logging.getLogger("dask_setup").level == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("bogus", ["LOUD", "getLogger", "", "12"])
    def test_bogus_env_level_falls_back_to_info(self, monkeypatch, bogus):
        """This runs at import time -- it must never raise."""
        monkeypatch.setenv("DASK_SETUP_LOG_LEVEL", bogus)
        configure_from_env()
        assert logging.getLogger("dask_setup").level == logging.INFO

    @pytest.mark.unit
    def test_json_format_from_env(self, monkeypatch):
        monkeypatch.setenv("DASK_SETUP_LOG_FORMAT", "json")
        configure_from_env()
        handler = logging.getLogger("dask_setup").handlers[0]
        assert type(handler.formatter).__name__ == "JSONFormatter"
