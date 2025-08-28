"""Centralized logging configuration for dask_setup.

This module provides structured logging with consistent formatting across all dask_setup modules.
Supports both human-readable and structured (JSON) output formats.
"""

from __future__ import annotations

import logging
import os
import sys


class StructuredFormatter(logging.Formatter):
    """Custom formatter that adds structure and context to log messages."""

    def __init__(self, include_extra: bool = True, use_color: bool = True):
        """Initialize the structured formatter.

        Args:
            include_extra: Whether to include extra context fields in the output
            use_color: Whether to use color coding for log levels (terminal output)
        """
        self.include_extra = include_extra
        self.use_color = use_color and sys.stderr.isatty()

        # Color codes for terminal output
        self.colors = {
            "DEBUG": "\033[36m",  # Cyan
            "INFO": "\033[32m",  # Green
            "WARNING": "\033[33m",  # Yellow
            "ERROR": "\033[31m",  # Red
            "CRITICAL": "\033[35m",  # Magenta
            "RESET": "\033[0m",  # Reset
        }

        super().__init__()

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record with structured information."""
        # Base format
        level_name = record.levelname
        if self.use_color:
            level_color = self.colors.get(level_name, "")
            reset_color = self.colors["RESET"]
            level_name = f"{level_color}{level_name:<8}{reset_color}"
        else:
            level_name = f"{level_name:<8}"

        # Module context
        module_name = record.name.replace("dask_setup.", "")

        # Base message
        base_msg = f"{level_name} [{module_name}] {record.getMessage()}"

        # Add extra context if available and requested
        if self.include_extra and hasattr(record, "_extra_context"):
            context_parts = []
            for key, value in record._extra_context.items():
                context_parts.append(f"{key}={value}")

            if context_parts:
                context_str = " | ".join(context_parts)
                base_msg += f" ({context_str})"

        # Add exception info if present
        if record.exc_info:
            base_msg += "\n" + self.formatException(record.exc_info)

        return base_msg


class DaskSetupLogger:
    """Centralized logger for dask_setup with contextual information."""

    _loggers: dict[str, logging.Logger] = {}
    _configured = False

    @classmethod
    def configure(
        cls,
        level: int = logging.INFO,
        format_style: str = "structured",  # "structured" or "json"
        include_extra: bool = True,
        use_color: bool = True,
    ) -> None:
        """Configure logging for the entire dask_setup package.

        Args:
            level: Logging level (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format_style: Output format style ("structured" or "json")
            include_extra: Whether to include extra context in output
            use_color: Whether to use color coding (auto-detected for terminals)
        """
        if cls._configured:
            return  # Already configured

        # Configure root dask_setup logger
        root_logger = logging.getLogger("dask_setup")
        root_logger.setLevel(level)

        # Clear any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Create console handler
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)

        if format_style == "json":
            # JSON formatter for structured logging
            import json

            class JSONFormatter(logging.Formatter):
                def format(self, record):
                    log_data = {
                        "timestamp": self.formatTime(record),
                        "level": record.levelname,
                        "module": record.name,
                        "message": record.getMessage(),
                    }

                    if hasattr(record, "_extra_context"):
                        log_data.update(record._extra_context)

                    if record.exc_info:
                        log_data["exception"] = self.formatException(record.exc_info)

                    return json.dumps(log_data)

            formatter = JSONFormatter()
        else:
            # Structured human-readable formatter
            formatter = StructuredFormatter(include_extra=include_extra, use_color=use_color)

        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

        # Prevent propagation to avoid double logging
        root_logger.propagate = False

        cls._configured = True

    @classmethod
    def get_logger(cls, name: str) -> DaskSetupLoggerAdapter:
        """Get a logger for the specified module.

        Args:
            name: Module name (will be prefixed with 'dask_setup.' if not already)

        Returns:
            DaskSetupLoggerAdapter instance
        """
        # Ensure logging is configured
        if not cls._configured:
            cls.configure()

        # Normalize name
        if not name.startswith("dask_setup."):
            name = f"dask_setup.{name}"

        # Get or create logger
        if name not in cls._loggers:
            logger = logging.getLogger(name)
            cls._loggers[name] = logger

        return DaskSetupLoggerAdapter(cls._loggers[name])


class DaskSetupLoggerAdapter:
    """Logger adapter that provides contextual logging methods."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def _log_with_context(self, level: int, msg: str, *args, **context) -> None:
        """Log a message with optional context."""
        if self.logger.isEnabledFor(level):
            record = self.logger.makeRecord(
                name=self.logger.name, level=level, fn="", lno=0, msg=msg, args=args, exc_info=None
            )

            # Add context if provided
            if context:
                record._extra_context = context  # type: ignore

            self.logger.handle(record)

    def debug(self, msg: str, *args, **context) -> None:
        """Log a debug message with optional context."""
        self._log_with_context(logging.DEBUG, msg, *args, **context)

    def info(self, msg: str, *args, **context) -> None:
        """Log an info message with optional context."""
        self._log_with_context(logging.INFO, msg, *args, **context)

    def warning(self, msg: str, *args, **context) -> None:
        """Log a warning message with optional context."""
        self._log_with_context(logging.WARNING, msg, *args, **context)

    def error(self, msg: str, *args, **context) -> None:
        """Log an error message with optional context."""
        self._log_with_context(logging.ERROR, msg, *args, **context)

    def critical(self, msg: str, *args, **context) -> None:
        """Log a critical message with optional context."""
        self._log_with_context(logging.CRITICAL, msg, *args, **context)

    def exception(self, msg: str, *args, **context) -> None:
        """Log an exception with traceback."""
        if self.logger.isEnabledFor(logging.ERROR):
            record = self.logger.makeRecord(
                name=self.logger.name,
                level=logging.ERROR,
                fn="",
                lno=0,
                msg=msg,
                args=args,
                exc_info=sys.exc_info(),
            )

            if context:
                record._extra_context = context  # type: ignore

            self.logger.handle(record)


def get_logger(module_name: str) -> DaskSetupLoggerAdapter:
    """Convenience function to get a logger for a module.

    Args:
        module_name: Name of the module (e.g., 'client', 'resources')

    Returns:
        DaskSetupLoggerAdapter instance
    """
    return DaskSetupLogger.get_logger(module_name)


def configure_logging(
    level: str | int = "INFO",
    format_style: str = "structured",
    use_color: bool | None = None,
) -> None:
    """Configure logging for dask_setup package.

    Args:
        level: Logging level (string or logging constant)
        format_style: "structured" for human-readable, "json" for structured data
        use_color: Whether to use color output (None = auto-detect)
    """
    # Convert string level to int if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper())

    # Auto-detect color support if not specified
    if use_color is None:
        use_color = sys.stderr.isatty()

    DaskSetupLogger.configure(
        level=level,
        format_style=format_style,
        use_color=use_color,
    )


def configure_from_env() -> None:
    """Configure logging based on environment variables.

    Environment variables:
    - DASK_SETUP_LOG_LEVEL: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    - DASK_SETUP_LOG_FORMAT: Format style (structured, json)
    - DASK_SETUP_LOG_COLOR: Use color output (true, false, auto)
    """
    # Get log level from environment
    level = os.getenv("DASK_SETUP_LOG_LEVEL", "INFO").upper()
    try:
        level_int = getattr(logging, level)
    except AttributeError:
        level_int = logging.INFO

    # Get format style
    format_style = os.getenv("DASK_SETUP_LOG_FORMAT", "structured").lower()
    if format_style not in ("structured", "json"):
        format_style = "structured"

    # Get color setting
    color_setting = os.getenv("DASK_SETUP_LOG_COLOR", "auto").lower()
    if color_setting == "true":
        use_color = True
    elif color_setting == "false":
        use_color = False
    else:  # auto
        use_color = sys.stderr.isatty()

    configure_logging(level=level_int, format_style=format_style, use_color=use_color)
