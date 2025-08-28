"""Custom exceptions for the dask_setup package."""

from __future__ import annotations


class DaskSetupError(Exception):
    """Base exception for all dask_setup errors."""

    pass


class InsufficientResourcesError(DaskSetupError):
    """Raised when system resources are insufficient for the requested configuration."""

    def __init__(
        self, required_mem: float, available_mem: float, suggested_actions: list[str] | None = None
    ) -> None:
        self.required_mem = required_mem
        self.available_mem = available_mem
        self.suggested_actions = suggested_actions or []

        message = (
            f"❌ Insufficient memory for configuration:\n"
            f"   - Required: {required_mem:.1f} GB\n"
            f"   - Available: {available_mem:.1f} GB\n"
            f"   - Shortfall: {required_mem - available_mem:.1f} GB"
        )

        if self.suggested_actions:
            message += "\n\n💡 Suggestions:\n" + "\n".join(
                f"   {i + 1}. {action}" for i, action in enumerate(self.suggested_actions)
            )

        super().__init__(message)


class InvalidConfigurationError(DaskSetupError):
    """Raised when configuration parameters are invalid."""

    pass


class ResourceDetectionError(DaskSetupError):
    """Raised when resource detection fails."""

    pass
