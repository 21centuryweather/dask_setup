"""Configuration management for dask_setup with profile support."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, ClassVar

from .error_handling import ConfigurationValidationError


@dataclass
class DaskSetupConfig:
    """Configuration for dask_setup with validation and defaults.

    This class encapsulates all configuration parameters for setup_dask_client(),
    provides validation, and supports serialization for profile management.
    """

    # Core parameters (matches setup_dask_client signature)
    workload_type: str = "io"
    max_workers: int | None = None
    reserve_mem_gb: float = 50.0
    max_mem_gb: float | None = None
    dashboard: bool = True
    adaptive: bool = False
    min_workers: int | None = None

    # Extended parameters for advanced configuration
    temp_base_dir: str | None = None
    dashboard_port: int | None = None
    silence_logs: bool = False

    # Memory management thresholds (advanced)
    memory_target: float = 0.75
    memory_spill: float = 0.85
    memory_pause: float = 0.92
    memory_terminate: float = 0.98

    # Spill compression settings
    spill_compression: str = "auto"
    comm_compression: bool = False

    # Parallel I/O settings
    spill_threads: int | None = None

    # Xarray integration
    suggest_chunks: bool = False

    # I/O optimization settings
    io_format: str | None = None  # "zarr", "netcdf", or None for auto-detect
    io_target_chunk_mb: tuple[float, float] = (128, 512)
    io_access_pattern: str = "auto"  # "sequential", "random", "streaming", "compute", "auto"
    io_storage_location: str = "auto"  # "local", "cloud", "network", "auto"
    io_compression_level: int | None = None  # Override default compression level

    # Profile metadata
    name: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)

    # Internal fields
    _skip_validation: bool = field(default=False, init=False)

    # Validation constraints
    VALID_WORKLOAD_TYPES: ClassVar[set[str]] = {"cpu", "io", "mixed"}
    MIN_RESERVE_MEM: ClassVar[float] = 1.0
    MAX_RESERVE_MEM: ClassVar[float] = 1000.0
    VALID_COMPRESSION_ALGORITHMS: ClassVar[set[str]] = {
        "auto",
        "lz4",
        "zstd",
        "snappy",
        "gzip",
        "blosc",
        "zlib",
        "bz2",
        "lzma",
        "false",
    }
    VALID_IO_FORMATS: ClassVar[set[str]] = {"zarr", "netcdf"}
    VALID_IO_ACCESS_PATTERNS: ClassVar[set[str]] = {
        "sequential",
        "random",
        "streaming",
        "compute",
        "auto",
    }
    VALID_IO_STORAGE_LOCATIONS: ClassVar[set[str]] = {"local", "cloud", "network", "auto"}

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Skip validation during merge operations
        if not self._skip_validation:
            self.validate()

    def validate(self) -> None:
        """Validate all configuration parameters.

        Raises:
            ConfigurationValidationError: If any parameter is invalid
        """
        field_errors = {}

        # Validate workload_type
        if self.workload_type not in self.VALID_WORKLOAD_TYPES:
            field_errors["workload_type"] = (
                f"must be one of {sorted(self.VALID_WORKLOAD_TYPES)} (got '{self.workload_type}')"
            )

        # Validate workers
        if self.max_workers is not None and self.max_workers <= 0:
            field_errors["max_workers"] = f"must be positive (got {self.max_workers})"

        if self.min_workers is not None and self.min_workers <= 0:
            field_errors["min_workers"] = f"must be positive (got {self.min_workers})"

        if (
            self.max_workers is not None
            and self.min_workers is not None
            and self.min_workers > self.max_workers
        ):
            field_errors["min_workers"] = f"cannot be greater than max_workers ({self.max_workers})"

        # Validate memory parameters
        if not (self.MIN_RESERVE_MEM <= self.reserve_mem_gb <= self.MAX_RESERVE_MEM):
            field_errors["reserve_mem_gb"] = (
                f"must be between {self.MIN_RESERVE_MEM} and {self.MAX_RESERVE_MEM} GB (got {self.reserve_mem_gb})"
            )

        if self.max_mem_gb is not None and self.max_mem_gb <= 0:
            field_errors["max_mem_gb"] = f"must be positive (got {self.max_mem_gb})"

        # Validate memory thresholds (should be increasing)
        thresholds = [
            ("memory_target", self.memory_target),
            ("memory_spill", self.memory_spill),
            ("memory_pause", self.memory_pause),
            ("memory_terminate", self.memory_terminate),
        ]

        for name, value in thresholds:
            if not (0.0 < value <= 1.0):
                field_errors[name] = f"must be between 0.0 and 1.0 (got {value})"

        # Check threshold ordering
        if not (self.memory_target < self.memory_spill < self.memory_pause < self.memory_terminate):
            field_errors["memory_thresholds"] = (
                "must be in increasing order: target < spill < pause < terminate"
            )

        # Validate dashboard port
        if self.dashboard_port is not None and not (1024 <= self.dashboard_port <= 65535):
            field_errors["dashboard_port"] = (
                f"must be between 1024 and 65535 (got {self.dashboard_port})"
            )

        # Validate compression settings
        if self.spill_compression not in self.VALID_COMPRESSION_ALGORITHMS:
            field_errors["spill_compression"] = (
                f"must be one of {sorted(self.VALID_COMPRESSION_ALGORITHMS)} (got '{self.spill_compression}')"
            )

        # Validate spill threads
        if self.spill_threads is not None:
            if not isinstance(self.spill_threads, int) or self.spill_threads <= 0:
                field_errors["spill_threads"] = "must be a positive integer"
            elif self.spill_threads > 16:
                field_errors["spill_threads"] = "should not exceed 16 for performance reasons"

        # Validate I/O optimization settings
        if self.io_format is not None and self.io_format not in self.VALID_IO_FORMATS:
            field_errors["io_format"] = f"must be one of {sorted(self.VALID_IO_FORMATS)}"

        if self.io_access_pattern not in self.VALID_IO_ACCESS_PATTERNS:
            field_errors["io_access_pattern"] = (
                f"must be one of {sorted(self.VALID_IO_ACCESS_PATTERNS)}"
            )

        if self.io_storage_location not in self.VALID_IO_STORAGE_LOCATIONS:
            field_errors["io_storage_location"] = (
                f"must be one of {sorted(self.VALID_IO_STORAGE_LOCATIONS)}"
            )

        if (
            len(self.io_target_chunk_mb) != 2
            or self.io_target_chunk_mb[0] <= 0
            or self.io_target_chunk_mb[1] <= 0
            or self.io_target_chunk_mb[0] >= self.io_target_chunk_mb[1]
        ):
            field_errors["io_target_chunk_mb"] = (
                "must be a tuple of (min, max) positive values with min < max"
            )

        if self.io_compression_level is not None and (
            not isinstance(self.io_compression_level, int)
            or not (0 <= self.io_compression_level <= 9)
        ):
            field_errors["io_compression_level"] = "must be an integer between 0 and 9"

        if field_errors:
            # Use enhanced ConfigurationValidationError for better error messages
            raise ConfigurationValidationError(
                field_errors=field_errors,
                invalid_config={k: getattr(self, k, None) for k in field_errors},
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary for serialization."""
        return {
            # Core parameters
            "workload_type": self.workload_type,
            "max_workers": self.max_workers,
            "reserve_mem_gb": self.reserve_mem_gb,
            "max_mem_gb": self.max_mem_gb,
            "dashboard": self.dashboard,
            "adaptive": self.adaptive,
            "min_workers": self.min_workers,
            # Extended parameters
            "temp_base_dir": self.temp_base_dir,
            "dashboard_port": self.dashboard_port,
            "silence_logs": self.silence_logs,
            # Memory thresholds
            "memory_target": self.memory_target,
            "memory_spill": self.memory_spill,
            "memory_pause": self.memory_pause,
            "memory_terminate": self.memory_terminate,
            # Compression settings
            "spill_compression": self.spill_compression,
            "comm_compression": self.comm_compression,
            # Parallel I/O settings
            "spill_threads": self.spill_threads,
            # Xarray integration
            "suggest_chunks": self.suggest_chunks,
            # I/O optimization settings
            "io_format": self.io_format,
            "io_target_chunk_mb": self.io_target_chunk_mb,
            "io_access_pattern": self.io_access_pattern,
            "io_storage_location": self.io_storage_location,
            "io_compression_level": self.io_compression_level,
            # Metadata
            "name": self.name,
            "description": self.description,
            "tags": self.tags.copy(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], skip_validation: bool = False) -> DaskSetupConfig:
        """Create configuration from dictionary.

        Args:
            data: Dictionary with configuration parameters
            skip_validation: If True, skip validation during creation

        Returns:
            DaskSetupConfig instance
        """
        # Filter out None values and unknown keys
        valid_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields and v is not None}

        # Create instance with optional validation skip
        instance = cls(**filtered_data)
        if skip_validation:
            instance._skip_validation = True

        return instance

    def merge_with(self, other: DaskSetupConfig | None) -> DaskSetupConfig:
        """Merge this configuration with another, with other taking precedence.

        Args:
            other: Configuration to merge (higher precedence)

        Returns:
            New merged configuration
        """
        if other is None:
            return self

        # Start with self's values
        merged_dict = self.to_dict()
        other_dict = other.to_dict()

        # Override with non-None values from other
        for key, value in other_dict.items():
            if value is not None and key != "tags":  # Tags are handled specially
                merged_dict[key] = value

        # Merge tags specially (combine lists)
        merged_dict["tags"] = list(set(self.tags + other.tags))

        # Create merged config and validate at the end
        merged = DaskSetupConfig.from_dict(merged_dict, skip_validation=True)
        merged.validate()  # Validate the final result
        return merged

    def get_setup_client_kwargs(self) -> dict[str, Any]:
        """Get parameters for setup_dask_client() function.

        Returns:
            Dictionary of parameters that can be passed to setup_dask_client()
        """
        return {
            "workload_type": self.workload_type,
            "max_workers": self.max_workers,
            "reserve_mem_gb": self.reserve_mem_gb,
            "max_mem_gb": self.max_mem_gb,
            "dashboard": self.dashboard,
            "adaptive": self.adaptive,
            "min_workers": self.min_workers,
        }

    def validate_against_environment(self) -> list[str]:
        """Validate configuration against current environment.

        Returns:
            List of warning messages about potentially problematic settings
        """
        warnings = []

        # Check for potentially high memory reservations
        if self.reserve_mem_gb > 100:
            warnings.append(
                f"High memory reservation ({self.reserve_mem_gb} GB) may cause "
                "insufficient memory for workers"
            )

        # Check for adaptive scaling with single worker
        if self.adaptive and self.max_workers == 1:
            warnings.append("Adaptive scaling with max_workers=1 has no effect")

        # Check workload type vs environment
        if os.getenv("PBS_JOBFS") and self.workload_type == "io":
            # This is actually good - no warning needed
            pass
        elif not os.getenv("PBS_JOBFS") and self.reserve_mem_gb > 20:
            warnings.append(
                "High memory reservation without PBS_JOBFS detected - "
                "consider using a lower reserve_mem_gb for non-HPC environments"
            )

        return warnings


@dataclass
class ConfigProfile:
    """A named configuration profile with metadata.

    Profiles allow users to save and reuse common configurations for
    different types of workloads or environments.
    """

    name: str
    config: DaskSetupConfig
    builtin: bool = False
    created_at: str | None = None
    modified_at: str | None = None

    def __post_init__(self) -> None:
        """Set profile name in config if not already set."""
        if not self.config.name:
            self.config.name = self.name

    @property
    def description(self) -> str:
        """Get profile description."""
        return self.config.description or f"Configuration profile: {self.name}"

    @property
    def tags(self) -> list[str]:
        """Get profile tags."""
        return self.config.tags.copy()

    def to_dict(self) -> dict[str, Any]:
        """Convert profile to dictionary for serialization."""
        return {
            "name": self.name,
            "config": self.config.to_dict(),
            "builtin": self.builtin,
            "created_at": self.created_at,
            "modified_at": self.modified_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ConfigProfile:
        """Create profile from dictionary.

        Args:
            data: Dictionary with profile data

        Returns:
            ConfigProfile instance
        """
        config_data = data.get("config", {})
        config = DaskSetupConfig.from_dict(config_data)

        return cls(
            name=data["name"],
            config=config,
            builtin=data.get("builtin", False),
            created_at=data.get("created_at"),
            modified_at=data.get("modified_at"),
        )
