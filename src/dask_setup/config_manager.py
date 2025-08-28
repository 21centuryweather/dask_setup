"""Configuration file management and profile loading/saving."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import yaml

from .config import ConfigProfile, DaskSetupConfig
from .error_handling import ConfigurationValidationError
from .exceptions import InvalidConfigurationError


class ConfigManager:
    """Manages loading and saving of configuration profiles."""

    def __init__(self, config_dir: Path | str | None = None) -> None:
        """Initialize configuration manager.

        Args:
            config_dir: Configuration directory path. If None, uses ~/.dask_setup/
        """
        if config_dir is None:
            config_dir = Path.home() / ".dask_setup"

        self.config_dir = Path(config_dir)
        self.profiles_dir = self.config_dir / "profiles"
        self.builtin_profiles: dict[str, ConfigProfile] = {}

        # Initialize builtin profiles
        self._init_builtin_profiles()

    def _init_builtin_profiles(self) -> None:
        """Initialize built-in configuration profiles."""
        self.builtin_profiles = {
            "climate_analysis": ConfigProfile(
                name="climate_analysis",
                config=DaskSetupConfig(
                    workload_type="cpu",
                    reserve_mem_gb=60.0,
                    adaptive=False,
                    dashboard=True,
                    description="Optimized for climate data analysis with large arrays and heavy compute",
                    tags=["climate", "cpu-heavy", "large-memory", "analysis"],
                ),
                builtin=True,
            ),
            "zarr_io_heavy": ConfigProfile(
                name="zarr_io_heavy",
                config=DaskSetupConfig(
                    workload_type="io",
                    reserve_mem_gb=40.0,
                    adaptive=False,
                    dashboard=True,
                    description="Optimized for heavy Zarr I/O operations with many files",
                    tags=["zarr", "io-heavy", "files", "storage"],
                ),
                builtin=True,
            ),
            "development": ConfigProfile(
                name="development",
                config=DaskSetupConfig(
                    workload_type="mixed",
                    max_workers=2,
                    reserve_mem_gb=8.0,
                    adaptive=False,
                    dashboard=True,
                    description="Lightweight configuration for development and testing",
                    tags=["development", "testing", "lightweight", "local"],
                ),
                builtin=True,
            ),
            "production": ConfigProfile(
                name="production",
                config=DaskSetupConfig(
                    workload_type="mixed",
                    reserve_mem_gb=80.0,
                    adaptive=True,
                    min_workers=4,
                    dashboard=False,
                    silence_logs=True,
                    description="Robust production configuration with conservative memory usage",
                    tags=["production", "robust", "conservative", "adaptive"],
                ),
                builtin=True,
            ),
            "interactive": ConfigProfile(
                name="interactive",
                config=DaskSetupConfig(
                    workload_type="mixed",
                    max_workers=4,
                    reserve_mem_gb=20.0,
                    adaptive=False,
                    dashboard=True,
                    description="Optimized for interactive Jupyter notebook usage",
                    tags=["interactive", "jupyter", "notebook", "balanced"],
                ),
                builtin=True,
            ),
        }

    def ensure_config_dir(self) -> None:
        """Ensure configuration directory exists."""
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.profiles_dir.mkdir(parents=True, exist_ok=True)

        # Create README if it doesn't exist
        readme_path = self.config_dir / "README.md"
        if not readme_path.exists():
            readme_content = """# Dask Setup Configuration Directory

This directory contains your dask_setup configuration profiles.

## Files
- `profiles/` - Directory containing your custom profile files (YAML format)
- Built-in profiles are available without files and cannot be modified

## Built-in Profiles
- `climate_analysis` - CPU-heavy workloads with large memory
- `zarr_io_heavy` - I/O intensive operations 
- `development` - Lightweight for testing
- `production` - Conservative production settings
- `interactive` - Balanced for Jupyter notebooks

## Creating Custom Profiles
Use `dask-setup profile create <name>` or manually create YAML files in the profiles/ directory.
"""
            readme_path.write_text(readme_content)

    def list_profiles(self) -> dict[str, ConfigProfile]:
        """List all available profiles (builtin + user).

        Returns:
            Dictionary of profile name -> ConfigProfile
        """
        profiles = self.builtin_profiles.copy()

        # Load user profiles
        if self.profiles_dir.exists():
            for profile_file in self.profiles_dir.glob("*.yaml"):
                try:
                    profile = self.load_profile_from_file(profile_file)
                    profiles[profile.name] = profile
                except Exception as e:
                    # Log warning but don't fail
                    print(
                        f"Warning: Could not load profile {profile_file.name}: {e}", file=sys.stderr
                    )

        return profiles

    def get_profile(self, name: str) -> ConfigProfile | None:
        """Get a specific profile by name.

        Args:
            name: Profile name

        Returns:
            ConfigProfile if found, None otherwise
        """
        # Check builtin profiles first
        if name in self.builtin_profiles:
            return self.builtin_profiles[name]

        # Check user profiles
        profile_file = self.profiles_dir / f"{name}.yaml"
        if profile_file.exists():
            try:
                return self.load_profile_from_file(profile_file)
            except Exception as e:
                raise InvalidConfigurationError(f"Failed to load profile '{name}': {e}") from e

        return None

    def save_profile(self, profile: ConfigProfile) -> None:
        """Save a profile to disk.

        Args:
            profile: Profile to save

        Raises:
            InvalidConfigurationError: If trying to save builtin profile or validation fails
        """
        if profile.builtin:
            raise InvalidConfigurationError(f"Cannot save builtin profile '{profile.name}'")

        self.ensure_config_dir()

        # Update timestamps
        now = datetime.now().isoformat()
        if profile.created_at is None:
            profile.created_at = now
        profile.modified_at = now

        # Validate before saving
        try:
            profile.config.validate()
        except ConfigurationValidationError as e:
            # Convert enhanced error to simple error for backward compatibility
            raise InvalidConfigurationError(
                f"Configuration validation failed: {str(e).split(':', 1)[-1].strip()}"
            ) from e
        except InvalidConfigurationError as e:
            if "Configuration validation failed" in str(e):
                raise e  # Already wrapped
            else:
                raise InvalidConfigurationError(f"Configuration validation failed: {e}") from e

        # Save to file
        profile_file = self.profiles_dir / f"{profile.name}.yaml"
        with open(profile_file, "w") as f:
            yaml.safe_dump(profile.to_dict(), f, default_flow_style=False, indent=2)

    def delete_profile(self, name: str) -> bool:
        """Delete a user profile.

        Args:
            name: Profile name to delete

        Returns:
            True if deleted, False if not found

        Raises:
            InvalidConfigurationError: If trying to delete builtin profile
        """
        if name in self.builtin_profiles:
            raise InvalidConfigurationError(f"Cannot delete builtin profile '{name}'")

        profile_file = self.profiles_dir / f"{name}.yaml"
        if profile_file.exists():
            profile_file.unlink()
            return True
        return False

    def load_profile_from_file(self, file_path: Path) -> ConfigProfile:
        """Load a profile from a YAML file.

        Args:
            file_path: Path to YAML file

        Returns:
            ConfigProfile instance

        Raises:
            InvalidConfigurationError: If file is invalid
        """
        try:
            with open(file_path) as f:
                data = yaml.safe_load(f)

            if not isinstance(data, dict):
                raise InvalidConfigurationError("Profile file must contain a YAML object")

            return ConfigProfile.from_dict(data)

        except yaml.YAMLError as e:
            raise InvalidConfigurationError(f"Invalid YAML in profile file: {e}") from e
        except OSError as e:
            raise InvalidConfigurationError(f"Could not read profile file: {e}") from e

    def validate_profile(self, name: str) -> tuple[bool, list[str], list[str]]:
        """Validate a profile and return results.

        Args:
            name: Profile name

        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        profile = self.get_profile(name)
        if profile is None:
            return False, [f"Profile '{name}' not found"], []

        errors = []
        warnings = []

        # Validate configuration
        try:
            profile.config.validate()
        except InvalidConfigurationError as e:
            errors.append(str(e))

        # Check environment-specific warnings
        env_warnings = profile.config.validate_against_environment()
        warnings.extend(env_warnings)

        return len(errors) == 0, errors, warnings

    def create_profile_interactively(self, name: str) -> ConfigProfile:
        """Create a new profile interactively via CLI prompts.

        Args:
            name: Profile name

        Returns:
            Created ConfigProfile
        """
        print(f"\nCreating profile '{name}'...\n")

        # Basic settings
        print("1. Workload Type")
        print("   cpu   - CPU-intensive (NumPy, analysis)")
        print("   io    - I/O-intensive (file operations)")
        print("   mixed - Balanced compute and I/O")
        workload_type = input("Enter workload type [io]: ").strip() or "io"

        print("\n2. Memory Settings")
        reserve_mem_gb = input("Memory to reserve for system (GB) [50]: ").strip()
        reserve_mem_gb = float(reserve_mem_gb) if reserve_mem_gb else 50.0

        max_workers_input = input("Maximum workers (blank for auto): ").strip()
        max_workers = int(max_workers_input) if max_workers_input else None

        print("\n3. Advanced Settings")
        dashboard = input("Enable dashboard [Y/n]: ").strip().lower() != "n"
        adaptive = input("Enable adaptive scaling [y/N]: ").strip().lower() == "y"

        min_workers = None
        if adaptive:
            min_workers_input = input("Minimum workers for adaptive scaling: ").strip()
            min_workers = int(min_workers_input) if min_workers_input else None

        description = input("\nProfile description (optional): ").strip()
        tags_input = input("Tags (comma-separated, optional): ").strip()
        tags = [tag.strip() for tag in tags_input.split(",")] if tags_input else []

        # Create configuration
        config = DaskSetupConfig(
            workload_type=workload_type,
            max_workers=max_workers,
            reserve_mem_gb=reserve_mem_gb,
            dashboard=dashboard,
            adaptive=adaptive,
            min_workers=min_workers,
            name=name,
            description=description,
            tags=tags,
        )

        profile = ConfigProfile(name=name, config=config)
        return profile
