"""Configuration file management and profile loading/saving."""

from __future__ import annotations

import os
import sys
import warnings
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from .config import ConfigProfile, DaskSetupConfig
from .error_handling import ConfigurationValidationError
from .exceptions import InvalidConfigurationError

if TYPE_CHECKING:
    from .types import ResourceSpec

#: Profile format version written to every saved profile.
#: Used to detect profiles created by newer versions of ``dask_setup``.
PROFILE_FORMAT_VERSION: str = "1.7"

#: Maximum allowed ``based_on`` chain depth before a cycle is declared.
_MAX_INHERITANCE_DEPTH: int = 16


class ConfigManager:
    """Manages loading and saving of configuration profiles."""

    def __init__(
        self,
        config_dir: Path | str | None = None,
        site_profiles_dir: Path | str | None = None,
    ) -> None:
        """Initialize configuration manager.

        Parameters
        ----------
        config_dir:
            User configuration directory.  Defaults to ``~/.dask_setup/``.
        site_profiles_dir:
            Site-wide profiles directory.  Defaults to the value of the
            ``$DASK_SETUP_PROFILE_DIR`` environment variable, falling back
            to ``/etc/dask_setup/profiles``.  Site profiles are loaded
            *after* builtins but *before* user profiles, so user profiles
            always take precedence over site-wide defaults.
        """
        if config_dir is None:
            config_dir = Path.home() / ".dask_setup"

        self.config_dir = Path(config_dir)
        self.profiles_dir = self.config_dir / "profiles"

        # Site-wide profiles directory
        if site_profiles_dir is not None:
            self.site_profiles_dir = Path(site_profiles_dir)
        else:
            env_dir = os.getenv("DASK_SETUP_PROFILE_DIR")
            self.site_profiles_dir = Path(env_dir) if env_dir else Path("/etc/dask_setup/profiles")

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
        """List all available profiles (builtin + site-wide + user).

        Profiles are merged in precedence order — user profiles overwrite
        site-wide profiles, which overwrite builtins of the same name.

        Returns:
            Dictionary of profile name -> ConfigProfile
        """
        # Start with builtins (lowest precedence)
        profiles = self.builtin_profiles.copy()

        # Layer in site-wide profiles
        if self.site_profiles_dir.exists():
            for profile_file in sorted(self.site_profiles_dir.glob("*.yaml")):
                try:
                    profile = self.load_profile_from_file(profile_file)
                    profiles[profile.name] = profile
                except Exception as e:
                    print(
                        f"Warning: Could not load site profile {profile_file.name}: {e}",
                        file=sys.stderr,
                    )

        # Layer in user profiles (highest precedence)
        if self.profiles_dir.exists():
            for profile_file in sorted(self.profiles_dir.glob("*.yaml")):
                try:
                    profile = self.load_profile_from_file(profile_file)
                    profiles[profile.name] = profile
                except Exception as e:
                    print(
                        f"Warning: Could not load profile {profile_file.name}: {e}", file=sys.stderr
                    )

        return profiles

    def get_profile(self, name: str) -> ConfigProfile | None:
        """Get a specific profile by name.

        Search order: user profiles → site-wide profiles → builtin profiles.

        Args:
            name: Profile name

        Returns:
            ConfigProfile if found, None otherwise
        """
        # User profiles take highest precedence
        profile_file = self.profiles_dir / f"{name}.yaml"
        if profile_file.exists():
            try:
                return self.load_profile_from_file(profile_file)
            except Exception as e:
                raise InvalidConfigurationError(f"Failed to load profile '{name}': {e}") from e

        # Site-wide profiles
        site_file = self.site_profiles_dir / f"{name}.yaml"
        if site_file.exists():
            try:
                return self.load_profile_from_file(site_file)
            except Exception as e:
                raise InvalidConfigurationError(f"Failed to load site profile '{name}': {e}") from e

        # Built-in profiles
        if name in self.builtin_profiles:
            return self.builtin_profiles[name]

        return None

    def save_profile(self, profile: ConfigProfile) -> None:
        """Save a profile to disk.

        The current :data:`PROFILE_FORMAT_VERSION` is always written to the
        ``version`` field so that future versions of ``dask_setup`` can
        detect and warn about profiles created by newer releases.

        Args:
            profile: Profile to save

        Raises:
            InvalidConfigurationError: If trying to save builtin profile or validation fails
        """
        if profile.builtin:
            raise InvalidConfigurationError(f"Cannot save builtin profile '{profile.name}'")

        self.ensure_config_dir()

        # Stamp with current format version
        profile.profile_version = PROFILE_FORMAT_VERSION

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

    def load_profile_from_file(
        self,
        file_path: Path,
        _inheritance_chain: frozenset[str] | None = None,
    ) -> ConfigProfile:
        """Load a profile from a YAML file, resolving any ``based_on`` inheritance.

        Parameters
        ----------
        file_path:
            Path to the YAML profile file.
        _inheritance_chain:
            Internal parameter used to detect circular inheritance chains.
            Callers should not set this.

        Returns
        -------
        ConfigProfile
            Fully resolved profile (with inherited config already merged in).

        Raises
        ------
        InvalidConfigurationError
            If the file is unreadable, contains invalid YAML, references a
            non-existent parent, or has a circular ``based_on`` chain.
        """
        if _inheritance_chain is None:
            _inheritance_chain = frozenset()

        try:
            with open(file_path) as f:
                data = yaml.safe_load(f)

            if not isinstance(data, dict):
                raise InvalidConfigurationError("Profile file must contain a YAML object")

        except yaml.YAMLError as e:
            raise InvalidConfigurationError(f"Invalid YAML in profile file: {e}") from e
        except OSError as e:
            raise InvalidConfigurationError(f"Could not read profile file: {e}") from e

        # --- Version check ---------------------------------------------------
        profile_ver = data.get("version")
        if profile_ver is not None:
            try:
                if tuple(int(x) for x in str(profile_ver).split(".")) > tuple(
                    int(x) for x in PROFILE_FORMAT_VERSION.split(".")
                ):
                    warnings.warn(
                        f"Profile '{data.get('name', file_path.stem)}' was created with "
                        f"dask_setup profile format {profile_ver!r} but the installed version "
                        f"only understands {PROFILE_FORMAT_VERSION!r}. "
                        "Some settings may be ignored.",
                        UserWarning,
                        stacklevel=4,
                    )
            except (ValueError, TypeError):
                pass  # Unparseable version string — skip check

        # --- Inheritance resolution ------------------------------------------
        based_on = data.get("based_on")
        if based_on is not None:
            profile_name = data.get("name", file_path.stem)

            # Cycle detection
            if based_on in _inheritance_chain:
                raise InvalidConfigurationError(
                    f"Circular profile inheritance detected: "
                    f"{' -> '.join(sorted(_inheritance_chain))} -> {based_on}"
                )
            if len(_inheritance_chain) >= _MAX_INHERITANCE_DEPTH:
                raise InvalidConfigurationError(
                    f"Profile inheritance chain is too deep (max {_MAX_INHERITANCE_DEPTH}). "
                    "Check for accidental cycles."
                )

            # Resolve the parent — use get_profile() so builtins/site/user all work
            base_profile = self.get_profile(based_on)
            if base_profile is None:
                raise InvalidConfigurationError(
                    f"Profile '{profile_name}' declares based_on='{based_on}' "
                    f"but no profile named '{based_on}' could be found."
                )

            # Merge: start from the base's full config dict, overlay child's
            # *explicitly specified* overrides (from the raw YAML `config:` section).
            base_config_dict: dict[str, Any] = base_profile.config.to_dict()
            child_config_overrides: dict[str, Any] = data.get("config", {})
            merged_config_dict = {**base_config_dict, **child_config_overrides}
            data["config"] = merged_config_dict

        return ConfigProfile.from_dict(data)

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

    def import_profile_from_url(
        self,
        url: str,
        name_override: str | None = None,
        force: bool = False,
    ) -> ConfigProfile:
        """Fetch a YAML profile from *url* and install it as a user profile.

        Parameters
        ----------
        url:
            HTTP/HTTPS URL that returns a raw YAML profile file.
        name_override:
            If provided, the profile is saved under this name instead of the
            name embedded in the YAML file.
        force:
            If ``True``, overwrite an existing profile of the same name.
            If ``False`` (default), raise :exc:`InvalidConfigurationError`
            when the target name already exists.

        Returns
        -------
        ConfigProfile
            The imported (and saved) profile.

        Raises
        ------
        InvalidConfigurationError
            If the URL cannot be fetched, the content is invalid, or the
            profile name already exists and *force* is ``False``.
        """
        import urllib.error
        import urllib.request

        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": f"dask_setup/{PROFILE_FORMAT_VERSION}"},
            )
            with urllib.request.urlopen(req, timeout=30) as response:
                content = response.read().decode("utf-8")
        except urllib.error.URLError as e:
            raise InvalidConfigurationError(f"Could not fetch profile from {url!r}: {e}") from e
        except Exception as e:
            raise InvalidConfigurationError(f"Unexpected error fetching {url!r}: {e}") from e

        try:
            data = yaml.safe_load(content)
        except yaml.YAMLError as e:
            raise InvalidConfigurationError(f"Invalid YAML returned from {url!r}: {e}") from e

        if not isinstance(data, dict):
            raise InvalidConfigurationError(
                f"URL {url!r} did not return a YAML object (got {type(data).__name__})"
            )

        if name_override:
            data["name"] = name_override

        if "name" not in data:
            raise InvalidConfigurationError(
                f"Profile from {url!r} has no 'name' field and no --name override was given."
            )

        profile = ConfigProfile.from_dict(data)
        profile.builtin = False

        # Conflict check
        existing = self.get_profile(profile.name)
        if existing is not None and not existing.builtin and not force:
            raise InvalidConfigurationError(
                f"A profile named '{profile.name}' already exists. "
                "Use force=True (or --force on the CLI) to overwrite it."
            )

        self.save_profile(profile)
        return profile

    @staticmethod
    def get_profile_schema() -> dict[str, Any]:
        """Return the JSON Schema for the profile YAML format.

        The schema can be used by editors (VS Code, PyCharm, etc.) for
        autocompletion and inline validation of ``.yaml`` profile files.

        Returns
        -------
        dict
            A JSON Schema (draft-07) describing the profile YAML structure.
        """
        from .schema import PROFILE_SCHEMA

        return PROFILE_SCHEMA

    def auto_select_profile(self, resources: ResourceSpec | None = None) -> str:
        """Select the most appropriate builtin profile for the current environment.

        Inspects PBS/SLURM environment variables, detected resources, and the
        runtime environment (e.g. Jupyter) to pick a named profile.

        This is called automatically when ``profile="auto"`` is passed to
        :func:`~dask_setup.client.setup_dask_client`.

        Parameters
        ----------
        resources : ResourceSpec or None
            Detected system resources.  When ``None``, the method falls back
            to environment variable inspection only.

        Returns
        -------
        str
            Name of the selected builtin profile.  Always one of the builtin
            profile names (guaranteed to exist in :attr:`builtin_profiles`).

        Notes
        -----
        Selection logic (first match wins):

        1. **Jupyter environment** → ``"interactive"``
        2. **Small machine** (≤ 8 cores *or* ≤ 16 GiB total RAM) → ``"development"``
        3. **I/O-heavy PBS job** (``$PBS_JOBFS`` set, ``≥ 16 cores``) → ``"zarr_io_heavy"``
        4. **Large climate/science HPC job** (``≥ 48 cores`` or ``≥ 128 GiB``) → ``"climate_analysis"``
        5. **General HPC job** with substantial resources → ``"production"``
        6. Fallback → ``"development"``
        """
        from .environment import is_jupyter

        total_cores: int = 0
        total_mem_gib: float = 0.0

        if resources is not None:
            total_cores = resources.total_cores
            total_mem_gib = resources.total_mem_bytes / (1024**3)

        # --- Rule 1: Jupyter / interactive notebook ----------------------
        if is_jupyter():
            return "interactive"

        # --- Rule 2: Small / laptop machine ------------------------------
        is_small = (total_cores > 0 and total_cores <= 8) or (
            total_mem_gib > 0 and total_mem_gib <= 16.0
        )
        if is_small:
            return "development"

        # --- Rule 3: PBS job with JOBFS → I/O-heavy path -----------------
        has_jobfs = bool(os.getenv("PBS_JOBFS"))
        is_hpc = bool(
            os.getenv("SLURM_JOB_ID")
            or os.getenv("PBS_JOBID")
            or os.getenv("NCPUS")
            or os.getenv("SLURM_CPUS_ON_NODE")
        )
        if has_jobfs and total_cores >= 16:
            return "zarr_io_heavy"

        # --- Rule 4: Large climate/science job ---------------------------
        is_large = total_cores >= 48 or total_mem_gib >= 128.0
        if is_large and is_hpc:
            return "climate_analysis"

        # --- Rule 5: General HPC job with decent resources ---------------
        if is_hpc and total_cores >= 16:
            return "production"

        # --- Fallback ----------------------------------------------------
        return "development"

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
