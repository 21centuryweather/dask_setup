# Configuration Reference

## DaskSetupConfig

`DaskSetupConfig` is a dataclass that captures every single-node option in one place. Pass it to `setup_dask_client(config=...)` to avoid the ambiguity of individual keyword arguments (where a value matching its default is silently ignored when merging with a profile).

```python
from dask_setup import setup_dask_client, DaskSetupConfig

config = DaskSetupConfig(
    workload_type="cpu",
    max_workers=8,
    reserve_mem_gb=32.0,
    spill_compression="lz4",
)
client, cluster, dask_tmp = setup_dask_client(config=config)
```

### Core Fields

| Field | Default | Description |
|-------|---------|-------------|
| `workload_type` | `"io"` | `"cpu"`, `"io"`, `"mixed"`, `"gpu"`, or `"auto"` |
| `max_workers` | `None` (all cores) | Hard cap on worker count |
| `reserve_mem_gb` | auto (20% RAM, 4–50 GiB, never >½ the machine) | GiB to reserve for OS / I/O cache |
| `max_mem_gb` | `None` (total RAM) | Upper bound on Dask's total memory |
| `dashboard` | `True` | Enable dashboard and print SSH tunnel |
| `dashboard_port` | `None` (random) | Specific dashboard port |
| `adaptive` | `False` | Enable single-node adaptive scaling |
| `min_workers` | `None` | Minimum workers when `adaptive=True` |
| `silence_logs` | `False` | `True` suppresses all but error-level worker logs; `False` leaves Dask at its own `WARNING` default |
| `suggest_chunks` | `False` | Print xarray chunking guidance after setup |
| `fallback_on_detection_failure` | `False` | Use conservative defaults if resource detection fails, rather than raising |
| `adaptive_memory` | `False` | Auto-tune memory thresholds after cluster start |

### Memory Threshold Fields

These control when workers start spilling to disk. The defaults are conservative and suitable for most HPC workloads.

| Field | Default | Meaning |
|-------|---------|---------|
| `memory_target` | `0.75` | Start moving data to disk at 75% usage |
| `memory_spill` | `0.85` | Spill aggressively at 85% |
| `memory_pause` | `0.92` | Pause scheduling new tasks at 92% |
| `memory_terminate` | `0.98` | Kill worker at 98% (last resort) |

### Compression & I/O Fields

| Field | Default | Description |
|-------|---------|-------------|
| `spill_compression` | `"auto"` | Algorithm for spill files. Options: `"lz4"`, `"zstd"`, `"snappy"`, `"gzip"`, `"blosc"`, `"zlib"`, `"bz2"`, `"lzma"`, `"blosc2"` (and blosc2 variants), `"false"` |
| `comm_compression` | `False` | Compress worker-to-worker network traffic |
| `spill_threads` | `None` | Size of the worker I/O thread pool for spill operations |

**Compression guidance:**
- `"lz4"` — fast, low CPU overhead, good all-around default
- `"zstd"` — better ratio, slightly more CPU; use when disk is the bottleneck
- `"snappy"` — very fast, low ratio; use for I/O-heavy workloads on shared storage
- `"blosc2:zstd"` — best ratio for Zarr v3 workflows when blosc2 is installed
- `"false"` — disable compression entirely

**spill_threads guidance:**
- Fast local SSD/NVMe: `4–8`
- Network-attached storage: `2–4`
- Slow or heavily-shared storage: `1–2`

### I/O Optimisation Fields

These feed into `recommend_io_chunks()`. See the [IO-Optimization](io-optimization.md) page for details.

| Field | Default | Description |
|-------|---------|-------------|
| `io_format` | `None` (auto) | `"zarr"`, `"zarr_v3"`, `"netcdf"`, `"kerchunk"`, or `"parquet"` |
| `io_target_chunk_mb` | `(128, 512)` | Target chunk size range in MiB |
| `io_access_pattern` | `"auto"` | `"sequential"`, `"random"`, `"streaming"`, `"compute"` |
| `io_storage_location` | `"auto"` | `"local"`, `"cloud"`, `"network"` |
| `io_compression_level` | `None` | Override default compression level (0–9) |

### Profile Metadata Fields

These are only meaningful when saving a config as a named profile.

| Field | Default | Description |
|-------|---------|-------------|
| `name` | `""` | Profile name |
| `description` | `""` | Human-readable description |
| `tags` | `[]` | Searchable tags (e.g. `["climate", "io-heavy"]`) |

---

## Configuration Profiles

Profiles let you save and reuse common configurations across jobs and team members.

### Using a Built-in Profile

```python
client, cluster, dask_tmp = setup_dask_client(profile="climate_analysis")
```

### Built-in Profiles

| Profile | workload_type | reserve_mem_gb | Notes |
|---------|--------------|----------------|-------|
| `climate_analysis` | `cpu` | 60 | Heavy compute, large arrays |
| `zarr_io_heavy` | `io` | 40 | Heavy Zarr I/O, many files |
| `development` | `mixed` | 8 | 2 workers max — lightweight local testing |
| `production` | `mixed` | 80 | Adaptive, dashboard off, logs silenced |
| `interactive` | `mixed` | 20 | 4 workers, dashboard on — Jupyter use |

### Profile Auto-Selection

Pass `profile="auto"` and `dask_setup` will inspect your current PBS/SLURM job request (CPUs, memory, jobfs) and pick the most appropriate built-in profile automatically:

```python
client, cluster, dask_tmp = setup_dask_client(profile="auto")
```

### Creating a Custom Profile

```python
from dask_setup import DaskSetupConfig, ConfigManager
from dask_setup.config import ConfigProfile

config = DaskSetupConfig(
    workload_type="io",
    reserve_mem_gb=40.0,
    spill_compression="lz4",
    name="my_io_profile",
    description="Optimized for large NetCDF processing",
    tags=["netcdf", "io-heavy"],
)

manager = ConfigManager()
manager.save_profile(ConfigProfile(name="my_io_profile", config=config))
```

Profiles are saved as YAML in `~/.dask_setup/profiles/`.

### Profile Inheritance (`based_on:`)

A profile can inherit all settings from a parent and override only specific fields:

```yaml
# ~/.dask_setup/profiles/fat_nodes.yaml
name: fat_nodes
based_on: climate_analysis
description: "climate_analysis but for 512 GB fat nodes"
config:
  reserve_mem_gb: 80.0
```

Everything not listed under `config:` is inherited from `climate_analysis`. Circular chains and chains deeper than 16 levels are detected and rejected.

In Python:

```python
from dask_setup.config import ConfigProfile

profile = ConfigProfile(
    name="fat_nodes",
    based_on="climate_analysis",
    config=DaskSetupConfig(reserve_mem_gb=80.0, name="fat_nodes"),
)
manager.save_profile(profile)
```

### Site-Wide Profiles

System administrators can ship profiles for all users by placing YAML files in `/etc/dask_setup/profiles/` (or the directory pointed to by `$DASK_SETUP_PROFILE_DIR`).

Site profiles are loaded between builtins and user profiles. User profiles always win on name conflicts:

```
builtin  <  site-wide (/etc/dask_setup/profiles/)  <  user (~/.dask_setup/profiles/)
```

`ConfigManager` accepts a `site_profiles_dir` parameter to override the site path in tests:

```python
manager = ConfigManager(site_profiles_dir="/project/shared/dask_profiles")
```

### Profile Versioning

Every profile saved by `save_profile()` carries a `version: "1.7"` field (matching `PROFILE_FORMAT_VERSION`). Loading a newer profile emits a `UserWarning`:

```python
from dask_setup import PROFILE_FORMAT_VERSION
print(PROFILE_FORMAT_VERSION)  # "1.7"
```

### Importing a Profile from a URL or File

Download and install a shared team profile in one step:

```bash
dask-setup import https://example.com/profiles/team_analysis.yaml
dask-setup import /shared/profiles/team_analysis.yaml --name my_team_profile
dask-setup import https://... --force   # overwrite if exists
```

In Python:

```python
manager.import_profile_from_url(
    "https://example.com/profiles/team_analysis.yaml",
    name_override="team_analysis",
    force=True,
)
```

### Loading and Inspecting Profiles

```python
manager = ConfigManager()

# List all profiles (builtin + site-wide + user)
for name, profile in manager.list_profiles().items():
    print(f"{name}: {profile.description}")

# Get a specific profile
profile = manager.get_profile("climate_analysis")
print(profile.config.workload_type)   # "cpu"

# Validate a profile
is_valid, errors, warnings = manager.validate_profile("my_io_profile")

# Delete a user profile
manager.delete_profile("my_io_profile")
```

---

## JSON Schema

A JSON Schema (draft-07) describing the profile YAML format ships with the package. Use it for editor validation and autocomplete:

```bash
# Print schema to stdout
dask-setup schema

# Write to a file for VS Code / PyCharm
dask-setup schema -o profile_schema.json
```

In Python:

```python
from dask_setup import PROFILE_SCHEMA
import json
print(json.dumps(PROFILE_SCHEMA, indent=2))

# or via ConfigManager
from dask_setup import ConfigManager
schema = ConfigManager.get_profile_schema()
```

---

## Priority and Merging

Settings are layered, lowest to highest:

```
library defaults  <  config= or profile=  <  explicit keyword args
```

A keyword argument you leave unset (`None`) inherits from the layer below.
Passing a value always overrides, **even when that value equals the library
default** — `setup_dask_client(profile="climate_analysis", reserve_mem_gb=50.0)`
gets 50.0, not the profile's figure.

> **Changed in v2.2.** Explicit arguments used to be detected by comparing them
> against the default value, so `workload_type="io"` or `dashboard=True` looked
> identical to "not supplied" and were silently dropped, leaving you with the
> profile's value. Passing a value now always means what it says.

### `config=` and `profile=` share a layer

They do **not** stack. Whichever you pass supplies the base configuration, and
if you pass both, the profile wins outright and the config object is ignored.

This is a real limitation rather than a preference. A `ConfigProfile` holds a
fully populated `DaskSetupConfig`, and nothing records which fields the
profile's YAML actually set — so merging a profile *over* a config object would
apply the profile's untouched defaults as well, silently overwriting settings
you had deliberately chosen. Refusing to stack them is the safe reading.

Pass one or the other, and put the differences in explicit keyword arguments:

```python
# Not this — the config object is discarded.
setup_dask_client(config=my_config, profile="climate_analysis")

# This.
setup_dask_client(profile="climate_analysis", reserve_mem_gb=80.0, adaptive=True)
```

---

## CLI Reference

The `dask-setup` command manages profiles from the terminal.

```bash
# List all available profiles
dask-setup list

# Filter by tag
dask-setup list --tags climate,io-heavy

# Show full details and validate a profile
dask-setup show climate_analysis

# Create a new profile
dask-setup create my_profile

# Create a profile based on an existing one
dask-setup create my_profile --from-profile zarr_io_heavy

# Validate a specific profile
dask-setup validate my_profile

# Validate all profiles at once
dask-setup validate --all

# Export a profile to YAML (stdout or file)
dask-setup export climate_analysis
dask-setup export climate_analysis -o my_profile.yaml

# Import a profile from a URL or local file
dask-setup import https://example.com/profiles/team.yaml
dask-setup import /shared/team.yaml --name team_analysis --force

# Print the JSON Schema (for editor integration)
dask-setup schema
dask-setup schema -o profile_schema.json

# Delete a user profile
dask-setup delete my_profile

# Run a synthetic performance benchmark against a profile
dask-setup benchmark --profile development --size small --operation mean
```

See the [Benchmarking](benchmarking.md) page for the full `benchmark` subcommand reference, and [Multi-Node](multi-node.md) for the `submit` subcommand.
