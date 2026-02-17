# Dask Setup Recipes

A comprehensive collection of practical examples showcasing various ways to use `dask_setup` for HPC-optimized single-node Dask clusters.

> **Quick Link**: [Main dask_setup Documentation](../../README.md)

## Quick Start

### Installation

```bash
# Core dependencies
pip install dask-setup dask distributed

# Optional dependencies for full functionality
pip install xarray zarr rechunker netcdf4 h5netcdf psutil pyyaml

# For visualization examples
pip install matplotlib seaborn

# For advanced storage examples  
pip install s3fs gcsfs fsspec
```

### Running Examples

**Local Development:**
```bash
cd examples/recipes/01_basic
python basic_usage.py --workload cpu

# View dashboard (if enabled)
# Follow SSH tunnel instructions printed by the script
```

**On NCI Gadi (PBS):**
```bash
#PBS -q normalsr
#PBS -l ncpus=48
#PBS -l mem=190gb
#PBS -l jobfs=100gb
#PBS -l walltime=4:00:00
#PBS -l storage=gdata/hh5+gdata/gb02
#PBS -l wd

module use /g/data/hh5/public/modules/
module load conda_concept/analysis3-unstable

export TMPDIR="$PBS_JOBFS"
cd examples/recipes/08_real_world_science
python climate_analysis_case.py
```

## Recipe Categories

### 🚀 [01_basic](01_basic/) - Getting Started
**Focus:** Core patterns and workload types
- `basic_usage.py` - CPU, I/O, and mixed workload demonstrations
- Resource detection and cluster configuration basics
- Dashboard setup and SSH tunneling
- Safe cluster shutdown patterns

### ⚙️ [02_config_management](02_config_management/) - Configuration & Profiles
**Focus:** Advanced configuration management
- `custom_profiles.py` - YAML profiles and parameter overrides
- `profile_comparison.py` - Built-in vs custom profile benchmarks
- Configuration validation and error handling
- Environment-specific optimizations

### 💾 [03_memory_compression](03_memory_compression/) - Memory & Compression
**Focus:** Memory management and spill optimization
- `memory_compression.py` - Spill thresholds and compression algorithms
- `adaptive_scaling.py` - Dynamic worker scaling
- Memory monitoring and diagnostics
- PBS_JOBFS utilization strategies

### 📊 [04_storage_io](04_storage_io/) - Storage Format Intelligence
**Focus:** Format-specific I/O optimization
- `storage_format_intelligence.py` - Zarr vs NetCDF optimization
- `cloud_storage_patterns.py` - S3/GCS access patterns
- Chunking strategies for different storage backends
- Performance benchmarking across formats

### 🔀 [05_xarray_chunking](05_xarray_chunking/) - Xarray Integration
**Focus:** Intelligent chunking for xarray datasets
- `xarray_best_practices.py` - Automated chunk size recommendations
- `rechunking_strategies.py` - Rechunker integration patterns
- Dimension analysis and memory-efficient operations
- Visualization of chunking strategies

### 🖥️ [06_hpc_pbs_slurm](06_hpc_pbs_slurm/) - HPC Integration
**Focus:** HPC scheduler integration
- `hpc_jobscript_demo.py` - Job script generation and resource detection
- `ssh_tunnel_helpers.py` - Dashboard access from login nodes
- Environment variable parsing (PBS/SLURM)
- Multi-node considerations and limitations

### 🚨 [07_error_troubleshoot](07_error_troubleshoot/) - Error Handling
**Focus:** Common issues and debugging
- `common_errors.py` - Synthetic error reproduction and solutions
- `diagnostic_tools.py` - Cluster health monitoring
- Memory constraint troubleshooting
- Network and storage debugging

### 🌡️ [08_real_world_science](08_real_world_science/) - Scientific Use Cases
**Focus:** End-to-end scientific workflows
- `climate_analysis_case.py` - Multi-year climate data processing
- `satellite_processing.py` - Large-scale remote sensing workflows
- `oceanographic_analysis.py` - Time series and gridded ocean data
- Reproducible research patterns

### 📈 [09_perf_optimization](09_perf_optimization/) - Performance Tuning
**Focus:** Benchmarking and optimization
- `benchmark_scaling.py` - Worker scaling analysis
- `memory_profiling.py` - Detailed memory usage patterns
- `io_throughput_analysis.py` - Storage performance characterization
- Bottleneck identification and mitigation

### 🔗 [10_tool_integration](10_tool_integration/) - Tool Ecosystem
**Focus:** Integration with related tools
- `rechunker_zarr_netcdf.py` - Rechunker workflows
- `kerchunk_integration.py` - Virtual datasets
- `jupyter_notebook_patterns.py` - Interactive development
- CI/CD integration examples

## Conventions

### Command Line Interface
All recipes support consistent CLI patterns:
```bash
# Help and usage
python script_name.py --help

# Common flags
--workload {cpu,io,mixed}     # Workload type
--workers N                   # Maximum workers  
--memory N                    # Reserve memory (GB)
--dashboard / --no-dashboard  # Dashboard control
--verbose                     # Detailed logging
--output-dir DIR              # Output location
```

### Environment Variables
```bash
# Override default settings
export DASK_SETUP_RESERVE_MEM=60   # Reserve memory (GB)
export DASK_SETUP_MAX_WORKERS=8    # Worker limit
export DASK_SETUP_WORKLOAD=cpu     # Default workload type

# HPC-specific
export PBS_JOBFS=/tmp              # Override jobfs location (testing)
export TMPDIR=/custom/temp         # Override temp directory
```

### Logging and Output
- **INFO level:** Progress and key metrics
- **DEBUG level:** Detailed cluster information
- **WARNING level:** Performance suggestions
- **ERROR level:** Critical issues with solutions

Recipes save outputs to:
- `outputs/` - Generated data files and plots
- `logs/` - Detailed execution logs
- `configs/` - Reusable configuration files

## Dashboard Access

### Local Development
```bash
# Dashboard URL printed by all recipes
Dask dashboard: http://127.0.0.1:8787/status
```

### HPC Systems (SSH Tunneling)
```bash
# Command printed by recipes running on compute nodes
ssh -N -L 8787:gadi-cpu-clx-1234.gadi.nci.org.au:45678 gadi.nci.org.au

# Then open in local browser:
http://localhost:8787
```

### Dashboard Features
- **Status:** Worker health and resource utilization
- **Task Stream:** Real-time task execution
- **Progress:** Completion status for long operations
- **Memory:** Per-worker memory usage and spill activity
- **Profile:** Performance profiling data

## Recipe Selection Guide

### Choose by Workload Type

| **Workload Pattern** | **Recommended Recipes** | **Key Features** |
|---------------------|------------------------|------------------|
| **Heavy Computation** | 01_basic, 05_xarray_chunking, 08_real_world_science | Many processes, 1 thread each, CPU-optimized |
| **File I/O Intensive** | 04_storage_io, 06_hpc_pbs_slurm, 10_tool_integration | Few processes, many threads, I/O-optimized |
| **Mixed Workflows** | 02_config_management, 07_error_troubleshoot, 09_perf_optimization | Balanced process/thread topology |

### Choose by Experience Level

| **Level** | **Start With** | **Then Try** |
|-----------|----------------|--------------|
| **Beginner** | 01_basic → 05_xarray_chunking | 08_real_world_science |
| **Intermediate** | 02_config_management → 04_storage_io → 06_hpc_pbs_slurm | 09_perf_optimization |
| **Advanced** | 07_error_troubleshoot → 10_tool_integration → 03_memory_compression | All recipes |

### Choose by Use Case

| **Use Case** | **Primary Recipe** | **Supporting Recipes** |
|--------------|-------------------|----------------------|
| **Climate Data Analysis** | 08_real_world_science | 05_xarray_chunking, 04_storage_io |
| **Large Dataset Processing** | 04_storage_io | 03_memory_compression, 10_tool_integration |
| **HPC Job Optimization** | 06_hpc_pbs_slurm | 09_perf_optimization, 02_config_management |
| **Interactive Development** | 01_basic | 07_error_troubleshoot, 02_config_management |

## Common Pitfalls and Solutions

### Memory Issues
**Problem:** `"Task requires > memory_limit"`
```python
# Solution 1: Reduce workers
client, cluster, tmp = setup_dask_client("cpu", max_workers=1)

# Solution 2: Increase memory reservation
client, cluster, tmp = setup_dask_client("cpu", reserve_mem_gb=30)
```

### Performance Issues
**Problem:** Slower than expected processing
```python
# Check chunk sizes (target: 256-512 MiB)
print(f"Chunk sizes: {ds.chunks}")

# Verify workload type matches usage
setup_dask_client("io")  # For file-heavy operations
setup_dask_client("cpu") # For computation-heavy operations
```

### Dashboard Access Issues
**Problem:** Cannot access dashboard on HPC
```bash
# Ensure SSH tunnel targets correct compute node
ssh -N -L 8787:<ACTUAL_COMPUTE_NODE>:<ACTUAL_PORT> <HPC_LOGIN>

# Check firewall settings
ssh -N -L 8787:localhost:<PORT> <HPC_LOGIN>
```

## Quick Start & Testing

### Recipe Launcher (Recommended)
```bash
# List all available recipes
python run_recipe.py --list

# Check dependencies
python run_recipe.py --check-deps

# Run a specific recipe
python run_recipe.py 01_basic
python run_recipe.py 08_real_world_science --help
```

### Direct Execution
```bash
# Run recipes directly
cd 01_basic && python basic_usage.py
cd 08_real_world_science && python climate_analysis_case.py --workers 2
```

### Installation Testing
```bash
# Verify environment and dependencies
python test_recipes.py

# Install all dependencies
pip install -r requirements.txt
```

### Smoke Tests
```bash
# Test all recipes with minimal data
cd examples/recipes/tests
python -m pytest test_examples.py -v

# Test specific category
python -m pytest test_examples.py::test_basic_recipes -v
```

### Performance Validation
```bash
# Benchmark suite
cd examples/recipes/09_perf_optimization
python benchmark_scaling.py --workers 1 2 4 8 --output benchmark_results.csv
```

### Integration Testing
```bash
# End-to-end workflow validation
cd examples/recipes
python -m pytest --integration -v
```

## Contributing

### Adding New Recipes
1. Choose appropriate category directory (01_basic to 10_tool_integration)
2. Follow naming convention: `descriptive_name.py`
3. Include docstring with purpose, requirements, and expected outputs
4. Add CLI support with argparse
5. Include error handling and user guidance
6. Add tests in `tests/test_examples.py`

### Recipe Structure Template
```python
#!/usr/bin/env python
"""Brief description of the recipe's purpose and key features.

Requirements:
- List required packages
- Note any special environment needs

Outputs:
- Describe what files/data are created
- Note any visualization generated
"""

import argparse
import sys
from pathlib import Path

from dask_setup import setup_dask_client

def main():
    """Main recipe implementation."""
    parser = argparse.ArgumentParser(description=__doc__)
    # Add standard arguments
    args = parser.parse_args()
    
    # Recipe implementation
    try:
        client, cluster, tmp = setup_dask_client(args.workload)
        # ... recipe logic ...
    finally:
        if 'client' in locals():
            client.close()
        if 'cluster' in locals():
            cluster.close()

if __name__ == "__main__":
    main()
```

## Dependencies and Requirements

### Core Requirements
- Python 3.11+
- dask-setup (latest)
- dask, distributed

### Optional Dependencies by Recipe Category
- **Storage I/O:** xarray, zarr, netcdf4, h5netcdf, rechunker
- **Visualization:** matplotlib, seaborn, hvplot
- **Cloud Storage:** s3fs, gcsfs, fsspec
- **HPC Integration:** psutil, pyyaml
- **Scientific Computing:** numpy, scipy, pandas

### System Requirements
- **Memory:** Minimum 8 GB RAM, 16+ GB recommended
- **Storage:** Fast local storage preferred (SSD/NVMe)
- **Network:** Required for cloud storage examples

## License and Citation

These recipes are distributed under the same license as dask_setup (Apache-2.0).

When using these recipes in research, please cite:
```bibtex
@software{dask_setup_recipes,
  title={Dask Setup: HPC-Optimized Single-Node Dask Clusters - Recipe Collection},
  url={https://github.com/21centuryweather/dask_setup},
  version={1.0.0},
  year={2024}
}
```

---

**Need Help?** 
- 📖 Check the [main documentation](../../README.md)
- 🐛 [Report issues](https://github.com/21centuryweather/dask_setup/issues)
- 💬 [Start a discussion](https://github.com/21centuryweather/dask_setup/discussions)