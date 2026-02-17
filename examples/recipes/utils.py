#!/usr/bin/env python
"""Shared utilities for dask_setup recipes.

This module provides common functionality used across multiple recipe examples,
including timing, formatting, dashboard helpers, and test data generation.
"""

import contextlib
import socket
import time
from pathlib import Path
from typing import Any, Dict, Optional, Union

try:
    import numpy as np
except ImportError:
    np = None

try:
    import xarray as xr
except ImportError:
    xr = None


def pretty_bytes(bytes_value: Union[int, float]) -> str:
    """Convert bytes to human-readable format.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        Human-readable string (e.g., "1.5 GB", "256 MB")
    """
    if bytes_value < 1024:
        return f"{bytes_value} B"
    elif bytes_value < 1024**2:
        return f"{bytes_value / 1024:.1f} KB"
    elif bytes_value < 1024**3:
        return f"{bytes_value / 1024**2:.1f} MB"
    elif bytes_value < 1024**4:
        return f"{bytes_value / 1024**3:.1f} GB"
    else:
        return f"{bytes_value / 1024**4:.1f} TB"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "2m 30s", "1h 15m", "45s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


@contextlib.contextmanager
def timer(description: str = "Operation"):
    """Context manager for timing operations.
    
    Args:
        description: Description of the operation being timed
        
    Yields:
        Dictionary with timing information (updated in real-time)
        
    Example:
        with timer("Data processing") as t:
            process_data()
            print(f"Halfway done, elapsed: {t['elapsed']:.1f}s")
        print(f"Total time: {t['total']:.1f}s")
    """
    timing_info = {'elapsed': 0.0, 'total': 0.0}
    start_time = time.perf_counter()
    
    def update_elapsed():
        timing_info['elapsed'] = time.perf_counter() - start_time
    
    try:
        print(f"⏱️  Starting {description}...")
        yield timing_info
    finally:
        end_time = time.perf_counter()
        timing_info['total'] = end_time - start_time
        timing_info['elapsed'] = timing_info['total']
        print(f"✅ {description} completed in {format_duration(timing_info['total'])}")


def get_dashboard_info(cluster) -> Dict[str, Any]:
    """Extract dashboard information from a cluster.
    
    Args:
        cluster: Dask LocalCluster instance
        
    Returns:
        Dictionary with dashboard information
    """
    info = {}
    
    if hasattr(cluster, 'dashboard_link') and cluster.dashboard_link:
        info['url'] = cluster.dashboard_link
        
        # Extract port and hostname
        try:
            from urllib.parse import urlparse
            parsed = urlparse(cluster.dashboard_link)
            info['port'] = parsed.port or 8787
            info['hostname'] = parsed.hostname or 'localhost'
        except Exception:
            info['port'] = 8787
            info['hostname'] = 'localhost'
    
    # Get system hostname
    try:
        system_hostname = socket.gethostname()
        info['system_hostname'] = system_hostname
        
        # Check if we're on an HPC system
        hpc_systems = ['gadi', 'raijin', 'magnus', 'pawsey', 'frontera', 'stampede']
        info['is_hpc'] = any(hpc in system_hostname.lower() for hpc in hpc_systems)
        
        if info['is_hpc']:
            port = info.get('port', 8787)
            info['ssh_tunnel'] = f"ssh -N -L 8787:{system_hostname}:{port} <your_login_node>"
    except Exception:
        info['system_hostname'] = 'unknown'
        info['is_hpc'] = False
    
    return info


def print_dashboard_instructions(cluster) -> None:
    """Print dashboard access instructions.
    
    Args:
        cluster: Dask LocalCluster instance
    """
    info = get_dashboard_info(cluster)
    
    if 'url' not in info:
        print("❌ Dashboard not available")
        return
    
    print(f"\n🖥️  Dashboard Access:")
    print(f"   Direct URL: {info['url']}")
    
    if info.get('is_hpc', False):
        print(f"   🔗 SSH Tunnel (for HPC access):")
        print(f"      {info.get('ssh_tunnel', 'SSH tunnel command not available')}")
        print(f"      Then open: http://localhost:8787")
    
    print(f"   💡 Dashboard features:")
    print(f"      • Status: Worker health and resource usage")
    print(f"      • Task Stream: Real-time task execution")
    print(f"      • Memory: Per-worker memory and spill activity")
    print(f"      • Progress: Completion status for operations")


def create_sample_dataset(
    shape: tuple = (365, 180, 360),
    chunks: tuple = None,
    variables: list = None,
    dims: tuple = ('time', 'lat', 'lon'),
    coords: dict = None
) -> 'xr.Dataset':
    """Create a sample xarray dataset for testing.
    
    Args:
        shape: Shape of the data arrays
        chunks: Chunk sizes (if None, not chunked)
        variables: List of variable names to create
        dims: Dimension names
        coords: Coordinate arrays (if None, auto-generated)
        
    Returns:
        xarray Dataset
    """
    if xr is None:
        raise ImportError("xarray is required for create_sample_dataset")
    
    if np is None:
        raise ImportError("numpy is required for create_sample_dataset")
    
    if variables is None:
        variables = ['temperature', 'precipitation']
    
    # Create coordinates
    if coords is None:
        coords = {}
        if 'time' in dims:
            coords['time'] = np.arange(shape[dims.index('time')])
        if 'lat' in dims:
            coords['lat'] = np.linspace(-90, 90, shape[dims.index('lat')])
        if 'lon' in dims:
            coords['lon'] = np.linspace(-180, 180, shape[dims.index('lon')])
    
    # Create data variables
    data_vars = {}
    for var in variables:
        if chunks is not None:
            import dask.array as da
            data = da.random.random(shape, chunks=chunks)
        else:
            data = np.random.random(shape)
        
        data_vars[var] = (dims, data)
    
    return xr.Dataset(data_vars, coords=coords)


def save_markdown_table(
    data: list,
    headers: list,
    filename: Union[str, Path],
    title: str = None,
    description: str = None
) -> None:
    """Save data as a formatted markdown table.
    
    Args:
        data: List of rows (each row is a list of values)
        headers: Column headers
        filename: Output filename
        title: Optional table title
        description: Optional description
    """
    filename = Path(filename)
    filename.parent.mkdir(parents=True, exist_ok=True)
    
    with open(filename, 'w') as f:
        if title:
            f.write(f"# {title}\n\n")
        
        if description:
            f.write(f"{description}\n\n")
        
        # Write header
        f.write("| " + " | ".join(headers) + " |\n")
        f.write("|" + "|".join(["-" * (len(h) + 2) for h in headers]) + "|\n")
        
        # Write data rows
        for row in data:
            formatted_row = [str(cell) for cell in row]
            f.write("| " + " | ".join(formatted_row) + " |\n")
        
        f.write(f"\n*Generated on {time.strftime('%Y-%m-%d %H:%M:%S')}*\n")
    
    print(f"💾 Saved table to: {filename}")


def get_memory_info() -> Dict[str, Any]:
    """Get current memory information.
    
    Returns:
        Dictionary with memory statistics
    """
    try:
        import psutil
        
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        return {
            'total_gb': mem.total / (1024**3),
            'available_gb': mem.available / (1024**3),
            'used_gb': mem.used / (1024**3),
            'percent': mem.percent,
            'swap_total_gb': swap.total / (1024**3),
            'swap_used_gb': swap.used / (1024**3),
            'swap_percent': swap.percent
        }
    except ImportError:
        return {'error': 'psutil not available'}


def get_worker_memory_stats(client) -> Dict[str, Any]:
    """Get memory statistics from Dask workers.
    
    Args:
        client: Dask distributed client
        
    Returns:
        Dictionary with worker memory statistics
    """
    try:
        def get_worker_memory():
            import psutil
            mem = psutil.virtual_memory()
            return {
                'used_mb': mem.used / (1024**2),
                'available_mb': mem.available / (1024**2),
                'percent': mem.percent
            }
        
        # Run on all workers
        worker_stats = client.run(get_worker_memory)
        
        # Aggregate stats
        total_used = sum(stats['used_mb'] for stats in worker_stats.values())
        total_available = sum(stats['available_mb'] for stats in worker_stats.values())
        avg_percent = sum(stats['percent'] for stats in worker_stats.values()) / len(worker_stats)
        
        return {
            'worker_count': len(worker_stats),
            'total_used_mb': total_used,
            'total_available_mb': total_available,
            'average_percent': avg_percent,
            'individual': worker_stats
        }
    except Exception as e:
        return {'error': str(e)}


def estimate_chunk_memory(ds: 'xr.Dataset', chunks: dict = None) -> Dict[str, Any]:
    """Estimate memory usage for dataset chunks.
    
    Args:
        ds: xarray Dataset
        chunks: Chunk specification (if None, uses current chunks)
        
    Returns:
        Dictionary with memory estimates
    """
    if xr is None:
        raise ImportError("xarray is required for estimate_chunk_memory")
    
    estimates = {}
    
    for var_name, var in ds.data_vars.items():
        # Get chunk specification
        if chunks:
            var_chunks = {}
            for dim, size in chunks.items():
                if dim in var.dims:
                    var_chunks[dim] = size
        else:
            # Use current chunks if available
            if hasattr(var.data, 'chunks'):
                var_chunks = dict(zip(var.dims, var.data.chunks))
            else:
                var_chunks = dict(zip(var.dims, var.shape))
        
        # Calculate chunk size
        chunk_shape = []
        for dim in var.dims:
            chunk_shape.append(var_chunks.get(dim, var.sizes[dim]))
        
        # Estimate memory per chunk
        dtype_size = np.dtype(var.dtype).itemsize
        chunk_elements = np.prod(chunk_shape)
        chunk_bytes = chunk_elements * dtype_size
        
        # Calculate total chunks
        total_chunks = 1
        for i, dim in enumerate(var.dims):
            dim_size = var.sizes[dim]
            chunk_size = chunk_shape[i]
            total_chunks *= (dim_size + chunk_size - 1) // chunk_size  # Ceiling division
        
        estimates[var_name] = {
            'chunk_shape': chunk_shape,
            'chunk_mb': chunk_bytes / (1024**2),
            'total_chunks': total_chunks,
            'total_mb': (chunk_bytes * total_chunks) / (1024**2)
        }
    
    return estimates


def print_memory_summary(estimates: Dict[str, Any]) -> None:
    """Print a summary of memory estimates.
    
    Args:
        estimates: Memory estimates from estimate_chunk_memory
    """
    print("\n💾 Memory Estimates:")
    print("-" * 50)
    
    total_mb = 0
    for var_name, info in estimates.items():
        print(f"Variable: {var_name}")
        print(f"  Chunk shape: {info['chunk_shape']}")
        print(f"  Chunk size: {info['chunk_mb']:.1f} MB")
        print(f"  Total chunks: {info['total_chunks']}")
        print(f"  Total memory: {info['total_mb']:.1f} MB")
        print()
        
        total_mb += info['total_mb']
    
    print(f"📊 Total dataset memory: {total_mb:.1f} MB ({total_mb/1024:.1f} GB)")


def check_environment() -> Dict[str, Any]:
    """Check the computing environment and available resources.
    
    Returns:
        Dictionary with environment information
    """
    env_info = {
        'platform': 'unknown',
        'python_version': 'unknown',
        'packages': {},
        'resources': {},
        'hpc': {}
    }
    
    # Platform information
    try:
        import platform
        env_info['platform'] = platform.system()
        env_info['python_version'] = platform.python_version()
    except ImportError:
        pass
    
    # Check available packages
    packages_to_check = [
        'dask', 'distributed', 'xarray', 'numpy', 'pandas', 
        'zarr', 'netcdf4', 'h5netcdf', 'rechunker', 'psutil'
    ]
    
    for pkg in packages_to_check:
        try:
            module = __import__(pkg)
            env_info['packages'][pkg] = getattr(module, '__version__', 'unknown')
        except ImportError:
            env_info['packages'][pkg] = None
    
    # Resource information
    try:
        import psutil
        env_info['resources'] = {
            'cpu_count': psutil.cpu_count(),
            'memory_gb': psutil.virtual_memory().total / (1024**3)
        }
    except ImportError:
        pass
    
    # HPC environment detection
    import os
    env_info['hpc'] = {
        'pbs_jobid': os.environ.get('PBS_JOBID'),
        'pbs_jobfs': os.environ.get('PBS_JOBFS'),
        'slurm_job_id': os.environ.get('SLURM_JOB_ID'),
        'ncpus': os.environ.get('NCPUS'),
        'hostname': socket.gethostname()
    }
    
    return env_info


def print_environment_summary(env_info: Dict[str, Any] = None) -> None:
    """Print a summary of the computing environment.
    
    Args:
        env_info: Environment information (if None, will check automatically)
    """
    if env_info is None:
        env_info = check_environment()
    
    print("🖥️  Computing Environment:")
    print("-" * 30)
    print(f"Platform: {env_info['platform']}")
    print(f"Python: {env_info['python_version']}")
    
    if env_info['resources']:
        res = env_info['resources']
        print(f"CPUs: {res.get('cpu_count', 'unknown')}")
        print(f"Memory: {res.get('memory_gb', 0):.1f} GB")
    
    # HPC info
    hpc = env_info['hpc']
    if hpc['pbs_jobid']:
        print(f"PBS Job: {hpc['pbs_jobid']}")
    if hpc['slurm_job_id']:
        print(f"SLURM Job: {hpc['slurm_job_id']}")
    if hpc['hostname']:
        print(f"Hostname: {hpc['hostname']}")
    
    # Package versions
    print(f"\n📦 Key Packages:")
    important_packages = ['dask', 'distributed', 'xarray', 'numpy']
    for pkg in important_packages:
        version = env_info['packages'].get(pkg)
        if version:
            print(f"  {pkg}: {version}")
        else:
            print(f"  {pkg}: ❌ Not installed")