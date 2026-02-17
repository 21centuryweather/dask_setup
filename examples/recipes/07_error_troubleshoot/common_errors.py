#!/usr/bin/env python
"""Common errors and troubleshooting demonstration.

This recipe demonstrates common errors encountered when using dask_setup with
scientific computing workflows, provides reproducible error scenarios, and
shows how to diagnose and fix these issues.

Requirements:
- dask_setup, dask, distributed
- xarray, numpy
- psutil (for memory monitoring)

Outputs:
- Synthetic error demonstrations
- Solution implementations
- Troubleshooting guide table
- Memory usage analysis
- Error recovery strategies

Key Learning Points:
- Memory limit exceeded errors and solutions
- Worker failures and recovery
- File access and permission issues
- Chunk size optimization problems
- Dashboard debugging techniques
- Performance bottleneck identification
"""

import argparse
import logging
import os
import sys
import tempfile
import time
import traceback
import warnings
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Add the parent directory to path so we can import dask_setup and utils
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import dask
    import dask.array as da
    import numpy as np
    import xarray as xr
    
    from dask_setup import setup_dask_client
    from utils import format_bytes, format_duration, save_markdown_table, timer
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed xarray numpy")
    sys.exit(1)

# Check for optional dependencies
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️  psutil not available - memory monitoring limited")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )


@contextmanager
def capture_error_context(error_name: str, expected_error: bool = True):
    """Context manager to capture and handle expected errors.
    
    Args:
        error_name: Name of the error scenario
        expected_error: Whether the error is expected (for demonstration)
    """
    print(f"\n🧪 TESTING ERROR SCENARIO: {error_name}")
    print("-" * 50)
    
    try:
        yield
        if expected_error:
            print(f"⚠️  Expected error did not occur in {error_name}")
        else:
            print(f"✅ Scenario {error_name} completed successfully")
    except Exception as e:
        error_type = type(e).__name__
        print(f"❌ Error caught: {error_type}")
        print(f"   Message: {str(e)}")
        
        if not expected_error:
            print(f"   Unexpected error in {error_name}")
            traceback.print_exc()
        
        return {
            'error_type': error_type,
            'error_message': str(e),
            'scenario': error_name,
            'expected': expected_error
        }
    
    return None


def get_memory_usage() -> Dict[str, float]:
    """Get current memory usage information.
    
    Returns:
        Dictionary with memory usage in GB
    """
    if not PSUTIL_AVAILABLE:
        return {'total': 0.0, 'available': 0.0, 'used': 0.0, 'percent': 0.0}
    
    memory = psutil.virtual_memory()
    return {
        'total': memory.total / (1024**3),
        'available': memory.available / (1024**3),
        'used': memory.used / (1024**3),
        'percent': memory.percent
    }


def demonstrate_memory_limit_error(client) -> Dict[str, Any]:
    """Demonstrate memory limit exceeded errors and solutions.
    
    Args:
        client: Dask client
        
    Returns:
        Dictionary with error demonstration results
    """
    print("\n" + "=" * 60)
    print("💾 MEMORY LIMIT ERROR DEMONSTRATION")
    print("=" * 60)
    
    results = {}
    
    # Get worker memory limits
    worker_info = client.scheduler_info()['workers']
    if not worker_info:
        print("❌ No workers available for memory testing")
        return {'error': 'No workers available'}
    
    # Calculate worker memory limit (approximate)
    worker_memory_limit = None
    for worker_id, info in worker_info.items():
        if 'memory_limit' in info:
            worker_memory_limit = info['memory_limit'] / (1024**3)  # Convert to GB
            break
    
    if worker_memory_limit is None:
        print("⚠️  Could not determine worker memory limit")
        worker_memory_limit = 2.0  # Assume 2GB default
    
    print(f"Worker memory limit: ~{worker_memory_limit:.1f} GB")
    
    # Scenario 1: Create array that exceeds memory limit
    print("\n🔴 SCENARIO 1: Creating oversized array")
    with capture_error_context("Oversized Array Creation", expected_error=True) as error1:
        # Try to create an array larger than worker memory
        # Target ~150% of worker memory limit
        target_gb = worker_memory_limit * 1.5
        target_bytes = int(target_gb * 1024**3)
        
        # Calculate array shape for target size (float64 = 8 bytes)
        array_size = target_bytes // 8
        shape = (int(np.sqrt(array_size)), int(np.sqrt(array_size)))
        
        print(f"   Attempting to create array of shape {shape}")
        print(f"   Estimated size: {target_gb:.1f} GB")
        
        # This should trigger a memory error
        large_array = da.random.random(shape, chunks=(1000, 1000))
        result = large_array.sum().compute()  # Force computation
        
        print(f"   Unexpected success: sum = {result}")
    
    results['oversized_array'] = error1
    
    # Scenario 2: Demonstrate proper chunking solution
    print("\n✅ SOLUTION 1: Proper chunking for large arrays")
    with capture_error_context("Proper Chunking", expected_error=False) as solution1:
        # Same array size but with appropriate chunks
        chunk_size_mb = min(256, worker_memory_limit * 1024 * 0.2)  # 20% of worker memory or 256MB
        chunk_elements = int(chunk_size_mb * 1024**2 / 8)  # 8 bytes per float64
        chunk_dim = int(np.sqrt(chunk_elements))
        
        print(f"   Creating same array with chunk size: {chunk_size_mb:.0f} MB")
        print(f"   Chunk dimensions: ({chunk_dim}, {chunk_dim})")
        
        chunked_array = da.random.random(shape, chunks=(chunk_dim, chunk_dim))
        
        # Perform computation that doesn't require loading all data
        mean_result = chunked_array.mean().compute()
        print(f"   Successfully computed mean: {mean_result:.6f}")
    
    results['proper_chunking'] = solution1
    
    # Scenario 3: Demonstrate memory spilling
    print("\n🔄 SCENARIO 3: Memory spilling demonstration")
    with capture_error_context("Memory Spilling", expected_error=False) as spill_demo:
        # Create multiple moderate-sized arrays to trigger spilling
        arrays = []
        spill_target_gb = worker_memory_limit * 0.9  # Target 90% of memory
        
        for i in range(5):
            array_gb = spill_target_gb / 5
            array_elements = int(array_gb * 1024**3 / 8)
            array_shape = (int(np.sqrt(array_elements)),) * 2
            chunk_size = min(1000, int(np.sqrt(array_elements) / 4))
            
            arr = da.random.random(array_shape, chunks=(chunk_size, chunk_size))
            arrays.append(arr)
            print(f"   Created array {i+1}: {array_shape} ({array_gb:.2f} GB)")
        
        # Perform operations that should trigger spilling
        print("   Computing sums (should trigger memory spilling)...")
        sums = [arr.sum().compute() for arr in arrays]
        print(f"   Computed {len(sums)} array sums successfully")
    
    results['memory_spilling'] = spill_demo
    
    return results


def demonstrate_worker_failures(client) -> Dict[str, Any]:
    """Demonstrate worker failure scenarios and recovery.
    
    Args:
        client: Dask client
        
    Returns:
        Dictionary with worker failure demonstration results
    """
    print("\n" + "=" * 60)
    print("👥 WORKER FAILURE DEMONSTRATION")
    print("=" * 60)
    
    results = {}
    initial_workers = len(client.scheduler_info()['workers'])
    print(f"Initial worker count: {initial_workers}")
    
    if initial_workers == 0:
        print("❌ No workers available for failure testing")
        return {'error': 'No workers available'}
    
    # Scenario 1: Demonstrate task cancellation recovery
    print("\n🔴 SCENARIO 1: Task interruption and recovery")
    with capture_error_context("Task Interruption", expected_error=False) as interrupt_demo:
        # Create a long-running computation
        print("   Starting long computation...")
        large_array = da.random.random((5000, 5000), chunks=(1000, 1000))
        
        # Start computation in background
        future = client.compute(large_array.sum(), sync=False)
        
        # Wait briefly then cancel
        time.sleep(1.0)
        print("   Cancelling computation...")
        client.cancel(future)
        
        # Try a new computation
        print("   Starting new computation after cancellation...")
        smaller_array = da.random.random((1000, 1000), chunks=(500, 500))
        result = smaller_array.mean().compute()
        print(f"   New computation succeeded: mean = {result:.6f}")
    
    results['task_interruption'] = interrupt_demo
    
    # Scenario 2: Demonstrate resource exhaustion
    print("\n🔴 SCENARIO 2: Resource exhaustion handling")
    with capture_error_context("Resource Exhaustion", expected_error=True) as resource_demo:
        # Try to submit many tasks simultaneously
        print("   Submitting many concurrent tasks...")
        
        futures = []
        for i in range(50):  # Submit many small tasks
            arr = da.random.random((100, 100), chunks=(50, 50))
            future = client.compute(arr.sum(), sync=False)
            futures.append(future)
        
        # Wait for some to complete
        completed = 0
        failed = 0
        
        for future in futures[:10]:  # Check first 10 only
            try:
                result = client.gather(future, timeout=5.0)
                completed += 1
            except Exception as e:
                failed += 1
                if failed == 1:  # Only print first error
                    print(f"   Task failure example: {type(e).__name__}")
        
        # Cancel remaining
        for future in futures[10:]:
            client.cancel(future)
        
        print(f"   Task summary: {completed} completed, {failed} failed")
    
    results['resource_exhaustion'] = resource_demo
    
    return results


def demonstrate_file_access_errors() -> Dict[str, Any]:
    """Demonstrate file access and I/O related errors.
    
    Returns:
        Dictionary with file access error results
    """
    print("\n" + "=" * 60)
    print("📁 FILE ACCESS ERROR DEMONSTRATION")
    print("=" * 60)
    
    results = {}
    
    # Scenario 1: Missing file error
    print("\n🔴 SCENARIO 1: Missing file access")
    with capture_error_context("Missing File", expected_error=True) as missing_file:
        nonexistent_file = "/path/that/does/not/exist.nc"
        print(f"   Attempting to open: {nonexistent_file}")
        
        # This should fail
        ds = xr.open_dataset(nonexistent_file)
        print("   Unexpected success opening non-existent file")
    
    results['missing_file'] = missing_file
    
    # Scenario 2: Permission denied error simulation
    print("\n🔴 SCENARIO 2: Permission issues")
    with capture_error_context("Permission Issues", expected_error=False) as perm_demo:
        # Create a temporary file and try to write to read-only location
        try:
            # Try to create file in a system directory (should fail gracefully)
            restricted_path = "/root/test_file.nc" if os.name != 'nt' else "C:\\Windows\\test_file.nc"
            
            print(f"   Testing write access to: {restricted_path}")
            
            # Create sample data
            data = np.random.random((10, 10))
            ds = xr.Dataset({'data': (['x', 'y'], data)})
            
            # This will likely fail with permission error
            ds.to_netcdf(restricted_path)
            print("   Unexpected success writing to restricted location")
            
        except (PermissionError, OSError) as e:
            print(f"   Expected permission error: {type(e).__name__}")
            print("   This is normal system protection")
        except Exception as e:
            print(f"   Other error: {type(e).__name__}: {e}")
    
    results['permission_issues'] = perm_demo
    
    # Scenario 3: Disk space simulation
    print("\n🔴 SCENARIO 3: Insufficient disk space handling")
    with capture_error_context("Disk Space", expected_error=False) as disk_demo:
        # Create temporary file that's reasonably sized
        temp_dir = tempfile.mkdtemp()
        temp_file = Path(temp_dir) / "large_test.nc"
        
        print(f"   Creating test file: {temp_file}")
        
        try:
            # Create moderately large dataset
            data = np.random.random((100, 200, 300))  # ~480MB uncompressed
            ds = xr.Dataset({
                'temperature': (['time', 'lat', 'lon'], data)
            })
            
            # Write with compression to manage size
            encoding = {'temperature': {'zlib': True, 'complevel': 6}}
            ds.to_netcdf(temp_file, encoding=encoding)
            
            file_size = temp_file.stat().st_size / (1024**2)
            print(f"   Successfully created file: {file_size:.1f} MB")
            
            # Clean up
            temp_file.unlink()
            os.rmdir(temp_dir)
            
        except Exception as e:
            print(f"   File creation error: {type(e).__name__}: {e}")
            # Clean up on error
            if temp_file.exists():
                temp_file.unlink()
            if os.path.exists(temp_dir):
                os.rmdir(temp_dir)
    
    results['disk_space'] = disk_demo
    
    return results


def demonstrate_chunking_problems(client) -> Dict[str, Any]:
    """Demonstrate chunk size optimization problems and solutions.
    
    Args:
        client: Dask client
        
    Returns:
        Dictionary with chunking problem results
    """
    print("\n" + "=" * 60)
    print("🧩 CHUNKING OPTIMIZATION PROBLEMS")
    print("=" * 60)
    
    results = {}
    
    # Scenario 1: Too many small chunks (overhead problem)
    print("\n🔴 SCENARIO 1: Too many small chunks")
    with capture_error_context("Small Chunks", expected_error=False) as small_chunks:
        print("   Creating array with very small chunks...")
        
        # Create array with tiny chunks (lots of overhead)
        array_shape = (2000, 2000)
        tiny_chunks = (10, 10)  # Very small chunks
        
        start_time = time.time()
        arr = da.random.random(array_shape, chunks=tiny_chunks)
        
        print(f"   Array shape: {array_shape}")
        print(f"   Chunk size: {tiny_chunks}")
        print(f"   Number of chunks: {arr.nchunks}")
        
        # This will be slow due to overhead
        result = arr.mean().compute()
        small_chunk_time = time.time() - start_time
        
        print(f"   Computation time: {small_chunk_time:.2f}s")
        print(f"   Result: {result:.6f}")
    
    results['small_chunks'] = {**small_chunks, 'time': small_chunk_time}
    
    # Scenario 2: Optimal chunking solution
    print("\n✅ SOLUTION 1: Optimal chunk sizing")
    with capture_error_context("Optimal Chunks", expected_error=False) as optimal_chunks:
        print("   Creating same array with optimal chunks...")
        
        # Same array with better chunks (target ~256MB chunks)
        target_chunk_mb = 256
        bytes_per_element = 8  # float64
        elements_per_chunk = (target_chunk_mb * 1024**2) // bytes_per_element
        chunk_dim = int(np.sqrt(elements_per_chunk))
        optimal_chunk_size = (chunk_dim, chunk_dim)
        
        start_time = time.time()
        arr_optimal = da.random.random(array_shape, chunks=optimal_chunk_size)
        
        print(f"   Array shape: {array_shape}")
        print(f"   Chunk size: {optimal_chunk_size}")
        print(f"   Number of chunks: {arr_optimal.nchunks}")
        print(f"   Target chunk size: ~{target_chunk_mb} MB")
        
        result = arr_optimal.mean().compute()
        optimal_time = time.time() - start_time
        
        print(f"   Computation time: {optimal_time:.2f}s")
        print(f"   Result: {result:.6f}")
        
        if 'time' in results['small_chunks']:
            speedup = results['small_chunks']['time'] / optimal_time
            print(f"   Speedup vs small chunks: {speedup:.2f}x")
    
    results['optimal_chunks'] = {**optimal_chunks, 'time': optimal_time}
    
    # Scenario 3: Chunks too large for memory
    print("\n🔴 SCENARIO 3: Chunks larger than memory")
    with capture_error_context("Large Chunks", expected_error=True) as large_chunks:
        # Try to create chunks that are too large for worker memory
        worker_info = client.scheduler_info()['workers']
        
        if worker_info:
            # Get approximate worker memory limit
            worker_memory = None
            for worker_id, info in worker_info.items():
                if 'memory_limit' in info:
                    worker_memory = info['memory_limit']
                    break
            
            if worker_memory:
                # Try to create chunks ~200% of worker memory
                target_bytes = int(worker_memory * 2.0)
                elements = target_bytes // 8  # float64
                chunk_dim = int(np.sqrt(elements))
                
                print(f"   Attempting chunk size: ({chunk_dim}, {chunk_dim})")
                print(f"   Estimated chunk size: {target_bytes / (1024**3):.2f} GB")
                
                large_chunk_array = da.random.random((chunk_dim * 2, chunk_dim * 2), 
                                                   chunks=(chunk_dim, chunk_dim))
                result = large_chunk_array.sum().compute()
                print(f"   Unexpected success: {result}")
            else:
                print("   Could not determine worker memory limit")
        else:
            print("   No workers available for large chunk test")
    
    results['large_chunks'] = large_chunks
    
    return results


def demonstrate_performance_bottlenecks(client) -> Dict[str, Any]:
    """Demonstrate common performance bottlenecks and solutions.
    
    Args:
        client: Dask client
        
    Returns:
        Dictionary with performance bottleneck results
    """
    print("\n" + "=" * 60)
    print("⚡ PERFORMANCE BOTTLENECK DEMONSTRATION")
    print("=" * 60)
    
    results = {}
    
    # Scenario 1: GIL-bound operations
    print("\n🔴 SCENARIO 1: GIL contention in threads")
    with capture_error_context("GIL Contention", expected_error=False) as gil_demo:
        print("   Testing CPU-intensive operations with threads...")
        
        # Create computation that's GIL-bound
        def cpu_bound_function(x):
            """CPU intensive function that holds GIL."""
            result = 0
            for i in range(int(x)):
                result += i ** 0.5
            return result
        
        # Create array and apply CPU-bound function
        arr = da.random.random((100, 100), chunks=(50, 50)) * 1000
        
        start_time = time.time()
        # This will be limited by GIL in thread-based workers
        result = arr.map_blocks(lambda x: np.vectorize(cpu_bound_function)(x), 
                              dtype=arr.dtype).sum().compute()
        gil_time = time.time() - start_time
        
        print(f"   CPU-bound computation time: {gil_time:.2f}s")
        print(f"   Result: {result:.2e}")
    
    results['gil_contention'] = {**gil_demo, 'time': gil_time}
    
    # Scenario 2: I/O bottleneck simulation
    print("\n🔴 SCENARIO 2: I/O bottleneck")
    with capture_error_context("I/O Bottleneck", expected_error=False) as io_demo:
        print("   Simulating I/O-intensive operations...")
        
        def simulate_io_delay(x):
            """Simulate I/O delay."""
            time.sleep(0.01)  # 10ms delay per chunk
            return x.sum()
        
        arr = da.random.random((200, 200), chunks=(50, 50))
        
        start_time = time.time()
        result = arr.map_blocks(simulate_io_delay, dtype=np.float64, 
                              drop_axis=[1]).sum().compute()
        io_time = time.time() - start_time
        
        print(f"   I/O simulation time: {io_time:.2f}s")
        print(f"   Result: {result:.2e}")
    
    results['io_bottleneck'] = {**io_demo, 'time': io_time}
    
    # Scenario 3: Memory bandwidth limitation
    print("\n🔴 SCENARIO 3: Memory bandwidth bottleneck")
    with capture_error_context("Memory Bandwidth", expected_error=False) as memory_demo:
        print("   Testing memory-intensive operations...")
        
        # Create large arrays for memory bandwidth test
        arr1 = da.random.random((1000, 1000), chunks=(500, 500))
        arr2 = da.random.random((1000, 1000), chunks=(500, 500))
        
        start_time = time.time()
        # Memory-intensive operations
        result = ((arr1 + arr2) * (arr1 - arr2) + (arr1 * arr2)).sum().compute()
        memory_time = time.time() - start_time
        
        print(f"   Memory-intensive computation time: {memory_time:.2f}s")
        print(f"   Result: {result:.2e}")
    
    results['memory_bandwidth'] = {**memory_demo, 'time': memory_time}
    
    return results


def create_troubleshooting_guide_table(
    memory_results: Dict,
    worker_results: Dict,
    file_results: Dict,
    chunking_results: Dict,
    performance_results: Dict,
    output_file: Path
) -> None:
    """Create comprehensive troubleshooting guide table.
    
    Args:
        memory_results: Memory error results
        worker_results: Worker failure results
        file_results: File access error results
        chunking_results: Chunking problem results
        performance_results: Performance bottleneck results
        output_file: Output file path
    """
    print("\n" + "=" * 60)
    print("📚 TROUBLESHOOTING GUIDE")
    print("=" * 60)
    
    # Create comprehensive troubleshooting data
    troubleshooting_data = [
        # Memory Issues
        ["Memory", "MemoryError/KilledWorker", "Reduce chunk size, increase workers", "Use chunks targeting 256-512MB"],
        ["Memory", "Task exceeds memory limit", "Lower reserve_mem_gb parameter", "Check worker memory with client.scheduler_info()"],
        ["Memory", "Spilling too aggressive", "Tune spill thresholds in config", "Monitor with dashboard memory tab"],
        
        # Worker Issues  
        ["Workers", "All workers killed", "Reduce memory pressure", "Check system resources with htop/top"],
        ["Workers", "Tasks stuck in queue", "Check worker connectivity", "Restart cluster if needed"],
        ["Workers", "No workers available", "Check cluster startup", "Verify setup_dask_client() call"],
        
        # File Access
        ["File I/O", "FileNotFoundError", "Check file paths", "Use absolute paths or verify working directory"],
        ["File I/O", "PermissionError", "Check file permissions", "Ensure write access to output directories"],
        ["File I/O", "DiskSpaceError", "Free up disk space", "Use $PBS_JOBFS or $TMPDIR for temporary files"],
        
        # Chunking
        ["Chunking", "Too many chunks (slow)", "Increase chunk size", "Target 256-512MB per chunk"],
        ["Chunking", "Chunks too large", "Decrease chunk size", "Stay under worker memory limit"],
        ["Chunking", "Uneven chunks", "Use explicit chunk sizes", "Avoid 'auto' chunking for consistent performance"],
        
        # Performance
        ["Performance", "GIL contention", "Use processes instead of threads", "Set workload_type='cpu' in setup_dask_client"],
        ["Performance", "I/O bottleneck", "Use more threads, fewer processes", "Set workload_type='io' in setup_dask_client"],
        ["Performance", "Network overhead", "Optimize chunk size", "Reduce data transfer between workers"],
        
        # Dashboard
        ["Dashboard", "Cannot access dashboard", "Check SSH tunnel", "Use printed tunnel command"],
        ["Dashboard", "Dashboard shows errors", "Check worker logs", "Look for memory/disk issues"],
        ["Dashboard", "Tasks stuck", "Check task dependencies", "Cancel and restart if needed"],
        
        # Configuration
        ["Config", "Import errors", "Check environment", "Verify all dependencies installed"],
        ["Config", "HPC detection fails", "Set env variables manually", "Export NCPUS, PBS_MEM etc. if needed"],
        ["Config", "Temp directory issues", "Check $PBS_JOBFS access", "Ensure temp directory is writable"]
    ]
    
    headers = ["Category", "Error/Issue", "Immediate Solution", "Best Practice"]
    
    # Print console table
    print("\n🔧 Common Issues and Solutions:")
    print_troubleshooting_table(headers, troubleshooting_data)
    
    # Add diagnostic commands
    diagnostic_data = [
        ["Memory Usage", "htop or top", "System memory monitoring"],
        ["Disk Usage", "df -h", "Check available disk space"],
        ["Dask Status", "client.scheduler_info()", "Worker status and resources"],
        ["Task Status", "client.who_has()", "Task distribution across workers"],
        ["Dashboard", "Print dashboard URL", "Visual cluster monitoring"],
        ["Worker Logs", "client.get_worker_logs()", "Detailed error messages"],
        ["Cluster Info", "cluster.scale_status()", "Scaling and resource info"],
        ["Environment", "echo $PBS_JOBFS $TMPDIR", "Check HPC environment variables"]
    ]
    
    print("\n🔍 Diagnostic Commands:")
    print_troubleshooting_table(["Diagnostic", "Command", "Description"], diagnostic_data)
    
    # Save to markdown
    all_data = troubleshooting_data + [["---", "---", "---", "---"]] + \
               [[f"Diagnostic: {row[0]}", row[1], row[2], ""] for row in diagnostic_data]
    
    save_markdown_table(
        data=all_data,
        headers=headers,
        filename=output_file,
        title="Dask Setup Troubleshooting Guide",
        description=(
            "Comprehensive guide to common errors and issues when using dask_setup. "
            "Includes symptoms, immediate solutions, and best practices for prevention. "
            "Use this guide to quickly diagnose and resolve problems in your workflows."
        )
    )
    
    # Print error summary
    print(f"\n📊 Error Demonstration Summary:")
    all_results = [memory_results, worker_results, file_results, chunking_results, performance_results]
    total_scenarios = sum(len([k for k in r.keys() if k != 'error']) for r in all_results if r)
    successful_demos = sum(
        sum(1 for k, v in r.items() 
            if k != 'error' and isinstance(v, dict) and v is not None)
        for r in all_results if r
    )
    
    print(f"   Total scenarios tested: {total_scenarios}")
    print(f"   Successful demonstrations: {successful_demos}")
    print(f"   Troubleshooting guide saved: {output_file}")


def print_troubleshooting_table(headers: List[str], data: List[List[str]]) -> None:
    """Print a formatted troubleshooting table.
    
    Args:
        headers: Column headers
        data: Table data rows
    """
    if not data:
        print("   No data to display")
        return
        
    # Calculate column widths
    col_widths = [len(h) for h in headers]
    for row in data:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))
    
    # Print header
    header_row = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(f"   | {header_row} |")
    print("   |" + "|".join("-" * (w + 2) for w in col_widths) + "|")
    
    # Print data rows
    for row in data:
        if row[0] == "---":  # Separator row
            sep_row = " | ".join("-" * col_widths[i] for i in range(len(headers)))
            print(f"   | {sep_row} |")
        else:
            data_row = " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row)))
            print(f"   | {data_row} |")


def print_debugging_tips() -> None:
    """Print debugging and diagnostic tips."""
    print("\n" + "=" * 60)
    print("🐛 DEBUGGING AND DIAGNOSTIC TIPS")
    print("=" * 60)
    
    print("""
🔍 DASHBOARD DEBUGGING:

   Access Dashboard:
   • Use SSH tunnel printed by setup_dask_client()
   • Navigate to Task Stream and Progress tabs
   • Memory tab shows worker memory usage
   • Workers tab shows individual worker status
   
   Reading Task Stream:
   • Red bars = failed tasks
   • Orange bars = retried tasks  
   • Long bars = slow tasks
   • Gaps = scheduling overhead
   
   Memory Monitoring:
   • Watch for memory spikes near limits
   • Yellow/red = approaching memory limits
   • Spilling = orange bars in memory graph

📊 PERFORMANCE PROFILING:

   Built-in Profiling:
   with dask.config.set(scheduler='single-threaded'):
       result = computation.compute()  # For debugging
   
   Memory Profiling:
   @dask.delayed
   def profile_memory(func, *args):
       import psutil
       proc = psutil.Process()
       before = proc.memory_info().rss
       result = func(*args)
       after = proc.memory_info().rss
       print(f"Memory delta: {(after-before)/1024**2:.1f} MB")
       return result

🚨 COMMON WARNING SIGNS:

   Memory Issues:
   • "Worker exceeded 95% memory budget"
   • "Spilling to disk" messages
   • Tasks taking much longer than expected
   
   I/O Issues:
   • "Connection refused" errors
   • Very slow file operations
   • Missing file or permission errors
   
   Chunk Issues:
   • Thousands of tiny tasks in dashboard
   • Very few large tasks (under-parallelized)
   • Memory usage spikes during rechunking

💡 OPTIMIZATION STRATEGIES:

   Memory Optimization:
   1. Start with fewer workers, more memory each
   2. Use larger chunks (256-512 MB target)
   3. Monitor spill-to-disk activity
   4. Consider data types (float32 vs float64)
   
   I/O Optimization:
   1. Use workload_type='io' for file-heavy work
   2. Prefer h5netcdf engine for parallel NetCDF reads
   3. Write to local storage ($PBS_JOBFS) first
   4. Batch small files into larger chunks
   
   Compute Optimization:
   1. Use workload_type='cpu' for computation
   2. Profile with single-threaded scheduler first
   3. Check for GIL-bound operations
   4. Consider numba.jit for tight loops

🔧 EMERGENCY FIXES:

   When Everything Breaks:
   1. client.restart()  # Restart all workers
   2. client.cancel(future)  # Cancel stuck tasks
   3. client.close(); cluster.close()  # Nuclear option
   4. Check system resources (htop, df -h)
   5. Clear temp directories if full
   
   Quick Diagnostics:
   • client.scheduler_info()['workers']  # Worker status
   • client.who_has()  # Task distribution  
   • client.get_worker_logs()  # Error messages
   • dask.config.get()  # Current configuration
""")


def main():
    """Main function demonstrating error troubleshooting."""
    parser = argparse.ArgumentParser(
        description="Demonstrate common errors and troubleshooting techniques",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--scenario",
        choices=["memory", "workers", "files", "chunking", "performance", "all"],
        default="all",
        help="Specific error scenario to demonstrate (default: all)"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="Number of Dask workers (default: 2)"
    )
    
    parser.add_argument(
        "--memory-limit",
        type=float,
        default=2.0,
        help="Memory limit per worker in GB (default: 2.0)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "outputs",
        help="Output directory for results"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output and debug logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging and output directory
    setup_logging(args.verbose)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    
    print("🐛 COMMON ERRORS AND TROUBLESHOOTING DEMONSTRATION")
    print("=" * 60)
    print(f"Testing scenarios: {args.scenario}")
    print(f"Workers: {args.workers}")
    print(f"Memory limit per worker: {args.memory_limit:.1f} GB")
    print(f"Output directory: {args.output_dir}")
    
    if PSUTIL_AVAILABLE:
        memory_info = get_memory_usage()
        print(f"System memory: {memory_info['used']:.1f}/{memory_info['total']:.1f} GB "
              f"({memory_info['percent']:.1f}% used)")
    
    try:
        # Create Dask client with limited resources for error testing
        client, cluster, temp_dir = setup_dask_client(
            workload_type="mixed",
            max_workers=args.workers,
            max_mem_gb=args.memory_limit * args.workers,
            reserve_mem_gb=1.0,  # Lower reserve for error testing
            dashboard=True
        )
        
        print(f"\n🖥️  Test Cluster: {len(client.scheduler_info()['workers'])} workers")
        print(f"   Memory per worker: ~{args.memory_limit:.1f} GB")
        print(f"   Temp directory: {temp_dir}")
        
        # Store results from each scenario
        all_results = {}
        
        # Run error scenarios based on selection
        if args.scenario in ["memory", "all"]:
            all_results["memory"] = demonstrate_memory_limit_error(client)
        
        if args.scenario in ["workers", "all"]:
            all_results["workers"] = demonstrate_worker_failures(client)
        
        if args.scenario in ["files", "all"]:
            all_results["files"] = demonstrate_file_access_errors()
        
        if args.scenario in ["chunking", "all"]:
            all_results["chunking"] = demonstrate_chunking_problems(client)
        
        if args.scenario in ["performance", "all"]:
            all_results["performance"] = demonstrate_performance_bottlenecks(client)
        
        # Create comprehensive troubleshooting guide
        guide_file = args.output_dir / "troubleshooting_guide.md"
        create_troubleshooting_guide_table(
            all_results.get("memory", {}),
            all_results.get("workers", {}),
            all_results.get("files", {}),
            all_results.get("chunking", {}),
            all_results.get("performance", {}),
            guide_file
        )
        
        # Print debugging tips
        print_debugging_tips()
        
        print(f"\n✅ Error troubleshooting demonstration completed!")
        print(f"📚 Troubleshooting guide: {guide_file}")
        
        # Clean up
        client.close()
        cluster.close()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Demonstration interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Demonstration failed: {e}")
        if args.verbose:
            traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())