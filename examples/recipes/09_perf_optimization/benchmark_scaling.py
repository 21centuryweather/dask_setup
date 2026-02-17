#!/usr/bin/env python
"""Performance optimization and scaling benchmarks.

This recipe demonstrates performance optimization techniques and scaling behavior
analysis for dask_setup workflows. It includes parameter sweeps, adaptive scaling
strategies, performance profiling, and optimization recommendations.

Requirements:
- dask_setup, dask, distributed
- xarray, numpy
- matplotlib (for performance plots)
- psutil (for resource monitoring)

Outputs:
- Scaling performance measurements
- Optimization recommendations
- Performance plots and analysis
- Adaptive scaling demonstrations
- Resource utilization profiles
- Benchmark comparison tables

Key Learning Points:
- Performance scaling patterns (strong vs weak scaling)
- Worker count optimization strategies
- Memory vs compute trade-offs
- Workload-specific configuration tuning
- Resource utilization monitoring
- Adaptive scaling implementation
- Performance bottleneck identification
"""

import argparse
import logging
import os
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Callable
from concurrent.futures import as_completed
from dataclasses import dataclass
from datetime import datetime

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
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("⚠️  matplotlib not available - plotting disabled")

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️  psutil not available - resource monitoring limited")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""
    workers: int
    task_type: str
    data_size_gb: float
    chunk_size_mb: float
    computation_time: float
    throughput_gb_s: float
    memory_peak_gb: float
    memory_efficiency: float
    cpu_utilization: float
    scaling_efficiency: float = 0.0
    notes: str = ""


@dataclass
class ResourceSnapshot:
    """Resource utilization snapshot."""
    timestamp: float
    cpu_percent: float
    memory_used_gb: float
    memory_percent: float
    disk_io_read_mb: float = 0.0
    disk_io_write_mb: float = 0.0


class PerformanceMonitor:
    """Monitor system resource utilization during benchmarks."""
    
    def __init__(self):
        self.snapshots = []
        self.monitoring = False
        self.start_time = None
        
    def start_monitoring(self):
        """Start resource monitoring."""
        if not PSUTIL_AVAILABLE:
            return
            
        self.monitoring = True
        self.start_time = time.time()
        self.snapshots = []
        
    def take_snapshot(self):
        """Take a resource utilization snapshot."""
        if not PSUTIL_AVAILABLE or not self.monitoring:
            return
            
        try:
            snapshot = ResourceSnapshot(
                timestamp=time.time() - self.start_time,
                cpu_percent=psutil.cpu_percent(interval=None),
                memory_used_gb=psutil.virtual_memory().used / (1024**3),
                memory_percent=psutil.virtual_memory().percent
            )
            
            # Get disk I/O if available
            try:
                disk_io = psutil.disk_io_counters()
                if disk_io:
                    snapshot.disk_io_read_mb = disk_io.read_bytes / (1024**2)
                    snapshot.disk_io_write_mb = disk_io.write_bytes / (1024**2)
            except:
                pass
                
            self.snapshots.append(snapshot)
        except Exception as e:
            print(f"⚠️  Resource monitoring error: {e}")
            
    def stop_monitoring(self) -> Dict[str, float]:
        """Stop monitoring and return summary statistics."""
        self.monitoring = False
        
        if not self.snapshots:
            return {}
            
        cpu_values = [s.cpu_percent for s in self.snapshots]
        memory_values = [s.memory_used_gb for s in self.snapshots]
        
        return {
            'cpu_mean': np.mean(cpu_values),
            'cpu_max': np.max(cpu_values),
            'memory_mean': np.mean(memory_values),
            'memory_max': np.max(memory_values),
            'duration': self.snapshots[-1].timestamp if self.snapshots else 0.0
        }


def create_benchmark_dataset(
    size_gb: float,
    chunk_size_mb: float = 256,
    dimensions: Tuple[str, ...] = ('time', 'lat', 'lon')
) -> da.Array:
    """Create a benchmark dataset of specified size.
    
    Args:
        size_gb: Target dataset size in GB
        chunk_size_mb: Chunk size in MB
        dimensions: Dataset dimensions
        
    Returns:
        Dask array for benchmarking
    """
    # Calculate array shape for target size
    bytes_per_element = 8  # float64
    total_elements = int(size_gb * 1024**3 / bytes_per_element)
    
    # Distribute elements across dimensions
    if len(dimensions) == 3:
        # Assume roughly equal distribution with time being largest
        time_size = int(total_elements ** (1/3) * 2)
        spatial_size = int((total_elements / time_size) ** 0.5)
        shape = (time_size, spatial_size, spatial_size)
    elif len(dimensions) == 2:
        dim_size = int(total_elements ** 0.5)
        shape = (dim_size, dim_size)
    else:
        shape = (total_elements,)
    
    # Calculate chunk size
    chunk_elements = int(chunk_size_mb * 1024**2 / bytes_per_element)
    
    if len(dimensions) == 3:
        chunk_time = min(shape[0], max(1, int(chunk_elements ** (1/3))))
        chunk_spatial = min(shape[1], max(1, int((chunk_elements / chunk_time) ** 0.5)))
        chunks = (chunk_time, chunk_spatial, chunk_spatial)
    elif len(dimensions) == 2:
        chunk_dim = min(shape[0], max(1, int(chunk_elements ** 0.5)))
        chunks = (chunk_dim, chunk_dim)
    else:
        chunks = (min(shape[0], chunk_elements),)
    
    # Create the array
    array = da.random.random(shape, chunks=chunks, dtype=np.float64)
    
    actual_size = array.nbytes / (1024**3)
    print(f"   Created array: shape={shape}, chunks={chunks}")
    print(f"   Target size: {size_gb:.2f} GB, Actual size: {actual_size:.2f} GB")
    
    return array


def benchmark_computation_scaling(
    computation_func: Callable,
    worker_counts: List[int],
    data_size_gb: float = 1.0,
    chunk_size_mb: float = 256,
    workload_type: str = "cpu",
    monitor: PerformanceMonitor = None
) -> List[BenchmarkResult]:
    """Benchmark computation scaling across different worker counts.
    
    Args:
        computation_func: Function to benchmark
        worker_counts: List of worker counts to test
        data_size_gb: Size of test data in GB
        chunk_size_mb: Chunk size in MB
        workload_type: Workload type for optimization
        monitor: Performance monitor
        
    Returns:
        List of benchmark results
    """
    print(f"\n📊 SCALING BENCHMARK: {computation_func.__name__}")
    print("=" * 60)
    print(f"Data size: {data_size_gb:.2f} GB")
    print(f"Chunk size: {chunk_size_mb} MB")
    print(f"Workload type: {workload_type}")
    print(f"Worker counts: {worker_counts}")
    
    results = []
    baseline_time = None
    
    for n_workers in worker_counts:
        print(f"\n🧮 Testing {n_workers} workers...")
        
        try:
            # Create fresh cluster for each test
            client, cluster, temp_dir = setup_dask_client(
                workload_type=workload_type,
                max_workers=n_workers,
                reserve_mem_gb=30.0,
                dashboard=False  # Disable for benchmarking
            )
            
            actual_workers = len(client.scheduler_info()['workers'])
            print(f"   Active workers: {actual_workers}")
            
            # Create benchmark data
            data = create_benchmark_dataset(data_size_gb, chunk_size_mb)
            
            # Start monitoring
            if monitor:
                monitor.start_monitoring()
            
            # Warm up
            _ = data.sum().compute()
            
            # Run benchmark
            start_time = time.time()
            if monitor:
                monitor.take_snapshot()
            
            result = computation_func(data)
            
            if monitor:
                monitor.take_snapshot()
            
            end_time = time.time()
            computation_time = end_time - start_time
            
            # Stop monitoring
            resource_stats = {}
            if monitor:
                resource_stats = monitor.stop_monitoring()
            
            # Calculate metrics
            throughput = data_size_gb / computation_time
            
            # Memory efficiency (rough estimate)
            expected_memory = data_size_gb / actual_workers
            actual_memory = resource_stats.get('memory_max', expected_memory)
            memory_efficiency = expected_memory / max(actual_memory, expected_memory)
            
            # Scaling efficiency
            if baseline_time is None:
                baseline_time = computation_time
                scaling_efficiency = 1.0
            else:
                ideal_time = baseline_time / n_workers
                scaling_efficiency = ideal_time / computation_time
            
            # Create result
            benchmark_result = BenchmarkResult(
                workers=actual_workers,
                task_type=computation_func.__name__,
                data_size_gb=data_size_gb,
                chunk_size_mb=chunk_size_mb,
                computation_time=computation_time,
                throughput_gb_s=throughput,
                memory_peak_gb=actual_memory,
                memory_efficiency=memory_efficiency,
                cpu_utilization=resource_stats.get('cpu_mean', 0.0),
                scaling_efficiency=scaling_efficiency
            )
            
            results.append(benchmark_result)
            
            print(f"   Computation time: {computation_time:.2f}s")
            print(f"   Throughput: {throughput:.2f} GB/s")
            print(f"   Scaling efficiency: {scaling_efficiency:.2f}")
            
            # Clean up
            client.close()
            cluster.close()
            
        except Exception as e:
            print(f"   ❌ Benchmark failed: {e}")
            continue
    
    return results


def benchmark_workload_fft(data: da.Array) -> Any:
    """FFT computation benchmark workload."""
    return da.fft.fft(data, axis=-1).real.sum()


def benchmark_workload_reduction(data: da.Array) -> Any:
    """Reduction operation benchmark workload."""
    return data.mean(axis=0).std()


def benchmark_workload_linalg(data: da.Array) -> Any:
    """Linear algebra benchmark workload."""
    if data.ndim >= 2:
        # Matrix operations on 2D slices
        result = 0
        for i in range(min(5, data.shape[0])):  # Limit iterations
            slice_data = data[i] if data.ndim > 2 else data
            if slice_data.ndim == 2 and min(slice_data.shape) > 1:
                result += da.linalg.norm(slice_data, axis=-1).sum()
        return result
    else:
        return da.linalg.norm(data)


def benchmark_workload_io_simulation(data: da.Array) -> Any:
    """I/O-heavy workload simulation."""
    # Simulate I/O with rechunking and reductions
    rechunked = data.rechunk((data.chunks[0], -1) if data.ndim > 1 else (-1,))
    return rechunked.sum(axis=-1 if data.ndim > 1 else 0).compute()


def analyze_chunk_size_optimization(
    worker_count: int,
    data_size_gb: float,
    chunk_sizes_mb: List[float],
    workload_type: str = "cpu"
) -> List[BenchmarkResult]:
    """Analyze optimal chunk size for given configuration.
    
    Args:
        worker_count: Number of workers to use
        data_size_gb: Size of test data
        chunk_sizes_mb: List of chunk sizes to test
        workload_type: Workload type
        
    Returns:
        List of benchmark results
    """
    print(f"\n🧩 CHUNK SIZE OPTIMIZATION ANALYSIS")
    print("=" * 60)
    print(f"Workers: {worker_count}")
    print(f"Data size: {data_size_gb:.2f} GB")
    print(f"Chunk sizes: {chunk_sizes_mb} MB")
    
    results = []
    
    # Use a simple computation for chunk size testing
    def test_computation(data):
        return data.mean().compute()
    
    for chunk_size in chunk_sizes_mb:
        print(f"\n🔧 Testing chunk size: {chunk_size} MB...")
        
        try:
            client, cluster, temp_dir = setup_dask_client(
                workload_type=workload_type,
                max_workers=worker_count,
                reserve_mem_gb=30.0,
                dashboard=False
            )
            
            # Create test data with specific chunk size
            data = create_benchmark_dataset(data_size_gb, chunk_size)
            
            print(f"   Number of chunks: {data.nchunks}")
            
            # Benchmark
            start_time = time.time()
            result = test_computation(data)
            end_time = time.time()
            
            computation_time = end_time - start_time
            throughput = data_size_gb / computation_time
            
            # Estimate overhead based on number of chunks
            chunk_overhead = data.nchunks * 0.001  # Rough estimate
            
            benchmark_result = BenchmarkResult(
                workers=len(client.scheduler_info()['workers']),
                task_type="chunk_optimization",
                data_size_gb=data_size_gb,
                chunk_size_mb=chunk_size,
                computation_time=computation_time,
                throughput_gb_s=throughput,
                memory_peak_gb=0.0,  # Not measured here
                memory_efficiency=1.0,
                cpu_utilization=0.0,
                notes=f"chunks={data.nchunks}, overhead~{chunk_overhead:.3f}s"
            )
            
            results.append(benchmark_result)
            
            print(f"   Computation time: {computation_time:.2f}s")
            print(f"   Throughput: {throughput:.2f} GB/s")
            
            client.close()
            cluster.close()
            
        except Exception as e:
            print(f"   ❌ Chunk size test failed: {e}")
            continue
    
    return results


def demonstrate_adaptive_scaling(
    initial_workers: int = 2,
    max_workers: int = 8,
    target_time: float = 60.0,
    workload_type: str = "cpu"
) -> Dict[str, Any]:
    """Demonstrate adaptive scaling strategy.
    
    Args:
        initial_workers: Starting number of workers
        max_workers: Maximum workers to scale to
        target_time: Target computation time in seconds
        workload_type: Workload type
        
    Returns:
        Dictionary with scaling results
    """
    print(f"\n🔄 ADAPTIVE SCALING DEMONSTRATION")
    print("=" * 60)
    print(f"Initial workers: {initial_workers}")
    print(f"Max workers: {max_workers}")
    print(f"Target time: {target_time}s")
    
    scaling_history = []
    current_workers = initial_workers
    data_size = 2.0  # GB
    
    for iteration in range(3):  # Limit iterations for demo
        print(f"\n🔄 Iteration {iteration + 1}: {current_workers} workers")
        
        try:
            client, cluster, temp_dir = setup_dask_client(
                workload_type=workload_type,
                max_workers=current_workers,
                reserve_mem_gb=30.0,
                dashboard=False
            )
            
            # Create benchmark workload
            data = create_benchmark_dataset(data_size, 256)
            
            # Time the computation
            start_time = time.time()
            result = benchmark_workload_reduction(data).compute()
            actual_time = time.time() - start_time
            
            scaling_history.append({
                'iteration': iteration + 1,
                'workers': current_workers,
                'data_size_gb': data_size,
                'computation_time': actual_time,
                'target_time': target_time
            })
            
            print(f"   Actual time: {actual_time:.1f}s (target: {target_time:.1f}s)")
            
            # Adaptive scaling logic
            if actual_time > target_time * 1.1:  # Too slow, scale up
                if current_workers < max_workers:
                    scale_factor = min(2.0, target_time / actual_time)
                    new_workers = min(max_workers, int(current_workers * scale_factor))
                    print(f"   ⬆️  Scaling up: {current_workers} → {new_workers} workers")
                    current_workers = new_workers
                else:
                    print(f"   ⚠️  At maximum workers, increasing data size")
                    data_size *= 1.5  # Increase workload instead
            elif actual_time < target_time * 0.5:  # Too fast, could scale down
                if current_workers > 1:
                    new_workers = max(1, current_workers // 2)
                    print(f"   ⬇️  Could scale down: {current_workers} → {new_workers} workers")
                    print(f"   (Keeping current for demonstration)")
                else:
                    print(f"   ✅ Optimal scaling achieved")
                    break
            else:
                print(f"   ✅ Within target range, optimal scaling")
                break
            
            client.close()
            cluster.close()
            
        except Exception as e:
            print(f"   ❌ Adaptive scaling iteration failed: {e}")
            break
    
    return {
        'scaling_history': scaling_history,
        'final_workers': current_workers,
        'final_data_size': data_size,
        'iterations': len(scaling_history)
    }


def create_performance_plots(
    scaling_results: Dict[str, List[BenchmarkResult]],
    chunk_results: List[BenchmarkResult],
    output_dir: Path
) -> List[Path]:
    """Create performance analysis plots.
    
    Args:
        scaling_results: Scaling benchmark results by workload type
        chunk_results: Chunk size optimization results
        output_dir: Output directory for plots
        
    Returns:
        List of created plot files
    """
    if not MATPLOTLIB_AVAILABLE:
        print("❌ matplotlib not available - skipping plotting")
        return []
    
    print(f"\n📈 CREATING PERFORMANCE PLOTS")
    print("=" * 60)
    
    plot_dir = Path(output_dir) / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    
    plot_files = []
    
    # Plot 1: Scaling efficiency by workload type
    if scaling_results:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        for workload_type, results in scaling_results.items():
            if not results:
                continue
                
            workers = [r.workers for r in results]
            times = [r.computation_time for r in results]
            efficiencies = [r.scaling_efficiency for r in results]
            
            ax1.plot(workers, times, 'o-', label=f'{workload_type}', linewidth=2, markersize=6)
            ax2.plot(workers, efficiencies, 's-', label=f'{workload_type}', linewidth=2, markersize=6)
        
        ax1.set_xlabel('Number of Workers')
        ax1.set_ylabel('Computation Time (s)')
        ax1.set_title('Scaling Performance')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        ax1.set_yscale('log')
        
        ax2.set_xlabel('Number of Workers')
        ax2.set_ylabel('Scaling Efficiency')
        ax2.set_title('Scaling Efficiency')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.axhline(y=1.0, color='red', linestyle='--', alpha=0.7, label='Perfect scaling')
        
        plt.tight_layout()
        plot_file = plot_dir / "scaling_analysis.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
    
    # Plot 2: Throughput comparison
    if scaling_results:
        fig, ax = plt.subplots(figsize=(12, 8))
        
        for workload_type, results in scaling_results.items():
            if not results:
                continue
                
            workers = [r.workers for r in results]
            throughputs = [r.throughput_gb_s for r in results]
            
            ax.plot(workers, throughputs, 'o-', label=f'{workload_type}', 
                   linewidth=2, markersize=8)
        
        ax.set_xlabel('Number of Workers')
        ax.set_ylabel('Throughput (GB/s)')
        ax.set_title('Throughput Scaling by Workload Type')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plot_file = plot_dir / "throughput_scaling.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
    
    # Plot 3: Chunk size optimization
    if chunk_results:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))
        
        chunk_sizes = [r.chunk_size_mb for r in chunk_results]
        times = [r.computation_time for r in chunk_results]
        throughputs = [r.throughput_gb_s for r in chunk_results]
        
        ax1.plot(chunk_sizes, times, 'ro-', linewidth=2, markersize=6)
        ax1.set_xlabel('Chunk Size (MB)')
        ax1.set_ylabel('Computation Time (s)')
        ax1.set_title('Chunk Size vs Performance')
        ax1.grid(True, alpha=0.3)
        ax1.set_xscale('log')
        
        ax2.plot(chunk_sizes, throughputs, 'bo-', linewidth=2, markersize=6)
        ax2.set_xlabel('Chunk Size (MB)')
        ax2.set_ylabel('Throughput (GB/s)')
        ax2.set_title('Chunk Size vs Throughput')
        ax2.grid(True, alpha=0.3)
        ax2.set_xscale('log')
        
        # Highlight optimal chunk size
        optimal_idx = np.argmax(throughputs)
        optimal_chunk = chunk_sizes[optimal_idx]
        optimal_throughput = throughputs[optimal_idx]
        
        ax2.plot(optimal_chunk, optimal_throughput, 'go', markersize=12, 
                label=f'Optimal: {optimal_chunk} MB')
        ax2.legend()
        
        plt.tight_layout()
        plot_file = plot_dir / "chunk_optimization.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
    
    print(f"✅ Created {len(plot_files)} performance plots")
    return plot_files


def create_optimization_report(
    scaling_results: Dict[str, List[BenchmarkResult]],
    chunk_results: List[BenchmarkResult],
    adaptive_results: Dict[str, Any],
    output_file: Path
) -> None:
    """Create comprehensive performance optimization report.
    
    Args:
        scaling_results: Scaling benchmark results
        chunk_results: Chunk optimization results  
        adaptive_results: Adaptive scaling results
        output_file: Output file path
    """
    print(f"\n📋 CREATING OPTIMIZATION REPORT")
    print("=" * 60)
    
    # Prepare data for tables
    scaling_data = []
    chunk_data = []
    
    # Process scaling results
    for workload_type, results in scaling_results.items():
        for result in results:
            scaling_data.append([
                workload_type,
                result.workers,
                f"{result.computation_time:.2f}",
                f"{result.throughput_gb_s:.2f}",
                f"{result.scaling_efficiency:.2f}",
                f"{result.memory_peak_gb:.1f}" if result.memory_peak_gb > 0 else "N/A"
            ])
    
    # Process chunk results
    for result in chunk_results:
        chunk_data.append([
            f"{result.chunk_size_mb:.0f}",
            result.notes.split('chunks=')[1].split(',')[0] if 'chunks=' in result.notes else "N/A",
            f"{result.computation_time:.2f}",
            f"{result.throughput_gb_s:.2f}"
        ])
    
    # Create summary statistics
    best_scaling = {}
    for workload_type, results in scaling_results.items():
        if results:
            best_result = max(results, key=lambda x: x.throughput_gb_s)
            best_scaling[workload_type] = best_result
    
    # Find optimal chunk size
    optimal_chunk = None
    if chunk_results:
        optimal_chunk = max(chunk_results, key=lambda x: x.throughput_gb_s)
    
    # Write report
    with open(output_file, 'w') as f:
        f.write("# Performance Optimization Analysis Report\n\n")
        f.write(f"**Report generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Executive Summary\n\n")
        f.write("This report presents comprehensive performance analysis results for dask_setup ")
        f.write("configurations, including scaling behavior, chunk size optimization, and ")
        f.write("adaptive scaling strategies.\n\n")
        
        # Key findings
        f.write("### Key Findings\n\n")
        
        if best_scaling:
            f.write("**Scaling Performance:**\n")
            for workload_type, result in best_scaling.items():
                f.write(f"- {workload_type.upper()}: Best performance with {result.workers} workers ")
                f.write(f"({result.throughput_gb_s:.2f} GB/s throughput)\n")
        
        if optimal_chunk:
            f.write(f"**Chunk Optimization:** Optimal chunk size is {optimal_chunk.chunk_size_mb:.0f} MB ")
            f.write(f"({optimal_chunk.throughput_gb_s:.2f} GB/s throughput)\n")
        
        if adaptive_results:
            history = adaptive_results.get('scaling_history', [])
            if history:
                final_iteration = history[-1]
                f.write(f"**Adaptive Scaling:** Converged to {adaptive_results['final_workers']} workers ")
                f.write(f"in {len(history)} iterations\n")
        
        f.write("\n")
        
        # Detailed results
        f.write("## Scaling Performance Analysis\n\n")
        
        if scaling_data:
            f.write("### Scaling Results by Workload Type\n\n")
            f.write("| Workload | Workers | Time (s) | Throughput (GB/s) | Scaling Eff. | Memory (GB) |\n")
            f.write("|----------|---------|----------|-------------------|--------------|-------------|\n")
            for row in scaling_data:
                f.write("| " + " | ".join(row) + " |\n")
            f.write("\n")
        
        f.write("### Scaling Efficiency Analysis\n\n")
        f.write("**Strong Scaling:** Measures how performance improves with more workers for fixed problem size.\n")
        f.write("- Efficiency = 1.0: Perfect scaling\n")
        f.write("- Efficiency > 0.7: Good scaling\n")
        f.write("- Efficiency < 0.5: Poor scaling (consider fewer workers)\n\n")
        
        # Workload-specific analysis
        for workload_type, results in scaling_results.items():
            if not results:
                continue
                
            f.write(f"**{workload_type.upper()} Workload:**\n")
            
            best_efficiency = max(r.scaling_efficiency for r in results)
            worst_efficiency = min(r.scaling_efficiency for r in results)
            
            f.write(f"- Scaling efficiency range: {worst_efficiency:.2f} - {best_efficiency:.2f}\n")
            
            # Find sweet spot
            efficient_results = [r for r in results if r.scaling_efficiency > 0.7]
            if efficient_results:
                optimal_workers = max(efficient_results, key=lambda x: x.throughput_gb_s).workers
                f.write(f"- Recommended workers: {optimal_workers} (good efficiency + high throughput)\n")
            
            f.write("\n")
        
        # Chunk size analysis
        if chunk_data:
            f.write("## Chunk Size Optimization\n\n")
            f.write("| Chunk Size (MB) | Chunks | Time (s) | Throughput (GB/s) |\n")
            f.write("|-----------------|--------|----------|-------------------|\n")
            for row in chunk_data:
                f.write("| " + " | ".join(row) + " |\n")
            f.write("\n")
            
            if optimal_chunk:
                f.write("### Chunk Size Recommendations\n\n")
                f.write(f"**Optimal chunk size:** {optimal_chunk.chunk_size_mb:.0f} MB\n")
                f.write("- Balances memory usage and computational efficiency\n")
                f.write("- Minimizes task scheduling overhead\n")
                f.write("- Maximizes throughput for this workload\n\n")
                
                f.write("**General Guidelines:**\n")
                f.write("- Small chunks (< 50 MB): High overhead, poor performance\n")
                f.write("- Medium chunks (50-512 MB): Good balance for most workloads\n")
                f.write("- Large chunks (> 512 MB): Risk of memory issues, reduced parallelism\n\n")
        
        # Adaptive scaling
        if adaptive_results:
            f.write("## Adaptive Scaling Results\n\n")
            
            history = adaptive_results.get('scaling_history', [])
            if history:
                f.write("### Scaling History\n\n")
                f.write("| Iteration | Workers | Data Size (GB) | Time (s) | Target (s) |\n")
                f.write("|-----------|---------|----------------|----------|------------|\n")
                for entry in history:
                    f.write(f"| {entry['iteration']} | {entry['workers']} | ")
                    f.write(f"{entry['data_size_gb']:.1f} | {entry['computation_time']:.1f} | ")
                    f.write(f"{entry['target_time']:.1f} |\n")
                f.write("\n")
            
            f.write("### Adaptive Scaling Strategy\n\n")
            f.write("The adaptive scaling demonstration shows how to:\n")
            f.write("1. **Monitor performance** against target metrics\n")
            f.write("2. **Scale up** when performance is below target\n")
            f.write("3. **Scale down** when over-provisioned\n")
            f.write("4. **Adjust workload** when at resource limits\n\n")
        
        # Recommendations
        f.write("## Optimization Recommendations\n\n")
        
        f.write("### Configuration Tuning\n\n")
        f.write("Based on the benchmark results:\n\n")
        
        if best_scaling:
            f.write("**Worker Configuration:**\n")
            for workload_type, result in best_scaling.items():
                f.write(f"- {workload_type.upper()} workloads: {result.workers} workers optimal\n")
        
        if optimal_chunk:
            f.write(f"**Chunking Strategy:**\n")
            f.write(f"- Target chunk size: {optimal_chunk.chunk_size_mb:.0f} MB\n")
            f.write("- Monitor chunk count vs performance trade-off\n")
        
        f.write("\n**Memory Management:**\n")
        f.write("- Reserve adequate memory for spilling (30-40% of total)\n")
        f.write("- Monitor memory usage patterns in dashboard\n")
        f.write("- Consider memory-per-worker vs worker count trade-offs\n\n")
        
        f.write("### Workflow-Specific Guidelines\n\n")
        
        f.write("**CPU-Intensive Workloads:**\n")
        f.write("- Use `workload_type='cpu'` for process-based parallelism\n")
        f.write("- Scale workers close to available CPU cores\n")
        f.write("- Larger chunks often perform better (reduced overhead)\n\n")
        
        f.write("**I/O-Intensive Workloads:**\n")
        f.write("- Use `workload_type='io'` for thread-based parallelism\n")
        f.write("- More workers can help with concurrent I/O operations\n")
        f.write("- Medium chunk sizes balance I/O and memory usage\n\n")
        
        f.write("**Mixed Workloads:**\n")
        f.write("- Use `workload_type='mixed'` for balanced configuration\n")
        f.write("- Start with moderate worker counts and adjust based on profiling\n")
        f.write("- Monitor both CPU and memory utilization\n\n")
        
        f.write("### Monitoring and Profiling\n\n")
        f.write("**Key Metrics to Track:**\n")
        f.write("- Task execution time distribution\n")
        f.write("- Memory usage patterns and spilling\n")
        f.write("- CPU utilization across workers\n")
        f.write("- Data transfer and serialization costs\n\n")
        
        f.write("**Tools and Techniques:**\n")
        f.write("- Use Dask dashboard for real-time monitoring\n")
        f.write("- Profile with `dask.distributed.performance_report()`\n")
        f.write("- Monitor system resources with `htop`/`psutil`\n")
        f.write("- Use `client.profile()` for detailed analysis\n\n")
        
        f.write("---\n")
        f.write("*Generated by dask_setup performance optimization analysis*\n")
    
    print(f"✅ Optimization report saved: {output_file}")


def main():
    """Main function for performance optimization benchmarks."""
    parser = argparse.ArgumentParser(
        description="Performance optimization and scaling benchmarks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="Maximum number of workers to test (default: 8)"
    )
    
    parser.add_argument(
        "--data-sizes",
        nargs='+',
        type=float,
        default=[0.5, 1.0, 2.0],
        help="Data sizes in GB to test (default: 0.5 1.0 2.0)"
    )
    
    parser.add_argument(
        "--chunk-sizes",
        nargs='+',
        type=float,
        default=[64, 128, 256, 512, 1024],
        help="Chunk sizes in MB to test (default: 64 128 256 512 1024)"
    )
    
    parser.add_argument(
        "--workload-types",
        nargs='+',
        choices=["cpu", "io", "mixed"],
        default=["cpu", "mixed"],
        help="Workload types to test (default: cpu mixed)"
    )
    
    parser.add_argument(
        "--skip-scaling",
        action="store_true",
        help="Skip scaling benchmarks"
    )
    
    parser.add_argument(
        "--skip-chunking",
        action="store_true",
        help="Skip chunk optimization"
    )
    
    parser.add_argument(
        "--skip-adaptive",
        action="store_true",
        help="Skip adaptive scaling demonstration"
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
    
    print("⚡ PERFORMANCE OPTIMIZATION & SCALING BENCHMARKS")
    print("=" * 60)
    print(f"Max workers: {args.max_workers}")
    print(f"Data sizes: {args.data_sizes} GB")
    print(f"Chunk sizes: {args.chunk_sizes} MB") 
    print(f"Workload types: {args.workload_types}")
    print(f"Output directory: {args.output_dir}")
    
    if PSUTIL_AVAILABLE:
        memory_info = psutil.virtual_memory()
        print(f"System memory: {memory_info.total / (1024**3):.1f} GB")
        print(f"Available memory: {memory_info.available / (1024**3):.1f} GB")
    
    try:
        # Initialize performance monitor
        monitor = PerformanceMonitor()
        
        # Results storage
        scaling_results = {}
        chunk_results = []
        adaptive_results = {}
        
        # Worker counts to test
        worker_counts = list(range(1, args.max_workers + 1, max(1, args.max_workers // 4)))
        if args.max_workers not in worker_counts:
            worker_counts.append(args.max_workers)
        
        # Scaling benchmarks
        if not args.skip_scaling:
            benchmark_functions = [
                benchmark_workload_reduction,
                benchmark_workload_fft,
                benchmark_workload_linalg
            ]
            
            for workload_type in args.workload_types:
                print(f"\n🚀 SCALING BENCHMARKS: {workload_type.upper()} WORKLOAD")
                print("=" * 60)
                
                workload_results = []
                
                for func in benchmark_functions:
                    results = benchmark_computation_scaling(
                        func, worker_counts, 
                        data_size_gb=args.data_sizes[0],  # Use smallest size for scaling
                        workload_type=workload_type,
                        monitor=monitor
                    )
                    workload_results.extend(results)
                
                if workload_results:
                    scaling_results[workload_type] = workload_results
        
        # Chunk size optimization
        if not args.skip_chunking:
            chunk_results = analyze_chunk_size_optimization(
                worker_count=min(4, args.max_workers),
                data_size_gb=args.data_sizes[0],
                chunk_sizes_mb=args.chunk_sizes,
                workload_type=args.workload_types[0]
            )
        
        # Adaptive scaling demonstration
        if not args.skip_adaptive:
            adaptive_results = demonstrate_adaptive_scaling(
                initial_workers=2,
                max_workers=min(6, args.max_workers),
                workload_type=args.workload_types[0]
            )
        
        # Create performance plots
        plot_files = create_performance_plots(
            scaling_results, chunk_results, args.output_dir
        )
        
        # Create optimization report
        report_file = args.output_dir / "performance_optimization_report.md"
        create_optimization_report(
            scaling_results, chunk_results, adaptive_results, report_file
        )
        
        print("\n" + "=" * 60)
        print("✅ PERFORMANCE OPTIMIZATION ANALYSIS COMPLETED!")
        print("=" * 60)
        print(f"📄 Optimization report: {report_file}")
        print(f"📊 Performance plots: {len(plot_files)} created")
        
        # Summary statistics
        total_benchmarks = sum(len(results) for results in scaling_results.values()) + len(chunk_results)
        print(f"🧮 Total benchmarks executed: {total_benchmarks}")
        
        if scaling_results:
            print(f"📈 Scaling analysis: {len(scaling_results)} workload types")
        
        if chunk_results:
            optimal_chunk = max(chunk_results, key=lambda x: x.throughput_gb_s)
            print(f"🧩 Optimal chunk size: {optimal_chunk.chunk_size_mb:.0f} MB")
        
        if adaptive_results:
            iterations = len(adaptive_results.get('scaling_history', []))
            final_workers = adaptive_results.get('final_workers', 0)
            print(f"🔄 Adaptive scaling: {iterations} iterations → {final_workers} workers")
        
    except KeyboardInterrupt:
        print("\n\n🛑 Benchmarks interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Benchmarks failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())