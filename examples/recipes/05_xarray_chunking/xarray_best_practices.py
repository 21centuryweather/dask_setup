#!/usr/bin/env python
"""Xarray chunking best practices and optimization.

This recipe demonstrates intelligent chunking strategies for xarray datasets,
implementing the 256-512 MiB per chunk rule and comparing different approaches
for optimal memory usage and performance.

Requirements:
- dask_setup, dask, distributed
- xarray, numpy
- rechunker (optional, for rechunking demonstrations)

Outputs:
- Console output showing chunking recommendations
- Comparison of different chunking strategies
- Performance analysis and recommendations
- Optional chunking visualization

Key Learning Points:
- How to choose optimal chunk sizes for your data
- When to use .chunk() vs Rechunker
- Memory-efficient chunking patterns
- Impact of chunk size on performance
- Bad vs good chunking patterns
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add the parent directory to path so we can import dask_setup and utils
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import dask
    import dask.array as da
    import numpy as np
    import xarray as xr
    
    from dask_setup import setup_dask_client
    from utils import (
        create_sample_dataset, estimate_chunk_memory, format_duration,
        print_memory_summary, save_markdown_table, timer
    )
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed xarray numpy")
    sys.exit(1)

# Optional dependencies
try:
    import rechunker
    RECHUNKER_AVAILABLE = True
except ImportError:
    RECHUNKER_AVAILABLE = False

try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )


def calculate_optimal_chunks(
    ds: xr.Dataset,
    target_chunk_mb: Tuple[float, float] = (256, 512),
    workload_type: str = "balanced"
) -> Dict[str, int]:
    """Calculate optimal chunk sizes based on the 256-512 MiB rule.
    
    Args:
        ds: xarray Dataset to analyze
        target_chunk_mb: Target chunk size range in MB
        workload_type: Type of workload ("cpu", "io", "balanced")
        
    Returns:
        Dictionary with recommended chunk sizes per dimension
    """
    print(f"\n🎯 Calculating optimal chunks for {workload_type} workload")
    print(f"   Target chunk size: {target_chunk_mb[0]}-{target_chunk_mb[1]} MB")
    
    # Get the largest data variable for analysis
    largest_var = max(ds.data_vars.values(), key=lambda v: v.nbytes)
    var_name = largest_var.name
    
    print(f"   Analyzing variable: {var_name}")
    print(f"   Shape: {largest_var.shape}")
    print(f"   Dimensions: {largest_var.dims}")
    
    # Calculate bytes per element
    dtype_size = np.dtype(largest_var.dtype).itemsize
    print(f"   Data type: {largest_var.dtype} ({dtype_size} bytes/element)")
    
    # Target chunk size in bytes
    target_min_bytes = target_chunk_mb[0] * 1024 * 1024
    target_max_bytes = target_chunk_mb[1] * 1024 * 1024
    target_bytes = (target_min_bytes + target_max_bytes) / 2
    
    # Calculate target number of elements per chunk
    target_elements = target_bytes / dtype_size
    
    # Strategy based on workload type
    chunks = {}
    
    if workload_type == "cpu":
        # For CPU workloads, prefer square-ish chunks for better cache locality
        print("   Strategy: Square-ish chunks for CPU optimization")
        
        # Calculate chunk dimensions to get roughly target elements
        total_dims = len(largest_var.dims)
        elements_per_dim = int(target_elements ** (1.0 / total_dims))
        
        for dim in largest_var.dims:
            dim_size = largest_var.sizes[dim]
            # Start with equal distribution, then adjust
            chunk_size = min(dim_size, max(1, elements_per_dim))
            chunks[dim] = chunk_size
            
    elif workload_type == "io":
        # For I/O workloads, prefer chunks that align with access patterns
        print("   Strategy: I/O-friendly chunks (preserve record dimensions)")
        
        # Identify likely record dimensions (time, often the first or unlimited dimension)
        record_dims = []
        for dim in largest_var.dims:
            if 'time' in dim.lower() or largest_var.dims.index(dim) == 0:
                record_dims.append(dim)
        
        # Keep record dimensions unchunked or lightly chunked
        remaining_elements = target_elements
        non_record_dims = [d for d in largest_var.dims if d not in record_dims]
        
        for dim in largest_var.dims:
            dim_size = largest_var.sizes[dim]
            if dim in record_dims:
                # Keep record dimensions relatively large
                chunks[dim] = min(dim_size, max(100, dim_size // 4))
            else:
                # Distribute remaining elements across spatial dimensions
                if non_record_dims:
                    elements_per_spatial = int(remaining_elements ** (1.0 / len(non_record_dims)))
                    chunk_size = min(dim_size, max(1, elements_per_spatial))
                    chunks[dim] = chunk_size
                else:
                    chunks[dim] = dim_size
                    
    else:  # balanced
        print("   Strategy: Balanced chunks")
        
        # Use a mixed approach
        total_elements = np.prod(largest_var.shape)
        
        for dim in largest_var.dims:
            dim_size = largest_var.sizes[dim]
            
            # Calculate proportional chunk size
            dim_fraction = dim_size / total_elements ** (1.0 / len(largest_var.dims))
            chunk_size = int(target_elements ** (1.0 / len(largest_var.dims)) * dim_fraction)
            chunk_size = min(dim_size, max(1, chunk_size))
            
            chunks[dim] = chunk_size
    
    # Refine chunks to hit target size more precisely
    current_elements = np.prod(list(chunks.values()))
    current_bytes = current_elements * dtype_size
    
    if current_bytes < target_min_bytes or current_bytes > target_max_bytes:
        # Adjust largest dimension
        adjustment_factor = (target_bytes / current_bytes) ** (1.0 / len(chunks))
        
        for dim in chunks:
            dim_size = largest_var.sizes[dim]
            chunks[dim] = min(dim_size, max(1, int(chunks[dim] * adjustment_factor)))
    
    # Final validation
    final_elements = np.prod(list(chunks.values()))
    final_bytes = final_elements * dtype_size
    final_mb = final_bytes / (1024 * 1024)
    
    print(f"   Recommended chunks: {chunks}")
    print(f"   Estimated chunk size: {final_mb:.1f} MB")
    
    return chunks


def analyze_chunking_options(
    ds: xr.Dataset,
    chunk_options: Dict[str, Dict[str, int]] = None
) -> Dict[str, Dict]:
    """Analyze different chunking options for a dataset.
    
    Args:
        ds: xarray Dataset to analyze
        chunk_options: Dictionary of chunking options to test
        
    Returns:
        Dictionary with analysis results for each option
    """
    if chunk_options is None:
        chunk_options = {
            "auto_cpu": calculate_optimal_chunks(ds, workload_type="cpu"),
            "auto_io": calculate_optimal_chunks(ds, workload_type="io"),
            "auto_balanced": calculate_optimal_chunks(ds, workload_type="balanced"),
            "small_chunks": {dim: min(100, size // 10) for dim, size in ds.sizes.items()},
            "large_chunks": {dim: min(1000, size // 2) for dim, size in ds.sizes.items()},
            "current": None  # Use existing chunks
        }
    
    print("\n" + "=" * 60)
    print("📊 CHUNKING OPTIONS ANALYSIS")
    print("=" * 60)
    
    results = {}
    
    for option_name, chunks in chunk_options.items():
        print(f"\n🔍 Analyzing: {option_name}")
        
        try:
            if chunks is None:
                # Use current chunking
                if hasattr(ds, 'chunks') and ds.chunks:
                    analysis_ds = ds
                    print("   Using current dataset chunks")
                else:
                    print("   No current chunks, using default chunking")
                    continue
            else:
                # Apply new chunking
                analysis_ds = ds.chunk(chunks)
                print(f"   Applied chunks: {chunks}")
            
            # Estimate memory usage
            memory_estimates = estimate_chunk_memory(analysis_ds, chunks)
            
            # Calculate total chunks and memory
            total_chunks = sum(est['total_chunks'] for est in memory_estimates.values())
            total_memory_mb = sum(est['total_mb'] for est in memory_estimates.values())
            
            # Get representative chunk size
            if memory_estimates:
                avg_chunk_mb = np.mean([est['chunk_mb'] for est in memory_estimates.values()])
            else:
                avg_chunk_mb = 0
            
            # Assess chunk size quality
            if 256 <= avg_chunk_mb <= 512:
                quality = "✅ Optimal"
            elif 128 <= avg_chunk_mb <= 1024:
                quality = "🟡 Acceptable"
            elif avg_chunk_mb < 128:
                quality = "🔴 Too small (overhead issues)"
            else:
                quality = "🔴 Too large (memory issues)"
            
            results[option_name] = {
                'chunks': chunks,
                'total_chunks': total_chunks,
                'total_memory_mb': total_memory_mb,
                'avg_chunk_mb': avg_chunk_mb,
                'quality': quality,
                'memory_estimates': memory_estimates
            }
            
            print(f"   Total chunks: {total_chunks}")
            print(f"   Avg chunk size: {avg_chunk_mb:.1f} MB")
            print(f"   Total memory: {total_memory_mb:.1f} MB")
            print(f"   Quality: {quality}")
            
        except Exception as e:
            print(f"   ❌ Analysis failed: {e}")
            results[option_name] = {
                'error': str(e),
                'quality': '❌ Failed'
            }
    
    return results


def benchmark_chunking_performance(
    ds: xr.Dataset,
    chunk_options: Dict[str, Dict[str, int]],
    operation: str = "mean",
    client = None
) -> Dict[str, float]:
    """Benchmark performance of different chunking strategies.
    
    Args:
        ds: Dataset to benchmark
        chunk_options: Chunking options to test
        operation: Operation to benchmark ('mean', 'sum', 'std')
        client: Dask client for computation
        
    Returns:
        Dictionary with timing results
    """
    print(f"\n⚡ PERFORMANCE BENCHMARK - {operation.upper()} OPERATION")
    print("=" * 50)
    
    results = {}
    
    for option_name, chunks in chunk_options.items():
        if chunks is None:
            continue  # Skip current chunks for now
            
        print(f"\n🏃 Testing: {option_name}")
        
        try:
            # Apply chunking
            chunked_ds = ds.chunk(chunks)
            
            # Select first data variable for testing
            var_name = list(ds.data_vars.keys())[0]
            data_var = chunked_ds[var_name]
            
            # Define the operation
            if operation == "mean":
                computation = lambda: data_var.mean().compute()
            elif operation == "sum":
                computation = lambda: data_var.sum().compute()
            elif operation == "std":
                computation = lambda: data_var.std().compute()
            else:
                print(f"   Unknown operation: {operation}")
                continue
            
            # Benchmark the operation
            with timer(f"{option_name} {operation}") as t:
                result = computation()
            
            results[option_name] = t['total']
            print(f"   Result: {float(result):.6f}")
            print(f"   Time: {t['total']:.2f}s")
            
        except Exception as e:
            print(f"   ❌ Benchmark failed: {e}")
            results[option_name] = float('inf')
    
    return results


def demonstrate_rechunking(
    ds: xr.Dataset,
    source_chunks: Dict[str, int],
    target_chunks: Dict[str, int],
    temp_dir: str = None
) -> Optional[float]:
    """Demonstrate rechunking with Rechunker library.
    
    Args:
        ds: Dataset to rechunk
        source_chunks: Initial chunk configuration
        target_chunks: Target chunk configuration 
        temp_dir: Temporary directory for intermediate storage
        
    Returns:
        Time taken for rechunking operation
    """
    if not RECHUNKER_AVAILABLE:
        print("⚠️  Rechunker not available, skipping rechunking demonstration")
        return None
        
    print("\n🔄 RECHUNKER DEMONSTRATION")
    print("=" * 40)
    
    try:
        # Create source dataset with initial chunking
        source_ds = ds.chunk(source_chunks)
        var_name = list(ds.data_vars.keys())[0]
        source_array = source_ds[var_name].data
        
        print(f"Source chunks: {source_chunks}")
        print(f"Target chunks: {target_chunks}")
        
        # Set up temporary storage
        if temp_dir is None:
            temp_dir = "./temp_rechunk"
        
        temp_store = f"{temp_dir}/temp.zarr"
        target_store = f"{temp_dir}/target.zarr"
        
        # Calculate maximum memory to use (conservative)
        max_mem = "100MB"  # Conservative for demonstration
        
        with timer("Rechunker operation") as t:
            # Create rechunking plan
            plan = rechunker.rechunk(
                source_array,
                target_chunks=target_chunks,
                max_mem=max_mem,
                target_store=target_store,
                temp_store=temp_store
            )
            
            # Execute the plan
            plan.execute()
        
        print(f"✅ Rechunking completed in {t['total']:.2f}s")
        print(f"   Temp store: {temp_store}")
        print(f"   Target store: {target_store}")
        
        # Clean up
        import shutil
        try:
            shutil.rmtree(temp_dir)
            print("   Cleaned up temporary files")
        except Exception:
            pass
            
        return t['total']
        
    except Exception as e:
        print(f"❌ Rechunking demonstration failed: {e}")
        return None


def create_chunking_comparison_table(
    analysis_results: Dict[str, Dict],
    performance_results: Dict[str, float] = None,
    output_file: Path = None
) -> None:
    """Create a comparison table of chunking strategies.
    
    Args:
        analysis_results: Results from analyze_chunking_options
        performance_results: Results from benchmark_chunking_performance
        output_file: Optional output file for markdown table
    """
    print("\n" + "=" * 60)
    print("📋 CHUNKING STRATEGY COMPARISON")
    print("=" * 60)
    
    # Prepare table data
    headers = ["Strategy", "Avg Chunk Size (MB)", "Total Chunks", "Total Memory (MB)", "Quality"]
    if performance_results:
        headers.append("Performance (s)")
        headers.append("Relative Speed")
    
    table_data = []
    
    # Get best performance for relative comparison
    if performance_results:
        best_time = min(t for t in performance_results.values() if t != float('inf'))
    
    # Sort by average chunk size (closer to optimal first)
    sorted_results = sorted(
        analysis_results.items(),
        key=lambda x: abs(x[1].get('avg_chunk_mb', 1000) - 384)  # 384 = middle of 256-512 range
    )
    
    for strategy, data in sorted_results:
        if 'error' in data:
            continue
            
        row = [
            strategy,
            f"{data['avg_chunk_mb']:.1f}",
            str(data['total_chunks']),
            f"{data['total_memory_mb']:.1f}",
            data['quality']
        ]
        
        if performance_results and strategy in performance_results:
            time_s = performance_results[strategy]
            if time_s != float('inf'):
                relative_speed = time_s / best_time
                row.extend([f"{time_s:.2f}", f"{relative_speed:.2f}x"])
            else:
                row.extend(["Failed", "N/A"])
        
        table_data.append(row)
    
    # Print table
    print("\nComparison Table:")
    col_widths = [max(len(str(row[i])) for row in [headers] + table_data) for i in range(len(headers))]
    
    # Print header
    header_row = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    print(f"| {header_row} |")
    print("|" + "|".join("-" * (w + 2) for w in col_widths) + "|")
    
    # Print data rows
    for row in table_data:
        data_row = " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row)))
        print(f"| {data_row} |")
    
    # Save to file if requested
    if output_file:
        save_markdown_table(
            data=table_data,
            headers=headers,
            filename=output_file,
            title="Xarray Chunking Strategy Comparison",
            description=(
                "Comparison of different chunking strategies for xarray datasets. "
                "Optimal chunk sizes are typically 256-512 MB. Lower performance times are better."
            )
        )


def print_chunking_best_practices() -> None:
    """Print chunking best practices and guidelines."""
    print("\n" + "=" * 60)
    print("💡 XARRAY CHUNKING BEST PRACTICES")
    print("=" * 60)
    
    print("""
🎯 OPTIMAL CHUNK SIZE RULE:
   • Target: 256-512 MB per chunk
   • Avoid: <100 MB (too much overhead) or >1 GB (memory issues)
   • Monitor: Actual memory usage vs estimates

📐 CHUNK SHAPE STRATEGIES:

   CPU-Intensive Workloads:
   • Square-ish chunks for better cache locality
   • Balance dimensions equally when possible
   • Example: (500, 500, 10) rather than (100, 100, 500)

   I/O-Intensive Workloads:
   • Preserve natural access patterns
   • Keep record dimensions (time) lightly chunked
   • Example: (365, 180, 90) for daily global data

   Balanced Workloads:
   • Mixed approach based on operation types
   • Consider downstream processing patterns

🔄 RECHUNKING STRATEGIES:

   Use .chunk() when:
   • Initial chunking for analysis
   • Simple chunk size adjustments
   • Interactive exploration

   Use Rechunker when:
   • Major chunk structure changes
   • Large datasets (>1 GB)
   • Need to optimize for storage

⚠️  COMMON PITFALLS:

   BAD Chunking Examples:
   • Too small: (10, 10, 10) → ~8KB chunks (high overhead)
   • Too large: (5000, 5000, 100) → ~20GB chunks (memory issues)
   • Unbalanced: (1, 3600, 1800) → inefficient parallelization

   GOOD Chunking Examples:
   • Balanced: (240, 512, 512) → ~512MB chunks
   • Time-aware: (365, 180, 90) → preserves daily records
   • Analysis-optimized: (100, 500, 500) → good for spatial ops

🔧 CONFIGURATION EXAMPLES:

   # For large climate datasets
   ds.chunk({"time": 365, "lat": 180, "lon": 360})  # ~390MB chunks
   
   # For high-resolution spatial data
   ds.chunk({"time": 30, "y": 1000, "x": 1000})   # ~240MB chunks
   
   # For time series analysis
   ds.chunk({"time": -1, "station": 100})          # Keep time continuous

📊 MEMORY CALCULATION:
   chunk_bytes = chunk_elements × dtype_size
   
   Example: (500, 500, 10) float64 array
   = 500 × 500 × 10 × 8 bytes = 20MB ✓ (too small)
   
   Better: (500, 500, 100) float64 array  
   = 500 × 500 × 100 × 8 bytes = 200MB ✓ (acceptable)
""")


def parse_chunk_specification(chunk_spec: str) -> Dict[str, int]:
    """Parse chunk specification from command line.
    
    Args:
        chunk_spec: String like "time:240,lat:512,lon:512"
        
    Returns:
        Dictionary of chunk sizes
    """
    chunks = {}
    if chunk_spec:
        for pair in chunk_spec.split(','):
            dim, size = pair.split(':')
            chunks[dim.strip()] = int(size.strip())
    return chunks


def main():
    """Main function demonstrating xarray chunking best practices."""
    parser = argparse.ArgumentParser(
        description="Demonstrate xarray chunking best practices",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--data-shape",
        nargs=3,
        type=int,
        default=[365, 180, 360],
        help="Shape of test dataset (time, lat, lon) (default: 365 180 360)"
    )
    
    parser.add_argument(
        "--custom-chunks",
        type=str,
        default=None,
        help="Custom chunk specification, e.g. 'time:120,lat:90,lon:180'"
    )
    
    parser.add_argument(
        "--benchmark",
        choices=['mean', 'sum', 'std'],
        default='mean',
        help="Operation to benchmark (default: mean)"
    )
    
    parser.add_argument(
        "--workload",
        choices=['cpu', 'io', 'mixed'],
        default='mixed',
        help="Workload type for cluster (default: mixed)"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of workers (default: 4)"
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
    
    print("🧩 DASK_SETUP XARRAY CHUNKING BEST PRACTICES")
    print("=" * 60)
    print(f"Dataset shape: {args.data_shape} (time, lat, lon)")
    print(f"Benchmark operation: {args.benchmark}")
    print(f"Workload type: {args.workload}")
    print(f"Workers: {args.workers}")
    
    try:
        # Create Dask client
        client, cluster, temp_dir = setup_dask_client(
            workload_type=args.workload,
            max_workers=args.workers,
            dashboard=True
        )
        
        print(f"\n🖥️  Cluster: {len(client.scheduler_info()['workers'])} workers")
        print(f"   Temp directory: {temp_dir}")
        
        # Create sample dataset
        print(f"\n📊 Creating sample dataset...")
        ds = create_sample_dataset(
            shape=tuple(args.data_shape),
            dims=('time', 'lat', 'lon'),
            variables=['temperature', 'precipitation']
        )
        
        print(f"   Dataset size: {ds.nbytes / (1024**2):.1f} MB")
        print(f"   Variables: {list(ds.data_vars.keys())}")
        
        # Add custom chunking option if provided
        chunk_options = {}
        if args.custom_chunks:
            custom_chunks = parse_chunk_specification(args.custom_chunks)
            chunk_options['custom'] = custom_chunks
            print(f"   Custom chunks: {custom_chunks}")
        
        # Analyze chunking options
        analysis_results = analyze_chunking_options(ds, chunk_options)
        
        # Benchmark performance
        valid_options = {k: v['chunks'] for k, v in analysis_results.items() 
                        if 'error' not in v and v['chunks'] is not None}
        
        if valid_options:
            performance_results = benchmark_chunking_performance(
                ds, valid_options, args.benchmark, client
            )
        else:
            performance_results = {}
        
        # Create comparison table
        output_file = args.output_dir / "chunking_comparison.md"
        create_chunking_comparison_table(
            analysis_results, performance_results, output_file
        )
        
        # Demonstrate rechunking if available
        if RECHUNKER_AVAILABLE and valid_options:
            # Use the most different chunk strategies for demonstration
            source_chunks = valid_options.get('small_chunks', {})
            target_chunks = valid_options.get('auto_balanced', {})
            
            if source_chunks and target_chunks:
                rechunk_time = demonstrate_rechunking(
                    ds, source_chunks, target_chunks, 
                    temp_dir + "/rechunker_demo"
                )
        
        # Print best practices
        print_chunking_best_practices()
        
        print(f"\n✅ Chunking analysis completed!")
        print(f"📄 Results saved to: {output_file}")
        
        # Clean up
        client.close()
        cluster.close()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Analysis interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Analysis failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())