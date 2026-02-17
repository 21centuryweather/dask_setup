#!/usr/bin/env python
"""Memory compression and spilling optimization demonstration.

This recipe demonstrates advanced memory management features in dask_setup,
including memory spilling thresholds, compression algorithms, and monitoring
techniques for optimal performance on memory-constrained systems.

Requirements:
- dask_setup, dask, distributed
- numpy (for large array operations)
- psutil (for memory monitoring)

Outputs:
- Console output showing memory usage patterns
- Markdown summary table of compression performance
- Spill monitoring metrics

Key Learning Points:
- Memory spilling thresholds and when they trigger
- Impact of different compression algorithms
- Monitoring worker memory usage in real-time
- Optimal configurations for memory-constrained environments
"""

import argparse
import logging
import os
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
    import psutil

    from dask_setup import setup_dask_client
    from dask_setup.config import DaskSetupConfig
    from utils import (
        format_duration,
        get_worker_memory_stats,
        pretty_bytes,
        save_markdown_table,
        timer,
    )
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed numpy psutil")
    sys.exit(1)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def get_spill_stats(client) -> Dict[str, any]:
    """Get spill statistics from workers.

    Args:
        client: Dask distributed client

    Returns:
        Dictionary with spill statistics
    """

    def worker_spill_info():
        """Get spill information from a worker."""
        try:
            import gc
            import os
            import psutil

            # Get process memory info
            process = psutil.Process()
            mem_info = process.memory_info()

            # Try to get spill directory info if it exists
            spill_info = {
                "memory_rss_mb": mem_info.rss / (1024**2),
                "memory_vms_mb": mem_info.vms / (1024**2),
                "memory_percent": process.memory_percent(),
            }

            # Check if TMPDIR exists and get its usage
            tmpdir = os.environ.get("TMPDIR", "/tmp")
            if os.path.exists(tmpdir):
                try:
                    disk_usage = psutil.disk_usage(tmpdir)
                    spill_info.update(
                        {
                            "tmpdir_total_gb": disk_usage.total / (1024**3),
                            "tmpdir_used_gb": disk_usage.used / (1024**3),
                            "tmpdir_free_gb": disk_usage.free / (1024**3),
                            "tmpdir_percent": (disk_usage.used / disk_usage.total) * 100,
                        }
                    )
                except Exception:
                    pass

            # Trigger garbage collection for memory stats
            collected = gc.collect()
            spill_info["gc_collected"] = collected

            return spill_info

        except Exception as e:
            return {"error": str(e)}

    try:
        worker_stats = client.run(worker_spill_info)

        # Aggregate statistics
        if worker_stats:
            total_rss = sum(stats.get("memory_rss_mb", 0) for stats in worker_stats.values())
            total_vms = sum(stats.get("memory_vms_mb", 0) for stats in worker_stats.values())
            avg_percent = sum(
                stats.get("memory_percent", 0) for stats in worker_stats.values()
            ) / len(worker_stats)

            # Aggregate disk usage
            total_tmpdir_used = sum(
                stats.get("tmpdir_used_gb", 0) for stats in worker_stats.values()
            )
            total_tmpdir_free = sum(
                stats.get("tmpdir_free_gb", 0) for stats in worker_stats.values()
            )

            return {
                "worker_count": len(worker_stats),
                "total_memory_rss_mb": total_rss,
                "total_memory_vms_mb": total_vms,
                "average_memory_percent": avg_percent,
                "total_tmpdir_used_gb": total_tmpdir_used,
                "total_tmpdir_free_gb": total_tmpdir_free,
                "individual_workers": worker_stats,
            }
        else:
            return {"error": "No worker statistics available"}

    except Exception as e:
        return {"error": str(e)}


def monitor_memory_during_computation(
    client, computation_func, description: str = "Computation"
) -> Tuple[any, List[Dict]]:
    """Monitor memory usage during a computation.

    Args:
        client: Dask distributed client
        computation_func: Function that performs computation and returns result
        description: Description of the computation

    Returns:
        Tuple of (computation_result, memory_timeline)
    """
    print(f"\n📊 Monitoring memory during: {description}")

    memory_timeline = []
    start_time = time.perf_counter()

    # Initial memory snapshot
    initial_stats = get_spill_stats(client)
    memory_timeline.append({"time_elapsed": 0.0, "stage": "initial", **initial_stats})

    # Run computation with periodic memory monitoring
    import threading

    result = None
    computation_done = threading.Event()

    def run_computation():
        nonlocal result
        result = computation_func()
        computation_done.set()

    def monitor_memory():
        while not computation_done.is_set():
            elapsed = time.perf_counter() - start_time
            stats = get_spill_stats(client)
            memory_timeline.append({"time_elapsed": elapsed, "stage": "computing", **stats})
            time.sleep(0.5)  # Monitor every 500ms

    # Start computation and monitoring
    comp_thread = threading.Thread(target=run_computation)
    monitor_thread = threading.Thread(target=monitor_memory)

    comp_thread.start()
    monitor_thread.start()

    comp_thread.join()
    computation_done.set()
    monitor_thread.join()

    # Final memory snapshot
    final_stats = get_spill_stats(client)
    memory_timeline.append(
        {"time_elapsed": time.perf_counter() - start_time, "stage": "final", **final_stats}
    )

    return result, memory_timeline


def test_compression_algorithm(
    compression: str,
    array_size_gb: float = 2.0,
    max_workers: int = 2,
    reserve_mem_gb: float = 20.0,
    verbose: bool = False,
) -> Dict[str, any]:
    """Test a specific compression algorithm.

    Args:
        compression: Compression algorithm to test
        array_size_gb: Size of arrays to create (in GB)
        max_workers: Maximum number of workers
        reserve_mem_gb: Memory to reserve for OS
        verbose: Enable verbose output

    Returns:
        Dictionary with test results
    """
    print(f"\n🧪 Testing compression: {compression}")
    print("-" * 40)

    # Configure Dask with specific compression
    config = DaskSetupConfig(
        workload_type="cpu",
        max_workers=max_workers,
        reserve_mem_gb=reserve_mem_gb,
        spill_compression=compression,
        memory_target=0.60,  # More aggressive spilling
        memory_spill=0.70,
        memory_pause=0.85,
        memory_terminate=0.95,
    )

    try:
        # Create cluster
        client, cluster, temp_dir = setup_dask_client(
            workload_type=config.workload_type,
            max_workers=config.max_workers,
            reserve_mem_gb=config.reserve_mem_gb,
        )

        print(f"   Temp directory: {temp_dir}")
        print(f"   Workers: {len(client.scheduler_info()['workers'])}")

        # Calculate array dimensions to get approximately the requested size
        elements_per_gb = (1024**3) // 8  # 8 bytes per float64
        total_elements = int(array_size_gb * elements_per_gb)

        # Use a shape that will likely cause spilling
        shape = (int(np.sqrt(total_elements / 4)), int(np.sqrt(total_elements / 4)), 4)
        chunk_size = min(1000, shape[0] // 4)  # Reasonable chunk size

        print(f"   Array shape: {shape}")
        print(f"   Chunk size: {chunk_size}")

        # Define computation that will cause memory pressure
        def heavy_computation():
            # Create large arrays
            x = da.random.random(shape, chunks=(chunk_size, chunk_size, 2), dtype="float64")
            y = da.random.random(shape, chunks=(chunk_size, chunk_size, 2), dtype="float64")

            # Operations that require intermediate storage
            z = (x + y) * 2.0
            w = da.sin(z) + da.cos(z)

            # Reduction that forces computation
            result = w.sum().compute()
            return result

        # Monitor memory during computation
        with timer(f"Heavy computation with {compression}") as t:
            result, memory_timeline = monitor_memory_during_computation(
                client, heavy_computation, f"heavy computation with {compression}"
            )

        # Get final spill statistics
        final_spill_stats = get_spill_stats(client)

        # Clean up
        client.close()
        cluster.close()

        return {
            "compression": compression,
            "array_size_gb": array_size_gb,
            "computation_time": t["total"],
            "result": float(result),
            "memory_timeline": memory_timeline,
            "final_spill_stats": final_spill_stats,
            "temp_dir": temp_dir,
            "success": True,
        }

    except Exception as e:
        print(f"   ❌ Test failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()

        return {
            "compression": compression,
            "array_size_gb": array_size_gb,
            "computation_time": float("inf"),
            "result": None,
            "error": str(e),
            "success": False,
        }


def run_compression_benchmark(
    compressions: List[str] = None,
    array_size_gb: float = 1.5,
    max_workers: int = 2,
    reserve_mem_gb: float = 20.0,
    verbose: bool = False,
) -> Dict[str, Dict]:
    """Run benchmark comparing different compression algorithms.

    Args:
        compressions: List of compression algorithms to test
        array_size_gb: Size of test arrays
        max_workers: Maximum workers to use
        reserve_mem_gb: Memory to reserve
        verbose: Enable verbose output

    Returns:
        Dictionary with benchmark results
    """
    if compressions is None:
        compressions = ["lz4", "zstd", "snappy", "auto"]

    print("🔬 COMPRESSION ALGORITHM BENCHMARK")
    print("=" * 50)
    print(f"Array size: {array_size_gb} GB per test")
    print(f"Max workers: {max_workers}")
    print(f"Reserved memory: {reserve_mem_gb} GB")

    results = {}

    for compression in compressions:
        try:
            result = test_compression_algorithm(
                compression=compression,
                array_size_gb=array_size_gb,
                max_workers=max_workers,
                reserve_mem_gb=reserve_mem_gb,
                verbose=verbose,
            )
            results[compression] = result

            if result["success"]:
                print(f"   ✅ {compression}: {result['computation_time']:.2f}s")
            else:
                print(f"   ❌ {compression}: {result.get('error', 'Unknown error')}")

        except KeyboardInterrupt:
            print(f"\n🛑 Benchmark interrupted during {compression}")
            break
        except Exception as e:
            print(f"   ❌ {compression}: Unexpected error: {e}")
            results[compression] = {"compression": compression, "success": False, "error": str(e)}

    return results


def analyze_benchmark_results(results: Dict[str, Dict]) -> None:
    """Analyze and display benchmark results.

    Args:
        results: Benchmark results from run_compression_benchmark
    """
    print("\n" + "=" * 60)
    print("📊 BENCHMARK ANALYSIS")
    print("=" * 60)

    # Filter successful results
    successful_results = {k: v for k, v in results.items() if v.get("success", False)}

    if not successful_results:
        print("❌ No successful benchmark results to analyze")
        return

    # Sort by computation time
    sorted_results = sorted(successful_results.items(), key=lambda x: x[1]["computation_time"])

    print("🏆 Performance Ranking (faster is better):")
    for i, (compression, data) in enumerate(sorted_results, 1):
        time_s = data["computation_time"]
        if i == 1:
            print(f"🥇 {i}. {compression:10}: {time_s:6.2f}s (fastest)")
        elif i == 2:
            print(f"🥈 {i}. {compression:10}: {time_s:6.2f}s")
        elif i == 3:
            print(f"🥉 {i}. {compression:10}: {time_s:6.2f}s")
        else:
            print(f"   {i}. {compression:10}: {time_s:6.2f}s")

    # Relative performance
    fastest_time = sorted_results[0][1]["computation_time"]
    print(f"\n⚡ Relative Performance (vs fastest):")
    for compression, data in sorted_results:
        relative = data["computation_time"] / fastest_time
        print(f"   {compression:10}: {relative:5.2f}x")

    # Memory analysis
    print(f"\n💾 Memory Usage Analysis:")
    for compression, data in successful_results.items():
        spill_stats = data.get("final_spill_stats", {})
        if "total_memory_rss_mb" in spill_stats:
            rss_mb = spill_stats["total_memory_rss_mb"]
            avg_percent = spill_stats.get("average_memory_percent", 0)
            tmpdir_used = spill_stats.get("total_tmpdir_used_gb", 0)
            print(
                f"   {compression:10}: RSS={rss_mb:6.1f}MB, "
                f"Usage={avg_percent:5.1f}%, Spill={tmpdir_used:5.1f}GB"
            )


def save_benchmark_summary(results: Dict[str, Dict], output_file: Path) -> None:
    """Save benchmark results as a markdown table.

    Args:
        results: Benchmark results
        output_file: Output file path
    """
    # Prepare data for table
    table_data = []
    headers = [
        "Compression",
        "Time (s)",
        "Relative Performance",
        "Memory (MB)",
        "Spill (GB)",
        "Status",
    ]

    # Get baseline (fastest successful result)
    successful = {k: v for k, v in results.items() if v.get("success", False)}
    if successful:
        fastest_time = min(v["computation_time"] for v in successful.values())
    else:
        fastest_time = 1.0

    for compression, data in results.items():
        if data.get("success", False):
            time_s = data["computation_time"]
            relative = time_s / fastest_time

            spill_stats = data.get("final_spill_stats", {})
            memory_mb = spill_stats.get("total_memory_rss_mb", 0)
            spill_gb = spill_stats.get("total_tmpdir_used_gb", 0)

            status = "✅ Success"
        else:
            time_s = "N/A"
            relative = "N/A"
            memory_mb = "N/A"
            spill_gb = "N/A"
            status = f"❌ {data.get('error', 'Failed')[:30]}"

        table_data.append(
            [
                compression,
                f"{time_s:.2f}" if isinstance(time_s, float) else time_s,
                f"{relative:.2f}x" if isinstance(relative, float) else relative,
                f"{memory_mb:.1f}" if isinstance(memory_mb, (int, float)) else memory_mb,
                f"{spill_gb:.1f}" if isinstance(spill_gb, (int, float)) else spill_gb,
                status,
            ]
        )

    save_markdown_table(
        data=table_data,
        headers=headers,
        filename=output_file,
        title="Memory Compression Benchmark Results",
        description=(
            "Comparison of different compression algorithms for Dask memory spilling. "
            "Lower computation time and memory usage are better. "
            "Spill amount indicates how much data was written to temporary storage."
        ),
    )


def print_memory_management_tips() -> None:
    """Print memory management tips and best practices."""
    print("\n" + "=" * 60)
    print("💡 MEMORY MANAGEMENT BEST PRACTICES")
    print("=" * 60)

    print("""
🎯 COMPRESSION ALGORITHM SELECTION:
   • lz4: Fastest compression, good for CPU-bound workloads
   • zstd: Better compression ratio, good for limited disk space
   • snappy: Very fast, minimal CPU overhead
   • auto: Let Dask choose based on data characteristics

⚙️ SPILLING THRESHOLD TUNING:
   • memory.target (default 0.75): Start spilling at 75% memory usage
   • memory.spill (default 0.85): Aggressive spilling threshold
   • memory.pause (default 0.92): Pause new tasks when memory is high
   • memory.terminate (default 0.98): Kill worker as last resort

💾 MEMORY OPTIMIZATION STRATEGIES:
   • Use fewer, larger workers for memory-intensive tasks
   • Increase reserve_mem_gb on shared systems
   • Monitor spill directory usage (especially $PBS_JOBFS)
   • Consider adaptive scaling for variable workloads

🚨 WARNING - HPC STORAGE MANAGEMENT:
   • Spill files are written to $PBS_JOBFS (if available)
   • Clean up temp directories after job completion
   • Monitor disk quota usage in job scripts
   • Consider compression impact on job runtime vs disk usage

🔧 CONFIGURATION EXAMPLES:
   # Memory-constrained system
   DaskSetupConfig(
       workload_type="cpu",
       max_workers=1,
       reserve_mem_gb=8.0,
       memory_target=0.60,
       spill_compression="zstd"
   )
   
   # Fast processing priority
   DaskSetupConfig(
       workload_type="cpu", 
       max_workers=4,
       reserve_mem_gb=4.0,
       memory_target=0.80,
       spill_compression="lz4"
   )
""")


def main():
    """Main function demonstrating memory compression and spilling."""
    parser = argparse.ArgumentParser(
        description="Demonstrate memory compression and spilling optimization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--compressions",
        nargs="+",
        default=["lz4", "zstd", "snappy"],
        help="Compression algorithms to test (default: lz4 zstd snappy)",
    )

    parser.add_argument(
        "--array-size", type=float, default=1.0, help="Size of test arrays in GB (default: 1.0)"
    )

    parser.add_argument(
        "--workers", type=int, default=2, help="Maximum number of workers (default: 2)"
    )

    parser.add_argument(
        "--memory", type=float, default=20.0, help="Memory to reserve for OS in GB (default: 20.0)"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "outputs",
        help="Output directory for results",
    )

    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output and debug logging"
    )

    args = parser.parse_args()

    # Setup logging and output directory
    setup_logging(args.verbose)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("🧠 DASK_SETUP MEMORY COMPRESSION & SPILLING DEMONSTRATION")
    print("=" * 70)
    print(f"Testing compressions: {', '.join(args.compressions)}")
    print(f"Array size: {args.array_size} GB per test")
    print(f"Max workers: {args.workers}")
    print(f"Reserved memory: {args.memory} GB")
    print(f"Output directory: {args.output_dir}")

    # Show system memory info
    try:
        mem = psutil.virtual_memory()
        print(
            f"\n🖥️ System Memory: {mem.total / (1024**3):.1f} GB total, "
            f"{mem.available / (1024**3):.1f} GB available ({mem.percent:.1f}% used)"
        )
    except Exception:
        pass

    try:
        # Run benchmark
        start_time = time.perf_counter()
        results = run_compression_benchmark(
            compressions=args.compressions,
            array_size_gb=args.array_size,
            max_workers=args.workers,
            reserve_mem_gb=args.memory,
            verbose=args.verbose,
        )

        # Analyze results
        analyze_benchmark_results(results)

        # Save results
        output_file = args.output_dir / "memory_compression_benchmark.md"
        save_benchmark_summary(results, output_file)

        # Print tips
        print_memory_management_tips()

        total_time = time.perf_counter() - start_time
        print(f"\n✅ Memory compression benchmark completed!")
        print(f"🕒 Total runtime: {format_duration(total_time)}")
        print(f"📄 Results saved to: {output_file}")

    except KeyboardInterrupt:
        print("\n\n🛑 Benchmark interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Benchmark failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
