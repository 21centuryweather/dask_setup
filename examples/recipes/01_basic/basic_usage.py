#!/usr/bin/env python
"""Basic dask_setup patterns – CPU, I/O, Mixed workloads.

This recipe demonstrates the three core workload types supported by dask_setup,
showing resource detection, temp directory configuration, and safe shutdown patterns.

Requirements:
- dask_setup, dask, distributed
- numpy (for demonstration workloads)

Outputs:
- Console output showing cluster configuration
- Dashboard access instructions
- Performance timing for each workload type

Key Learning Points:
- How to choose the right workload_type for your use case
- Resource detection and memory allocation
- Dashboard access and SSH tunneling
- Proper cluster lifecycle management
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Add the parent directory to path so we can import dask_setup
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))

try:
    import dask
    import dask.array as da
    import dask.bag as db
    import numpy as np
    from dask.distributed import as_completed, wait

    from dask_setup import setup_dask_client
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed numpy")
    sys.exit(1)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def print_cluster_info(client, cluster, temp_dir: str) -> None:
    """Print detailed information about the cluster configuration."""
    print("=" * 60)
    print("🔧 CLUSTER CONFIGURATION")
    print("=" * 60)

    # Scheduler info
    scheduler_info = client.scheduler_info()
    workers = scheduler_info.get("workers", {})

    print(f"📊 Scheduler: {client.scheduler.address}")
    print(f"👥 Workers: {len(workers)}")

    if workers:
        first_worker = next(iter(workers.values()))
        print(f"🧵 Threads per worker: {first_worker.get('nthreads', 'unknown')}")
        memory_limit = first_worker.get("memory_limit", 0)
        if memory_limit > 0:
            memory_gb = memory_limit / (1024**3)
            print(f"💾 Memory per worker: {memory_gb:.1f} GB")
        else:
            print(f"💾 Memory per worker: Not set")

    print(f"📁 Temp/spill directory: {temp_dir}")

    # Dashboard info
    if hasattr(cluster, "dashboard_link") and cluster.dashboard_link:
        print(f"🖥️  Dashboard: {cluster.dashboard_link}")

        # SSH tunnel hint for HPC users
        try:
            import socket

            hostname = socket.gethostname()
            if any(hpc in hostname.lower() for hpc in ["gadi", "raijin", "magnus", "pawsey"]):
                port = cluster.dashboard_link.split(":")[-1].split("/")[0]
                print(
                    f"🔗 SSH tunnel (for HPC): ssh -N -L 8787:{hostname}:{port} <your_login_node>"
                )
        except Exception:
            pass

    print("=" * 60)


def run_cpu_demo(
    max_workers: int = None, reserve_mem_gb: float = 50.0, verbose: bool = False
) -> tuple:
    """Demonstrate CPU-bound workload optimization.

    Uses many processes with 1 thread each for compute-heavy operations
    that benefit from avoiding the GIL.
    """
    print("\n🚀 CPU-BOUND WORKLOAD DEMO")
    print("-" * 40)
    print("Best for: Heavy computation, NumPy operations, mathematical reductions")
    print("Topology: Many processes, 1 thread each (avoids GIL)")

    start_time = time.perf_counter()

    try:
        # Create CPU-optimized cluster
        client, cluster, temp_dir = setup_dask_client(
            workload_type="cpu",
            max_workers=max_workers,
            reserve_mem_gb=reserve_mem_gb,
            dashboard=True,
        )

        print_cluster_info(client, cluster, temp_dir)

        # CPU-intensive workload: Large matrix operations
        print("\n🧮 Running CPU-intensive matrix operations...")

        # Create large random arrays
        x = da.random.random((5000, 5000), chunks=(1000, 1000))
        y = da.random.random((5000, 5000), chunks=(1000, 1000))

        # Compute-heavy operations
        result = (x + y).T @ x
        mean_result = result.mean()

        # Execute and time
        compute_start = time.perf_counter()
        final_result = mean_result.compute()
        compute_time = time.perf_counter() - compute_start

        print(f"✅ Matrix computation completed: result = {final_result:.6f}")
        print(f"⏱️  Computation time: {compute_time:.2f} seconds")

        return client, cluster, temp_dir, compute_time

    except Exception as e:
        print(f"❌ CPU demo failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return None, None, None, float("inf")


def run_io_demo(
    max_workers: int = None, reserve_mem_gb: float = 50.0, verbose: bool = False
) -> tuple:
    """Demonstrate I/O-bound workload optimization.

    Uses fewer processes with many threads each for I/O-heavy operations
    that benefit from high concurrency.
    """
    print("\n📁 I/O-BOUND WORKLOAD DEMO")
    print("-" * 40)
    print("Best for: File operations, network I/O, database queries")
    print("Topology: Few processes, many threads each (maximizes I/O concurrency)")

    start_time = time.perf_counter()

    try:
        # Create I/O-optimized cluster
        client, cluster, temp_dir = setup_dask_client(
            workload_type="io",
            max_workers=max_workers,
            reserve_mem_gb=reserve_mem_gb,
            dashboard=True,
        )

        print_cluster_info(client, cluster, temp_dir)

        # I/O simulation: Many small tasks (simulates file operations)
        print("\n📂 Running I/O simulation (many concurrent small tasks)...")

        # Create many small delayed tasks that simulate I/O
        @dask.delayed
        def simulate_file_operation(i):
            """Simulate a small file operation with some I/O-like delay."""
            # Simulate some processing + I/O wait
            data = np.random.random(100)  # Small data processing
            time.sleep(0.01)  # Simulate I/O wait
            return data.sum()

        # Create many tasks
        tasks = [simulate_file_operation(i) for i in range(200)]

        # Execute and time
        compute_start = time.perf_counter()
        results = dask.compute(*tasks)
        compute_time = time.perf_counter() - compute_start

        print(f"✅ Processed {len(tasks)} simulated file operations")
        print(f"📊 Average result: {np.mean(results):.6f}")
        print(f"⏱️  Total I/O time: {compute_time:.2f} seconds")

        return client, cluster, temp_dir, compute_time

    except Exception as e:
        print(f"❌ I/O demo failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return None, None, None, float("inf")


def run_mixed_demo(
    max_workers: int = None, reserve_mem_gb: float = 50.0, verbose: bool = False
) -> tuple:
    """Demonstrate mixed workload optimization.

    Balanced topology for workflows that combine computation and I/O.
    """
    print("\n🔀 MIXED WORKLOAD DEMO")
    print("-" * 40)
    print("Best for: ETL pipelines, data preprocessing, mixed compute/I/O")
    print("Topology: Balanced processes and threads (flexible for mixed tasks)")

    start_time = time.perf_counter()

    try:
        # Create mixed-workload cluster
        client, cluster, temp_dir = setup_dask_client(
            workload_type="mixed",
            max_workers=max_workers,
            reserve_mem_gb=reserve_mem_gb,
            dashboard=True,
        )

        print_cluster_info(client, cluster, temp_dir)

        # Mixed workload: Data processing pipeline
        print("\n⚙️  Running mixed compute/I/O pipeline...")

        # Stage 1: Generate data (compute-heavy)
        data = da.random.random((2000, 2000), chunks=(500, 500))

        # Stage 2: Transform data (mixed compute/memory)
        transformed = data * 2 + da.sin(data)

        # Stage 3: Reduction (compute-heavy)
        aggregated = transformed.mean(axis=0)

        # Stage 4: Final processing (simulate I/O + compute)
        @dask.delayed
        def process_chunk(chunk_data):
            # Simulate some processing
            processed = np.array(chunk_data) ** 2
            time.sleep(0.005)  # Simulate small I/O operation
            return processed.mean()

        # Convert to delayed and process
        delayed_result = dask.delayed(aggregated.compute)()
        final_delayed = process_chunk(delayed_result)

        # Execute pipeline
        compute_start = time.perf_counter()
        final_result = final_delayed.compute()
        compute_time = time.perf_counter() - compute_start

        print(f"✅ Mixed pipeline completed: result = {final_result:.6f}")
        print(f"⏱️  Pipeline time: {compute_time:.2f} seconds")

        return client, cluster, temp_dir, compute_time

    except Exception as e:
        print(f"❌ Mixed demo failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return None, None, None, float("inf")


def print_performance_summary(cpu_time: float, io_time: float, mixed_time: float) -> None:
    """Print a summary of performance results."""
    print("\n" + "=" * 60)
    print("📊 PERFORMANCE SUMMARY")
    print("=" * 60)

    times = [("CPU-bound", cpu_time), ("I/O-bound", io_time), ("Mixed", mixed_time)]

    # Filter out failed runs
    valid_times = [(name, t) for name, t in times if t != float("inf")]

    if not valid_times:
        print("❌ No successful runs to compare")
        return

    for name, runtime in valid_times:
        if runtime == min(t for _, t in valid_times):
            print(f"🥇 {name:12}: {runtime:6.2f}s (fastest)")
        else:
            print(f"   {name:12}: {runtime:6.2f}s")

    print("\n💡 WORKLOAD TYPE RECOMMENDATIONS:")
    print("• CPU-bound: Heavy math, NumPy operations, scientific computing")
    print("• I/O-bound: File processing, network operations, database queries")
    print("• Mixed: ETL pipelines, data preprocessing, general-purpose")


def print_common_pitfalls() -> None:
    """Print common pitfalls and solutions."""
    print("\n" + "=" * 60)
    print("⚠️  COMMON PITFALLS & SOLUTIONS")
    print("=" * 60)

    print("""
🔴 Problem: "Task requires > memory_limit"
✅ Solution: Reduce workers or increase memory reservation
   setup_dask_client("cpu", max_workers=1, reserve_mem_gb=30)

🔴 Problem: Slower than expected performance  
✅ Solution: Choose correct workload type and check chunks
   # For heavy computation:
   setup_dask_client("cpu")
   
   # For file operations:
   setup_dask_client("io")

🔴 Problem: Dashboard not accessible on HPC
✅ Solution: Use SSH tunnel (command printed above)
   ssh -N -L 8787:compute_node:port login_node

🔴 Problem: Cluster startup is slow
✅ Solution: Check temp directory location and disk space
   # Temp files should go to fast local storage ($PBS_JOBFS)
""")


def main():
    """Main function to run the basic usage demos."""
    parser = argparse.ArgumentParser(
        description="Demonstrate basic dask_setup usage patterns",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--workload",
        choices=["cpu", "io", "mixed", "all"],
        default="all",
        help="Which workload demo to run (default: all)",
    )

    parser.add_argument(
        "--workers", type=int, default=None, help="Maximum number of workers (default: auto-detect)"
    )

    parser.add_argument(
        "--memory", type=float, default=50.0, help="Memory to reserve for OS in GB (default: 50.0)"
    )

    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output and debug logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    print("🎯 DASK_SETUP BASIC USAGE DEMONSTRATION")
    print("=" * 60)
    print(f"Running workload type(s): {args.workload}")
    print(f"Max workers: {args.workers or 'auto-detect'}")
    print(f"Reserved memory: {args.memory} GB")

    # Run demonstrations
    cpu_time = io_time = mixed_time = float("inf")
    active_clusters = []

    try:
        if args.workload in ["cpu", "all"]:
            cpu_client, cpu_cluster, _, cpu_time = run_cpu_demo(
                args.workers, args.memory, args.verbose
            )
            if cpu_client:
                active_clusters.append((cpu_client, cpu_cluster))

        if args.workload in ["io", "all"]:
            io_client, io_cluster, _, io_time = run_io_demo(args.workers, args.memory, args.verbose)
            if io_client:
                active_clusters.append((io_client, io_cluster))

        if args.workload in ["mixed", "all"]:
            mixed_client, mixed_cluster, _, mixed_time = run_mixed_demo(
                args.workers, args.memory, args.verbose
            )
            if mixed_client:
                active_clusters.append((mixed_client, mixed_cluster))

        # Print results
        if args.workload == "all":
            print_performance_summary(cpu_time, io_time, mixed_time)

        print_common_pitfalls()

        print(f"\n✅ Demo completed successfully!")
        print(f"🕒 Total runtime: {time.perf_counter() - time.time():.1f} seconds")

    except KeyboardInterrupt:
        print("\n\n🛑 Demo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1
    finally:
        # Clean shutdown
        print("\n🧹 Cleaning up clusters...")
        for client, cluster in active_clusters:
            try:
                client.close()
                cluster.close()
            except Exception as e:
                if args.verbose:
                    print(f"Warning during cleanup: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
