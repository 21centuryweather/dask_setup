#!/usr/bin/env python
"""Advanced configuration management and profile demonstration.

This recipe showcases the sophisticated configuration management system in dask_setup,
including built-in profiles, custom YAML profiles, parameter overrides, and
runtime configuration management.

Requirements:
- dask_setup, dask, distributed
- numpy (for benchmarking)
- pyyaml (for profile loading)

Outputs:
- Console output showing different configuration options
- Performance comparison between configurations
- Generated custom profile examples

Key Learning Points:
- Using built-in profiles vs custom configurations
- YAML profile creation and management
- Runtime configuration overrides
- Performance impact of different configurations
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

# Add the parent directory to path so we can import dask_setup
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))

try:
    import dask
    import dask.array as da
    import numpy as np
    import yaml

    from dask_setup import setup_dask_client
    from dask_setup.config import DaskSetupConfig
    from dask_setup.config_manager import ConfigManager
    from dask_setup.exceptions import InvalidConfigurationError
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed numpy pyyaml")
    sys.exit(1)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def demonstrate_builtin_profiles(verbose: bool = False) -> Dict[str, float]:
    """Demonstrate using built-in configuration profiles."""
    print("\n" + "=" * 60)
    print("🏗️  BUILT-IN PROFILE DEMONSTRATION")
    print("=" * 60)

    # Get profile manager
    manager = ConfigManager()

    # List all built-in profiles
    profiles = manager.list_profiles()
    builtin_profiles = {name: prof for name, prof in profiles.items() if prof.builtin}

    print(f"📋 Available built-in profiles ({len(builtin_profiles)}):")
    for name, profile in builtin_profiles.items():
        print(f"  • {name:15}: {profile.config.description}")
        if verbose:
            print(
                f"    Workload: {profile.config.workload_type}, "
                f"Memory: {profile.config.reserve_mem_gb} GB, "
                f"Adaptive: {profile.config.adaptive}"
            )

    # Benchmark a few profiles
    benchmark_results = {}
    test_profiles = ["climate_analysis", "interactive", "development"]

    for profile_name in test_profiles:
        if profile_name not in builtin_profiles:
            continue

        print(f"\n🧪 Testing profile: {profile_name}")
        print("-" * 30)

        try:
            # Use the profile
            client, cluster, temp_dir = setup_dask_client(profile=profile_name)

            # Print configuration
            profile = builtin_profiles[profile_name]
            config = profile.config
            print(f"   Workload type: {config.workload_type}")
            print(f"   Max workers: {config.max_workers or 'auto'}")
            print(f"   Reserve memory: {config.reserve_mem_gb} GB")
            print(f"   Adaptive: {config.adaptive}")

            # Run benchmark
            start_time = time.perf_counter()

            # Simple computation benchmark
            x = da.random.random((2000, 2000), chunks=(500, 500))
            result = (x + x.T).mean().compute()

            runtime = time.perf_counter() - start_time
            benchmark_results[profile_name] = runtime

            print(f"   Benchmark result: {result:.6f}")
            print(f"   Runtime: {runtime:.2f} seconds")

            # Clean up
            client.close()
            cluster.close()

        except Exception as e:
            print(f"   ❌ Profile {profile_name} failed: {e}")
            if verbose:
                import traceback

                traceback.print_exc()
            benchmark_results[profile_name] = float("inf")

    return benchmark_results


def demonstrate_inline_config_override(verbose: bool = False) -> Optional[float]:
    """Demonstrate inline configuration parameter override."""
    print("\n" + "=" * 60)
    print("⚙️  INLINE CONFIGURATION OVERRIDE")
    print("=" * 60)

    print("Showing explicit parameter configuration with dask.config overrides...")

    try:
        # Custom configuration with inline overrides
        with dask.config.set(
            {
                "distributed.scheduler.idle-timeout": "30s",
                "distributed.worker.daemon": False,
                "distributed.worker.memory.rebalance.measure": "managed_in_memory",
            }
        ):
            print("🔧 Custom Dask configuration:")
            print(
                f"   scheduler.idle-timeout: {dask.config.get('distributed.scheduler.idle-timeout')}"
            )
            print(f"   worker.daemon: {dask.config.get('distributed.worker.daemon')}")
            print(
                f"   worker.memory.rebalance.measure: {dask.config.get('distributed.worker.memory.rebalance.measure')}"
            )

            # Create cluster with explicit parameters
            client, cluster, temp_dir = setup_dask_client(
                workload_type="mixed",
                max_workers=4,
                reserve_mem_gb=30.0,
                dashboard=True,
                adaptive=False,
            )

            # Show final configuration
            print("\n📊 Final cluster configuration:")
            scheduler_info = client.scheduler_info()
            workers = scheduler_info.get("workers", {})

            print(f"   Workers: {len(workers)}")
            if workers:
                first_worker = next(iter(workers.values()))
                print(f"   Threads per worker: {first_worker.get('nthreads', 'unknown')}")
                memory_limit = first_worker.get("memory_limit", 0)
                if memory_limit > 0:
                    memory_gb = memory_limit / (1024**3)
                    print(f"   Memory per worker: {memory_gb:.1f} GB")

            print(f"   Temp directory: {temp_dir}")

            # Run performance test
            print("\n🏃 Running performance test...")
            start_time = time.perf_counter()

            # Mixed workload test
            data = da.random.random((1500, 1500), chunks=(300, 300))
            transformed = (data * 2 + da.sin(data)).rechunk((150, 150))
            result = transformed.mean().compute()

            runtime = time.perf_counter() - start_time
            print(f"   Result: {result:.6f}")
            print(f"   Runtime: {runtime:.2f} seconds")

            # Clean up
            client.close()
            cluster.close()

            return runtime

    except Exception as e:
        print(f"❌ Inline configuration failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return None


def load_and_test_yaml_profile(profile_path: Path, verbose: bool = False) -> Optional[float]:
    """Load and test a custom YAML profile."""
    print(f"\n" + "=" * 60)
    print(f"📄 YAML PROFILE LOADING")
    print("=" * 60)

    if not profile_path.exists():
        print(f"❌ Profile file not found: {profile_path}")
        return None

    try:
        # Load YAML profile
        print(f"📂 Loading profile from: {profile_path}")

        with open(profile_path, "r") as f:
            profile_data = yaml.safe_load(f)

        print(f"✅ Profile loaded: {profile_data['name']}")
        print(f"   Description: {profile_data.get('description', 'No description')}")
        print(f"   Workload type: {profile_data.get('workload_type', 'Not specified')}")
        print(f"   Reserve memory: {profile_data.get('reserve_mem_gb', 'Not specified')} GB")

        if "tags" in profile_data:
            print(f"   Tags: {', '.join(profile_data['tags'])}")

        if "notes" in profile_data:
            print(f"   Notes: {profile_data['notes'][:100]}...")

        # Create DaskSetupConfig from YAML data
        # Remove non-config fields
        config_data = {
            k: v
            for k, v in profile_data.items()
            if k not in ["name", "description", "tags", "notes"]
        }

        try:
            config = DaskSetupConfig(**config_data)
            print(f"\n✅ Configuration validated successfully")
        except Exception as e:
            print(f"❌ Configuration validation failed: {e}")
            return None

        # Use the configuration
        print(f"\n🚀 Starting cluster with YAML profile...")

        # We'll manually apply the config since we don't have profile manager integration
        client, cluster, temp_dir = setup_dask_client(
            workload_type=config.workload_type,
            max_workers=config.max_workers,
            reserve_mem_gb=config.reserve_mem_gb,
            max_mem_gb=config.max_mem_gb,
            dashboard=config.dashboard,
            adaptive=config.adaptive,
            min_workers=config.min_workers,
            suggest_chunks=config.suggest_chunks,
        )

        # Performance test
        print("\n🔬 Running YAML profile performance test...")
        start_time = time.perf_counter()

        # CPU-intensive test (since this is a CPU profile)
        x = da.random.random((3000, 3000), chunks=(600, 600))
        y = da.random.random((3000, 3000), chunks=(600, 600))
        result = (x @ y.T).mean().compute()

        runtime = time.perf_counter() - start_time
        print(f"   Matrix operation result: {result:.6f}")
        print(f"   Runtime: {runtime:.2f} seconds")

        # Clean up
        client.close()
        cluster.close()

        return runtime

    except yaml.YAMLError as e:
        print(f"❌ YAML parsing error: {e}")
        return None
    except Exception as e:
        print(f"❌ YAML profile test failed: {e}")
        if verbose:
            import traceback

            traceback.print_exc()
        return None


def create_custom_profile_example() -> None:
    """Create example custom profile files."""
    print(f"\n" + "=" * 60)
    print(f"📝 CREATING CUSTOM PROFILE EXAMPLES")
    print("=" * 60)

    examples_dir = Path(__file__).parent.parent / "configs"
    examples_dir.mkdir(exist_ok=True)

    # I/O-heavy profile
    io_profile = {
        "name": "io_heavy",
        "description": "High-throughput I/O operations with many threads",
        "workload_type": "io",
        "max_workers": 2,
        "reserve_mem_gb": 40.0,
        "dashboard": True,
        "adaptive": False,
        "spill_compression": "snappy",  # Fast compression for I/O
        "comm_compression": False,
        "tags": ["io-heavy", "multithreaded", "file-operations"],
        "notes": "Optimized for file processing, data loading, and I/O-bound operations.",
    }

    io_profile_path = examples_dir / "io_heavy_profile.yaml"
    with open(io_profile_path, "w") as f:
        yaml.dump(io_profile, f, default_flow_style=False, sort_keys=False)

    print(f"✅ Created I/O-heavy profile: {io_profile_path}")

    # Memory-conservative profile
    conservative_profile = {
        "name": "memory_conservative",
        "description": "Conservative memory usage for shared systems",
        "workload_type": "mixed",
        "max_workers": 2,
        "reserve_mem_gb": 80.0,  # Large reservation
        "max_mem_gb": 32.0,  # Cap total usage
        "dashboard": False,  # Reduce overhead
        "adaptive": True,
        "min_workers": 1,
        "memory_target": 0.60,  # More conservative spilling
        "memory_spill": 0.70,
        "memory_pause": 0.85,
        "memory_terminate": 0.95,
        "spill_compression": "zstd",  # High compression ratio
        "tags": ["conservative", "shared-system", "low-memory"],
        "notes": "Safe configuration for shared systems with limited resources.",
    }

    conservative_profile_path = examples_dir / "conservative_profile.yaml"
    with open(conservative_profile_path, "w") as f:
        yaml.dump(conservative_profile, f, default_flow_style=False, sort_keys=False)

    print(f"✅ Created conservative profile: {conservative_profile_path}")

    print(f"\n💡 Profile files created in: {examples_dir}")
    print(f"   Load with: setup_dask_client(profile='path/to/profile.yaml')")
    print(f"   Or use ConfigManager for persistent profile management")


def print_best_practices() -> None:
    """Print configuration management best practices."""
    print("\n" + "=" * 60)
    print("💡 CONFIGURATION MANAGEMENT BEST PRACTICES")
    print("=" * 60)

    print("""
🔧 CONFIGURATION HIERARCHY (highest to lowest precedence):
   1. Explicit function parameters
   2. Environment variables (DASK_SETUP_*)
   3. Profile configuration
   4. Built-in defaults

📝 YAML PROFILE ORGANIZATION:
   • Keep profiles in version control
   • Use descriptive names and tags
   • Include detailed notes and use cases
   • Validate configuration before deployment

🎯 PROFILE SELECTION GUIDELINES:
   • climate_analysis: Large-scale scientific computing
   • interactive: Jupyter notebooks and development
   • production: Robust server deployments
   • development: Testing and debugging

⚡ PERFORMANCE TUNING:
   • Measure before optimizing
   • Test with representative workloads
   • Monitor memory usage and spilling
   • Adjust based on actual resource usage

🔒 SECURITY CONSIDERATIONS:
   • Avoid hardcoding sensitive paths
   • Use relative paths where possible
   • Review dashboard exposure in production
   • Consider firewall rules for HPC systems
""")


def print_performance_summary(results: Dict[str, Optional[float]]) -> None:
    """Print performance comparison summary."""
    print("\n" + "=" * 60)
    print("📊 CONFIGURATION PERFORMANCE COMPARISON")
    print("=" * 60)

    # Filter valid results
    valid_results = {
        name: time for name, time in results.items() if time is not None and time != float("inf")
    }

    if not valid_results:
        print("❌ No successful performance measurements to compare")
        return

    # Sort by performance
    sorted_results = sorted(valid_results.items(), key=lambda x: x[1])

    print(f"Performance ranking (lower is better):")
    for i, (name, runtime) in enumerate(sorted_results, 1):
        if i == 1:
            print(f"🥇 {i}. {name:20}: {runtime:6.2f}s (fastest)")
        elif i == 2:
            print(f"🥈 {i}. {name:20}: {runtime:6.2f}s")
        elif i == 3:
            print(f"🥉 {i}. {name:20}: {runtime:6.2f}s")
        else:
            print(f"   {i}. {name:20}: {runtime:6.2f}s")

    # Show relative performance
    fastest_time = sorted_results[0][1]
    print(f"\nRelative performance (vs fastest):")
    for name, runtime in sorted_results:
        relative = runtime / fastest_time
        print(f"   {name:20}: {relative:5.2f}x")


def main():
    """Main function demonstrating configuration management."""
    parser = argparse.ArgumentParser(
        description="Demonstrate advanced configuration management",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--demo",
        choices=["builtin", "inline", "yaml", "create", "all"],
        default="all",
        help="Which demonstration to run (default: all)",
    )

    parser.add_argument(
        "--profile",
        type=Path,
        default=None,
        help="Path to YAML profile for testing (for --demo yaml)",
    )

    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output and debug logging"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    print("🎛️  DASK_SETUP CONFIGURATION MANAGEMENT DEMONSTRATION")
    print("=" * 70)

    results = {}

    try:
        # Built-in profiles demo
        if args.demo in ["builtin", "all"]:
            builtin_results = demonstrate_builtin_profiles(args.verbose)
            results.update(builtin_results)

        # Inline configuration demo
        if args.demo in ["inline", "all"]:
            inline_result = demonstrate_inline_config_override(args.verbose)
            if inline_result is not None:
                results["inline_override"] = inline_result

        # YAML profile demo
        if args.demo in ["yaml", "all"]:
            profile_path = args.profile or (
                Path(__file__).parent.parent / "configs" / "cpu_profile.yaml"
            )
            yaml_result = load_and_test_yaml_profile(profile_path, args.verbose)
            if yaml_result is not None:
                results["yaml_profile"] = yaml_result

        # Create profile examples
        if args.demo in ["create", "all"]:
            create_custom_profile_example()

        # Print summary
        if results:
            print_performance_summary(results)

        print_best_practices()

        print(f"\n✅ Configuration management demo completed successfully!")

    except KeyboardInterrupt:
        print("\n\n🛑 Demo interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
