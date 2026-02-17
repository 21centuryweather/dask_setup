#!/usr/bin/env python
"""Storage format intelligence and I/O optimization demonstration.

This recipe demonstrates intelligent handling of different storage formats,
showcasing NetCDF to Zarr conversion workflows, engine selection optimization,
and I/O performance analysis with dask_setup.

Requirements:
- dask_setup, dask, distributed
- xarray, numpy
- netcdf4, h5netcdf (NetCDF engines)
- zarr (Zarr storage)

Outputs:
- Generated test NetCDF files
- Converted Zarr store (~50 MB)
- Performance comparison analysis
- Engine performance benchmarks
- I/O optimization recommendations

Key Learning Points:
- NetCDF vs Zarr performance characteristics
- Engine selection (netcdf4 vs h5netcdf)
- Parallel I/O optimization
- Chunking strategies for different formats
- File system caching effects
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
    import xarray as xr

    from dask_setup import setup_dask_client
    from utils import format_duration, save_markdown_table, timer
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed xarray numpy")
    sys.exit(1)

# Check for optional dependencies
NETCDF_ENGINES = []
try:
    import netCDF4

    NETCDF_ENGINES.append("netcdf4")
except ImportError:
    pass

try:
    import h5netcdf

    NETCDF_ENGINES.append("h5netcdf")
except ImportError:
    pass

try:
    import zarr

    ZARR_AVAILABLE = True
except ImportError:
    ZARR_AVAILABLE = False


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


def create_sample_netcdf_files(
    output_dir: Path,
    num_files: int = 20,
    time_per_file: int = 30,
    spatial_shape: Tuple[int, int] = (180, 360),
) -> List[Path]:
    """Create sample NetCDF files for testing.

    Args:
        output_dir: Directory to create files in
        num_files: Number of files to create
        time_per_file: Time steps per file
        spatial_shape: Spatial dimensions (lat, lon)

    Returns:
        List of created file paths
    """
    print(f"\n📁 Creating {num_files} sample NetCDF files...")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    lat_size, lon_size = spatial_shape
    file_paths = []

    # Create coordinates
    lat = np.linspace(-90, 90, lat_size)
    lon = np.linspace(-180, 180, lon_size)

    with timer(f"Creating {num_files} NetCDF files") as t:
        for i in range(num_files):
            # Create time coordinate for this file
            time_start = i * time_per_file
            time = np.arange(time_start, time_start + time_per_file)

            # Generate realistic-looking data
            np.random.seed(42 + i)  # Reproducible data

            # Temperature: seasonal cycle + noise
            temp_base = 15 * np.cos(2 * np.pi * time / 365.25) + 273.15  # Kelvin
            temp_data = temp_base[:, None, None] + 5 * np.random.random(
                (time_per_file, lat_size, lon_size)
            )

            # Precipitation: more variable
            precip_data = np.abs(np.random.gamma(2, 2, (time_per_file, lat_size, lon_size)))

            # Create dataset
            ds = xr.Dataset(
                {
                    "temperature": (
                        ["time", "lat", "lon"],
                        temp_data,
                        {
                            "units": "K",
                            "long_name": "Near-surface air temperature",
                            "standard_name": "air_temperature",
                        },
                    ),
                    "precipitation": (
                        ["time", "lat", "lon"],
                        precip_data,
                        {
                            "units": "mm/day",
                            "long_name": "Daily precipitation",
                            "standard_name": "precipitation_flux",
                        },
                    ),
                },
                coords={
                    "time": time,
                    "lat": ("lat", lat, {"units": "degrees_north"}),
                    "lon": ("lon", lon, {"units": "degrees_east"}),
                },
            )

            # Add global attributes
            ds.attrs.update(
                {
                    "title": f"Sample climate data file {i + 1}",
                    "institution": "dask_setup recipe demonstration",
                    "source": "Synthetic data for I/O testing",
                    "conventions": "CF-1.8",
                }
            )

            # Save file
            filename = f"sample_data_{i + 1:03d}.nc"
            file_path = output_dir / filename

            # Use compression to make realistic file sizes
            encoding = {
                "temperature": {"zlib": True, "complevel": 4},
                "precipitation": {"zlib": True, "complevel": 4},
            }

            ds.to_netcdf(file_path, encoding=encoding)
            file_paths.append(file_path)

            if (i + 1) % 5 == 0:
                print(f"   Created {i + 1}/{num_files} files...")

    # Calculate total size
    total_size = sum(f.stat().st_size for f in file_paths) / (1024**2)
    print(f"✅ Created {num_files} files ({total_size:.1f} MB total)")
    print(f"   Average file size: {total_size / num_files:.1f} MB")
    print(f"   Files located in: {output_dir}")

    return file_paths


def benchmark_netcdf_engines(
    file_paths: List[Path], engines: List[str] = None, client=None
) -> Dict[str, Dict]:
    """Benchmark different NetCDF engines for reading performance.

    Args:
        file_paths: List of NetCDF files to read
        engines: List of engines to test
        client: Dask client for parallel operations

    Returns:
        Dictionary with benchmark results
    """
    if engines is None:
        engines = NETCDF_ENGINES

    if not engines:
        print("❌ No NetCDF engines available for benchmarking")
        return {}

    print(f"\n🏎️  NETCDF ENGINE PERFORMANCE BENCHMARK")
    print("=" * 50)
    print(f"Testing engines: {engines}")
    print(f"Files to process: {len(file_paths)}")

    results = {}

    for engine in engines:
        print(f"\n🧪 Testing engine: {engine}")
        print("-" * 30)

        try:
            # Test 1: Single file reading
            with timer(f"{engine} single file") as t:
                ds_single = xr.open_dataset(file_paths[0], engine=engine)
                _ = ds_single.temperature.values  # Force loading
                ds_single.close()

            single_file_time = t["total"]

            # Test 2: Multi-file reading with xr.open_mfdataset
            with timer(f"{engine} multi-file (mfdataset)") as t:
                ds_multi = xr.open_mfdataset(
                    file_paths[:10],  # Use subset for faster testing
                    engine=engine,
                    chunks={"time": 30, "lat": 90, "lon": 180},  # Reasonable chunks
                    parallel=True,
                    combine="by_coords",
                )

                # Test a computation that requires reading data
                temp_mean = ds_multi.temperature.mean().compute()
                ds_multi.close()

            multi_file_time = t["total"]

            # Test 3: Chunked reading performance
            with timer(f"{engine} chunked computation") as t:
                ds_chunked = xr.open_mfdataset(
                    file_paths[:5],  # Smaller subset for chunked test
                    engine=engine,
                    chunks={"time": 10, "lat": 60, "lon": 120},  # Smaller chunks
                    parallel=True,
                )

                # More complex computation
                temp_std = ds_chunked.temperature.std(dim="time").compute()
                ds_chunked.close()

            chunked_time = t["total"]

            results[engine] = {
                "single_file_time": single_file_time,
                "multi_file_time": multi_file_time,
                "chunked_time": chunked_time,
                "success": True,
                "temp_mean": float(temp_mean),
                "error": None,
            }

            print(f"   Single file: {single_file_time:.2f}s")
            print(f"   Multi-file:  {multi_file_time:.2f}s")
            print(f"   Chunked:     {chunked_time:.2f}s")
            print(f"   Temp mean:   {temp_mean:.2f} K")

        except Exception as e:
            print(f"   ❌ Engine {engine} failed: {e}")
            results[engine] = {
                "single_file_time": float("inf"),
                "multi_file_time": float("inf"),
                "chunked_time": float("inf"),
                "success": False,
                "error": str(e),
            }

    return results


def convert_netcdf_to_zarr(
    netcdf_files: List[Path], zarr_output: Path, client=None, rechunk: bool = True
) -> Dict[str, any]:
    """Convert NetCDF files to Zarr format.

    Args:
        netcdf_files: List of NetCDF files to convert
        zarr_output: Output Zarr store path
        client: Dask client for parallel operations
        rechunk: Whether to rechunk during conversion

    Returns:
        Dictionary with conversion results
    """
    if not ZARR_AVAILABLE:
        print("❌ Zarr not available, skipping conversion")
        return {"success": False, "error": "Zarr not installed"}

    print(f"\n📦 NETCDF TO ZARR CONVERSION")
    print("=" * 40)
    print(f"Input files: {len(netcdf_files)}")
    print(f"Output store: {zarr_output}")

    try:
        with timer("NetCDF to Zarr conversion") as t:
            # Choose best available engine
            engine = "h5netcdf" if "h5netcdf" in NETCDF_ENGINES else "netcdf4"
            print(f"Using engine: {engine}")

            # Open multi-file dataset
            print("📂 Opening NetCDF files...")
            ds = xr.open_mfdataset(
                netcdf_files,
                engine=engine,
                chunks={"time": 60, "lat": 90, "lon": 180},  # Zarr-friendly chunks
                parallel=True,
                combine="by_coords",
            )

            print(f"   Dataset shape: {dict(ds.dims)}")
            print(f"   Variables: {list(ds.data_vars.keys())}")
            print(f"   Original chunks: {ds.temperature.chunks}")

            # Rechunk if requested (often beneficial for Zarr)
            if rechunk:
                print("🔄 Rechunking for optimal Zarr performance...")
                # Optimize chunks for Zarr - larger chunks are often better
                ds = ds.chunk({"time": 120, "lat": 180, "lon": 360})
                print(f"   New chunks: {ds.temperature.chunks}")

            # Remove existing zarr store if it exists
            if zarr_output.exists():
                import shutil

                shutil.rmtree(zarr_output)

            # Convert to Zarr
            print("💾 Writing to Zarr...")
            encoding = {
                "temperature": {"compressor": zarr.Blosc(cname="lz4", clevel=5)},
                "precipitation": {"compressor": zarr.Blosc(cname="lz4", clevel=5)},
            }

            ds.to_zarr(zarr_output, mode="w", consolidated=True, encoding=encoding)
            ds.close()

        # Verify the conversion
        print("✅ Verifying Zarr store...")
        zarr_ds = xr.open_zarr(zarr_output, consolidated=True)

        # Calculate store size
        store_size = sum(f.stat().st_size for f in zarr_output.rglob("*") if f.is_file())
        store_size_mb = store_size / (1024**2)

        # Get compression info
        zarr_store = zarr.open(zarr_output)
        compression_info = {}
        for var in ["temperature", "precipitation"]:
            if var in zarr_store:
                compressor = zarr_store[var].compressor
                compression_info[var] = str(compressor) if compressor else "none"

        zarr_ds.close()

        results = {
            "success": True,
            "conversion_time": t["total"],
            "zarr_size_mb": store_size_mb,
            "compression_info": compression_info,
            "chunks": dict(zarr_ds.chunks) if hasattr(zarr_ds, "chunks") else {},
            "consolidated": True,
        }

        print(f"   Conversion time: {t['total']:.2f}s")
        print(f"   Zarr store size: {store_size_mb:.1f} MB")
        print(f"   Compression: {compression_info}")

        return results

    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        return {"success": False, "error": str(e), "conversion_time": float("inf")}


def benchmark_zarr_vs_netcdf(
    netcdf_files: List[Path], zarr_store: Path, client=None
) -> Dict[str, Dict]:
    """Compare performance between NetCDF and Zarr formats.

    Args:
        netcdf_files: NetCDF files to benchmark
        zarr_store: Zarr store to benchmark
        client: Dask client for operations

    Returns:
        Dictionary with comparison results
    """
    print(f"\n⚔️  ZARR VS NETCDF PERFORMANCE COMPARISON")
    print("=" * 50)

    results = {}

    # Test NetCDF performance
    try:
        print("🧪 Testing NetCDF performance...")
        engine = "h5netcdf" if "h5netcdf" in NETCDF_ENGINES else "netcdf4"

        with timer("NetCDF operations") as t:
            # Open dataset
            ds_nc = xr.open_mfdataset(
                netcdf_files,
                engine=engine,
                chunks={"time": 60, "lat": 90, "lon": 180},
                parallel=True,
            )

            # Perform operations
            temp_mean = ds_nc.temperature.mean().compute()
            temp_annual = ds_nc.temperature.resample(time="1Y").mean().compute()
            spatial_std = ds_nc.temperature.std(dim=["lat", "lon"]).compute()

            ds_nc.close()

        results["netcdf"] = {
            "total_time": t["total"],
            "temp_mean": float(temp_mean),
            "success": True,
        }

        print(f"   NetCDF time: {t['total']:.2f}s")

    except Exception as e:
        print(f"   ❌ NetCDF benchmark failed: {e}")
        results["netcdf"] = {"success": False, "error": str(e)}

    # Test Zarr performance
    if ZARR_AVAILABLE and zarr_store.exists():
        try:
            print("🧪 Testing Zarr performance...")

            with timer("Zarr operations") as t:
                # Open dataset
                ds_zarr = xr.open_zarr(zarr_store, consolidated=True)

                # Same operations as NetCDF test
                temp_mean = ds_zarr.temperature.mean().compute()
                temp_annual = ds_zarr.temperature.resample(time="1Y").mean().compute()
                spatial_std = ds_zarr.temperature.std(dim=["lat", "lon"]).compute()

                ds_zarr.close()

            results["zarr"] = {
                "total_time": t["total"],
                "temp_mean": float(temp_mean),
                "success": True,
            }

            print(f"   Zarr time: {t['total']:.2f}s")

            # Calculate speedup
            if results.get("netcdf", {}).get("success", False):
                speedup = results["netcdf"]["total_time"] / results["zarr"]["total_time"]
                print(f"   Zarr speedup: {speedup:.2f}x")
                results["speedup"] = speedup

        except Exception as e:
            print(f"   ❌ Zarr benchmark failed: {e}")
            results["zarr"] = {"success": False, "error": str(e)}

    return results


def demonstrate_filesystem_caching(file_paths: List[Path], client=None) -> Dict[str, float]:
    """Demonstrate the effect of filesystem caching on I/O performance.

    Args:
        file_paths: Files to use for testing
        client: Dask client

    Returns:
        Dictionary with caching results
    """
    print(f"\n🗂️  FILESYSTEM CACHING DEMONSTRATION")
    print("=" * 40)

    results = {}
    test_files = file_paths[:5]  # Use subset for faster testing

    # Clear filesystem cache (Linux/Unix systems)
    def attempt_cache_clear():
        try:
            # Try to clear caches (requires sudo, may not work)
            os.system("sync")  # Sync filesystem
            # Note: 'echo 3 > /proc/sys/vm/drop_caches' requires root
            print("   Attempted cache sync (full cache clear requires root)")
        except Exception:
            print("   Cache clear not available")

    try:
        print("🔄 First read (cold cache)...")
        attempt_cache_clear()

        with timer("Cold cache read") as t1:
            ds = xr.open_mfdataset(
                test_files,
                engine="netcdf4" if "netcdf4" in NETCDF_ENGINES else "h5netcdf",
                chunks={"time": 30, "lat": 60, "lon": 120},
            )
            result1 = ds.temperature.mean().compute()
            ds.close()

        results["cold_cache"] = t1["total"]
        print(f"   Cold cache time: {t1['total']:.2f}s")

        print("🔥 Second read (warm cache)...")

        with timer("Warm cache read") as t2:
            ds = xr.open_mfdataset(
                test_files,
                engine="netcdf4" if "netcdf4" in NETCDF_ENGINES else "h5netcdf",
                chunks={"time": 30, "lat": 60, "lon": 120},
            )
            result2 = ds.temperature.mean().compute()
            ds.close()

        results["warm_cache"] = t2["total"]
        print(f"   Warm cache time: {t2['total']:.2f}s")

        # Calculate cache speedup
        cache_speedup = results["cold_cache"] / results["warm_cache"]
        results["cache_speedup"] = cache_speedup
        print(f"   Cache speedup: {cache_speedup:.2f}x")

    except Exception as e:
        print(f"❌ Caching demonstration failed: {e}")
        results["error"] = str(e)

    return results


def print_io_best_practices() -> None:
    """Print I/O optimization best practices."""
    print("\n" + "=" * 60)
    print("💡 STORAGE FORMAT & I/O BEST PRACTICES")
    print("=" * 60)

    print("""
📊 STORAGE FORMAT SELECTION:

   NetCDF (.nc):
   ✅ Pros: Widely supported, self-describing, CF conventions
   ❌ Cons: Single-threaded writes, limited cloud optimization
   🎯 Best for: Standard climate data, long-term archival
   
   Zarr (.zarr):
   ✅ Pros: Chunked storage, cloud-native, concurrent writes
   ❌ Cons: Newer format, less universal support
   🎯 Best for: Analysis workflows, cloud storage, large datasets

🔧 ENGINE SELECTION:

   netcdf4 engine:
   • Default choice, widely compatible
   • Good for single-threaded operations
   • Limited parallel reading due to GIL
   
   h5netcdf engine:  
   • Pure Python, no GIL limitations
   • Better parallel reading performance
   • Recommended for multi-file datasets
   
   Example:
   ds = xr.open_mfdataset(files, engine='h5netcdf', parallel=True)

📁 PARALLEL I/O OPTIMIZATION:

   Multi-file Reading:
   • Use chunks={} for automatic chunking
   • Set parallel=True for concurrent reads
   • Choose engine wisely (h5netcdf > netcdf4 for parallel)
   
   Chunking Strategy:
   • NetCDF: Align with file structure
   • Zarr: Optimize for access patterns
   • Target 256-512 MB chunks for analysis
   
   Example:
   ds = xr.open_mfdataset(
       files,
       engine='h5netcdf',
       chunks={'time': 'auto', 'lat': 180, 'lon': 360},
       parallel=True,
       combine='by_coords'
   )

💾 ZARR OPTIMIZATION:

   Writing Best Practices:
   • Use consolidated=True for metadata efficiency
   • Choose appropriate compressor (lz4 for speed, zstd for size)
   • Rechunk before writing for optimal layout
   
   Example:
   encoding = {
       'temperature': {'compressor': zarr.Blosc(cname='lz4', clevel=5)},
       'chunks': (120, 180, 360)  # Time, lat, lon
   }
   ds.to_zarr('output.zarr', mode='w', consolidated=True, encoding=encoding)

🚀 PERFORMANCE OPTIMIZATION:

   Filesystem Caching:
   • First access is always slower (cold cache)
   • Repeated access benefits from OS caching
   • Consider this in benchmarks and workflows
   
   Workload Configuration:
   • Use workload_type='io' for I/O-heavy operations
   • Increase thread count for concurrent file access
   • Monitor memory usage with large file counts
   
   Example:
   client, cluster, tmp = setup_dask_client(
       workload_type='io',      # Optimize for I/O
       max_workers=4,           # Balance threads vs processes
       reserve_mem_gb=40        # Leave room for file caches
   )

⚠️  COMMON PITFALLS:

   ❌ Using netcdf4 engine for many parallel files
   ✅ Use h5netcdf for better parallel performance
   
   ❌ Very small chunks (<50 MB) causing overhead
   ✅ Target 256-512 MB chunks for optimal performance
   
   ❌ Not consolidating Zarr metadata
   ✅ Always use consolidated=True when writing Zarr
   
   ❌ Ignoring compression in Zarr stores
   ✅ Use appropriate compressor (lz4/zstd) for your data
   
   ❌ Not considering filesystem caching in benchmarks
   ✅ Account for cold vs warm cache effects
""")


def create_performance_summary_table(
    engine_results: Dict[str, Dict],
    conversion_results: Dict[str, any],
    comparison_results: Dict[str, Dict],
    caching_results: Dict[str, float],
    output_file: Path,
) -> None:
    """Create a comprehensive performance summary table.

    Args:
        engine_results: NetCDF engine benchmark results
        conversion_results: NetCDF to Zarr conversion results
        comparison_results: Format comparison results
        caching_results: Filesystem caching results
        output_file: Output file path
    """
    print("\n" + "=" * 60)
    print("📋 PERFORMANCE SUMMARY TABLE")
    print("=" * 60)

    # Engine comparison table
    if engine_results:
        print("\n🏎️  NetCDF Engine Performance:")
        engine_headers = ["Engine", "Single File (s)", "Multi-file (s)", "Chunked (s)", "Status"]
        engine_data = []

        for engine, results in engine_results.items():
            if results["success"]:
                engine_data.append(
                    [
                        engine,
                        f"{results['single_file_time']:.2f}",
                        f"{results['multi_file_time']:.2f}",
                        f"{results['chunked_time']:.2f}",
                        "✅ Success",
                    ]
                )
            else:
                engine_data.append([engine, "N/A", "N/A", "N/A", f"❌ {results['error'][:30]}"])

        # Print engine table
        print_table(engine_headers, engine_data)

    # Format comparison
    if comparison_results:
        print("\n⚔️  Format Comparison:")
        format_headers = ["Format", "Total Time (s)", "Relative Speed", "Status"]
        format_data = []

        for format_name, results in comparison_results.items():
            if format_name in ["netcdf", "zarr"] and results.get("success"):
                relative_speed = (
                    "1.0x (baseline)"
                    if format_name == "netcdf"
                    else f"{comparison_results.get('speedup', 1.0):.2f}x"
                )
                format_data.append(
                    [
                        format_name.upper(),
                        f"{results['total_time']:.2f}",
                        relative_speed,
                        "✅ Success",
                    ]
                )

        if format_data:
            print_table(format_headers, format_data)

    # Caching effects
    if caching_results:
        print("\n🗂️  Filesystem Caching Effects:")
        cache_headers = ["Cache State", "Time (s)", "Speedup"]
        cache_data = [
            ["Cold Cache", f"{caching_results.get('cold_cache', 0):.2f}", "1.0x (baseline)"],
            [
                "Warm Cache",
                f"{caching_results.get('warm_cache', 0):.2f}",
                f"{caching_results.get('cache_speedup', 1.0):.2f}x",
            ],
        ]
        print_table(cache_headers, cache_data)

    # Save comprehensive table to markdown
    all_data = []
    all_headers = ["Category", "Item", "Value", "Notes"]

    # Add engine results
    for engine, results in engine_results.items():
        if results["success"]:
            all_data.append(
                [
                    "NetCDF Engine",
                    f"{engine} multi-file",
                    f"{results['multi_file_time']:.2f}s",
                    "Parallel reading",
                ]
            )

    # Add format comparison
    for format_name, results in comparison_results.items():
        if format_name in ["netcdf", "zarr"] and results.get("success"):
            all_data.append(
                [
                    "Format",
                    format_name.upper(),
                    f"{results['total_time']:.2f}s",
                    "Complete workflow",
                ]
            )

    # Add conversion info
    if conversion_results.get("success"):
        all_data.append(
            [
                "Conversion",
                "NetCDF → Zarr",
                f"{conversion_results['conversion_time']:.2f}s",
                f"{conversion_results['zarr_size_mb']:.1f}MB output",
            ]
        )

    # Add caching
    if "cache_speedup" in caching_results:
        all_data.append(
            ["Caching", "Speedup", f"{caching_results['cache_speedup']:.2f}x", "Warm vs cold cache"]
        )

    save_markdown_table(
        data=all_data,
        headers=all_headers,
        filename=output_file,
        title="Storage Format Intelligence Performance Summary",
        description=(
            "Comprehensive performance analysis of storage formats, engines, and I/O patterns. "
            "Results show the impact of format choice, engine selection, and filesystem caching "
            "on scientific data processing workflows."
        ),
    )


def print_table(headers: List[str], data: List[List[str]]) -> None:
    """Print a formatted table to console.

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

    # Print data
    for row in data:
        data_row = " | ".join(str(row[i]).ljust(col_widths[i]) for i in range(len(row)))
        print(f"   | {data_row} |")


def main():
    """Main function demonstrating storage format intelligence."""
    parser = argparse.ArgumentParser(
        description="Demonstrate storage format intelligence and I/O optimization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--num-files", type=int, default=20, help="Number of NetCDF files to create (default: 20)"
    )

    parser.add_argument(
        "--skip-creation",
        action="store_true",
        help="Skip NetCDF file creation (use existing files)",
    )

    parser.add_argument(
        "--engines", nargs="+", default=None, help="NetCDF engines to test (default: all available)"
    )

    parser.add_argument(
        "--workers", type=int, default=4, help="Number of Dask workers (default: 4)"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "outputs",
        help="Output directory for files and results",
    )

    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output and debug logging"
    )

    args = parser.parse_args()

    # Setup logging and output directory
    setup_logging(args.verbose)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    print("📊 STORAGE FORMAT INTELLIGENCE & I/O OPTIMIZATION")
    print("=" * 60)
    print(f"Available NetCDF engines: {NETCDF_ENGINES}")
    print(f"Zarr available: {'✅ Yes' if ZARR_AVAILABLE else '❌ No'}")
    print(f"Output directory: {args.output_dir}")

    if not NETCDF_ENGINES:
        print("❌ No NetCDF engines available. Please install netcdf4 or h5netcdf")
        return 1

    try:
        # Create Dask client optimized for I/O
        client, cluster, temp_dir = setup_dask_client(
            workload_type="io",  # Optimize for I/O operations
            max_workers=args.workers,
            reserve_mem_gb=30.0,
            dashboard=True,
        )

        print(f"\n🖥️  I/O-Optimized Cluster: {len(client.scheduler_info()['workers'])} workers")
        print(f"   Temp directory: {temp_dir}")

        # Create or find NetCDF files
        netcdf_dir = args.output_dir / "sample_netcdf"

        if not args.skip_creation or not netcdf_dir.exists():
            file_paths = create_sample_netcdf_files(
                netcdf_dir,
                num_files=args.num_files,
                time_per_file=30,
                spatial_shape=(90, 180),  # Smaller for faster testing
            )
        else:
            file_paths = sorted(list(netcdf_dir.glob("*.nc")))
            print(f"📁 Using existing {len(file_paths)} NetCDF files from {netcdf_dir}")

        if not file_paths:
            print("❌ No NetCDF files available for testing")
            return 1

        # Benchmark NetCDF engines
        engines_to_test = args.engines or NETCDF_ENGINES
        engine_results = benchmark_netcdf_engines(file_paths, engines_to_test, client)

        # Convert to Zarr
        zarr_store = args.output_dir / "converted_data.zarr"
        conversion_results = convert_netcdf_to_zarr(file_paths, zarr_store, client)

        # Compare formats
        comparison_results = benchmark_zarr_vs_netcdf(file_paths, zarr_store, client)

        # Demonstrate caching effects
        caching_results = demonstrate_filesystem_caching(file_paths, client)

        # Create performance summary
        summary_file = args.output_dir / "storage_io_performance.md"
        create_performance_summary_table(
            engine_results, conversion_results, comparison_results, caching_results, summary_file
        )

        # Print best practices
        print_io_best_practices()

        print(f"\n✅ Storage format demonstration completed!")
        print(f"📄 Performance summary: {summary_file}")
        if zarr_store.exists():
            zarr_size = sum(f.stat().st_size for f in zarr_store.rglob("*") if f.is_file()) / (
                1024**2
            )
            print(f"📦 Zarr store created: {zarr_store} ({zarr_size:.1f} MB)")

        # Clean up
        client.close()
        cluster.close()

    except KeyboardInterrupt:
        print("\n\n🛑 Demonstration interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Demonstration failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
