#!/usr/bin/env python
"""Rechunker integration for safe NetCDF to Zarr conversion.

This recipe demonstrates end-to-end integration with rechunker for safe,
memory-efficient conversion of NetCDF datasets to optimally-chunked Zarr stores.
It includes pipeline design, progress monitoring, validation, and cleanup strategies.

Requirements:
- dask_setup, dask, distributed
- xarray, numpy
- zarr, rechunker
- netcdf4 (for NetCDF I/O)
- fsspec (for storage backends)

Outputs:
- Rechunked Zarr stores with optimal layouts
- Validation and integrity reports
- Performance analysis and timing
- Storage optimization recommendations
- Pipeline configuration examples
- Error recovery and cleanup procedures

Key Learning Points:
- Safe rechunking strategies with memory limits
- NetCDF to Zarr conversion best practices
- Temporary storage management on HPC systems
- Progress monitoring and error recovery
- Storage format optimization techniques
- Integration with scientific data pipelines
"""

import argparse
import logging
import os
import sys
import time
import warnings
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass

# Add the parent directory to path so we can import dask_setup and utils
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import dask
    import dask.array as da
    import numpy as np
    import xarray as xr
    import zarr

    from dask_setup import setup_dask_client
    from utils import format_bytes, format_duration, save_markdown_table, timer
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed xarray numpy zarr")
    sys.exit(1)

# Check for rechunker
try:
    import rechunker

    RECHUNKER_AVAILABLE = True
except ImportError:
    RECHUNKER_AVAILABLE = False
    print("❌ rechunker not available - this recipe requires rechunker")
    print("Install with: pip install rechunker")

# Check for optional dependencies
try:
    import fsspec

    FSSPEC_AVAILABLE = True
except ImportError:
    FSSPEC_AVAILABLE = False
    print("⚠️  fsspec not available - some storage backends may be limited")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S",
    )


@dataclass
class RechunkConfig:
    """Configuration for rechunking operations."""

    source_chunks: Dict[str, int]
    target_chunks: Dict[str, int]
    max_mem: str
    temp_store_path: Path
    target_store_path: Path
    consolidate_output: bool = True
    validate_result: bool = True


@dataclass
class RechunkResult:
    """Results from rechunking operation."""

    success: bool
    source_size_gb: float
    target_size_gb: float
    rechunk_time: float
    temp_storage_peak_gb: float
    compression_ratio: float
    chunk_efficiency: float
    validation_passed: bool = False
    error_message: str = ""


class RechunkPipeline:
    """Pipeline for safe NetCDF to Zarr rechunking operations."""

    def __init__(self, temp_dir: Path, client=None):
        self.temp_dir = Path(temp_dir)
        self.client = client
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def analyze_source_dataset(self, source_path: Union[Path, List[Path]]) -> Dict[str, Any]:
        """Analyze source dataset to recommend optimal chunking strategy.

        Args:
            source_path: Path to NetCDF file(s)

        Returns:
            Dictionary with analysis results and recommendations
        """
        print(f"\n🔍 ANALYZING SOURCE DATASET")
        print("=" * 50)

        if isinstance(source_path, list):
            print(f"Multi-file dataset: {len(source_path)} files")
            ds = xr.open_mfdataset(source_path, engine="netcdf4")
        else:
            print(f"Single file: {source_path}")
            ds = xr.open_dataset(source_path, engine="netcdf4")

        analysis = {
            "dimensions": dict(ds.dims),
            "variables": {},
            "total_size_gb": 0.0,
            "recommendations": {},
        }

        print(f"Dataset dimensions: {analysis['dimensions']}")
        print(f"Variables found: {list(ds.data_vars.keys())}")

        total_bytes = 0

        for var_name, var in ds.data_vars.items():
            var_bytes = var.nbytes
            total_bytes += var_bytes

            analysis["variables"][var_name] = {
                "dtype": str(var.dtype),
                "shape": var.shape,
                "dims": var.dims,
                "size_gb": var_bytes / (1024**3),
                "current_chunks": getattr(var, "chunks", "not chunked"),
            }

            print(f"   {var_name}: {var.shape} ({var.dtype}) = {var_bytes / (1024**3):.2f} GB")

        analysis["total_size_gb"] = total_bytes / (1024**3)
        print(f"Total dataset size: {analysis['total_size_gb']:.2f} GB")

        # Generate chunking recommendations
        analysis["recommendations"] = self._recommend_chunks(ds, analysis)

        ds.close()
        return analysis

    def _recommend_chunks(self, ds: xr.Dataset, analysis: Dict) -> Dict[str, Any]:
        """Generate optimal chunking recommendations."""
        dims = analysis["dimensions"]
        total_size = analysis["total_size_gb"]

        # Target chunk size in bytes (256-512 MB is often optimal)
        target_chunk_mb = min(512, max(256, total_size * 1024 / 20))  # ~5% of dataset or 256-512MB
        target_chunk_bytes = target_chunk_mb * 1024**2

        recommendations = {
            "target_chunk_size_mb": target_chunk_mb,
            "chunking_strategy": {},
            "memory_estimate_gb": 0.0,
        }

        # Calculate chunks for each dimension
        chunk_strategy = {}

        # Time dimension: often benefits from moderate chunking
        if "time" in dims:
            time_size = dims["time"]
            # Target 30-365 time steps per chunk depending on data frequency
            if time_size > 3650:  # Daily data for >10 years
                chunk_strategy["time"] = min(365, max(30, time_size // 50))
            elif time_size > 365:  # Daily data for 1-10 years
                chunk_strategy["time"] = min(180, max(30, time_size // 10))
            else:  # Less frequent or shorter data
                chunk_strategy["time"] = min(time_size, max(10, time_size // 5))

        # Spatial dimensions: balance parallelism and chunk size
        spatial_dims = [
            d for d in dims.keys() if d in ["lat", "lon", "latitude", "longitude", "x", "y"]
        ]

        for dim in spatial_dims:
            dim_size = dims[dim]
            # Aim for chunks that create reasonable spatial tiles
            if dim_size > 1440:  # High resolution (0.25 degree or finer)
                chunk_strategy[dim] = min(720, max(180, dim_size // 4))
            elif dim_size > 360:  # Medium resolution (1 degree)
                chunk_strategy[dim] = min(360, max(90, dim_size // 2))
            else:  # Coarse resolution
                chunk_strategy[dim] = dim_size  # Don't chunk small spatial dimensions

        # Level/depth dimensions: usually don't chunk unless very large
        level_dims = [d for d in dims.keys() if d in ["level", "lev", "plev", "depth", "z"]]
        for dim in level_dims:
            dim_size = dims[dim]
            if dim_size > 100:
                chunk_strategy[dim] = min(50, max(10, dim_size // 4))
            else:
                chunk_strategy[dim] = dim_size

        recommendations["chunking_strategy"] = chunk_strategy

        # Estimate memory requirements
        # Roughly 3x the chunk size for rechunker operations
        estimated_chunk_size = target_chunk_bytes
        memory_factor = 3.0  # Safety factor for intermediate arrays
        recommendations["memory_estimate_gb"] = (estimated_chunk_size * memory_factor) / (1024**3)

        print(f"\n💡 CHUNKING RECOMMENDATIONS:")
        print(f"   Target chunk size: {target_chunk_mb} MB")
        print(f"   Recommended chunks: {chunk_strategy}")
        print(f"   Estimated memory need: {recommendations['memory_estimate_gb']:.1f} GB")

        return recommendations

    def create_rechunk_plan(
        self,
        source_path: Union[Path, List[Path]],
        target_store: Path,
        target_chunks: Dict[str, int],
        max_mem: str,
        temp_store_suffix: str = "temp_rechunk",
    ) -> rechunker.api.Rechunked:
        """Create rechunker plan for safe conversion.

        Args:
            source_path: Source NetCDF file(s)
            target_store: Target Zarr store path
            target_chunks: Target chunk sizes by dimension
            max_mem: Maximum memory per worker (e.g., "4GB")
            temp_store_suffix: Suffix for temporary storage

        Returns:
            Rechunker plan object
        """
        if not RECHUNKER_AVAILABLE:
            raise ImportError("rechunker is required for this operation")

        print(f"\n📋 CREATING RECHUNK PLAN")
        print("=" * 50)

        # Load source dataset
        if isinstance(source_path, list):
            ds = xr.open_mfdataset(source_path, chunks={}, engine="netcdf4")
        else:
            ds = xr.open_dataset(source_path, chunks={}, engine="netcdf4")

        print(f"Source chunks: {dict(ds.chunks) if hasattr(ds, 'chunks') else 'not chunked'}")
        print(f"Target chunks: {target_chunks}")
        print(f"Memory limit: {max_mem}")

        # Create temp store path
        temp_store = self.temp_dir / f"{temp_store_suffix}.zarr"

        # Remove existing stores
        if target_store.exists():
            print(f"Removing existing target store: {target_store}")
            shutil.rmtree(target_store)

        if temp_store.exists():
            print(f"Removing existing temp store: {temp_store}")
            shutil.rmtree(temp_store)

        print(f"Temp store: {temp_store}")
        print(f"Target store: {target_store}")

        try:
            # Create rechunker plan
            rechunk_plan = rechunker.rechunk(
                ds.to_array().data,  # Convert to single dask array
                target_chunks=target_chunks,
                max_mem=max_mem,
                target_store=str(target_store),
                temp_store=str(temp_store),
                target_options={"overwrite": True},
                temp_options={"overwrite": True},
            )

            print(f"✅ Rechunk plan created successfully")
            print(f"   Intermediate tasks: {len(rechunk_plan.plan)}")

            return rechunk_plan, ds, temp_store

        except Exception as e:
            ds.close()
            raise RuntimeError(f"Failed to create rechunk plan: {e}")

    def execute_rechunk_plan(
        self,
        rechunk_plan: rechunker.api.Rechunked,
        source_ds: xr.Dataset,
        target_store: Path,
        temp_store: Path,
        progress_callback: callable = None,
    ) -> RechunkResult:
        """Execute rechunking plan with monitoring and error handling.

        Args:
            rechunk_plan: Rechunker plan to execute
            source_ds: Source dataset
            target_store: Target Zarr store path
            temp_store: Temporary store path
            progress_callback: Optional progress callback function

        Returns:
            RechunkResult with execution details
        """
        print(f"\n🚀 EXECUTING RECHUNK PLAN")
        print("=" * 50)

        start_time = time.time()
        source_size = sum(var.nbytes for var in source_ds.data_vars.values()) / (1024**3)

        try:
            # Execute the rechunking plan
            with timer("Rechunking execution") as t:
                if progress_callback:
                    # Execute with progress monitoring
                    rechunk_plan.execute(retries=2)
                else:
                    # Simple execution
                    rechunk_plan.execute()

            execution_time = t["total"]

            # Close source dataset
            source_ds.close()

            # Measure results
            target_size = self._calculate_zarr_size(target_store) / (1024**3)
            temp_peak_size = (
                self._calculate_zarr_size(temp_store) / (1024**3) if temp_store.exists() else 0.0
            )

            # Calculate metrics
            compression_ratio = source_size / max(target_size, 0.001)  # Avoid division by zero

            # Estimate chunk efficiency (simplified)
            chunk_efficiency = min(1.0, source_size / max(target_size * 1.5, source_size))

            result = RechunkResult(
                success=True,
                source_size_gb=source_size,
                target_size_gb=target_size,
                rechunk_time=execution_time,
                temp_storage_peak_gb=temp_peak_size,
                compression_ratio=compression_ratio,
                chunk_efficiency=chunk_efficiency,
            )

            print(f"✅ Rechunking completed successfully")
            print(f"   Execution time: {execution_time:.1f}s")
            print(f"   Source size: {source_size:.2f} GB")
            print(f"   Target size: {target_size:.2f} GB")
            print(f"   Compression ratio: {compression_ratio:.2f}x")
            print(f"   Temp storage peak: {temp_peak_size:.2f} GB")

            return result

        except Exception as e:
            # Clean up on error
            if source_ds:
                source_ds.close()

            print(f"❌ Rechunking failed: {e}")

            return RechunkResult(
                success=False,
                source_size_gb=source_size,
                target_size_gb=0.0,
                rechunk_time=time.time() - start_time,
                temp_storage_peak_gb=0.0,
                compression_ratio=1.0,
                chunk_efficiency=0.0,
                error_message=str(e),
            )

    def _calculate_zarr_size(self, zarr_path: Path) -> int:
        """Calculate total size of Zarr store in bytes."""
        if not zarr_path.exists():
            return 0

        total_size = 0
        for file_path in zarr_path.rglob("*"):
            if file_path.is_file():
                total_size += file_path.stat().st_size

        return total_size

    def validate_conversion(
        self,
        source_path: Union[Path, List[Path]],
        target_store: Path,
        sample_fraction: float = 0.01,
    ) -> Dict[str, Any]:
        """Validate that conversion was successful and data integrity is maintained.

        Args:
            source_path: Original NetCDF file(s)
            target_store: Converted Zarr store
            sample_fraction: Fraction of data to sample for validation

        Returns:
            Validation results dictionary
        """
        print(f"\n✅ VALIDATING CONVERSION")
        print("=" * 50)

        validation = {
            "structure_match": False,
            "data_integrity": False,
            "chunk_structure": {},
            "compression_info": {},
            "sample_comparison": {},
            "errors": [],
        }

        try:
            # Load both datasets
            if isinstance(source_path, list):
                source_ds = xr.open_mfdataset(source_path, engine="netcdf4")
            else:
                source_ds = xr.open_dataset(source_path, engine="netcdf4")

            target_ds = xr.open_zarr(target_store, consolidated=True)

            # Check structure
            print("Checking dataset structure...")

            # Compare dimensions
            if dict(source_ds.dims) == dict(target_ds.dims):
                validation["structure_match"] = True
                print("   ✅ Dimensions match")
            else:
                validation["errors"].append("Dimension mismatch")
                print(f"   ❌ Dimension mismatch: {dict(source_ds.dims)} vs {dict(target_ds.dims)}")

            # Compare variables
            source_vars = set(source_ds.data_vars.keys())
            target_vars = set(target_ds.data_vars.keys())

            if source_vars == target_vars:
                print("   ✅ Variables match")
            else:
                missing = source_vars - target_vars
                extra = target_vars - source_vars
                validation["errors"].append(f"Variable mismatch: missing {missing}, extra {extra}")
                print(f"   ❌ Variables mismatch: missing {missing}, extra {extra}")

            # Check chunking structure
            print("Analyzing chunk structure...")
            for var_name in target_ds.data_vars:
                if hasattr(target_ds[var_name], "chunks"):
                    chunks = target_ds[var_name].chunks
                    validation["chunk_structure"][var_name] = {
                        "chunks": [len(c) for c in chunks],
                        "chunk_sizes": chunks,
                    }
                    print(f"   {var_name}: {chunks}")

            # Get compression info
            zarr_store = zarr.open(target_store)
            for var_name in target_ds.data_vars:
                if var_name in zarr_store:
                    compressor = zarr_store[var_name].compressor
                    validation["compression_info"][var_name] = (
                        str(compressor) if compressor else "none"
                    )

            print(f"Compression: {validation['compression_info']}")

            # Sample data comparison
            print(f"Comparing data samples ({sample_fraction:.1%} of dataset)...")

            sample_passed = True
            for var_name in source_vars.intersection(target_vars):
                try:
                    # Select random sample
                    var_shape = source_ds[var_name].shape
                    sample_indices = {}

                    for dim_name, dim_size in zip(source_ds[var_name].dims, var_shape):
                        sample_size = max(1, int(dim_size * sample_fraction))
                        if sample_size < dim_size:
                            # Random sample
                            indices = np.sort(
                                np.random.choice(dim_size, sample_size, replace=False)
                            )
                            sample_indices[dim_name] = indices
                        else:
                            # Use all data
                            sample_indices[dim_name] = slice(None)

                    # Extract samples
                    source_sample = source_ds[var_name].isel(**sample_indices).compute()
                    target_sample = target_ds[var_name].isel(**sample_indices).compute()

                    # Compare
                    if np.allclose(source_sample, target_sample, equal_nan=True):
                        validation["sample_comparison"][var_name] = "passed"
                        print(f"   ✅ {var_name}: Data matches")
                    else:
                        validation["sample_comparison"][var_name] = "failed"
                        sample_passed = False
                        print(f"   ❌ {var_name}: Data mismatch detected")

                        # Additional diagnostics
                        diff = np.abs(source_sample - target_sample)
                        max_diff = np.nanmax(diff)
                        mean_diff = np.nanmean(diff)
                        print(f"      Max difference: {max_diff}")
                        print(f"      Mean difference: {mean_diff}")

                except Exception as e:
                    validation["sample_comparison"][var_name] = f"error: {e}"
                    sample_passed = False
                    print(f"   ❌ {var_name}: Comparison error: {e}")

            validation["data_integrity"] = sample_passed

            # Clean up
            source_ds.close()
            target_ds.close()

            print(f"✅ Validation completed")
            print(f"   Structure valid: {validation['structure_match']}")
            print(f"   Data integrity: {validation['data_integrity']}")

        except Exception as e:
            validation["errors"].append(f"Validation error: {e}")
            print(f"❌ Validation failed: {e}")

        return validation

    def cleanup_temp_storage(self, temp_store: Path) -> bool:
        """Clean up temporary storage after successful conversion.

        Args:
            temp_store: Path to temporary storage

        Returns:
            True if cleanup successful
        """
        try:
            if temp_store.exists():
                print(f"🧹 Cleaning up temporary storage: {temp_store}")

                # Calculate size before removal
                temp_size = self._calculate_zarr_size(temp_store) / (1024**3)

                shutil.rmtree(temp_store)
                print(f"   Removed {temp_size:.2f} GB of temporary data")

                return True
            else:
                print(f"No temporary storage to clean: {temp_store}")
                return True

        except Exception as e:
            print(f"❌ Failed to clean up temporary storage: {e}")
            return False


def create_sample_netcdf_dataset(
    output_path: Path, time_steps: int = 1000, spatial_shape: Tuple[int, int] = (180, 360)
) -> Path:
    """Create a sample NetCDF dataset for testing.

    Args:
        output_path: Path for output file
        time_steps: Number of time steps
        spatial_shape: Spatial dimensions (lat, lon)

    Returns:
        Path to created file
    """
    print(f"\n📁 Creating sample NetCDF dataset")
    print(f"   Output: {output_path}")
    print(f"   Time steps: {time_steps}")
    print(f"   Spatial shape: {spatial_shape}")

    lat_size, lon_size = spatial_shape

    # Create coordinate arrays
    time = np.arange(time_steps)
    lat = np.linspace(-90, 90, lat_size)
    lon = np.linspace(-180, 180, lon_size)

    # Create realistic data patterns
    np.random.seed(42)  # Reproducible

    # Temperature with spatial and temporal patterns
    lat_mesh, lon_mesh, time_mesh = np.meshgrid(lat, lon, time, indexing="ij")

    # Base temperature with latitude gradient
    temp_base = 15 - 30 * np.abs(lat_mesh / 90)

    # Seasonal cycle
    seasonal = 10 * np.cos(2 * np.pi * time_mesh / 365.25)

    # Random variability
    noise = 5 * np.random.random((lat_size, lon_size, time_steps))

    temperature = temp_base + seasonal + noise + 273.15  # Convert to Kelvin

    # Precipitation with different patterns
    precip_base = 5 * (1 - np.abs(lat_mesh / 60))  # More rain near equator
    precip_seasonal = 3 * np.cos(2 * np.pi * time_mesh / 365.25 + np.pi)
    precip_noise = np.maximum(0, np.random.gamma(2, 2, (lat_size, lon_size, time_steps)))

    precipitation = precip_base + precip_seasonal + precip_noise

    # Create xarray dataset
    ds = xr.Dataset(
        {
            "temperature": (
                ["lat", "lon", "time"],
                temperature,
                {
                    "units": "K",
                    "long_name": "Near-surface air temperature",
                    "standard_name": "air_temperature",
                },
            ),
            "precipitation": (
                ["lat", "lon", "time"],
                precipitation,
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
            "title": "Sample dataset for rechunker integration testing",
            "institution": "dask_setup recipe demonstration",
            "source": "Synthetic data for rechunking examples",
            "conventions": "CF-1.8",
            "history": f"Created {datetime.now().isoformat()}",
        }
    )

    # Save with compression
    encoding = {
        "temperature": {"zlib": True, "complevel": 4, "shuffle": True},
        "precipitation": {"zlib": True, "complevel": 4, "shuffle": True},
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(output_path, encoding=encoding)

    file_size = output_path.stat().st_size / (1024**3)
    print(f"✅ Created {file_size:.2f} GB NetCDF file")

    return output_path


def demonstrate_rechunking_workflow(
    source_file: Path, temp_dir: Path, output_dir: Path, client, max_mem: str = "4GB"
) -> Dict[str, Any]:
    """Demonstrate complete rechunking workflow.

    Args:
        source_file: Source NetCDF file
        temp_dir: Temporary storage directory
        output_dir: Output directory
        client: Dask client
        max_mem: Maximum memory per worker

    Returns:
        Dictionary with workflow results
    """
    print(f"\n🔄 COMPLETE RECHUNKING WORKFLOW DEMONSTRATION")
    print("=" * 60)

    # Initialize pipeline
    pipeline = RechunkPipeline(temp_dir, client)

    results = {
        "analysis": {},
        "rechunk_result": None,
        "validation": {},
        "cleanup_success": False,
        "total_time": 0.0,
    }

    workflow_start = time.time()

    try:
        # Step 1: Analyze source dataset
        analysis = pipeline.analyze_source_dataset(source_file)
        results["analysis"] = analysis

        # Step 2: Create rechunk plan
        target_chunks = analysis["recommendations"]["chunking_strategy"]
        target_store = output_dir / "rechunked_data.zarr"

        # Ensure we have valid chunk sizes for all dimensions
        if not target_chunks:
            # Fallback chunking strategy
            target_chunks = {"time": 100, "lat": 90, "lon": 180}
            print(f"Using fallback chunking: {target_chunks}")

        print(f"Creating rechunk plan with chunks: {target_chunks}")

        rechunk_plan, source_ds, temp_store = pipeline.create_rechunk_plan(
            source_file, target_store, target_chunks, max_mem
        )

        # Step 3: Execute rechunking
        rechunk_result = pipeline.execute_rechunk_plan(
            rechunk_plan, source_ds, target_store, temp_store
        )
        results["rechunk_result"] = rechunk_result

        # Step 4: Validate conversion
        if rechunk_result.success:
            validation = pipeline.validate_conversion(source_file, target_store)
            results["validation"] = validation
            rechunk_result.validation_passed = validation.get("data_integrity", False)

        # Step 5: Cleanup temporary storage
        cleanup_success = pipeline.cleanup_temp_storage(temp_store)
        results["cleanup_success"] = cleanup_success

        results["total_time"] = time.time() - workflow_start

        print(f"\n✅ WORKFLOW COMPLETED")
        print(f"   Total time: {results['total_time']:.1f}s")
        print(f"   Rechunking successful: {rechunk_result.success}")
        print(f"   Validation passed: {rechunk_result.validation_passed}")
        print(f"   Cleanup successful: {cleanup_success}")

    except Exception as e:
        print(f"❌ Workflow failed: {e}")
        results["error"] = str(e)
        results["total_time"] = time.time() - workflow_start

    return results


def create_integration_report(workflow_results: List[Dict[str, Any]], output_file: Path) -> None:
    """Create comprehensive integration report.

    Args:
        workflow_results: List of workflow results
        output_file: Output file path
    """
    print(f"\n📋 CREATING INTEGRATION REPORT")
    print("=" * 60)

    # Aggregate statistics
    successful_workflows = [
        r for r in workflow_results if r.get("rechunk_result", {}).get("success", False)
    ]
    total_workflows = len(workflow_results)

    # Calculate summary stats
    total_data_processed = sum(
        r.get("rechunk_result", {}).get("source_size_gb", 0) for r in successful_workflows
    )

    avg_compression = (
        np.mean(
            [
                r.get("rechunk_result", {}).get("compression_ratio", 1.0)
                for r in successful_workflows
            ]
        )
        if successful_workflows
        else 1.0
    )

    avg_time = (
        np.mean([r.get("rechunk_result", {}).get("rechunk_time", 0) for r in successful_workflows])
        if successful_workflows
        else 0.0
    )

    with open(output_file, "w") as f:
        f.write("# Rechunker Integration Analysis Report\n\n")
        f.write(f"**Report generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("## Executive Summary\n\n")
        f.write("This report summarizes the integration of rechunker with dask_setup for ")
        f.write("safe, efficient NetCDF to Zarr conversion workflows.\n\n")

        # Success metrics
        f.write("### Workflow Success Metrics\n\n")
        f.write(f"- **Total workflows executed:** {total_workflows}\n")
        f.write(
            f"- **Successful conversions:** {len(successful_workflows)} ({len(successful_workflows) / total_workflows * 100:.1f}%)\n"
        )
        f.write(f"- **Data processed:** {total_data_processed:.2f} GB\n")
        f.write(f"- **Average compression ratio:** {avg_compression:.2f}x\n")
        f.write(f"- **Average processing time:** {avg_time:.1f} seconds\n\n")

        # Detailed results
        f.write("## Detailed Workflow Results\n\n")

        for i, result in enumerate(workflow_results, 1):
            f.write(f"### Workflow {i}\n\n")

            # Analysis results
            if "analysis" in result:
                analysis = result["analysis"]
                f.write(f"**Source Analysis:**\n")
                f.write(f"- Dataset size: {analysis.get('total_size_gb', 0):.2f} GB\n")
                f.write(f"- Dimensions: {analysis.get('dimensions', {})}\n")
                f.write(f"- Variables: {len(analysis.get('variables', {}))}\n")

                if "recommendations" in analysis:
                    recs = analysis["recommendations"]
                    f.write(f"- Recommended chunk size: {recs.get('target_chunk_size_mb', 0)} MB\n")
                    f.write(f"- Chunking strategy: {recs.get('chunking_strategy', {})}\n")

            # Rechunking results
            if "rechunk_result" in result and result["rechunk_result"]:
                rr = result["rechunk_result"]
                f.write(f"\n**Rechunking Results:**\n")
                f.write(f"- Success: {'✅' if rr.success else '❌'}\n")
                f.write(f"- Processing time: {rr.rechunk_time:.1f}s\n")
                f.write(f"- Source size: {rr.source_size_gb:.2f} GB\n")
                f.write(f"- Target size: {rr.target_size_gb:.2f} GB\n")
                f.write(f"- Compression ratio: {rr.compression_ratio:.2f}x\n")
                f.write(f"- Temp storage peak: {rr.temp_storage_peak_gb:.2f} GB\n")

                if not rr.success:
                    f.write(f"- Error: {rr.error_message}\n")

            # Validation results
            if "validation" in result and result["validation"]:
                val = result["validation"]
                f.write(f"\n**Validation Results:**\n")
                f.write(f"- Structure match: {'✅' if val.get('structure_match') else '❌'}\n")
                f.write(f"- Data integrity: {'✅' if val.get('data_integrity') else '❌'}\n")

                if val.get("errors"):
                    f.write(f"- Validation errors: {len(val['errors'])}\n")

            f.write(f"\n")

        # Best practices section
        f.write("## Best Practices for Rechunker Integration\n\n")

        f.write("### Memory Management\n\n")
        f.write("**Memory Sizing Guidelines:**\n")
        f.write("- Set `max_mem` to 60-80% of available worker memory\n")
        f.write("- Reserve memory for Dask overhead and spilling\n")
        f.write("- Monitor temp storage usage during execution\n")
        f.write("- Use `$PBS_JOBFS` or fast local storage for temp data\n\n")

        f.write("**Example Configuration:**\n")
        f.write("```python\n")
        f.write("# For 16 GB worker memory\n")
        f.write("client, cluster, temp_dir = setup_dask_client(\n")
        f.write("    workload_type='cpu',\n")
        f.write("    max_workers=4,\n")
        f.write("    reserve_mem_gb=40  # Leave room for rechunker\n")
        f.write(")\n")
        f.write("max_mem = '6GB'  # ~60% of available per worker\n")
        f.write("```\n\n")

        f.write("### Chunking Strategy\n\n")
        f.write("**Optimal Chunk Sizes:**\n")
        f.write("- Target 256-512 MB chunks for balanced performance\n")
        f.write("- Time dimension: 30-365 steps depending on frequency\n")
        f.write("- Spatial dimensions: Balance parallelism and locality\n")
        f.write("- Avoid chunking small dimensions (<100 elements)\n\n")

        f.write("**Dimension-Specific Guidelines:**\n")
        f.write("- Time: Match analysis patterns (monthly, seasonal, annual)\n")
        f.write("- Latitude/Longitude: Create reasonable spatial tiles\n")
        f.write("- Vertical levels: Usually don't chunk unless very large\n\n")

        f.write("### Storage Optimization\n\n")
        f.write("**Zarr Configuration:**\n")
        f.write("- Use consolidated metadata (`consolidated=True`)\n")
        f.write("- Choose appropriate compressor (lz4 for speed, zstd for size)\n")
        f.write("- Set proper chunk sizes in encoding\n")
        f.write("- Validate conversion before cleanup\n\n")

        f.write("**Temporary Storage:**\n")
        f.write("- Use fastest available storage for temp data\n")
        f.write("- Ensure sufficient space (2-3x source size)\n")
        f.write("- Clean up temp data after successful conversion\n")
        f.write("- Monitor disk usage during execution\n\n")

        f.write("### Error Handling and Recovery\n\n")
        f.write("**Common Issues and Solutions:**\n")
        f.write("- Memory errors: Reduce `max_mem` or chunk size\n")
        f.write("- Disk space errors: Check temp storage availability\n")
        f.write("- Validation failures: Check data types and missing values\n")
        f.write("- Network timeouts: Use local temp storage\n\n")

        f.write("**Recovery Strategies:**\n")
        f.write("- Save intermediate results for restart capability\n")
        f.write("- Use retries for transient failures\n")
        f.write("- Implement checkpointing for large datasets\n")
        f.write("- Monitor progress and resource usage\n\n")

        f.write("### Performance Optimization\n\n")
        f.write("**Scaling Considerations:**\n")
        f.write("- More workers can help with I/O parallelism\n")
        f.write("- Balance worker count with memory per worker\n")
        f.write("- Consider network bandwidth for distributed storage\n")
        f.write("- Profile memory usage patterns\n\n")

        f.write("**Workflow Integration:**\n")
        f.write("- Batch similar conversions together\n")
        f.write("- Use pipeline orchestration for complex workflows\n")
        f.write("- Implement progress monitoring and alerts\n")
        f.write("- Plan for data validation and quality checks\n\n")

        f.write("---\n")
        f.write("*Generated by dask_setup rechunker integration analysis*\n")

    print(f"✅ Integration report saved: {output_file}")


def main():
    """Main function for rechunker integration demonstration."""
    parser = argparse.ArgumentParser(
        description="Rechunker integration for NetCDF to Zarr conversion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--source-file", type=Path, help="Source NetCDF file (will create sample if not provided)"
    )

    parser.add_argument(
        "--create-sample", action="store_true", help="Create sample NetCDF file for testing"
    )

    parser.add_argument(
        "--sample-size",
        choices=["small", "medium", "large"],
        default="medium",
        help="Sample dataset size (default: medium)",
    )

    parser.add_argument(
        "--max-mem", default="4GB", help="Maximum memory per worker for rechunking (default: 4GB)"
    )

    parser.add_argument(
        "--workers", type=int, default=4, help="Number of Dask workers (default: 4)"
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent.parent / "outputs",
        help="Output directory for results",
    )

    parser.add_argument(
        "--temp-dir", type=Path, help="Temporary storage directory (default: output_dir/temp)"
    )

    parser.add_argument("--skip-validation", action="store_true", help="Skip data validation step")

    parser.add_argument(
        "--keep-temp", action="store_true", help="Keep temporary storage (don't cleanup)"
    )

    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose output and debug logging"
    )

    args = parser.parse_args()

    if not RECHUNKER_AVAILABLE:
        print("❌ This recipe requires rechunker. Install with: pip install rechunker")
        return 1

    # Setup logging and directories
    setup_logging(args.verbose)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.temp_dir is None:
        args.temp_dir = args.output_dir / "temp"
    args.temp_dir.mkdir(parents=True, exist_ok=True)

    print("🔄 RECHUNKER INTEGRATION DEMONSTRATION")
    print("=" * 60)
    print(f"Max memory per worker: {args.max_mem}")
    print(f"Workers: {args.workers}")
    print(f"Output directory: {args.output_dir}")
    print(f"Temp directory: {args.temp_dir}")

    try:
        # Create Dask client optimized for rechunking
        client, cluster, dask_temp = setup_dask_client(
            workload_type="cpu",  # CPU-intensive rechunking operations
            max_workers=args.workers,
            reserve_mem_gb=30.0,  # Leave room for rechunker memory
            dashboard=True,
        )

        print(f"\n🖥️  Rechunking Cluster: {len(client.scheduler_info()['workers'])} workers")
        print(f"   Dask temp directory: {dask_temp}")

        # Determine or create source file
        if args.source_file and args.source_file.exists():
            source_file = args.source_file
            print(f"📂 Using provided source file: {source_file}")
        else:
            print(f"📁 Creating sample dataset ({args.sample_size})...")

            # Set sample parameters based on size
            size_configs = {
                "small": (200, (90, 180)),  # ~0.1 GB
                "medium": (1000, (180, 360)),  # ~2 GB
                "large": (2000, (360, 720)),  # ~16 GB
            }

            time_steps, spatial_shape = size_configs[args.sample_size]

            source_file = args.output_dir / f"sample_data_{args.sample_size}.nc"

            if not source_file.exists() or args.create_sample:
                source_file = create_sample_netcdf_dataset(source_file, time_steps, spatial_shape)
            else:
                print(f"📂 Using existing sample file: {source_file}")

        # Run rechunking workflow
        workflow_results = []

        workflow_result = demonstrate_rechunking_workflow(
            source_file=source_file,
            temp_dir=args.temp_dir,
            output_dir=args.output_dir,
            client=client,
            max_mem=args.max_mem,
        )

        workflow_results.append(workflow_result)

        # Create integration report
        report_file = args.output_dir / "rechunker_integration_report.md"
        create_integration_report(workflow_results, report_file)

        print("\n" + "=" * 60)
        print("✅ RECHUNKER INTEGRATION COMPLETED!")
        print("=" * 60)
        print(f"📄 Integration report: {report_file}")

        # Summary statistics
        if workflow_result.get("rechunk_result"):
            rr = workflow_result["rechunk_result"]
            if rr.success:
                print(f"📊 Conversion successful:")
                print(f"   Source size: {rr.source_size_gb:.2f} GB")
                print(f"   Target size: {rr.target_size_gb:.2f} GB")
                print(f"   Compression: {rr.compression_ratio:.2f}x")
                print(f"   Processing time: {rr.rechunk_time:.1f}s")
                print(f"   Validation: {'✅ Passed' if rr.validation_passed else '❌ Failed'}")
            else:
                print(f"❌ Conversion failed: {rr.error_message}")

        # Output file locations
        target_zarr = args.output_dir / "rechunked_data.zarr"
        if target_zarr.exists():
            zarr_size = sum(f.stat().st_size for f in target_zarr.rglob("*") if f.is_file()) / (
                1024**3
            )
            print(f"📦 Zarr output: {target_zarr} ({zarr_size:.2f} GB)")

        # Clean up
        client.close()
        cluster.close()

        print(f"🧹 Cleanup: {'Temp files preserved' if args.keep_temp else 'Temp files removed'}")

    except KeyboardInterrupt:
        print("\n\n🛑 Integration demonstration interrupted by user")
        return 1
    except Exception as e:
        print(f"\n❌ Integration demonstration failed: {e}")
        if args.verbose:
            import traceback

            traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
