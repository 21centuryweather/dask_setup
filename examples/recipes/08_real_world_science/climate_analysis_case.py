#!/usr/bin/env python
"""Real-world climate analysis case study.

This recipe demonstrates a complete end-to-end scientific workflow using dask_setup
to analyze multi-year climate data. It includes data loading, quality control,
temporal and spatial analysis, regridding, visualization, and output generation
with proper checkpointing and progress monitoring.

Requirements:
- dask_setup, dask, distributed
- xarray, numpy, pandas
- matplotlib, cartopy (optional for advanced plots)
- netcdf4, zarr (for I/O)
- scipy (for regridding)

Outputs:
- Quality-controlled climate dataset
- Annual and seasonal climatologies
- Temperature trend analysis
- Regridded output on standard grid
- Publication-ready figures
- Zarr archives of processed data
- Analysis progress checkpoints

Key Learning Points:
- End-to-end scientific data processing workflow
- Temporal aggregation and analysis patterns
- Spatial regridding and interpolation
- Memory-efficient large dataset handling
- Progress monitoring and checkpointing
- Reproducible analysis documentation
"""

import argparse
import logging
import os
import sys
import time
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta

# Add the parent directory to path so we can import dask_setup and utils
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import dask
    import dask.array as da
    import numpy as np
    import pandas as pd
    import xarray as xr
    
    from dask_setup import setup_dask_client
    from utils import format_bytes, format_duration, save_markdown_table, timer
except ImportError as e:
    print(f"❌ Missing required dependency: {e}")
    print("Please install: pip install dask-setup dask distributed xarray numpy pandas")
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
    import cartopy
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    CARTOPY_AVAILABLE = True
except ImportError:
    CARTOPY_AVAILABLE = False
    print("⚠️  cartopy not available - advanced mapping disabled")

try:
    import scipy
    from scipy import interpolate
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    print("⚠️  scipy not available - regridding disabled")

try:
    import zarr
    ZARR_AVAILABLE = True
except ImportError:
    ZARR_AVAILABLE = False
    print("⚠️  zarr not available - archival limited")


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the recipe."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )


class AnalysisCheckpointer:
    """Handles analysis checkpointing and progress tracking."""
    
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = self.checkpoint_dir / "analysis_progress.json"
        self.load_progress()
    
    def load_progress(self):
        """Load existing progress from file."""
        if self.progress_file.exists():
            import json
            with open(self.progress_file, 'r') as f:
                self.progress = json.load(f)
        else:
            self.progress = {
                'steps_completed': [],
                'last_updated': None,
                'data_info': {}
            }
    
    def save_progress(self):
        """Save current progress to file."""
        import json
        self.progress['last_updated'] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def mark_step_complete(self, step_name: str, info: Dict = None):
        """Mark a step as complete with optional metadata."""
        if step_name not in self.progress['steps_completed']:
            self.progress['steps_completed'].append(step_name)
        
        if info:
            self.progress['data_info'][step_name] = info
        
        self.save_progress()
        print(f"✅ Checkpoint: {step_name} completed")
    
    def is_step_complete(self, step_name: str) -> bool:
        """Check if a step has been completed."""
        return step_name in self.progress['steps_completed']
    
    def get_checkpoint_path(self, filename: str) -> Path:
        """Get path for a checkpoint file."""
        return self.checkpoint_dir / filename


def create_synthetic_climate_data(
    output_dir: Path,
    start_year: int = 2000,
    end_year: int = 2020,
    spatial_resolution: float = 1.0
) -> List[Path]:
    """Create synthetic climate data files for the case study.
    
    Args:
        output_dir: Directory to create files
        start_year: Start year for data
        end_year: End year for data (exclusive)
        spatial_resolution: Grid resolution in degrees
        
    Returns:
        List of created file paths
    """
    print(f"\n🌍 Creating synthetic climate dataset ({start_year}-{end_year-1})")
    print("=" * 60)
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Define grid
    lat_points = int(180 / spatial_resolution) + 1
    lon_points = int(360 / spatial_resolution) + 1
    
    lats = np.linspace(-90, 90, lat_points)
    lons = np.linspace(-180, 180, lon_points)
    
    print(f"Grid resolution: {spatial_resolution}° ({lat_points}×{lon_points} grid)")
    
    file_paths = []
    
    with timer(f"Creating {end_year - start_year} years of data") as t:
        for year in range(start_year, end_year):
            print(f"   Creating data for {year}...")
            
            # Create time coordinate for the year
            start_date = pd.Timestamp(f"{year}-01-01")
            end_date = pd.Timestamp(f"{year+1}-01-01")
            times = pd.date_range(start_date, end_date, freq='D', inclusive='left')
            
            # Create realistic climate patterns
            np.random.seed(42 + year)  # Consistent but varying seed
            
            # Base temperature field with latitude dependence
            lat_mesh, lon_mesh = np.meshgrid(lats, lons, indexing='ij')
            
            # Seasonal temperature cycle
            day_of_year = np.arange(1, len(times) + 1)
            seasonal_cycle = 10 * np.cos(2 * np.pi * day_of_year / 365.25 - np.pi/2)
            
            # Latitude-dependent base temperature
            base_temp = 30 - 60 * np.abs(lat_mesh / 90)  # Warmer at equator
            
            # Add topographic variation (simple mountain/ocean pattern)
            topo_var = 5 * np.sin(3 * np.pi * lat_mesh / 180) * np.cos(4 * np.pi * lon_mesh / 180)
            
            # Combine temperature components
            temp_data = np.zeros((len(times), lat_points, lon_points))
            precip_data = np.zeros((len(times), lat_points, lon_points))
            
            for i, (day, seasonal_temp) in enumerate(zip(day_of_year, seasonal_cycle)):
                # Temperature with trends and variability
                trend = 0.02 * (year - start_year)  # 0.02°C/year warming trend
                daily_noise = np.random.normal(0, 2, (lat_points, lon_points))
                
                temp_data[i] = (base_temp + seasonal_temp + topo_var + 
                              trend + daily_noise + 273.15)  # Convert to Kelvin
                
                # Precipitation with seasonal and spatial patterns
                # More rain in tropics and during summer
                seasonal_precip = np.maximum(0, 
                    5 * (1 - np.abs(lat_mesh / 30)) * (1 + np.cos(2 * np.pi * day / 365.25)))
                daily_precip_noise = np.maximum(0, np.random.gamma(1, 2, (lat_points, lon_points)))
                
                precip_data[i] = seasonal_precip + daily_precip_noise
            
            # Create xarray dataset
            ds = xr.Dataset({
                'temperature': (['time', 'lat', 'lon'], temp_data, {
                    'units': 'K',
                    'long_name': 'Near-surface air temperature',
                    'standard_name': 'air_temperature'
                }),
                'precipitation': (['time', 'lat', 'lon'], precip_data, {
                    'units': 'mm/day',
                    'long_name': 'Daily precipitation',
                    'standard_name': 'precipitation_flux'
                })
            }, coords={
                'time': times,
                'lat': ('lat', lats, {'units': 'degrees_north'}),
                'lon': ('lon', lons, {'units': 'degrees_east'})
            })
            
            # Add global attributes
            ds.attrs.update({
                'title': f'Synthetic climate data for {year}',
                'institution': 'dask_setup climate analysis case study',
                'source': 'Synthetic data with realistic patterns',
                'conventions': 'CF-1.8',
                'history': f'Created {datetime.now().isoformat()}',
                'references': 'dask_setup examples repository'
            })
            
            # Add some missing data to make it realistic
            if year % 3 == 0:  # Every third year has some missing data
                missing_mask = np.random.random(temp_data.shape) < 0.01  # 1% missing
                ds['temperature'] = ds['temperature'].where(~missing_mask)
                ds['precipitation'] = ds['precipitation'].where(~missing_mask)
            
            # Save with compression
            filename = f"climate_data_{year}.nc"
            file_path = output_dir / filename
            
            encoding = {
                'temperature': {'zlib': True, 'complevel': 4, 'shuffle': True},
                'precipitation': {'zlib': True, 'complevel': 4, 'shuffle': True},
                'time': {'units': 'days since 1950-01-01'}
            }
            
            ds.to_netcdf(file_path, encoding=encoding)
            file_paths.append(file_path)
    
    # Calculate total size
    total_size = sum(f.stat().st_size for f in file_paths) / (1024**3)
    print(f"✅ Created {len(file_paths)} files ({total_size:.2f} GB total)")
    print(f"   Average file size: {total_size*1024/len(file_paths):.0f} MB")
    
    return file_paths


def load_and_preprocess_data(
    file_paths: List[Path],
    checkpointer: AnalysisCheckpointer,
    client
) -> xr.Dataset:
    """Load and preprocess the climate dataset.
    
    Args:
        file_paths: List of data files
        checkpointer: Analysis checkpointer
        client: Dask client
        
    Returns:
        Preprocessed dataset
    """
    step_name = "data_loading"
    
    if checkpointer.is_step_complete(step_name):
        print(f"📂 Loading preprocessed data from checkpoint...")
        checkpoint_file = checkpointer.get_checkpoint_path("preprocessed_data.zarr")
        if checkpoint_file.exists() and ZARR_AVAILABLE:
            return xr.open_zarr(checkpoint_file)
        else:
            print("   Checkpoint file not found, reprocessing...")
    
    print("\n📂 LOADING AND PREPROCESSING CLIMATE DATA")
    print("=" * 60)
    
    with timer("Data loading and preprocessing") as t:
        # Load multi-file dataset
        print("Loading raw data files...")
        ds = xr.open_mfdataset(
            file_paths,
            engine='netcdf4',
            chunks={'time': 365, 'lat': 90, 'lon': 180},  # ~1 year chunks
            parallel=True,
            combine='by_coords'
        )
        
        print(f"   Dataset shape: {dict(ds.dims)}")
        print(f"   Variables: {list(ds.data_vars.keys())}")
        print(f"   Time range: {ds.time.min().values} to {ds.time.max().values}")
        
        # Quality control
        print("Performing quality control...")
        
        # Flag unrealistic values
        temp_valid = (ds.temperature > 200) & (ds.temperature < 350)  # Reasonable T range
        precip_valid = ds.precipitation >= 0  # Non-negative precipitation
        
        # Apply quality control masks
        ds = ds.where(temp_valid & precip_valid)
        
        # Calculate data availability
        temp_availability = temp_valid.mean().compute()
        precip_availability = precip_valid.mean().compute()
        
        print(f"   Temperature data availability: {temp_availability:.1%}")
        print(f"   Precipitation data availability: {precip_availability:.1%}")
        
        # Add derived variables
        print("Computing derived variables...")
        
        # Temperature in Celsius
        ds['temperature_celsius'] = ds['temperature'] - 273.15
        ds['temperature_celsius'].attrs.update({
            'units': 'degrees_C',
            'long_name': 'Near-surface air temperature in Celsius'
        })
        
        # Monthly precipitation totals
        ds['precipitation_monthly'] = ds['precipitation'].resample(time='1M').sum()
        ds['precipitation_monthly'].attrs.update({
            'units': 'mm/month',
            'long_name': 'Monthly precipitation total'
        })
        
        # Save checkpoint
        if ZARR_AVAILABLE:
            checkpoint_file = checkpointer.get_checkpoint_path("preprocessed_data.zarr")
            print(f"Saving preprocessed data checkpoint...")
            
            # Remove existing checkpoint
            if checkpoint_file.exists():
                import shutil
                shutil.rmtree(checkpoint_file)
            
            encoding = {
                var: {'compressor': zarr.Blosc(cname='lz4', clevel=5)}
                for var in ds.data_vars
            }
            
            ds.to_zarr(checkpoint_file, consolidated=True, encoding=encoding)
            print(f"   Checkpoint saved: {checkpoint_file}")
    
    # Mark step complete
    data_info = {
        'processing_time': t['total'],
        'dataset_dims': dict(ds.dims),
        'variables': list(ds.data_vars.keys()),
        'time_range': [str(ds.time.min().values), str(ds.time.max().values)],
        'temp_availability': float(temp_availability),
        'precip_availability': float(precip_availability)
    }
    
    checkpointer.mark_step_complete(step_name, data_info)
    
    return ds


def compute_climatologies(
    ds: xr.Dataset,
    checkpointer: AnalysisCheckpointer,
    client
) -> Dict[str, xr.Dataset]:
    """Compute annual and seasonal climatologies.
    
    Args:
        ds: Input dataset
        checkpointer: Analysis checkpointer
        client: Dask client
        
    Returns:
        Dictionary of climatology datasets
    """
    step_name = "climatologies"
    
    if checkpointer.is_step_complete(step_name):
        print(f"📊 Loading climatologies from checkpoint...")
        results = {}
        for clim_type in ['annual', 'seasonal', 'monthly']:
            checkpoint_file = checkpointer.get_checkpoint_path(f"climatology_{clim_type}.zarr")
            if checkpoint_file.exists() and ZARR_AVAILABLE:
                results[clim_type] = xr.open_zarr(checkpoint_file)
        if len(results) == 3:
            return results
        else:
            print("   Some checkpoint files missing, recomputing...")
    
    print("\n📊 COMPUTING CLIMATOLOGIES")
    print("=" * 60)
    
    climatologies = {}
    
    with timer("Climatology computation") as t:
        # Annual climatology
        print("Computing annual climatology...")
        annual_clim = ds.groupby('time.year').mean()
        annual_clim = annual_clim.compute()
        climatologies['annual'] = annual_clim
        
        print(f"   Annual data shape: {dict(annual_clim.dims)}")
        
        # Seasonal climatology
        print("Computing seasonal climatology...")
        seasonal_clim = ds.groupby('time.season').mean()
        seasonal_clim = seasonal_clim.compute()
        climatologies['seasonal'] = seasonal_clim
        
        print(f"   Seasonal data shape: {dict(seasonal_clim.dims)}")
        
        # Monthly climatology (average for each month across all years)
        print("Computing monthly climatology...")
        monthly_clim = ds.groupby('time.month').mean()
        monthly_clim = monthly_clim.compute()
        climatologies['monthly'] = monthly_clim
        
        print(f"   Monthly data shape: {dict(monthly_clim.dims)}")
        
        # Save checkpoints
        if ZARR_AVAILABLE:
            for clim_type, clim_data in climatologies.items():
                checkpoint_file = checkpointer.get_checkpoint_path(f"climatology_{clim_type}.zarr")
                print(f"Saving {clim_type} climatology checkpoint...")
                
                if checkpoint_file.exists():
                    import shutil
                    shutil.rmtree(checkpoint_file)
                
                encoding = {
                    var: {'compressor': zarr.Blosc(cname='lz4', clevel=5)}
                    for var in clim_data.data_vars
                }
                
                clim_data.to_zarr(checkpoint_file, consolidated=True, encoding=encoding)
    
    # Calculate some statistics
    temp_stats = {}
    precip_stats = {}
    
    for clim_type, clim_data in climatologies.items():
        temp_mean = float(clim_data['temperature_celsius'].mean())
        temp_std = float(clim_data['temperature_celsius'].std())
        precip_mean = float(clim_data['precipitation'].mean())
        precip_std = float(clim_data['precipitation'].std())
        
        temp_stats[clim_type] = {'mean': temp_mean, 'std': temp_std}
        precip_stats[clim_type] = {'mean': precip_mean, 'std': precip_std}
        
        print(f"   {clim_type.capitalize()} temperature: {temp_mean:.1f}±{temp_std:.1f}°C")
        print(f"   {clim_type.capitalize()} precipitation: {precip_mean:.1f}±{precip_std:.1f} mm/day")
    
    # Mark step complete
    clim_info = {
        'processing_time': t['total'],
        'temperature_stats': temp_stats,
        'precipitation_stats': precip_stats,
        'climatology_dims': {k: dict(v.dims) for k, v in climatologies.items()}
    }
    
    checkpointer.mark_step_complete(step_name, clim_info)
    
    return climatologies


def analyze_temperature_trends(
    ds: xr.Dataset,
    checkpointer: AnalysisCheckpointer,
    client
) -> Dict[str, Any]:
    """Analyze temperature trends over time.
    
    Args:
        ds: Input dataset
        checkpointer: Analysis checkpointer
        client: Dask client
        
    Returns:
        Dictionary with trend analysis results
    """
    step_name = "temperature_trends"
    
    if checkpointer.is_step_complete(step_name):
        print(f"📈 Loading trend analysis from checkpoint...")
        # For this analysis, we'll recompute as results are lightweight
        pass
    
    print("\n📈 ANALYZING TEMPERATURE TRENDS")
    print("=" * 60)
    
    results = {}
    
    with timer("Temperature trend analysis") as t:
        # Global mean temperature time series
        print("Computing global mean temperature time series...")
        
        # Weight by latitude (cosine weighting)
        weights = np.cos(np.deg2rad(ds.lat))
        weights = weights / weights.sum()
        
        global_temp = (ds['temperature_celsius'] * weights).sum(['lat', 'lon'])
        global_temp = global_temp.compute()
        
        # Annual mean global temperature
        annual_global = global_temp.resample(time='1Y').mean().compute()
        
        # Linear trend calculation
        years = annual_global.time.dt.year.values
        temps = annual_global.values
        
        # Simple linear regression
        from scipy.stats import linregress
        slope, intercept, r_value, p_value, std_err = linregress(years, temps)
        
        print(f"   Global temperature trend: {slope:.3f}°C/year")
        print(f"   R² = {r_value**2:.3f}, p-value = {p_value:.2e}")
        
        results['global_trend'] = {
            'slope': slope,
            'intercept': intercept,
            'r_squared': r_value**2,
            'p_value': p_value,
            'std_error': std_err,
            'time_series': {
                'years': years.tolist(),
                'temperatures': temps.tolist()
            }
        }
        
        # Regional trend analysis
        print("Computing regional temperature trends...")
        
        # Define regions
        regions = {
            'Arctic': {'lat_min': 60, 'lat_max': 90},
            'Tropics': {'lat_min': -23.5, 'lat_max': 23.5},
            'Antarctic': {'lat_min': -90, 'lat_max': -60},
            'Northern_Temperate': {'lat_min': 23.5, 'lat_max': 60},
            'Southern_Temperate': {'lat_min': -60, 'lat_max': -23.5}
        }
        
        regional_trends = {}
        
        for region_name, bounds in regions.items():
            # Select region
            lat_mask = (ds.lat >= bounds['lat_min']) & (ds.lat <= bounds['lat_max'])
            region_data = ds['temperature_celsius'].where(lat_mask, drop=True)
            
            # Regional mean (area-weighted)
            region_weights = np.cos(np.deg2rad(region_data.lat))
            region_weights = region_weights / region_weights.sum()
            
            regional_temp = (region_data * region_weights).sum(['lat', 'lon'])
            annual_regional = regional_temp.resample(time='1Y').mean().compute()
            
            # Compute trend
            regional_temps = annual_regional.values
            r_slope, r_intercept, r_r_value, r_p_value, r_std_err = linregress(years, regional_temps)
            
            regional_trends[region_name] = {
                'slope': r_slope,
                'intercept': r_intercept,
                'r_squared': r_r_value**2,
                'p_value': r_p_value,
                'std_error': r_std_err
            }
            
            print(f"   {region_name}: {r_slope:.3f}°C/year (R² = {r_r_value**2:.3f})")
        
        results['regional_trends'] = regional_trends
        
        # Spatial trend patterns
        print("Computing spatial trend patterns...")
        
        # For each grid point, compute linear trend
        def compute_pixel_trend(temp_series, years):
            """Compute linear trend for a single pixel."""
            valid_mask = ~np.isnan(temp_series)
            if valid_mask.sum() < 10:  # Need at least 10 points
                return np.nan, np.nan
            
            y_valid = years[valid_mask]
            t_valid = temp_series[valid_mask]
            
            try:
                slope, _, r_value, p_value, _ = linregress(y_valid, t_valid)
                return slope, p_value
            except:
                return np.nan, np.nan
        
        # Apply to each pixel (this is computationally intensive)
        print("   Computing spatial trends (this may take a while)...")
        
        annual_temp = ds['temperature_celsius'].resample(time='1Y').mean()
        years_array = annual_temp.time.dt.year.values
        
        # Use xarray's apply_ufunc for efficient computation
        trend_slopes, trend_pvalues = xr.apply_ufunc(
            lambda x: compute_pixel_trend(x, years_array),
            annual_temp,
            input_core_dims=[['time']],
            output_core_dims=[[], []],
            output_dtypes=[float, float],
            dask='parallelized',
            vectorize=True
        )
        
        trend_slopes = trend_slopes.compute()
        trend_pvalues = trend_pvalues.compute()
        
        results['spatial_trends'] = {
            'slope_field': trend_slopes,
            'pvalue_field': trend_pvalues,
            'significant_warming': float((trend_slopes > 0) & (trend_pvalues < 0.05)).sum(),
            'significant_cooling': float((trend_slopes < 0) & (trend_pvalues < 0.05)).sum()
        }
        
        significant_warming = ((trend_slopes > 0) & (trend_pvalues < 0.05)).sum().values
        significant_cooling = ((trend_slopes < 0) & (trend_pvalues < 0.05)).sum().values
        total_points = (~np.isnan(trend_slopes)).sum().values
        
        print(f"   Significant warming: {significant_warming}/{total_points} pixels ({100*significant_warming/total_points:.1f}%)")
        print(f"   Significant cooling: {significant_cooling}/{total_points} pixels ({100*significant_cooling/total_points:.1f}%)")
    
    # Mark step complete
    trend_info = {
        'processing_time': t['total'],
        'global_trend_slope': slope,
        'global_trend_r2': r_value**2,
        'regional_trend_count': len(regional_trends),
        'spatial_analysis': {
            'significant_warming_pixels': int(significant_warming),
            'significant_cooling_pixels': int(significant_cooling),
            'total_pixels': int(total_points)
        }
    }
    
    checkpointer.mark_step_complete(step_name, trend_info)
    
    return results


def regrid_to_standard_grid(
    ds: xr.Dataset,
    target_resolution: float,
    checkpointer: AnalysisCheckpointer,
    client
) -> xr.Dataset:
    """Regrid dataset to a standard grid resolution.
    
    Args:
        ds: Input dataset
        target_resolution: Target grid resolution in degrees
        checkpointer: Analysis checkpointer
        client: Dask client
        
    Returns:
        Regridded dataset
    """
    step_name = f"regridding_{target_resolution}deg"
    
    if checkpointer.is_step_complete(step_name):
        print(f"🗺️  Loading regridded data from checkpoint...")
        checkpoint_file = checkpointer.get_checkpoint_path(f"regridded_{target_resolution}deg.zarr")
        if checkpoint_file.exists() and ZARR_AVAILABLE:
            return xr.open_zarr(checkpoint_file)
        else:
            print("   Checkpoint file not found, reprocessing...")
    
    print(f"\n🗺️  REGRIDDING TO {target_resolution}° RESOLUTION")
    print("=" * 60)
    
    if not SCIPY_AVAILABLE:
        print("❌ scipy not available - skipping regridding")
        return ds
    
    with timer("Data regridding") as t:
        # Define target grid
        target_lat_points = int(180 / target_resolution) + 1
        target_lon_points = int(360 / target_resolution) + 1
        
        target_lats = np.linspace(-90, 90, target_lat_points)
        target_lons = np.linspace(-180, 180, target_lon_points)
        
        print(f"Original grid: {len(ds.lat)}×{len(ds.lon)}")
        print(f"Target grid: {target_lat_points}×{target_lon_points}")
        
        # Use xarray's interpolation for regridding
        print("Performing interpolation...")
        
        # Sample a subset of data for regridding (memory management)
        # Use annual means to reduce data volume
        annual_data = ds.resample(time='1Y').mean()
        
        # Interpolate to new grid
        regridded = annual_data.interp(
            lat=target_lats,
            lon=target_lons,
            method='linear'
        )
        
        regridded = regridded.compute()
        
        print(f"Regridded shape: {dict(regridded.dims)}")
        
        # Update attributes
        regridded.attrs.update(ds.attrs)
        regridded.attrs['regridding_method'] = 'linear interpolation'
        regridded.attrs['target_resolution'] = f'{target_resolution} degrees'
        regridded.attrs['regridding_timestamp'] = datetime.now().isoformat()
        
        # Save checkpoint
        if ZARR_AVAILABLE:
            checkpoint_file = checkpointer.get_checkpoint_path(f"regridded_{target_resolution}deg.zarr")
            print(f"Saving regridded data checkpoint...")
            
            if checkpoint_file.exists():
                import shutil
                shutil.rmtree(checkpoint_file)
            
            encoding = {
                var: {'compressor': zarr.Blosc(cname='lz4', clevel=5)}
                for var in regridded.data_vars
            }
            
            regridded.to_zarr(checkpoint_file, consolidated=True, encoding=encoding)
            print(f"   Checkpoint saved: {checkpoint_file}")
    
    # Mark step complete
    regrid_info = {
        'processing_time': t['total'],
        'original_shape': [len(ds.lat), len(ds.lon)],
        'target_shape': [target_lat_points, target_lon_points],
        'target_resolution': target_resolution,
        'method': 'linear interpolation'
    }
    
    checkpointer.mark_step_complete(step_name, regrid_info)
    
    return regridded


def create_analysis_plots(
    ds: xr.Dataset,
    climatologies: Dict[str, xr.Dataset],
    trend_results: Dict[str, Any],
    output_dir: Path,
    checkpointer: AnalysisCheckpointer
) -> List[Path]:
    """Create analysis plots and figures.
    
    Args:
        ds: Original dataset
        climatologies: Climatology datasets
        trend_results: Trend analysis results
        output_dir: Output directory for plots
        checkpointer: Analysis checkpointer
        
    Returns:
        List of created plot files
    """
    step_name = "plotting"
    
    if checkpointer.is_step_complete(step_name) and not True:  # Force replot for demo
        print(f"📊 Plots already generated, skipping...")
        return []
    
    if not MATPLOTLIB_AVAILABLE:
        print("❌ matplotlib not available - skipping plotting")
        return []
    
    print("\n📊 CREATING ANALYSIS PLOTS")
    print("=" * 60)
    
    plot_dir = Path(output_dir) / "plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    
    plot_files = []
    
    with timer("Plot generation") as t:
        # Plot 1: Global temperature time series
        print("Creating global temperature time series plot...")
        
        fig, ax = plt.subplots(figsize=(12, 6))
        
        if 'global_trend' in trend_results:
            years = trend_results['global_trend']['time_series']['years']
            temps = trend_results['global_trend']['time_series']['temperatures']
            slope = trend_results['global_trend']['slope']
            
            ax.plot(years, temps, 'b-', linewidth=2, label='Annual Global Mean')
            ax.plot(years, slope * np.array(years) + trend_results['global_trend']['intercept'], 
                   'r--', linewidth=2, 
                   label=f'Linear Trend ({slope:.3f}°C/year)')
            
            ax.set_xlabel('Year')
            ax.set_ylabel('Temperature (°C)')
            ax.set_title('Global Mean Temperature Trend')
            ax.legend()
            ax.grid(True, alpha=0.3)
        
        plot_file = plot_dir / "global_temperature_trend.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
        
        # Plot 2: Regional temperature trends
        print("Creating regional trends comparison...")
        
        if 'regional_trends' in trend_results:
            fig, ax = plt.subplots(figsize=(10, 6))
            
            regions = list(trend_results['regional_trends'].keys())
            trends = [trend_results['regional_trends'][r]['slope'] for r in regions]
            errors = [trend_results['regional_trends'][r]['std_error'] for r in regions]
            
            bars = ax.bar(range(len(regions)), trends, yerr=errors, 
                         capsize=5, alpha=0.7, color='skyblue')
            
            ax.set_xticks(range(len(regions)))
            ax.set_xticklabels(regions, rotation=45, ha='right')
            ax.set_ylabel('Temperature Trend (°C/year)')
            ax.set_title('Regional Temperature Trends')
            ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
            ax.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for i, (bar, trend) in enumerate(zip(bars, trends)):
                ax.text(bar.get_x() + bar.get_width()/2, 
                       bar.get_height() + errors[i] + 0.001,
                       f'{trend:.3f}', ha='center', va='bottom', fontsize=9)
        
        plot_file = plot_dir / "regional_temperature_trends.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
        
        # Plot 3: Seasonal climatology
        print("Creating seasonal climatology plot...")
        
        if 'seasonal' in climatologies:
            seasonal = climatologies['seasonal']
            
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            axes = axes.flatten()
            
            seasons = ['DJF', 'MAM', 'JJA', 'SON']
            season_names = ['Winter', 'Spring', 'Summer', 'Autumn']
            
            for i, (season, season_name) in enumerate(zip(seasons, season_names)):
                if season in seasonal.season:
                    temp_data = seasonal['temperature_celsius'].sel(season=season)
                    
                    im = axes[i].contourf(temp_data.lon, temp_data.lat, temp_data, 
                                        levels=20, cmap='RdYlBu_r')
                    axes[i].set_title(f'{season_name} ({season}) Temperature')
                    axes[i].set_xlabel('Longitude')
                    axes[i].set_ylabel('Latitude')
                    
                    # Add colorbar
                    cbar = plt.colorbar(im, ax=axes[i], shrink=0.8)
                    cbar.set_label('Temperature (°C)')
        
        plt.tight_layout()
        plot_file = plot_dir / "seasonal_climatology.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
        
        # Plot 4: Spatial temperature trend
        print("Creating spatial temperature trend plot...")
        
        if 'spatial_trends' in trend_results:
            fig, ax = plt.subplots(figsize=(14, 8))
            
            trend_field = trend_results['spatial_trends']['slope_field']
            pvalue_field = trend_results['spatial_trends']['pvalue_field']
            
            # Mask non-significant trends
            significant = pvalue_field < 0.05
            masked_trends = trend_field.where(significant)
            
            im = ax.contourf(trend_field.lon, trend_field.lat, masked_trends,
                           levels=np.linspace(-0.05, 0.05, 21),
                           cmap='RdBu_r', extend='both')
            
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            ax.set_title('Significant Temperature Trends (°C/year, p < 0.05)')
            
            cbar = plt.colorbar(im, ax=ax, shrink=0.7)
            cbar.set_label('Temperature Trend (°C/year)')
            
            # Add coastlines if cartopy available
            if CARTOPY_AVAILABLE:
                ax.coastlines()
        
        plot_file = plot_dir / "spatial_temperature_trends.png"
        fig.savefig(plot_file, dpi=300, bbox_inches='tight')
        plt.close(fig)
        plot_files.append(plot_file)
        
        print(f"✅ Created {len(plot_files)} plots in {plot_dir}")
    
    # Mark step complete
    plot_info = {
        'processing_time': t['total'],
        'plots_created': len(plot_files),
        'plot_directory': str(plot_dir)
    }
    
    checkpointer.mark_step_complete(step_name, plot_info)
    
    return plot_files


def create_analysis_summary(
    checkpointer: AnalysisCheckpointer,
    output_dir: Path
) -> Path:
    """Create a comprehensive analysis summary report.
    
    Args:
        checkpointer: Analysis checkpointer with progress info
        output_dir: Output directory
        
    Returns:
        Path to summary report
    """
    print("\n📄 CREATING ANALYSIS SUMMARY REPORT")
    print("=" * 60)
    
    summary_file = output_dir / "climate_analysis_summary.md"
    
    with open(summary_file, 'w') as f:
        f.write("# Climate Analysis Case Study Summary\n\n")
        f.write(f"**Analysis completed:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write("## Analysis Overview\n\n")
        f.write("This report summarizes a comprehensive climate data analysis workflow ")
        f.write("demonstrating dask_setup capabilities for scientific computing.\n\n")
        
        # Progress summary
        f.write("## Analysis Progress\n\n")
        progress = checkpointer.progress
        
        f.write(f"**Steps completed:** {len(progress['steps_completed'])}\n\n")
        for step in progress['steps_completed']:
            f.write(f"- ✅ {step.replace('_', ' ').title()}\n")
        
        f.write("\n## Processing Details\n\n")
        
        # Data loading info
        if 'data_loading' in progress['data_info']:
            info = progress['data_info']['data_loading']
            f.write("### Data Loading and Preprocessing\n\n")
            f.write(f"- **Processing time:** {info['processing_time']:.1f} seconds\n")
            f.write(f"- **Dataset dimensions:** {info['dataset_dims']}\n")
            f.write(f"- **Variables:** {', '.join(info['variables'])}\n")
            f.write(f"- **Time range:** {info['time_range'][0]} to {info['time_range'][1]}\n")
            f.write(f"- **Data availability:** Temperature {info['temp_availability']:.1%}, ")
            f.write(f"Precipitation {info['precip_availability']:.1%}\n\n")
        
        # Climatology info
        if 'climatologies' in progress['data_info']:
            info = progress['data_info']['climatologies']
            f.write("### Climatology Analysis\n\n")
            f.write(f"- **Processing time:** {info['processing_time']:.1f} seconds\n")
            f.write("- **Temperature statistics:**\n")
            for clim_type, stats in info['temperature_stats'].items():
                f.write(f"  - {clim_type.capitalize()}: {stats['mean']:.1f}±{stats['std']:.1f}°C\n")
            f.write("- **Precipitation statistics:**\n")
            for clim_type, stats in info['precipitation_stats'].items():
                f.write(f"  - {clim_type.capitalize()}: {stats['mean']:.1f}±{stats['std']:.1f} mm/day\n")
            f.write("\n")
        
        # Trend analysis info
        if 'temperature_trends' in progress['data_info']:
            info = progress['data_info']['temperature_trends']
            f.write("### Temperature Trend Analysis\n\n")
            f.write(f"- **Processing time:** {info['processing_time']:.1f} seconds\n")
            f.write(f"- **Global trend:** {info['global_trend_slope']:.3f}°C/year ")
            f.write(f"(R² = {info['global_trend_r2']:.3f})\n")
            f.write(f"- **Regional trends:** {info['regional_trend_count']} regions analyzed\n")
            
            spatial = info['spatial_analysis']
            f.write(f"- **Spatial analysis:** {spatial['significant_warming_pixels']}/{spatial['total_pixels']} pixels ")
            f.write(f"showing significant warming ({100*spatial['significant_warming_pixels']/spatial['total_pixels']:.1f}%)\n\n")
        
        # Regridding info
        for step_name, step_info in progress['data_info'].items():
            if step_name.startswith('regridding_'):
                f.write("### Data Regridding\n\n")
                f.write(f"- **Processing time:** {step_info['processing_time']:.1f} seconds\n")
                f.write(f"- **Original resolution:** {step_info['original_shape'][0]}×{step_info['original_shape'][1]}\n")
                f.write(f"- **Target resolution:** {step_info['target_shape'][0]}×{step_info['target_shape'][1]} ")
                f.write(f"({step_info['target_resolution']}°)\n")
                f.write(f"- **Method:** {step_info['method']}\n\n")
                break
        
        # Plotting info
        if 'plotting' in progress['data_info']:
            info = progress['data_info']['plotting']
            f.write("### Visualization\n\n")
            f.write(f"- **Processing time:** {info['processing_time']:.1f} seconds\n")
            f.write(f"- **Plots created:** {info['plots_created']}\n")
            f.write(f"- **Plot directory:** `{info['plot_directory']}`\n\n")
        
        f.write("## Key Findings\n\n")
        f.write("This synthetic climate analysis demonstrates:\n\n")
        f.write("1. **Efficient processing** of multi-year climate datasets using dask_setup\n")
        f.write("2. **Robust trend detection** with statistical significance testing\n")
        f.write("3. **Comprehensive climatology** computation across temporal scales\n")
        f.write("4. **Spatial analysis** capabilities for regional studies\n")
        f.write("5. **Automated checkpointing** for reproducible workflows\n\n")
        
        f.write("## Workflow Benefits\n\n")
        f.write("- **Memory efficiency:** Large datasets processed in chunks\n")
        f.write("- **Parallel processing:** Multi-core utilization for computations\n")
        f.write("- **Progress tracking:** Checkpointing prevents data loss\n")
        f.write("- **Reproducibility:** Complete workflow documentation\n")
        f.write("- **Scalability:** Adaptable to larger datasets and HPC systems\n\n")
        
        f.write("## Technical Notes\n\n")
        f.write("This analysis was performed using the dask_setup library, which provides ")
        f.write("HPC-optimized Dask configurations for scientific computing workflows. ")
        f.write("The synthetic dataset mimics real climate data characteristics including ")
        f.write("spatial patterns, temporal trends, and missing values.\n\n")
        
        f.write("---\n")
        f.write("*Generated by dask_setup climate analysis case study*\n")
    
    print(f"✅ Analysis summary saved: {summary_file}")
    return summary_file


def main():
    """Main function for the climate analysis case study."""
    parser = argparse.ArgumentParser(
        description="Real-world climate analysis case study",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--start-year",
        type=int,
        default=2000,
        help="Start year for analysis (default: 2000)"
    )
    
    parser.add_argument(
        "--end-year",
        type=int,
        default=2010,
        help="End year for analysis (default: 2010)"
    )
    
    parser.add_argument(
        "--spatial-resolution",
        type=float,
        default=2.0,
        help="Grid resolution in degrees (default: 2.0)"
    )
    
    parser.add_argument(
        "--regrid-resolution",
        type=float,
        default=5.0,
        help="Target regridding resolution in degrees (default: 5.0)"
    )
    
    parser.add_argument(
        "--skip-data-creation",
        action="store_true",
        help="Skip synthetic data creation (use existing files)"
    )
    
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of Dask workers (default: 4)"
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
    
    print("🌍 REAL-WORLD CLIMATE ANALYSIS CASE STUDY")
    print("=" * 60)
    print(f"Analysis period: {args.start_year}-{args.end_year-1}")
    print(f"Spatial resolution: {args.spatial_resolution}°")
    print(f"Regridding resolution: {args.regrid_resolution}°")
    print(f"Workers: {args.workers}")
    print(f"Output directory: {args.output_dir}")
    
    try:
        # Initialize checkpointer
        checkpointer = AnalysisCheckpointer(args.output_dir / "checkpoints")
        
        # Create Dask client optimized for climate analysis
        client, cluster, temp_dir = setup_dask_client(
            workload_type="mixed",  # Mixed I/O and compute
            max_workers=args.workers,
            reserve_mem_gb=40.0,
            dashboard=True
        )
        
        print(f"\n🖥️  Analysis Cluster: {len(client.scheduler_info()['workers'])} workers")
        print(f"   Temp directory: {temp_dir}")
        
        # Step 1: Create or find climate data
        data_dir = args.output_dir / "climate_data"
        
        if not args.skip_data_creation or not data_dir.exists():
            file_paths = create_synthetic_climate_data(
                data_dir,
                start_year=args.start_year,
                end_year=args.end_year,
                spatial_resolution=args.spatial_resolution
            )
        else:
            file_paths = sorted(list(data_dir.glob("*.nc")))
            print(f"📁 Using existing {len(file_paths)} climate files from {data_dir}")
        
        if not file_paths:
            print("❌ No climate data files available for analysis")
            return 1
        
        # Step 2: Load and preprocess data
        ds = load_and_preprocess_data(file_paths, checkpointer, client)
        
        # Step 3: Compute climatologies
        climatologies = compute_climatologies(ds, checkpointer, client)
        
        # Step 4: Analyze temperature trends
        trend_results = analyze_temperature_trends(ds, checkpointer, client)
        
        # Step 5: Regrid to standard resolution
        regridded_ds = regrid_to_standard_grid(
            ds, args.regrid_resolution, checkpointer, client
        )
        
        # Step 6: Create analysis plots
        plot_files = create_analysis_plots(
            ds, climatologies, trend_results, args.output_dir, checkpointer
        )
        
        # Step 7: Create comprehensive summary
        summary_file = create_analysis_summary(checkpointer, args.output_dir)
        
        print("\n" + "=" * 60)
        print("✅ CLIMATE ANALYSIS CASE STUDY COMPLETED!")
        print("=" * 60)
        print(f"📄 Analysis summary: {summary_file}")
        print(f"📊 Plots created: {len(plot_files)}")
        print(f"💾 Checkpoints saved in: {checkpointer.checkpoint_dir}")
        
        if ZARR_AVAILABLE:
            zarr_files = list(checkpointer.checkpoint_dir.glob("*.zarr"))
            total_zarr_size = sum(
                sum(f.stat().st_size for f in zf.rglob('*') if f.is_file()) 
                for zf in zarr_files
            ) / (1024**3)
            print(f"🗄️  Zarr archives: {len(zarr_files)} files ({total_zarr_size:.2f} GB)")
        
        # Print final statistics
        completed_steps = len(checkpointer.progress['steps_completed'])
        print(f"📈 Analysis steps completed: {completed_steps}")
        print(f"🕐 Analysis started: {checkpointer.progress.get('last_updated', 'Unknown')}")
        
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