"""Tests for I/O optimization patterns for Zarr and NetCDF formats."""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest


# Test dependencies availability
def test_io_dependencies_unavailable():
    """Test that helpful errors are raised when dependencies are not available."""
    with patch("dask_setup.io_patterns.xr", None):
        from dask_setup.io_patterns import _ensure_dependencies

        try:
            from dask_setup.error_handling import DependencyError
        except ImportError:
            from dask_setup.io_patterns import DependencyError

        with pytest.raises(DependencyError, match="Missing dependency 'xarray'"):
            _ensure_dependencies()

    with patch("dask_setup.io_patterns.np", None):
        from dask_setup.io_patterns import _ensure_dependencies

        try:
            from dask_setup.error_handling import DependencyError
        except ImportError:
            from dask_setup.io_patterns import DependencyError

        with pytest.raises(DependencyError, match="Missing dependency 'numpy'"):
            _ensure_dependencies()


class TestStorageFormatDetection:
    """Tests for storage format detection."""

    def test_detect_zarr_format_from_path(self):
        """Test Zarr format detection from file paths."""
        from dask_setup.io_patterns import detect_storage_format

        # Test various Zarr path patterns
        zarr_paths = [
            "/path/to/data.zarr",
            "s3://bucket/data.zarr/",
            "gs://bucket/dataset.zarr",
            "/tmp/output.zarr/",
            "data.ZARR",  # case insensitive
        ]

        for path in zarr_paths:
            assert detect_storage_format(path) == "zarr"

    def test_detect_netcdf_format_from_path(self):
        """Test NetCDF format detection from file paths."""
        from dask_setup.io_patterns import detect_storage_format

        # Test various NetCDF path patterns
        netcdf_paths = [
            "/path/to/data.nc",
            "s3://bucket/climate.nc4",
            "https://server.com/dataset.netcdf",
            "/tmp/output.cdf",
            "data.NC",  # case insensitive
        ]

        for path in netcdf_paths:
            assert detect_storage_format(path) == "netcdf"

    def test_detect_unknown_format(self):
        """Test unknown format detection."""
        from dask_setup.io_patterns import detect_storage_format

        unknown_paths = ["/path/to/data.hdf5", "s3://bucket/data.tiff", "/tmp/data.csv"]

        for path in unknown_paths:
            assert detect_storage_format(path) == "unknown"


class TestZarrOptimizer:
    """Tests for Zarr-specific optimizations."""

    @pytest.fixture
    def zarr_optimizer(self):
        """Create a ZarrOptimizer instance."""
        from dask_setup.io_patterns import ZarrOptimizer

        return ZarrOptimizer()

    @pytest.fixture
    def mock_zarr_dataset(self):
        """Create a mock xarray Dataset for Zarr testing."""
        mock_ds = Mock()
        mock_ds.data_vars = {
            "temperature": Mock(
                dtype="float32", shape=(365, 721, 1440), dims=["time", "lat", "lon"]
            )
        }
        mock_ds.sizes = {"time": 365, "lat": 721, "lon": 1440}
        return mock_ds

    def test_zarr_format_detection(self, zarr_optimizer):
        """Test Zarr format detection logic."""
        assert zarr_optimizer.detect_format("/data/output.zarr")
        assert zarr_optimizer.detect_format("s3://bucket/data.zarr/")
        assert not zarr_optimizer.detect_format("/data/output.nc")

    def test_zarr_chunking_optimization(self, zarr_optimizer, mock_zarr_dataset):
        """Test Zarr-specific chunking recommendations."""
        with patch("dask_setup.io_patterns.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4
            mock_np.prod.return_value = 365 * 721 * 1440

            chunks = zarr_optimizer.optimize_chunks(mock_zarr_dataset, target_chunk_mb=(128, 512))

            assert isinstance(chunks, dict)
            # Should recommend chunking for large dataset
            assert len(chunks) > 0

    def test_zarr_compression_optimization(self, zarr_optimizer, mock_zarr_dataset):
        """Test Zarr compression recommendations."""
        compression = zarr_optimizer.optimize_compression(
            mock_zarr_dataset, storage_location="cloud"
        )

        assert "codec" in compression
        assert "level" in compression
        assert compression["codec"] in ["zstd", "lz4", "blosc"]

        # Cloud storage should prefer higher compression
        cloud_compression = zarr_optimizer.optimize_compression(
            mock_zarr_dataset, storage_location="s3://bucket/data"
        )
        assert cloud_compression["codec"] == "zstd"

    def test_zarr_storage_options_s3(self, zarr_optimizer):
        """Test S3-specific storage options for Zarr."""
        options = zarr_optimizer.optimize_storage_options(
            "s3://bucket/data.zarr", access_pattern="sequential"
        )

        assert "anon" in options
        assert "default_cache_type" in options
        assert "default_block_size" in options
        assert options["consolidated"] is True

    def test_zarr_storage_options_local(self, zarr_optimizer):
        """Test local storage options for Zarr."""
        options = zarr_optimizer.optimize_storage_options(
            "/local/path/data.zarr", access_pattern="random"
        )

        assert "consolidated" in options
        assert options["consolidated"] is True

    def test_throughput_estimation(self, zarr_optimizer):
        """Test I/O throughput estimation."""
        # Local storage should be fastest
        local_throughput = zarr_optimizer.estimate_throughput(
            chunk_mb=256, storage_location="local", access_pattern="sequential"
        )

        # Cloud storage should be slower
        cloud_throughput = zarr_optimizer.estimate_throughput(
            chunk_mb=256, storage_location="s3://bucket/data", access_pattern="sequential"
        )

        assert local_throughput > cloud_throughput

        # Random access should be slower than sequential
        random_throughput = zarr_optimizer.estimate_throughput(
            chunk_mb=256, storage_location="local", access_pattern="random"
        )

        assert local_throughput > random_throughput


class TestNetCDFOptimizer:
    """Tests for NetCDF-specific optimizations."""

    @pytest.fixture
    def netcdf_optimizer(self):
        """Create a NetCDFOptimizer instance."""
        from dask_setup.io_patterns import NetCDFOptimizer

        return NetCDFOptimizer()

    @pytest.fixture
    def mock_netcdf_dataset(self):
        """Create a mock xarray Dataset for NetCDF testing."""
        mock_ds = Mock()
        mock_ds.data_vars = {
            "temperature": Mock(
                dtype="float32", shape=(1000, 200, 300), dims=["time", "lat", "lon"]
            )
        }
        mock_ds.sizes = {"time": 1000, "lat": 200, "lon": 300}
        return mock_ds

    def test_netcdf_format_detection(self, netcdf_optimizer):
        """Test NetCDF format detection logic."""
        assert netcdf_optimizer.detect_format("/data/climate.nc")
        assert netcdf_optimizer.detect_format("https://server.com/data.nc4")
        assert not netcdf_optimizer.detect_format("/data/output.zarr")

    def test_netcdf_chunking_optimization(self, netcdf_optimizer, mock_netcdf_dataset):
        """Test NetCDF-specific chunking recommendations."""
        with patch("dask_setup.io_patterns.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4
            mock_np.prod.return_value = 1000 * 200 * 300

            chunks = netcdf_optimizer.optimize_chunks(
                mock_netcdf_dataset,
                target_chunk_mb=(64, 256),  # NetCDF prefers smaller chunks
            )

            assert isinstance(chunks, dict)
            # Should be more conservative with unlimited dimensions (time)
            if "time" in chunks and "lat" in chunks:
                # Should prefer chunking non-unlimited dimensions first
                assert chunks["lat"] <= chunks.get("time", float("inf"))

    def test_netcdf_compression_optimization(self, netcdf_optimizer, mock_netcdf_dataset):
        """Test NetCDF compression recommendations."""
        compression = netcdf_optimizer.optimize_compression(
            mock_netcdf_dataset, storage_location="local"
        )

        assert compression["codec"] == "zlib"  # Standard for NetCDF
        assert "level" in compression
        assert compression["shuffle"] is True
        assert "fletcher32" in compression

    def test_netcdf_storage_options(self, netcdf_optimizer):
        """Test NetCDF storage options."""
        options = netcdf_optimizer.optimize_storage_options(
            "/path/to/data.nc", access_pattern="streaming"
        )

        assert options["format"] == "NETCDF4"
        assert options["engine"] == "netcdf4"


@pytest.fixture
def sample_dataset():
    """Create a sample dataset for testing."""
    try:
        import numpy as np
        import xarray as xr

        rng = np.random.default_rng(42)
        data = rng.random((100, 200, 300)).astype(np.float32)
        ds = xr.Dataset({"temperature": (["time", "y", "x"], data)})
        return ds
    except ImportError:
        # Return mock if xarray not available
        mock_ds = Mock()
        mock_ds.data_vars = {"temperature": Mock(dtype="float32", shape=(100, 200, 300))}
        return mock_ds


class TestIORecommendation:
    """Tests for the IORecommendation container."""

    def test_io_recommendation_creation(self):
        """Test IORecommendation object creation and representation."""
        from dask_setup.io_patterns import IORecommendation

        rec = IORecommendation(
            format="zarr",
            chunks={"time": 50, "y": 100},
            compression={"codec": "zstd", "level": 3},
            storage_options={"consolidated": True},
            access_pattern="sequential",
            estimated_throughput_mb_s=150.5,
            warnings=["Test warning"],
        )

        assert rec.format == "zarr"
        assert rec.chunks == {"time": 50, "y": 100}
        assert rec.compression["codec"] == "zstd"
        assert rec.estimated_throughput_mb_s == 150.5
        assert len(rec.warnings) == 1

        # Test string representation
        repr_str = repr(rec)
        assert "IORecommendation" in repr_str
        assert "zarr" in repr_str
        assert "150.5MB/s" in repr_str


class TestRecommendIOChunks:
    """Tests for the main recommend_io_chunks function."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        """Setup common mocks for all tests."""
        with (
            patch("dask_setup.io_patterns.xr") as mock_xr,
            patch("dask_setup.io_patterns.np") as mock_np,
        ):
            mock_np.dtype.return_value.itemsize = 4
            mock_np.prod.return_value = 100 * 200 * 300
            mock_np.issubdtype.return_value = True
            yield mock_xr, mock_np

    def test_recommend_io_chunks_zarr_format(self, sample_dataset):
        """Test I/O chunk recommendations for Zarr format."""
        from dask_setup.io_patterns import recommend_io_chunks

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.ZarrOptimizer") as mock_optimizer_class,
        ):
            mock_detect.return_value = "zarr"
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer
            mock_optimizer.optimize_chunks.return_value = {"time": 50, "y": 100}
            mock_optimizer.optimize_compression.return_value = {"codec": "zstd", "level": 3}
            mock_optimizer.optimize_storage_options.return_value = {"consolidated": True}
            mock_optimizer.estimate_throughput.return_value = 150.0

            chunks = recommend_io_chunks(
                ds=sample_dataset, path_or_url="s3://bucket/data.zarr", target_chunk_mb=(128, 512)
            )

            assert isinstance(chunks, dict)
            assert chunks == {"time": 50, "y": 100}

    def test_recommend_io_chunks_netcdf_format(self, sample_dataset):
        """Test I/O chunk recommendations for NetCDF format."""
        from dask_setup.io_patterns import recommend_io_chunks

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.NetCDFOptimizer") as mock_optimizer_class,
        ):
            mock_detect.return_value = "netcdf"
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer
            mock_optimizer.optimize_chunks.return_value = {"time": 100, "lat": 50}
            mock_optimizer.optimize_compression.return_value = {"codec": "zlib", "level": 4}
            mock_optimizer.optimize_storage_options.return_value = {"format": "NETCDF4"}
            mock_optimizer.estimate_throughput.return_value = 80.0

            chunks = recommend_io_chunks(
                ds=sample_dataset, path_or_url="/path/to/climate.nc", format_hint="netcdf"
            )

            assert isinstance(chunks, dict)
            assert chunks == {"time": 100, "lat": 50}

    def test_recommend_io_chunks_verbose_mode(self, sample_dataset):
        """Test verbose mode returns full IORecommendation."""
        from dask_setup.io_patterns import IORecommendation, recommend_io_chunks

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.ZarrOptimizer") as mock_optimizer_class,
            patch("builtins.print"),  # Suppress print output
        ):
            mock_detect.return_value = "zarr"
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer
            mock_optimizer.optimize_chunks.return_value = {"time": 50, "y": 100}
            mock_optimizer.optimize_compression.return_value = {"codec": "zstd", "level": 3}
            mock_optimizer.optimize_storage_options.return_value = {"consolidated": True}
            mock_optimizer.estimate_throughput.return_value = 150.0

            result = recommend_io_chunks(
                ds=sample_dataset, path_or_url="s3://bucket/data.zarr", verbose=True
            )

            assert isinstance(result, IORecommendation)
            assert result.format == "zarr"
            assert result.chunks == {"time": 50, "y": 100}
            assert result.estimated_throughput_mb_s == 150.0

    def test_recommend_io_chunks_cloud_storage_detection(self, sample_dataset):
        """Test automatic cloud storage detection."""
        from dask_setup.io_patterns import recommend_io_chunks

        cloud_urls = [
            "s3://bucket/data.zarr",
            "gs://bucket/data.zarr",
            "azure://container/data.zarr",
        ]

        for url in cloud_urls:
            with (
                patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
                patch("dask_setup.io_patterns.ZarrOptimizer") as mock_optimizer_class,
            ):
                mock_detect.return_value = "zarr"
                mock_optimizer = Mock()
                mock_optimizer_class.return_value = mock_optimizer
                mock_optimizer.optimize_chunks.return_value = {}
                mock_optimizer.optimize_compression.return_value = {"codec": "zstd"}
                mock_optimizer.optimize_storage_options.return_value = {}
                mock_optimizer.estimate_throughput.return_value = 50.0

                chunks = recommend_io_chunks(ds=sample_dataset, path_or_url=url)

                # Should work without errors
                assert isinstance(chunks, dict)

    def test_recommend_io_chunks_unknown_format_fallback(self, sample_dataset):
        """Test fallback to Zarr for unknown formats."""
        from dask_setup.io_patterns import recommend_io_chunks

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.ZarrOptimizer") as mock_optimizer_class,
        ):
            mock_detect.return_value = "unknown"
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer
            mock_optimizer.optimize_chunks.return_value = {"time": 50}
            mock_optimizer.optimize_compression.return_value = {"codec": "zstd"}
            mock_optimizer.optimize_storage_options.return_value = {}
            mock_optimizer.estimate_throughput.return_value = 100.0

            chunks = recommend_io_chunks(ds=sample_dataset, path_or_url="/path/to/data.unknown")

            assert isinstance(chunks, dict)

    def test_recommend_io_chunks_warning_generation(self, sample_dataset):
        """Test that appropriate warnings are generated."""
        from dask_setup.io_patterns import recommend_io_chunks

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.ZarrOptimizer") as mock_optimizer_class,
            patch("builtins.print"),
        ):
            mock_detect.return_value = "unknown"  # Should trigger warning
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer
            mock_optimizer.optimize_chunks.return_value = {"time": 10}  # Small chunks
            mock_optimizer.optimize_compression.return_value = {"codec": "zstd"}
            mock_optimizer.optimize_storage_options.return_value = {}
            mock_optimizer.estimate_throughput.return_value = 100.0

            result = recommend_io_chunks(
                ds=sample_dataset,
                path_or_url="s3://bucket/data.unknown",  # Cloud + small chunks
                verbose=True,
            )

            # Should generate warnings
            assert len(result.warnings) > 0
            assert any(
                "could not detect storage format" in warning.lower() for warning in result.warnings
            )


class TestIntegrationScenarios:
    """Tests for realistic integration scenarios."""

    def test_climate_data_zarr_scenario(self):
        """Test chunking for climate data stored in Zarr."""
        from dask_setup.io_patterns import recommend_io_chunks

        # Mock large climate dataset
        mock_climate_ds = Mock()

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.ZarrOptimizer") as mock_optimizer_class,
        ):
            mock_detect.return_value = "zarr"
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer

            # Zarr should recommend larger chunks for cloud storage
            mock_optimizer.optimize_chunks.return_value = {"time": 30, "lat": 200, "lon": 400}
            mock_optimizer.optimize_compression.return_value = {"codec": "zstd", "level": 2}
            mock_optimizer.optimize_storage_options.return_value = {
                "consolidated": True,
                "default_cache_type": "readahead",
            }
            mock_optimizer.estimate_throughput.return_value = 120.0

            chunks = recommend_io_chunks(
                ds=mock_climate_ds,
                path_or_url="s3://climate-data/era5.zarr",
                access_pattern="compute",
                target_chunk_mb=(256, 512),
            )

            assert "time" in chunks
            assert isinstance(chunks, dict)

    def test_satellite_data_netcdf_scenario(self):
        """Test chunking for satellite data in NetCDF format."""
        from dask_setup.io_patterns import recommend_io_chunks

        mock_satellite_ds = Mock()

        with (
            patch("dask_setup.io_patterns.detect_storage_format") as mock_detect,
            patch("dask_setup.io_patterns.NetCDFOptimizer") as mock_optimizer_class,
        ):
            mock_detect.return_value = "netcdf"
            mock_optimizer = Mock()
            mock_optimizer_class.return_value = mock_optimizer

            # NetCDF should be more conservative with chunking
            mock_optimizer.optimize_chunks.return_value = {"time": 100, "y": 1000, "x": 1000}
            mock_optimizer.optimize_compression.return_value = {
                "codec": "zlib",
                "level": 4,
                "shuffle": True,
            }
            mock_optimizer.optimize_storage_options.return_value = {"format": "NETCDF4"}
            mock_optimizer.estimate_throughput.return_value = 90.0

            chunks = recommend_io_chunks(
                ds=mock_satellite_ds,
                path_or_url="https://data.server.com/satellite.nc4",
                access_pattern="streaming",
                target_chunk_mb=(64, 256),
            )

            assert isinstance(chunks, dict)


# Test with real xarray objects if available
try:
    import numpy as np
    import xarray as xr

    XARRAY_AVAILABLE = True
except ImportError:
    XARRAY_AVAILABLE = False


@pytest.mark.skipif(not XARRAY_AVAILABLE, reason="xarray not available")
class TestRealXarrayIOIntegration:
    """Integration tests with real xarray objects."""

    def test_recommend_io_chunks_real_dataset_zarr(self):
        """Test with actual xarray Dataset for Zarr."""
        from dask_setup.io_patterns import recommend_io_chunks

        # Create real dataset
        rng = np.random.default_rng(42)
        data = rng.random((50, 100, 200)).astype(np.float32)
        ds = xr.Dataset({"temperature": (["time", "y", "x"], data)})

        chunks = recommend_io_chunks(
            ds=ds, path_or_url="s3://bucket/climate.zarr", access_pattern="compute"
        )

        assert isinstance(chunks, dict)

    def test_recommend_io_chunks_real_dataset_netcdf(self):
        """Test with actual xarray Dataset for NetCDF."""
        from dask_setup.io_patterns import recommend_io_chunks

        # Create real dataset
        rng = np.random.default_rng(42)
        data = rng.random((100, 50, 100)).astype(np.float64)
        ds = xr.Dataset({"precipitation": (["time", "lat", "lon"], data)})

        result = recommend_io_chunks(
            ds=ds, path_or_url="/path/to/weather.nc4", access_pattern="streaming", verbose=True
        )

        from dask_setup.io_patterns import IORecommendation

        assert isinstance(result, IORecommendation)
        assert result.format == "netcdf"

    def test_recommend_io_chunks_chunked_dataset(self):
        """Test with already chunked dataset."""
        from dask_setup.io_patterns import recommend_io_chunks

        # Create chunked dataset
        rng = np.random.default_rng(42)
        data = rng.random((200, 300, 400)).astype(np.float32)
        da = xr.DataArray(data, dims=["time", "y", "x"])
        da_chunked = da.chunk({"time": 50, "y": 150, "x": 200})

        chunks = recommend_io_chunks(ds=da_chunked, format_hint="zarr", target_chunk_mb=(128, 256))

        assert isinstance(chunks, dict)
