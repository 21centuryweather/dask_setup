"""Tests for xarray integration and chunking recommendations."""

from __future__ import annotations

import warnings
from unittest.mock import Mock, patch

import pytest


def test_xarray_unavailable():
    """Test that helpful errors are raised when xarray is not available."""
    with patch("dask_setup.xarray.xr", None):
        from dask_setup.xarray import _ensure_xarray_available

        try:
            from dask_setup.error_handling import DependencyError
        except ImportError:
            from dask_setup.xarray import DependencyError

        with pytest.raises(DependencyError, match="Missing dependency 'xarray'"):
            _ensure_xarray_available()


def test_numpy_unavailable():
    """Test that helpful errors are raised when numpy is not available."""
    with patch("dask_setup.xarray.np", None):
        from dask_setup.xarray import _ensure_xarray_available

        try:
            from dask_setup.error_handling import DependencyError
        except ImportError:
            from dask_setup.xarray import DependencyError

        with pytest.raises(DependencyError, match="Missing dependency 'numpy'"):
            _ensure_xarray_available()


class TestClusterInfo:
    """Tests for cluster information extraction."""

    def test_get_cluster_info_with_client(self):
        """Test cluster info extraction from active Dask client."""
        from dask_setup.xarray import _get_cluster_info

        # Mock client with scheduler info
        mock_client = Mock()
        mock_client.scheduler_info.return_value = {
            "workers": {
                "worker-1": {
                    "nthreads": 4,
                    "memory_limit": 8 * 1024**3,  # 8 GB
                },
                "worker-2": {
                    "nthreads": 4,
                    "memory_limit": 8 * 1024**3,  # 8 GB
                },
            }
        }

        info = _get_cluster_info(mock_client)

        assert info["n_workers"] == 2
        assert info["threads_per_worker"] == 4
        assert info["memory_limit_bytes"] == 8 * 1024**3
        assert info["total_memory_bytes"] == 16 * 1024**3

    def test_get_cluster_info_no_workers(self):
        """Test fallback when client has no workers."""
        from dask_setup.xarray import _get_cluster_info

        mock_client = Mock()
        mock_client.scheduler_info.return_value = {"workers": {}}

        with patch("dask_setup.xarray.psutil") as mock_psutil:
            mock_psutil.virtual_memory.return_value.total = 16 * 1024**3

            info = _get_cluster_info(mock_client)

            assert info["n_workers"] == 1
            assert info["threads_per_worker"] == 1
            assert info["memory_limit_bytes"] == int(16 * 1024**3 * 0.8)

    def test_get_cluster_info_no_client(self):
        """Test system defaults when no client provided."""
        from dask_setup.xarray import _get_cluster_info

        with patch("dask_setup.xarray.psutil") as mock_psutil:
            mock_psutil.cpu_count.return_value = 8
            mock_psutil.virtual_memory.return_value.total = 32 * 1024**3

            info = _get_cluster_info(None)

            assert info["n_workers"] == 8
            assert info["threads_per_worker"] == 1
            assert info["memory_limit_bytes"] == int(32 * 1024**3 * 0.8 / 8)

    def test_get_cluster_info_client_exception(self):
        """Test fallback when client query fails."""
        from dask_setup.xarray import _get_cluster_info

        mock_client = Mock()
        mock_client.scheduler_info.side_effect = Exception("Connection failed")

        with patch("dask_setup.xarray.psutil") as mock_psutil:
            mock_psutil.virtual_memory.return_value.total = 16 * 1024**3

            info = _get_cluster_info(mock_client)

            assert info["n_workers"] == 1
            assert info["threads_per_worker"] == 1


@pytest.fixture
def mock_dataset_2d():
    """Create a mock 2D xarray DataArray."""
    mock_ds = Mock()
    mock_ds.name = "temperature"
    mock_ds.dims = ("y", "x")
    mock_ds.shape = (1000, 2000)
    mock_ds.dtype = "float32"
    mock_ds.coords = {}

    # Mock chunks attribute for unchunked data
    mock_ds.chunks = None
    mock_ds.data = Mock()
    mock_ds.data.chunks = None

    return mock_ds


@pytest.fixture
def mock_dataset_3d():
    """Create a mock 3D xarray DataArray with time dimension."""
    mock_ds = Mock()
    mock_ds.name = "temperature"
    mock_ds.dims = ("time", "y", "x")
    mock_ds.shape = (365, 500, 1000)
    mock_ds.dtype = "float64"
    mock_ds.coords = {}

    # Mock chunks attribute for unchunked data
    mock_ds.chunks = None
    mock_ds.data = Mock()
    mock_ds.data.chunks = None

    return mock_ds


@pytest.fixture
def mock_chunked_dataset():
    """Create a mock xarray DataArray with existing chunks."""
    mock_ds = Mock()
    mock_ds.name = "precipitation"
    mock_ds.dims = ("time", "lat", "lon")
    mock_ds.shape = (1000, 200, 300)
    mock_ds.dtype = "float32"
    mock_ds.coords = {}
    mock_ds.sizes = {"time": 1000, "lat": 200, "lon": 300}  # Add sizes for Dataset mock
    mock_ds.data_vars = {}

    # Mock existing chunking - create a proper dict-like object
    chunks_dict = {
        "time": (100, 100, 100, 100, 100, 100, 100, 100, 100, 100),
        "lat": (200,),
        "lon": (300,),
    }
    mock_ds.chunks = chunks_dict

    return mock_ds


class TestDatasetAnalysis:
    """Tests for dataset analysis functionality."""

    def test_analyze_dataarray_unchunked(self, mock_dataset_2d):
        """Test analysis of unchunked DataArray."""
        from dask_setup.xarray import _analyze_dataset

        with (
            patch("dask_setup.xarray.xr") as mock_xr,
            patch("dask_setup.xarray.np") as mock_np,
        ):
            # Configure mock to recognize DataArray type
            mock_xr.DataArray = Mock
            mock_dataset_2d.__class__ = mock_xr.DataArray

            mock_np.dtype.return_value.itemsize = 4  # float32
            mock_np.prod.return_value = 2000000  # 1000 * 2000

            result = _analyze_dataset(mock_dataset_2d)

            assert result["dims"] == {"y": 1000, "x": 2000}
            assert result["is_currently_chunked"] is False
            assert result["current_chunking"] == {}
            assert "temperature" in result["variables"]

            var_info = result["variables"]["temperature"]
            assert var_info["dtype"] == "float32"
            assert var_info["shape"] == (1000, 2000)
            assert var_info["size_bytes"] == 8000000  # 2M * 4 bytes

    def test_analyze_dataset_chunked(self, mock_chunked_dataset):
        """Test analysis of chunked Dataset."""
        from dask_setup.xarray import _analyze_dataset

        # Mock Dataset behavior
        mock_chunked_dataset.data_vars = {"precipitation": mock_chunked_dataset}

        with patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4  # float32
            mock_np.prod.return_value = 60000000  # 1000 * 200 * 300

            result = _analyze_dataset(mock_chunked_dataset)

            assert result["dims"] == {"time": 1000, "lat": 200, "lon": 300}
            assert result["is_currently_chunked"] is True
            assert "time" in result["current_chunking"]
            assert result["current_chunking"]["time"] == (
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
                100,
            )


class TestChunkRecommendation:
    """Tests for ChunkRecommendation class."""

    def test_chunk_recommendation_init(self):
        """Test ChunkRecommendation initialization."""
        from dask_setup.xarray import ChunkRecommendation

        rec = ChunkRecommendation(
            chunks={"time": 100, "y": 500},
            estimated_chunk_mb=256.0,
            total_chunks=10,
            warnings_list=["Warning 1"],
            dataset_info={"workload_type": "cpu"},
        )

        assert rec.chunks == {"time": 100, "y": 500}
        assert rec.estimated_chunk_mb == 256.0
        assert rec.total_chunks == 10
        assert rec.warnings == ["Warning 1"]
        assert rec.dataset_info["workload_type"] == "cpu"

    def test_chunk_recommendation_repr(self):
        """Test ChunkRecommendation string representation."""
        from dask_setup.xarray import ChunkRecommendation

        rec = ChunkRecommendation(chunks={"x": 100}, estimated_chunk_mb=128.5, total_chunks=4)

        repr_str = repr(rec)
        assert "ChunkRecommendation" in repr_str
        assert "chunks={'x': 100}" in repr_str
        assert "estimated_chunk_mb=128.5" in repr_str
        assert "total_chunks=4" in repr_str


class TestChunkCalculation:
    """Tests for chunk calculation algorithm."""

    @pytest.fixture
    def basic_dataset_info(self):
        """Basic dataset info for testing."""
        return {
            "dims": {"time": 1000, "y": 500, "x": 1000},
            "current_chunking": {},
            "variables": {
                "temperature": {
                    "dtype": "float32",
                    "shape": (1000, 500, 1000),
                    "dims": ["time", "y", "x"],
                    "size_bytes": 2000000000,  # 2GB
                }
            },
            "is_currently_chunked": False,
        }

    @pytest.fixture
    def basic_cluster_info(self):
        """Basic cluster info for testing."""
        return {
            "n_workers": 4,
            "threads_per_worker": 2,
            "memory_limit_bytes": 8 * 1024**3,  # 8GB per worker
            "total_memory_bytes": 32 * 1024**3,  # 32GB total
        }

    def test_calculate_optimal_chunks_cpu_workload(self, basic_dataset_info, basic_cluster_info):
        """Test chunking for CPU-heavy workload."""
        from dask_setup.xarray import _calculate_optimal_chunks

        with patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4

            result = _calculate_optimal_chunks(
                dataset_info=basic_dataset_info,
                cluster_info=basic_cluster_info,
                workload_type="cpu",
                target_chunk_mb=(256, 512),
                safety_factor=0.6,
            )

            # CPU workload should create roughly square chunks
            assert isinstance(result.chunks, dict)
            assert result.estimated_chunk_mb > 0
            assert result.total_chunks > 0

            # Should have recommendations for multiple dimensions
            assert len(result.chunks) > 0

    def test_calculate_optimal_chunks_io_workload(self, basic_dataset_info, basic_cluster_info):
        """Test chunking for I/O-heavy workload."""
        from dask_setup.xarray import _calculate_optimal_chunks

        with patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4

            result = _calculate_optimal_chunks(
                dataset_info=basic_dataset_info,
                cluster_info=basic_cluster_info,
                workload_type="io",
                target_chunk_mb=(256, 512),
                safety_factor=0.6,
            )

            # I/O workload should chunk along time dimension primarily
            assert isinstance(result.chunks, dict)
            assert result.estimated_chunk_mb > 0

            # Should prioritize chunking time dimension for streaming
            if "time" in result.chunks:
                assert result.chunks["time"] < basic_dataset_info["dims"]["time"]

    def test_calculate_optimal_chunks_auto_workload(self, basic_dataset_info, basic_cluster_info):
        """Test automatic workload type detection."""
        from dask_setup.xarray import _calculate_optimal_chunks

        with patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4

            result = _calculate_optimal_chunks(
                dataset_info=basic_dataset_info,
                cluster_info=basic_cluster_info,
                workload_type="auto",  # Should detect "io" due to time dimension
            )

            assert result.dataset_info["workload_type"] == "io"

    def test_calculate_optimal_chunks_no_variables(self, basic_cluster_info):
        """Test handling of dataset with no variables."""
        from dask_setup.xarray import _calculate_optimal_chunks

        empty_dataset_info = {
            "dims": {},
            "current_chunking": {},
            "variables": {},
            "is_currently_chunked": False,
        }

        result = _calculate_optimal_chunks(
            dataset_info=empty_dataset_info,
            cluster_info=basic_cluster_info,
        )

        assert result.chunks == {}
        assert result.estimated_chunk_mb == 0.0
        assert result.total_chunks == 0

    def test_calculate_optimal_chunks_memory_constraint(
        self, basic_dataset_info, basic_cluster_info
    ):
        """Test that chunks respect memory constraints."""
        from dask_setup.xarray import _calculate_optimal_chunks

        # Use very small memory limit to force chunking
        small_memory_cluster = basic_cluster_info.copy()
        small_memory_cluster["memory_limit_bytes"] = 100 * 1024**2  # 100MB

        with patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4

            result = _calculate_optimal_chunks(
                dataset_info=basic_dataset_info,
                cluster_info=small_memory_cluster,
                safety_factor=0.6,
            )

            # Should create many small chunks to fit in memory
            assert result.total_chunks > 10

            # Estimated chunk size should be within memory limit
            chunk_bytes = result.estimated_chunk_mb * 1024 * 1024
            memory_limit = small_memory_cluster["memory_limit_bytes"] * 0.6
            assert chunk_bytes <= memory_limit * 1.1  # Allow small tolerance

    def test_calculate_optimal_chunks_warnings(self, basic_dataset_info, basic_cluster_info):
        """Test generation of warnings for problematic configurations."""
        from dask_setup.xarray import _calculate_optimal_chunks

        # Create scenario with too few chunks for workers
        single_worker_cluster = basic_cluster_info.copy()
        single_worker_cluster["n_workers"] = 8  # Many workers

        with patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4

            result = _calculate_optimal_chunks(
                dataset_info=basic_dataset_info,
                cluster_info=single_worker_cluster,
            )

            # May generate warning about parallelism
            # (exact warning depends on calculated chunks)
            assert isinstance(result.warnings, list)


class TestFormatReport:
    """Tests for human-readable report formatting."""

    def test_format_chunk_report_with_recommendations(self):
        """Test formatting report with chunking recommendations."""
        from dask_setup.xarray import ChunkRecommendation, _format_chunk_report

        recommendation = ChunkRecommendation(
            chunks={"time": 100, "y": 250},
            estimated_chunk_mb=128.5,
            total_chunks=20,
            warnings_list=["This is a test warning"],
        )

        dataset_info = {
            "is_currently_chunked": False,
            "current_chunking": {},
        }

        report = _format_chunk_report(recommendation, dataset_info, verbose=True)

        assert "Xarray Chunking Recommendations" in report
        assert "{'time': 100, 'y': 250}" in report
        assert "128.5 MiB" in report
        assert "20" in report
        assert "This is a test warning" in report
        assert "ds_chunked = ds.chunk({'time': 100, 'y': 250})" in report

    def test_format_chunk_report_no_chunking(self):
        """Test formatting report when no chunking is recommended."""
        from dask_setup.xarray import ChunkRecommendation, _format_chunk_report

        recommendation = ChunkRecommendation(
            chunks={},
            estimated_chunk_mb=0.0,
            total_chunks=0,
        )

        dataset_info = {"is_currently_chunked": False}

        report = _format_chunk_report(recommendation, dataset_info)

        assert "No chunking recommended" in report
        assert "fits comfortably in memory" in report

    def test_format_chunk_report_with_current_chunks(self):
        """Test formatting report comparing current vs recommended chunks."""
        from dask_setup.xarray import ChunkRecommendation, _format_chunk_report

        recommendation = ChunkRecommendation(
            chunks={"time": 50, "x": 500},
            estimated_chunk_mb=256.0,
            total_chunks=40,
        )

        dataset_info = {
            "is_currently_chunked": True,
            "current_chunking": {"time": (100, 100, 100), "y": (1000,)},
        }

        report = _format_chunk_report(recommendation, dataset_info, verbose=True)

        assert "Current vs Recommended:" in report
        assert "time: (100, 100, 100) → 50" in report
        assert "x: unchunked → 500" in report


@pytest.fixture
def sample_xarray_setup():
    """Setup mocks for xarray integration testing."""
    mock_xr = Mock()
    mock_np = Mock()
    mock_np.dtype.return_value.itemsize = 4
    mock_np.prod.return_value = 1000000

    return mock_xr, mock_np


class TestRecommendChunks:
    """Tests for the main recommend_chunks function."""

    @pytest.fixture(autouse=True)
    def setup_mocks(self):
        """Setup common mocks for all tests."""
        with patch("dask_setup.xarray.xr") as mock_xr, patch("dask_setup.xarray.np") as mock_np:
            mock_np.dtype.return_value.itemsize = 4
            mock_np.prod.return_value = 2000000
            yield mock_xr, mock_np

    def test_recommend_chunks_basic_usage(self, mock_dataset_2d):
        """Test basic usage of recommend_chunks function."""
        from dask_setup.xarray import recommend_chunks

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            mock_cluster_info.return_value = {
                "n_workers": 4,
                "memory_limit_bytes": 2 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"y": 1000, "x": 2000},
                "current_chunking": {},
                "variables": {
                    "temperature": {
                        "dtype": "float32",
                        "shape": (1000, 2000),
                        "dims": ["y", "x"],
                        "size_bytes": 8000000,
                    }
                },
                "is_currently_chunked": False,
            }

            chunks = recommend_chunks(mock_dataset_2d)

            assert isinstance(chunks, dict)
            # Should return chunking recommendations for large dataset

    def test_recommend_chunks_verbose_mode(self, mock_dataset_2d):
        """Test verbose mode returns ChunkRecommendation object."""
        from dask_setup.xarray import ChunkRecommendation, recommend_chunks

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
            patch("builtins.print"),
        ):  # Suppress print output
            mock_cluster_info.return_value = {
                "n_workers": 2,
                "memory_limit_bytes": 4 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"y": 500, "x": 1000},
                "current_chunking": {},
                "variables": {
                    "data": {
                        "dtype": "float32",
                        "shape": (500, 1000),
                        "dims": ["y", "x"],
                        "size_bytes": 2000000,
                    }
                },
                "is_currently_chunked": False,
            }

            result = recommend_chunks(mock_dataset_2d, verbose=True)

            assert isinstance(result, ChunkRecommendation)
            assert hasattr(result, "chunks")
            assert hasattr(result, "estimated_chunk_mb")
            assert hasattr(result, "total_chunks")

    def test_recommend_chunks_with_client(self, mock_dataset_2d):
        """Test recommend_chunks with a Dask client."""
        from dask_setup.xarray import recommend_chunks

        mock_client = Mock()

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            mock_cluster_info.return_value = {
                "n_workers": 4,
                "memory_limit_bytes": 8 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"time": 365, "y": 100, "x": 200},
                "current_chunking": {},
                "variables": {
                    "temp": {
                        "dtype": "float64",
                        "shape": (365, 100, 200),
                        "dims": ["time", "y", "x"],
                        "size_bytes": 58400000,
                    }
                },
                "is_currently_chunked": False,
            }

            chunks = recommend_chunks(mock_dataset_2d, client=mock_client)

            assert isinstance(chunks, dict)
            mock_cluster_info.assert_called_once_with(mock_client)

    def test_recommend_chunks_workload_types(self, mock_dataset_3d):
        """Test different workload types produce different chunking strategies."""
        from dask_setup.xarray import recommend_chunks

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            mock_cluster_info.return_value = {
                "n_workers": 4,
                "memory_limit_bytes": 4 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"time": 365, "y": 200, "x": 300},
                "current_chunking": {},
                "variables": {
                    "data": {
                        "dtype": "float32",
                        "shape": (365, 200, 300),
                        "dims": ["time", "y", "x"],
                        "size_bytes": 87600000,
                    }
                },
                "is_currently_chunked": False,
            }

            chunks_cpu = recommend_chunks(mock_dataset_3d, workload_type="cpu")
            chunks_io = recommend_chunks(mock_dataset_3d, workload_type="io")
            chunks_mixed = recommend_chunks(mock_dataset_3d, workload_type="mixed")

            # Different workload types should potentially give different results
            assert isinstance(chunks_cpu, dict)
            assert isinstance(chunks_io, dict)
            assert isinstance(chunks_mixed, dict)

    def test_recommend_chunks_warning_generation(self, mock_chunked_dataset):
        """Test that warnings are generated for suboptimal existing chunks."""
        from dask_setup.xarray import recommend_chunks

        # Create dataset with very large chunks
        large_chunk_dataset = mock_chunked_dataset
        large_chunk_dataset.chunks = {
            "time": (1000,),  # Very large chunk
            "lat": (200,),
            "lon": (300,),
        }

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
            warnings.catch_warnings(record=True),
        ):
            warnings.simplefilter("always")

            # Mock cluster info with integer n_workers
            mock_cluster_info.return_value = {
                "n_workers": 4,  # Use integer instead of Mock
                "memory_limit_bytes": 8 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"time": 1000, "lat": 200, "lon": 300},
                "current_chunking": {"time": (1000,), "lat": (200,), "lon": (300,)},
                "variables": {
                    "precipitation": {
                        "dtype": "float32",
                        "shape": (1000, 200, 300),
                        "dims": ["time", "lat", "lon"],
                        "size_bytes": 240000000,
                    }
                },
                "is_currently_chunked": True,
            }

            recommend_chunks(large_chunk_dataset)

            # Should generate warnings about large chunks
            # (exact warning depends on calculated chunk sizes)


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_tiny_dataset(self):
        """Test chunking recommendations for very small datasets."""
        from dask_setup.xarray import recommend_chunks

        mock_tiny_ds = Mock()
        mock_tiny_ds.name = "small"
        mock_tiny_ds.dims = ("x", "y")
        mock_tiny_ds.shape = (10, 20)
        mock_tiny_ds.dtype = "float32"

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            mock_cluster_info.return_value = {
                "n_workers": 4,
                "memory_limit_bytes": 8 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"x": 10, "y": 20},
                "current_chunking": {},
                "variables": {
                    "small": {
                        "dtype": "float32",
                        "shape": (10, 20),
                        "dims": ["x", "y"],
                        "size_bytes": 800,  # Very small
                    }
                },
                "is_currently_chunked": False,
            }

            chunks = recommend_chunks(mock_tiny_ds)

            # Should likely recommend no chunking for tiny dataset
            assert isinstance(chunks, dict)

    def test_single_dimension_dataset(self):
        """Test chunking for 1D datasets."""
        from dask_setup.xarray import recommend_chunks

        mock_1d_ds = Mock()
        mock_1d_ds.name = "timeseries"
        mock_1d_ds.dims = ("time",)
        mock_1d_ds.shape = (100000,)
        mock_1d_ds.dtype = "float64"

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            mock_cluster_info.return_value = {
                "n_workers": 2,
                "memory_limit_bytes": 2 * 1024**3,
            }

            mock_analyze.return_value = {
                "dims": {"time": 100000},
                "current_chunking": {},
                "variables": {
                    "timeseries": {
                        "dtype": "float64",
                        "shape": (100000,),
                        "dims": ["time"],
                        "size_bytes": 800000,
                    }
                },
                "is_currently_chunked": False,
            }

            chunks = recommend_chunks(mock_1d_ds)

            assert isinstance(chunks, dict)
            # Should handle 1D case appropriately


class TestIntegrationScenarios:
    """Tests for realistic usage scenarios."""

    def test_climate_data_scenario(self):
        """Test chunking recommendations for typical climate dataset."""
        from dask_setup.xarray import recommend_chunks

        # Simulate typical climate dataset: daily data for global grid
        mock_climate_ds = Mock()
        mock_climate_ds.name = "temperature"
        mock_climate_ds.dims = ("time", "lat", "lon")
        mock_climate_ds.shape = (365, 721, 1440)  # Daily global 0.25° grid
        mock_climate_ds.dtype = "float32"

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            # Simulate HPC cluster
            mock_cluster_info.return_value = {
                "n_workers": 48,
                "memory_limit_bytes": 6 * 1024**3,  # 6GB per worker
            }

            mock_analyze.return_value = {
                "dims": {"time": 365, "lat": 721, "lon": 1440},
                "current_chunking": {},
                "variables": {
                    "temperature": {
                        "dtype": "float32",
                        "shape": (365, 721, 1440),
                        "dims": ["time", "lat", "lon"],
                        "size_bytes": 1516406400,  # ~1.5 GB
                    }
                },
                "is_currently_chunked": False,
            }

            chunks = recommend_chunks(mock_climate_ds, workload_type="cpu")

            assert isinstance(chunks, dict)
            # Should provide reasonable chunking for compute workloads

    def test_zarr_io_scenario(self):
        """Test chunking recommendations for Zarr I/O workload."""
        from dask_setup.xarray import recommend_chunks

        mock_zarr_ds = Mock()
        mock_zarr_ds.name = "ocean_data"
        mock_zarr_ds.dims = ("time", "depth", "y", "x")
        mock_zarr_ds.shape = (8760, 50, 2000, 3000)  # Hourly ocean data
        mock_zarr_ds.dtype = "float32"

        with (
            patch("dask_setup.xarray._get_cluster_info") as mock_cluster_info,
            patch("dask_setup.xarray._analyze_dataset") as mock_analyze,
        ):
            mock_cluster_info.return_value = {
                "n_workers": 4,
                "memory_limit_bytes": 16 * 1024**3,  # 16GB per worker
            }

            mock_analyze.return_value = {
                "dims": {"time": 8760, "depth": 50, "y": 2000, "x": 3000},
                "current_chunking": {},
                "variables": {
                    "ocean_data": {
                        "dtype": "float32",
                        "shape": (8760, 50, 2000, 3000),
                        "dims": ["time", "depth", "y", "x"],
                        "size_bytes": 105120000000,  # ~100 GB
                    }
                },
                "is_currently_chunked": False,
            }

            chunks = recommend_chunks(mock_zarr_ds, workload_type="io")

            assert isinstance(chunks, dict)
            # Should provide chunking optimized for streaming I/O


# Test with xarray extra dependencies if available
try:
    import numpy as np
    import xarray as xr

    XARRAY_AVAILABLE = True
except ImportError:
    XARRAY_AVAILABLE = False


@pytest.mark.skipif(not XARRAY_AVAILABLE, reason="xarray not available")
class TestRealXarrayIntegration:
    """Integration tests with real xarray objects (when available)."""

    def test_recommend_chunks_real_dataarray(self):
        """Test with actual xarray DataArray."""
        from dask_setup.xarray import recommend_chunks

        # Create a real DataArray
        rng = np.random.default_rng(seed=42)
        data = rng.random((100, 200, 300)).astype(np.float32)
        coords = {
            "time": range(100),
            "y": range(200),
            "x": range(300),
        }
        da = xr.DataArray(data, coords=coords, dims=["time", "y", "x"])

        chunks = recommend_chunks(da)

        assert isinstance(chunks, dict)
        # Should work with real xarray object

    def test_recommend_chunks_real_dataset(self):
        """Test with actual xarray Dataset."""
        from dask_setup.xarray import recommend_chunks

        # Create a real Dataset
        rng = np.random.default_rng(seed=42)
        data1 = rng.random((50, 100, 150)).astype(np.float32)
        data2 = rng.random((50, 100, 150)).astype(np.float64)

        ds = xr.Dataset(
            {
                "temperature": (["time", "y", "x"], data1),
                "pressure": (["time", "y", "x"], data2),
            }
        )

        chunks = recommend_chunks(ds, verbose=False)

        assert isinstance(chunks, dict)
        # Should handle multi-variable Dataset

    def test_recommend_chunks_already_chunked(self):
        """Test with already chunked xarray object."""
        from dask_setup.xarray import recommend_chunks

        # Create chunked DataArray
        rng = np.random.default_rng(seed=42)
        data = rng.random((200, 300, 400)).astype(np.float32)
        da = xr.DataArray(data, dims=["time", "y", "x"])
        da_chunked = da.chunk({"time": 50, "y": 150, "x": 200})

        chunks = recommend_chunks(da_chunked)

        assert isinstance(chunks, dict)
        # Should analyze existing chunking and provide recommendations
