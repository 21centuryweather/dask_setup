"""Tests for dask_setup.rechunk memory-safety warnings."""

from __future__ import annotations

import pytest

from dask_setup.rechunk import _warn_if_not_pure_split

xr = pytest.importorskip("xarray")
np = pytest.importorskip("numpy")


def _ds(chunks):
    return xr.Dataset({"v": (("time", "x"), np.zeros((100, 100)))}).chunk(chunks)


class TestNativeRechunkMemoryWarning:
    """_rechunk_native's docstring claimed peak memory stays within the
    per-worker budget "even for very large datasets".

    That is only true when every target chunk is a subdivision of one source
    chunk.  Enlarging a chunk requires every source chunk it overlaps to be
    resident at once -- which is the whole reason rechunker exists, and this
    is the fallback used when rechunker is unavailable.
    """

    @pytest.mark.unit
    def test_shrinking_chunks_is_silent(self, caplog):
        ds = _ds({"time": 10, "x": 10})
        with caplog.at_level("WARNING", logger="dask_setup.rechunk"):
            _warn_if_not_pure_split(ds, {"time": 5, "x": 5})
        assert caplog.records == []

    @pytest.mark.unit
    def test_identical_chunks_are_silent(self, caplog):
        ds = _ds({"time": 10, "x": 10})
        with caplog.at_level("WARNING", logger="dask_setup.rechunk"):
            _warn_if_not_pure_split(ds, {"time": 10, "x": 10})
        assert caplog.records == []

    @pytest.mark.unit
    def test_enlarging_a_chunk_warns(self, caplog):
        ds = _ds({"time": 10, "x": 10})
        with caplog.at_level("WARNING", logger="dask_setup.rechunk"):
            _warn_if_not_pure_split(ds, {"time": 50, "x": 10})
        assert len(caplog.records) == 1
        assert "not bounded" in caplog.records[0].getMessage()

    @pytest.mark.unit
    def test_the_warning_names_the_offending_dimension(self, caplog):
        ds = _ds({"time": 10, "x": 10})
        with caplog.at_level("WARNING", logger="dask_setup.rechunk"):
            _warn_if_not_pure_split(ds, {"time": 50, "x": 100})
        context = caplog.records[0]._extra_context
        assert "time:10->50" in context["dims"]
        assert "x:10->100" in context["dims"]

    @pytest.mark.unit
    def test_an_unchunked_dataset_does_not_crash(self, caplog):
        ds = xr.Dataset({"v": (("time", "x"), np.zeros((10, 10)))})
        with caplog.at_level("WARNING", logger="dask_setup.rechunk"):
            _warn_if_not_pure_split(ds, {"time": 5})
        assert caplog.records == []

    @pytest.mark.unit
    def test_unknown_dimensions_are_ignored(self, caplog):
        ds = _ds({"time": 10, "x": 10})
        with caplog.at_level("WARNING", logger="dask_setup.rechunk"):
            _warn_if_not_pure_split(ds, {"nonexistent": 999})
        assert caplog.records == []


class TestRechunkerIncompatibilityFallback:
    """rechunk_dataset falls back to xarray.to_zarr() only for library mismatches.

    It used to recognise just one: xarray's zarr_format signature change.
    rechunker 0.5 is written against the zarr 2 API, so on zarr 3 it fails with
    an AttributeError -- and rechunk_dataset raised instead of falling back,
    which made it unusable in any current environment.
    """

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "exc",
        [
            TypeError("extract_zarr_variable_encoding() missing 1 required argument: 'zarr_format'"),
            AttributeError("module 'zarr.core' has no attribute 'Array'"),
        ],
    )
    def test_library_mismatches_are_recognised(self, exc):
        from dask_setup.rechunk import _rechunker_incompatibility

        assert _rechunker_incompatibility(exc) is not None

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "exc",
        [
            MemoryError("out of memory"),
            OSError("No space left on device"),
            TypeError("unsupported operand type(s)"),
            AttributeError("'NoneType' object has no attribute 'chunks'"),
        ],
    )
    def test_real_failures_are_not_masked(self, exc):
        from dask_setup.rechunk import _rechunker_incompatibility

        assert _rechunker_incompatibility(exc) is None

    @pytest.mark.unit
    def test_zarr3_attribute_error_falls_back_to_native(self, tmp_path, monkeypatch):
        pytest.importorskip("zarr")
        import sys
        import types

        from dask_setup.rechunk import rechunk_dataset

        def rechunk(**kwargs):
            raise AttributeError("module 'zarr.core' has no attribute 'Array'")

        monkeypatch.setitem(sys.modules, "rechunker", types.SimpleNamespace(rechunk=rechunk))

        out = rechunk_dataset(
            _ds({"time": 10}),
            target_chunks={"time": 50},
            client=None,
            dask_tmp=tmp_path,
            output_path=tmp_path / "out.zarr",
        )

        assert out.chunks["time"] == (50, 50)
        np.testing.assert_array_equal(out["v"].values, np.zeros((100, 100)))

    @pytest.mark.unit
    def test_other_failures_still_raise(self, tmp_path, monkeypatch):
        import sys
        import types

        from dask_setup.rechunk import rechunk_dataset

        def rechunk(**kwargs):
            raise OSError("No space left on device")

        monkeypatch.setitem(sys.modules, "rechunker", types.SimpleNamespace(rechunk=rechunk))

        with pytest.raises(RuntimeError, match="No space left on device"):
            rechunk_dataset(
                _ds({"time": 10}),
                target_chunks={"time": 50},
                client=None,
                dask_tmp=tmp_path,
                output_path=tmp_path / "out.zarr",
            )
        assert not (tmp_path / "out.zarr").exists()
