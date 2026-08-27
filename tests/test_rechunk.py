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
