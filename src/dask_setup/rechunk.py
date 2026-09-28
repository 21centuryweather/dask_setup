"""Rechunking helper for xarray datasets.

Provides a thin, HPC-aware wrapper that rechunks xarray datasets into new
chunk shapes using either the ``rechunker`` library (preferred, when
compatible) or a native ``xarray.to_zarr()`` fallback.

- Routes both the intermediate temp store and the output Zarr store to the
  Dask spill directory (typically ``$PBS_JOBFS``) for fast local NVMe I/O.
- Handles temp-store cleanup automatically after execution.
- Emits structured log messages during rechunking so users can track progress.
- Provides clear error messages when optional dependencies are missing.
- Falls back gracefully when ``rechunker`` is incompatible with the installed
  xarray version (e.g. the ``zarr_format`` argument added in xarray ≥ 2024.x)
  or zarr version (rechunker 0.5 uses the zarr 2 API, removed in zarr 3).
"""

from __future__ import annotations

import contextlib
import shutil
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger("rechunk")

__all__ = ["rechunk_dataset"]


def _warn_if_not_pure_split(ds: Any, target_chunks: dict) -> None:
    """Log a warning when *target_chunks* enlarges chunks along any dimension.

    Enlarging a chunk means the native rechunk path has to hold several source
    chunks in memory to build one target chunk, which is exactly the case
    ``rechunker`` exists to handle.  Shrinking chunks is safe.
    """
    existing = getattr(ds, "chunksizes", None) or {}
    growing = {}
    for dim, target in target_chunks.items():
        sizes = existing.get(dim)
        if not sizes or not isinstance(target, int):
            continue
        source = max(sizes)
        if target > source:
            growing[dim] = (source, target)

    if growing:
        logger.warning(
            "Native rechunk enlarges chunks; peak memory is not bounded on this path",
            dims=",".join(f"{d}:{src}->{tgt}" for d, (src, tgt) in sorted(growing.items())),
            hint="install a rechunker/xarray combination that works together for a "
            "memory-bounded rechunk, or lower the target chunk sizes",
        )


def _rechunker_incompatibility(exc: BaseException) -> str | None:
    """Name the library incompatibility behind a rechunker failure, or ``None``.

    Only these failures fall back to :func:`_rechunk_native`; anything else
    (out of memory, disk full, bad chunks) is a real error and is re-raised.

    - xarray added a required ``zarr_format`` argument to
      ``extract_zarr_variable_encoding()``, which rechunker does not pass.
    - rechunker 0.5 is written against the zarr 2 API (``zarr.core.Array``
      and friends), which zarr 3 removed.  Before this was recognised,
      ``rechunk_dataset`` failed outright on any zarr 3 environment instead
      of falling back.
    """
    message = str(exc)
    if (
        isinstance(exc, TypeError)
        and "zarr_format" in message
        and "extract_zarr_variable_encoding" in message
    ):
        return "xarray's extract_zarr_variable_encoding() requires zarr_format"
    if isinstance(exc, AttributeError) and "zarr" in message:
        return "rechunker uses the zarr 2 API, which the installed zarr removed"
    return None


def _rechunk_native(ds: Any, target_chunks: dict, output_path: Path, xr: Any) -> None:
    """Rechunk *ds* to *target_chunks* and write to *output_path* via xarray.to_zarr().

    This is the fallback path used when ``rechunker`` is incompatible with the
    installed xarray or zarr version (see :func:`_rechunker_incompatibility`).

    .. warning::

       Unlike ``rechunker``, this path gives **no memory bound**.  ``rechunker``
       stages through an intermediate store precisely so that peak memory stays
       under an explicit ``max_mem``; ``ds.chunk(...).to_zarr(...)`` has no such
       mechanism.  Whenever a target chunk is larger than the source chunk along
       some dimension, producing it requires every source chunk it overlaps to
       be resident at once, and the graph can hold many such groups in flight.
       Peak memory is bounded by the per-worker limit only when every target
       chunk is a subdivision of a single source chunk -- i.e. a pure split.

    A warning is logged when the requested rechunk is not a pure split, so the
    risk is visible before the job dies rather than after.

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
    target_chunks : dict[str, int]
    output_path : Path  — destination Zarr store (must not already exist)
    xr : the xarray module (already imported by caller)
    """
    _warn_if_not_pure_split(ds, target_chunks)
    rechunked = ds.chunk(target_chunks)
    rechunked.to_zarr(str(output_path), mode="w")
    # Consolidate metadata so xr.open_zarr(consolidated=True) works
    with contextlib.suppress(Exception):
        import zarr as _zarr

        _zarr.consolidate_metadata(str(output_path))


def _open_rechunked(output_path: Path, ds: Any, xr: Any) -> Any:
    """Open the rechunked Zarr store and return Dataset or DataArray to match *ds*."""
    try:
        rechunked = xr.open_zarr(str(output_path), consolidated=True)
    except Exception:
        rechunked = xr.open_zarr(str(output_path), consolidated=False)

    if isinstance(ds, xr.DataArray):
        var_name = ds.name or list(rechunked.data_vars)[0]
        return rechunked[var_name]
    return rechunked


def rechunk_dataset(
    ds: Any,  # xr.Dataset | xr.DataArray — xarray is optional
    target_chunks: dict[str, int],
    client: Any,  # dask.distributed.Client
    dask_tmp: str | Path,
    output_path: str | Path | None = None,
    max_mem: str = "2GB",
    cleanup_temp: bool = True,
) -> Any:  # xr.Dataset | xr.DataArray
    """Rechunk an xarray Dataset using rechunker with HPC-friendly temp placement.

    Wraps the ``rechunker`` library to safely rechunk large datasets to new
    chunk shapes.  Both the intermediate Zarr temp store and the final output
    Zarr store are written to the Dask spill/temp directory (``dask_tmp``),
    which is typically ``$PBS_JOBFS`` fast local storage on NCI Gadi.

    Parameters
    ----------
    ds : xr.Dataset or xr.DataArray
        Dataset to rechunk.  Must be backed by dask arrays (opened lazily).
    target_chunks : dict[str, int]
        Target chunk sizes, e.g. ``{"time": 10, "lat": 200, "lon": 200}``.
        Dimensions not listed keep their existing chunk sizes.
    client : dask.distributed.Client
        Active Dask client used to execute the rechunking task graph.
    dask_tmp : str or Path
        Dask spill/temp directory — the third element returned by
        :func:`setup_dask_client`.  Intermediate and output stores are
        placed here unless *output_path* is specified.
    output_path : str, Path, or None
        Destination Zarr store path for the rechunked dataset.  If ``None``,
        a timestamped subdirectory under *dask_tmp* is used.  This path is
        **not** cleaned up automatically; it persists for the job's lifetime.
    max_mem : str
        Maximum memory rechunker may use per task, e.g. ``"1GB"``, ``"4GB"``.
        Defaults to ``"2GB"``.  Should be at most 50 % of per-worker memory.
    cleanup_temp : bool
        If ``True`` (default), the rechunker intermediate temp store is deleted
        after rechunking completes or fails.  The output store is never deleted
        automatically.

    Returns
    -------
    xr.Dataset or xr.DataArray
        The rechunked dataset opened lazily from *output_path*.

    Raises
    ------
    ImportError
        If ``rechunker`` or ``zarr`` are not installed.
    RuntimeError
        If rechunking fails for any reason.  Partial output is cleaned up.

    Notes
    -----
    Install optional dependencies with::

        pip install rechunker zarr

    Examples
    --------
    ::

        from dask_setup import setup_dask_client, recommend_chunks, rechunk_dataset

        client, cluster, tmp = setup_dask_client(workload_type="io")
        ds = xr.open_zarr("era5.zarr")
        chunks = recommend_chunks(ds, client)
        ds_rechunked = rechunk_dataset(ds, chunks, client, tmp)
        ds_rechunked.to_zarr("era5_rechunked.zarr")

    See Also
    --------
    recommend_chunks : Compute optimal target_chunks for a dataset.
    validate_chunks  : Check whether existing chunks need rechunking.
    """
    # --- Dependency checks ---------------------------------------------------
    try:
        import rechunker as _rechunker
    except ImportError as exc:
        raise ImportError(
            "rechunk_dataset requires the rechunker library.\n"
            "Install with: pip install rechunker zarr"
        ) from exc

    try:
        import zarr as _zarr  # noqa: F401 — confirms zarr is available
    except ImportError as exc:
        raise ImportError(
            "rechunk_dataset requires zarr for temporary Zarr stores.\n"
            "Install with: pip install rechunker zarr"
        ) from exc

    try:
        import xarray as xr
    except ImportError as exc:
        raise ImportError(
            "rechunk_dataset requires xarray.\nInstall with: pip install xarray"
        ) from exc

    # --- Path setup ----------------------------------------------------------
    dask_tmp = Path(dask_tmp)
    if not dask_tmp.exists():
        raise ValueError(
            f"dask_tmp directory does not exist: {dask_tmp}\n"
            "Pass the third return value of setup_dask_client() as dask_tmp."
        )

    timestamp = int(time.time())

    if output_path is None:
        output_path = dask_tmp / f"rechunked_{timestamp}.zarr"
        logger.debug("No output_path provided; using temp location", path=str(output_path))

    output_path = Path(output_path)
    temp_store_path = dask_tmp / f"rechunk_tmp_{timestamp}.zarr"

    logger.info(
        "Starting rechunking",
        target_chunks=str(target_chunks),
        output_path=str(output_path),
        temp_store=str(temp_store_path),
        max_mem=max_mem,
    )

    # --- Rechunking ----------------------------------------------------------
    try:
        plan = _rechunker.rechunk(
            source=ds,
            target_chunks=target_chunks,
            max_mem=max_mem,
            target_store=str(output_path),
            temp_store=str(temp_store_path),
        )
        plan.execute()
        logger.info("Rechunking complete", output_path=str(output_path))

    except Exception as exc:
        # Remove partial output to avoid leaving corrupt Zarr stores on disk
        for p in (output_path, temp_store_path):
            if p.exists():
                shutil.rmtree(p, ignore_errors=True)

        reason = _rechunker_incompatibility(exc)
        if reason is None:
            logger.warning("Rechunking failed; cleaned up partial output", error=str(exc))
            raise RuntimeError(
                f"rechunk_dataset failed: {exc}\n"
                f"  target_chunks = {target_chunks}\n"
                f"  output_path   = {output_path}\n"
                "Check that max_mem is not larger than per-worker memory, "
                "and that dask_tmp has sufficient free space."
            ) from exc

        logger.warning(
            "rechunker is incompatible with the installed libraries; "
            "falling back to native xarray.to_zarr() rechunking",
            reason=reason,
            error=str(exc),
        )
        try:
            _rechunk_native(ds, target_chunks, output_path, xr)
            logger.info("Rechunking complete (native fallback)", output_path=str(output_path))
        except Exception as fallback_exc:
            if output_path.exists():
                shutil.rmtree(output_path, ignore_errors=True)
            raise RuntimeError(
                f"rechunk_dataset failed (native fallback): {fallback_exc}\n"
                f"  target_chunks = {target_chunks}\n"
                f"  output_path   = {output_path}\n"
            ) from fallback_exc

    finally:
        # Always attempt to clean up the intermediate temp store
        if cleanup_temp and temp_store_path.exists():
            shutil.rmtree(temp_store_path, ignore_errors=True)
            logger.debug("Intermediate temp store removed", path=str(temp_store_path))

    # --- Open and return rechunked dataset -----------------------------------
    return _open_rechunked(output_path, ds, xr)
