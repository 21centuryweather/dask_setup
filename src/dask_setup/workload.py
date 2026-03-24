"""Workload type inference from dataset structure.

Provides :func:`infer_workload_type`, which inspects an xarray Dataset or
DataArray and returns one of ``"cpu"``, ``"io"``, or ``"mixed"`` based on
dimension names, variable dtypes, and array sizes.

This is used internally by :func:`~dask_setup.client.setup_dask_client` when
``workload_type="auto"`` is requested, and can also be called directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .logging import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger("workload")

# ---------------------------------------------------------------------------
# Signal tables
# ---------------------------------------------------------------------------

# Dimension names that strongly suggest compute-heavy (CPU-bound) workloads.
# These are common in climate / atmospheric / ocean science datasets.
_CPU_DIM_SIGNALS: frozenset[str] = frozenset(
    {
        "time",
        "lat",
        "latitude",
        "lon",
        "longitude",
        "level",
        "lev",
        "plev",
        "pressure",
        "depth",
        "height",
        "altitude",
        "x",
        "y",
        "z",
        "ni",
        "nj",
        "nk",
    }
)

# Variable names (lowercased) that appear in compute-heavy scientific datasets.
_CPU_VAR_SIGNALS: frozenset[str] = frozenset(
    {
        "temp",
        "temperature",
        "t",
        "u",
        "v",
        "w",
        "precip",
        "precipitation",
        "pr",
        "sst",
        "tas",
        "uas",
        "vas",
        "psl",
        "hus",
        "pressure",
        "humidity",
        "q",
        "rh",
        "wind_speed",
        "ws",
        "wind_dir",
        "wd",
        "geopotential",
        "phi",
        "salinity",
        "s",
        "density",
        "rho",
        "so",
        "thetao",
        "zos",
    }
)

# dtypes that indicate heavy numerical computation
_FLOAT_DTYPES: frozenset[str] = frozenset(
    {"float16", "float32", "float64", "float128", "complex64", "complex128"}
)

# dtypes that indicate storage / index / mask data (I/O-heavy)
_INT_OR_BYTE_DTYPES: frozenset[str] = frozenset(
    {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "bool",
        "bool_",
        "bytes_",
        "str_",
        "object_",
        "object",
    }
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def infer_workload_type(ds: Any | None = None) -> str:
    """Infer the most appropriate workload type from a dataset's structure.

    Examines variable dtypes, dimension names, and array sizes to decide
    whether the workload is primarily:

    - **CPU-bound** (→ ``"cpu"``): Many floating-point variables, scientific
      dimension names (time/lat/lon/lev), large arrays per variable.
    - **I/O-bound** (→ ``"io"``): Mostly integer / byte / bool variables,
      many small variables typical of lookup tables or mask datasets.
    - **Mixed** (→ ``"mixed"``): When the signal is ambiguous or no dataset
      is provided.

    Parameters
    ----------
    ds : xr.Dataset, xr.DataArray, or None
        Dataset to inspect. When ``None``, returns ``"mixed"`` immediately.

    Returns
    -------
    str
        One of ``"cpu"``, ``"io"``, or ``"mixed"``.

    Notes
    -----
    The heuristic is intentionally conservative — it returns ``"mixed"`` when
    the evidence is not strong (score difference ≤ 2).  Users can always
    override with an explicit ``workload_type=`` argument.

    The signal tables are tuned for CF-convention geoscience / climate
    datasets (the primary NCI Gadi use-case) but are general enough for most
    scientific array workloads.

    Examples
    --------
    ::

        import xarray as xr
        from dask_setup import infer_workload_type

        ds = xr.open_zarr("era5.zarr")
        wt = infer_workload_type(ds)  # likely "cpu"
        client, cluster, tmp = setup_dask_client(workload_type=wt)
    """
    if ds is None:
        logger.debug("No dataset provided — defaulting workload_type to 'mixed'")
        return "mixed"

    cpu_score = 0
    io_score = 0

    # ------------------------------------------------------------------
    # 1. Dimension name signals
    # ------------------------------------------------------------------
    try:
        dims = {str(d).lower() for d in ds.dims}
        cpu_dim_hits = dims & _CPU_DIM_SIGNALS
        if cpu_dim_hits:
            # Each matching dim adds 2 CPU points; cap at 8 so dimensions
            # alone cannot overwhelm the dtype signal.
            cpu_score += min(8, len(cpu_dim_hits) * 2)
            logger.debug("CPU-signal dimensions detected", dims=sorted(cpu_dim_hits))
    except Exception:  # noqa: S110
        pass

    # ------------------------------------------------------------------
    # 2. Variable dtype and name signals
    # ------------------------------------------------------------------
    try:
        if hasattr(ds, "data_vars"):
            variables = dict(ds.data_vars)
        else:
            # DataArray — treat as single variable
            var_name = getattr(ds, "name", None) or "data"
            variables = {var_name: ds}

        float_count = 0
        int_count = 0

        for var_name, var in variables.items():
            try:
                dtype_str = str(getattr(var, "dtype", "")).replace("dtype('", "").rstrip("')")
                if dtype_str in _FLOAT_DTYPES:
                    float_count += 1
                    # Boost CPU score if the variable name is a known science field
                    if str(var_name).lower() in _CPU_VAR_SIGNALS:
                        cpu_score += 2
                elif dtype_str in _INT_OR_BYTE_DTYPES:
                    int_count += 1
            except Exception:  # noqa: S110
                pass

        total = float_count + int_count
        if total > 0:
            float_ratio = float_count / total
            if float_ratio >= 0.70:
                cpu_score += 3
                logger.debug(
                    "Float-dominant dataset — CPU-bound signal",
                    float_vars=float_count,
                    total_vars=total,
                )
            elif float_ratio <= 0.30:
                io_score += 3
                logger.debug(
                    "Integer/byte-dominant dataset — I/O-bound signal",
                    int_vars=int_count,
                    total_vars=total,
                )

    except Exception:  # noqa: S110
        pass

    # ------------------------------------------------------------------
    # 3. Bytes-per-variable signal
    # ------------------------------------------------------------------
    try:
        nbytes = getattr(ds, "nbytes", 0) or 0
        n_vars = len(variables) if "variables" in dir() else 1
        if nbytes > 0 and n_vars > 0:
            bpv = nbytes / n_vars
            _100_mib = 100 * 1024**2
            _1_mib = 1 * 1024**2
            if bpv > _100_mib:
                # Large arrays per variable → compute-intensive
                cpu_score += 1
            elif bpv < _1_mib:
                # Many tiny variables → I/O pattern (lots of small reads)
                io_score += 1
    except Exception:  # noqa: S110
        pass

    # ------------------------------------------------------------------
    # 4. Decision — require a clear margin to avoid false positives
    # ------------------------------------------------------------------
    logger.debug("Workload inference scores", cpu_score=cpu_score, io_score=io_score)

    if cpu_score >= io_score + 3:
        result = "cpu"
    elif io_score >= cpu_score + 3:
        result = "io"
    else:
        result = "mixed"

    logger.info(
        "Inferred workload type",
        workload_type=result,
        cpu_score=cpu_score,
        io_score=io_score,
    )
    return result
