# I/O Optimization for Scientific Data Formats

`dask_setup` provides two levels of xarray chunking recommendations:

- **`recommend_chunks()`** — simple, cluster-aware chunking based on worker memory and workload type
- **`recommend_io_chunks()`** — storage-format-aware chunking optimised for Zarr, NetCDF, Zarr v3, Kerchunk, Parquet, and cloud storage

---

## recommend_chunks() — Simple Chunking

`recommend_chunks()` targets **256–512 MiB chunks** and respects the cluster's per-worker memory limit (60% safety factor).

```python
from dask_setup import setup_dask_client, recommend_chunks

client, cluster, dask_tmp = setup_dask_client("cpu")

# Cluster-aware recommendations
chunks = recommend_chunks(ds, client, verbose=True)
ds = ds.chunk(chunks)

# Without a client (uses psutil for memory)
chunks = recommend_chunks(ds, workload_type="cpu")
```

Enable chunking guidance at cluster setup time:

```python
client, cluster, dask_tmp = setup_dask_client("cpu", suggest_chunks=True)
```

Or pass a dataset to get recommendations tuned to its structure:

```python
ds = xr.open_zarr("era5.zarr")
client, cluster, dask_tmp, chunks = setup_dask_client(ds=ds, suggest_chunks=True)
ds_opt = ds.chunk(chunks)
```

**Strategies by workload type:**

| Type | Strategy |
|------|----------|
| `"cpu"` | Square-ish chunks — equal size across spatial dims for compute efficiency |
| `"io"` | Stream-friendly — chunk along the record/time dimension, keep spatial dims whole |
| `"mixed"` | Balanced — chunk spatial dims while keeping time dimension large |
| `"auto"` | Detect from dataset dimensions (time-like dim → `"io"`, else `"cpu"`) |

> This table is about **chunk shape**, which is a separate question from the
> worker topology the same `workload_type` selects. A `"io"` chunk shape is often
> right for NetCDF input even though an `"io"` *topology* is not — see
> [Workload Types](index.md#workload-types).

### chunk_domain — Pin a Dimension Group to Memory

`chunk_domain` lets you declare which set of dimensions should be *chunked* and which should be *fully loaded into memory* (one chunk spanning the entire dimension, equivalent to `ds.chunk({dim: -1})`):

```python
# chunk_domain="spatial" — chunk lat/lon, load all time steps at once
chunks = recommend_chunks(ds, chunk_domain="spatial")
ds = ds.chunk(chunks)
# → {"time": -1, "lat": 45, "lon": 90}

# chunk_domain="temporal" — chunk time, load full spatial grid per chunk
chunks = recommend_chunks(ds, chunk_domain="temporal")
ds = ds.chunk(chunks)
# → {"time": 30, "lat": -1, "lon": -1}
```

| Value | Dimensions chunked | Dimensions fully loaded |
|-------|--------------------|------------------------|
| `"spatial"` | `lat`, `lon`, and other spatial dims | `time`, `date`, `step`, and other temporal dims |
| `"temporal"` | `time`, `date`, `step`, and other temporal dims | `lat`, `lon`, and other spatial dims |
| `None` (default) | All dimensions | None |

**When to use `chunk_domain="spatial"`:** Workflows that reduce or aggregate across all time steps but need fine spatial resolution — e.g. computing a per-pixel climatology, fitting a trend at every grid point, or writing a spatially-partitioned Zarr store.

**When to use `chunk_domain="temporal"`:** Workflows that process the full spatial domain at each time step — e.g. spatial statistics (global mean, field norm), regridding, or applying a spatial filter to each time slice.

`recommend_chunks()` classifies dimension names automatically:

- **Distinctive names match anywhere in the dimension name.** Temporal: `"time"`, `"date"`, `"step"`, `"record"`, `"month"`, `"year"`, `"hour"`. Spatial: `"lat"`, `"lon"`, `"level"`, `"depth"`, `"height"`, `"pressure"`, `"north"`/`"south"`/`"east"`/`"west"`. So `nTime` is temporal and `latitude_1` is spatial.
- **Short names must match the whole dimension name**, optionally with an `n`/`num` prefix or a numeric suffix: `"x"`, `"y"`, `"z"`, `"ni"`, `"nj"`, `"lev"`. So `x`, `nx`, `x_2` and `lev1` are spatial — but `proxy`, `flux`, `max`, `zone` and `size` are not.

A warning is emitted if no dimensions in the dataset match the requested domain.

> **Changed in v2.2.** The short names were matched by substring too, which
> classified any dimension containing an `x`, `y` or `z` as spatial. On a
> dataset with a `flux` or `zone` dimension that silently drove chunk sizing
> for an axis that has nothing to do with space.

> **Tip:** `chunk_domain` can be combined with any `workload_type` and with `verbose=True` to see the locked dimensions in the printed report.

### Chunk Validation

`validate_chunks()` warns when existing chunking will produce chunks that are too large or too small relative to the cluster's per-worker memory limit:

```python
from dask_setup import validate_chunks

warnings = validate_chunks(ds, client)
for w in warnings:
    print(w)
```

Chunk size is estimated as the product of the chunk sizes across **all** dimensions — i.e. the actual in-memory footprint of one chunk from the Dask task graph. For a dataset chunked `{time: 24, level: 1, lat: 90, lon: 180}` at float32, the estimate is `24 × 1 × 90 × 180 × 4 bytes ≈ 1.5 MiB` per chunk, regardless of the full dataset extents.

Both `validate_chunks()` and the pre-recommendation check inside `recommend_chunks()` use this same logic — so datasets that are already well-chunked will not trigger spurious OOM warnings after calling `recommend_chunks()`.

---

## recommend_io_chunks() — Storage-Format-Aware Chunking

`recommend_io_chunks()` dispatches to a format-specific optimizer and returns an `IORecommendation` with chunks, compression settings, storage options, and throughput estimates.

```python
from dask_setup import recommend_io_chunks

rec = recommend_io_chunks(ds, path_or_url="s3://bucket/climate.zarr", verbose=True)
print(rec.chunks)
print(rec.compression)
print(rec.estimated_throughput_mb_s)

ds_opt = ds.chunk(rec.chunks)
```

### Supported Formats

| Format key | Detected by | Optimizer |
|-----------|-------------|-----------|
| `"zarr"` | `.zarr` extension, zarr store attributes | `ZarrOptimizer` |
| `"zarr_v3"` | `zarr.json` metadata, `zarr_format=3` store attribute | `ZarrV3Optimizer` |
| `"kerchunk"` | fsspec `ReferenceFileSystem`, `ManifestArray`, `.json` reference files | `KerchunkOptimizer` |
| `"netcdf"` | `.nc`, `.nc4`, `.cdf`, `.h5` extension | `NetCDFOptimizer` |
| `"parquet"` | `.parquet`, `.parq`, `.pq` extension (Dask DataFrame path) | see `recommend_parquet_chunks()` |

Format detection order: kerchunk → zarr_v3 → zarr → netcdf (kerchunk is checked first because it presents a zarr-like interface).

---

## Zarr Optimization

**Zarr (v2):** 128–512 MiB chunks, `zstd` for cloud, `lz4`/`blosc` for local.

```python
rec = recommend_io_chunks(
    ds,
    path_or_url="s3://climate-data/era5.zarr",
    access_pattern="compute",
    target_chunk_mb=(256, 512),
    verbose=True,
)
```

**Zarr v3:** `ZarrV3Optimizer` handles the new zarr-python ≥ 3.0 API, including sharding via `zarr.codecs.ShardingCodec`. When the outer chunk exceeds 64 MiB, a sharding config (outer/inner shapes, index codec) is returned in `rec.extra["sharding"]`.

```python
rec = recommend_io_chunks(ds, path_or_url="data.zarr")

if "sharding" in rec.extra:
    sharding_cfg = rec.extra["sharding"]
    # sharding_cfg contains outer_chunks, inner_chunks, index_codec
```

`ZarrV3Optimizer` automatically selects `blosc2:zstd` or `blosc2:lz4` when the `blosc2` package is installed.

**Best practices for Zarr:**
- Use 256–512 MiB target chunks for cloud; 128–256 MiB for local
- Keep time dimension large for temporal analysis
- Use consolidated metadata for faster initialisation
- Consider `zstd` for archival, `lz4` for interactive analysis

---

## NetCDF Optimization

NetCDF (HDF5 backend) is more conservative with chunk sizes due to HDF5 overhead.

```python
rec = recommend_io_chunks(
    ds,
    path_or_url="data.nc4",
    access_pattern="streaming",
    target_chunk_mb=(64, 256),
)
```

**Best practices for NetCDF:**
- 64–256 MiB chunks (HDF5 performance degrades with very large chunks)
- Be careful with unlimited dimensions (usually `time`)
- Enable shuffle filter and `zlib` compression for good ratios
- Consider least-significant-digit rounding for floating-point data

---

## Kerchunk / VirtualiZarr

`KerchunkOptimizer` detects datasets opened via a Kerchunk or VirtualiZarr reference filesystem (`fsspec.ReferenceFileSystem`, `ManifestArray`, `.json` reference files). It returns the **existing chunk layout unchanged** — rechunking a Kerchunk dataset requires a full data copy and is usually not advisable.

```python
import xarray as xr
from dask_setup import recommend_io_chunks

# Dataset opened via Kerchunk reference
ds = xr.open_zarr("combined.json", consolidated=False)

rec = recommend_io_chunks(ds, path_or_url="combined.json")
# rec.warnings will contain a note about fixed byte-range boundaries
print(rec.warnings)
```

---

## Parquet / Arrow Recommendations

For Dask DataFrame workloads, `recommend_parquet_chunks()` estimates optimal `rows_per_partition` based on per-worker memory limits and row byte-size:

```python
import dask.dataframe as dd
from dask_setup import setup_dask_client, recommend_parquet_chunks

client, cluster, _ = setup_dask_client("cpu")
df = dd.read_parquet("data/*.parquet")

rows = recommend_parquet_chunks(df, client)
df_opt = df.repartition(npartitions=len(df) // rows)

# For full details:
from dask_setup import ParquetRecommendation
rec = recommend_parquet_chunks(df, client, verbose=True)
print(rec.summary())
# rows_per_partition, compression, estimated_partition_mb,
# extra["row_group_size"], extra["write_metadata_file"]
```

Auto-selects compression: `snappy` for local storage, `zstd` for cloud. Warns on very wide tables (>500 columns).

---

## Cloud Storage

### AWS S3

```python
rec = recommend_io_chunks(
    ds,
    path_or_url="s3://my-bucket/data.zarr",
    access_pattern="sequential",
)
# rec.storage_options includes anon, default_cache_type, default_block_size
```

### Google Cloud Storage

```python
rec = recommend_io_chunks(ds, path_or_url="gs://my-bucket/data.zarr")
```

### Azure Blob Storage

```python
rec = recommend_io_chunks(ds, path_or_url="azure://container/data.zarr")
```

---

## Access Patterns

| Pattern | Chunk guidance | Use case |
|---------|---------------|----------|
| `"sequential"` | Medium–large | Time-series scans, linear pipelines |
| `"random"` | Small–medium | Interactive analysis, point lookups |
| `"streaming"` | Small | Real-time processing |
| `"compute"` | Large | CPU-intensive reductions |

---

## Chunk Size Reference

| Storage | Format | Recommended Range |
|---------|--------|-------------------|
| Local SSD | NetCDF | 64–256 MiB |
| Local SSD | Zarr | 128–512 MiB |
| Network (NFS/Lustre) | NetCDF | 32–128 MiB |
| Network (NFS/Lustre) | Zarr | 64–256 MiB |
| Cloud (S3/GCS) | NetCDF | 64–128 MiB |
| Cloud (S3/GCS) | Zarr | 128–512 MiB |

---

## Rechunking

`rechunk_dataset()` rewrites an xarray Dataset to a new Zarr store with different chunk sizes. It handles temp store placement, error messaging, and cleanup automatically:

```python
from dask_setup import rechunk_dataset

ds_rechunked = rechunk_dataset(
    ds,
    target_chunks={"time": 90, "lat": 360, "lon": 720},
    client=client,
    dask_tmp=dask_tmp,  # uses fast local storage for intermediate store
)
```

> Pass `dask_tmp` as the temp store so the shuffle stays on fast local storage (e.g. `$PBS_JOBFS`) rather than shared filesystem.

### rechunker compatibility

`rechunk_dataset()` uses the `rechunker` library when it is available and compatible. Newer xarray versions (≥ 2024.x) added a required `zarr_format` argument to an internal function that older `rechunker` releases do not pass. When this incompatibility is detected, `rechunk_dataset()` automatically falls back to a native `xarray.to_zarr()` path with no change to the user-facing API:

```
WARNING  [rechunk] rechunker is incompatible with the installed xarray version
         (extract_zarr_variable_encoding requires zarr_format).
         Falling back to native xarray.to_zarr() rechunking.
INFO     [rechunk] Rechunking complete (native fallback) ...
```

The native path is equally memory-safe for Dask-backed datasets — the scheduler writes one chunk at a time without materialising the full dataset. The `max_mem` parameter is ignored in fallback mode (Dask manages memory via its normal spill thresholds).

---

## Supported Compression Algorithms

`VALID_COMPRESSION_ALGORITHMS` includes:

- Standard: `"lz4"`, `"zstd"`, `"snappy"`, `"gzip"`, `"blosc"`, `"zlib"`, `"bz2"`, `"lzma"`, `"false"` (disabled)
- blosc2 (zarr ≥ 3.0): `"blosc2"`, `"blosc2:lz4"`, `"blosc2:lz4hc"`, `"blosc2:blosclz"`, `"blosc2:zstd"`, `"blosc2:zlib"`, `"blosc2:snappy"`

---

## Integration Example — Climate Data Pipeline

```python
import xarray as xr
from dask_setup import setup_dask_client, recommend_io_chunks, rechunk_dataset

client, cluster, dask_tmp = setup_dask_client(workload_type="io")

ds = xr.open_zarr("s3://climate-data/era5.zarr")

rec = recommend_io_chunks(
    ds,
    path_or_url="s3://climate-data/era5.zarr",
    access_pattern="compute",
    verbose=True,
)
ds_opt = ds.chunk(rec.chunks)

# Persist to a rechunked local store using fast scratch storage
ds_rechunked = rechunk_dataset(
    ds_opt,
    target_chunks=rec.chunks,
    client=client,
    dask_tmp=dask_tmp,
)

result = ds_rechunked.mean(["lat", "lon"]).compute()
```
