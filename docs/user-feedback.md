# User Feedback

Real questions from people using `dask_setup` on Gadi, with the answers — and
what changed in the library as a result.

Feedback that exposes a wrong default, a misleading doc, or a surprising result
is the most useful kind we get. If something behaved differently from how you
expected, that is worth reporting even if you found a workaround.

---

## `workload_type="io"` was 90× slower than `"cpu"` for NetCDF concatenation

**Reported:** August 2026 · **Status:** fixed in docs, runtime warning added

### The report

> A bit of feedback on the different `workload_type` options for `dask_setup`.
>
> **My use case:** opening 10 spatially aligned daily NetCDF files, concatenating
> in time and saving.
>
> **My expectation:** that `workload_type='io'` would be the best option for
> opening, concatenating and saving files.
>
> **Results:** with `workload_type='io'`, concatenating on 24 normal CPUs took
> 15 minutes. This felt far too long as it was only 10 daily files, so I tried an
> NCO solution, which took 40 seconds. But NCO can only use 1 CPU on Gadi.
>
> **Solution:** With `dask_setup` set to `workload_type='cpu'`, this task only
> takes 10 seconds, orders of magnitude faster than `'io'`, which appeared to me
> to be the better option based on the docs.
>
> Can you explain more about the expected tradeoffs between `cpu` and `io`
> workload types, and whether in my use case you would expect magnitudes better
> performance when using the `"cpu"` workload_type for this task?

### The answer

**Yes — `"cpu"` really is the right choice here. But the surprising number is the
15 minutes, not the 10 seconds.**

The expectation was completely reasonable, and the docs led there: they said
`"io"` was for *"opening many NetCDF/Zarr files concurrently"*. That is right for
Zarr and wrong for NetCDF, and the two behave very differently. That was a
documentation bug, not a user mistake.

#### What the two options actually do

On a 24-core node:

| | processes | threads each | total |
|---|---|---|---|
| `io` | 1 | 12 | 12 threads in one process |
| `cpu` | 24 | 1 | 24 independent processes |

`"io"` is a bet that your work releases the GIL while it waits, letting threads
overlap. That bet pays off for network and object-store reads, and for Zarr. It
loses badly for NetCDF.

#### Why NetCDF is the worst case for `"io"`

The HDF5 library underneath NetCDF4 is generally **not built thread-safe**.
Reading those files from several threads segfaults the process outright. xarray
knows this, so it routes *every* NetCDF read through a single process-wide lock.

In `"io"` mode your 12 threads are therefore not 12 readers. They are 12 threads
queuing on one lock, one at a time — no read parallelism at all, plus contention
overhead. And because zlib decompression happens inside the C library *while that
lock is held*, decompression is serialised too, which is usually the dominant
cost for daily climate files.

In `"cpu"` mode each of the 24 processes has its own lock and its own GIL, so
reads and decompression genuinely run in parallel.

There is a sharper failure mode too: `workload_type="io"` combined with
`xr.open_mfdataset(..., parallel=True)` can **kill the worker** with
`NetCDF: Can't open HDF5 attribute`. Dask then quietly recomputes the lost tasks
and tries again. Repeated worker death and recomputation is a plausible route
from "should take 10 seconds" to "took 15 minutes" — it looks like a hang rather
than an error. The tell is the worker count on the dashboard dropping and
recovering.

#### Is 90× expected?

Not quite the way it sounds. **10 seconds is the normal result.** NCO took 40 s on
one CPU, and 10 s across 24 processes is about 4× — sensible for 10 files, where
the file count caps parallelism. Lock serialisation *alone* should have made
`"io"` roughly as slow as single-threaded NCO, so ~40 s. That it took 900 s says
something pathological was happening, most likely the crash-and-retry loop above
rather than mere serialisation.

#### When `"io"` is actually the right choice

The distinction is not "I/O vs compute". It is **whether the library releases the
GIL and is thread-safe**:

- **Use `"io"`** for Zarr, S3/object storage, HTTP fetches — anything where
  threads genuinely overlap. Measured on the same data written as Zarr:
  serial 1.19 s → 5 threads 0.28 s, a **4.3× speedup**. Zarr decompresses via
  numcodecs, which releases the GIL, and has no global lock.
- **Use `"cpu"`** for NetCDF/HDF5 reads and NumPy-heavy work. Opening,
  concatenating and writing NetCDF is a `"cpu"` job despite feeling like I/O.
- **`"mixed"`** (12 processes × 2 threads) is a reasonable hedge for pipelines
  that do both.

> **Rule of thumb: NetCDF in → `"cpu"`. Zarr or object storage in → `"io"`.**

If you convert those dailies to Zarr once, the concat becomes a genuinely
threaded job and `"io"` becomes the better option for everything downstream.

### What changed as a result

1. **Docs corrected.** The workload table said `"io"` was for *"opening many
   NetCDF/Zarr files concurrently"*. It now distinguishes the two, and
   [Workload Types](index.md#workload-types) explains the GIL/thread-safety axis.
2. **A second doc bug, found while checking the first.** [Internals](internals.md)
   claimed the `"auto"` classifier treats *"a time-like dimension and many
   variables"* as I/O-bound. That is backwards — CF dimension names like
   `time`/`lat`/`lon` score toward `"cpu"`. Running the classifier on a dataset
   of this shape returns `"cpu"`, so `"auto"` would in fact have got this right.
3. **Runtime warning added.** `setup_dask_client(ds=...)` now detects
   NetCDF-backed input paired with `workload_type="io"` and says so:

   ```
   WARNING [client] workload_type='io' is usually the wrong choice for NetCDF/HDF5 input
           (suggestion=use workload_type='cpu' (one lock per process) for NetCDF)
   ```

   Zarr input is unaffected — that is what `"io"` is for. See
   [Troubleshooting](troubleshooting.md) for the full entry.

---

## Reporting feedback

Open an issue at
[21centuryweather/dask_setup/issues](https://github.com/21centuryweather/dask_setup/issues).
The most useful reports include:

- what you expected, and what the docs led you to expect
- the `workload_type` / profile you used and the node size (cores, memory)
- the input format (NetCDF, Zarr, …) and roughly how much data
- how you opened it — in particular whether you used
  `open_mfdataset(..., parallel=True)`
- a rough timing, and a comparison point if you have one

A "this felt too slow" report with a comparison number is worth more than a
perfectly minimal reproducer.
