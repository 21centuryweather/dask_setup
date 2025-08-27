# dask_setup
dask_setup helper designed for **single‑node** Dask runs on **Gadi**, with smart defaults for CPU-/IO-bound workloads and automatic routing of all temp/spill files to **`$PBS_JOBFS`**. It’s a drop‑in convenience wrapper around `dask.distributed.LocalCluster` + `Client` that minimizes trial‑and‑error and avoids common Out Of Memory (OOM) pitfalls.
