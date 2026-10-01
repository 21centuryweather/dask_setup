# Examples

Runnable notebooks from
[`examples/`](https://github.com/21centuryweather/dask_setup/tree/main/examples)
in the repository. Every notebook is executed when the site is built, so the
outputs below come from the current release, run on a GitHub Actions runner
(4 cores, 16 GB) outside any PBS or SLURM job. Worker counts, memory figures
and timings will differ on your machine or on a compute node.

To run them yourself:

```bash
git clone https://github.com/21centuryweather/dask_setup
cd dask_setup
pip install -e ".[dev,docs]" jupyterlab
jupyter lab examples
```

```{toctree}
:caption: Tutorial
:maxdepth: 1

examples/tutorial/dask_setup_tutorial
```

```{toctree}
:caption: Recipes
:maxdepth: 1
:glob:

examples/recipes/notebooks/*
```
