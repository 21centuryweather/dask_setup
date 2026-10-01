"""Sphinx configuration for the dask_setup documentation site."""

import os
import shutil
import tomllib
from pathlib import Path

_DOCS = Path(__file__).parent
_ROOT = _DOCS.parent
_pyproject = tomllib.loads((_ROOT / "pyproject.toml").read_text())

# Sphinx only renders files under docs/, so mirror the example notebooks into
# docs/examples/ (gitignored) on every build. configs/ comes along because
# 02_config_management loads ../configs/cpu_profile.yaml.
_EXAMPLES_SRC = _ROOT / "examples"
_EXAMPLES_DST = _DOCS / "examples"
shutil.rmtree(_EXAMPLES_DST, ignore_errors=True)
for _sub in ("recipes/notebooks", "recipes/configs", "tutorial"):
    shutil.copytree(
        _EXAMPLES_SRC / _sub,
        _EXAMPLES_DST / _sub,
        ignore=shutil.ignore_patterns(".ipynb_checkpoints"),
    )

project = "dask_setup"
author = "Sam Green"
copyright = "2026, Sam Green"
release = _pyproject["project"]["version"]
version = release

extensions = [
    "myst_nb",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

source_suffix = {".rst": "restructuredtext", ".md": "myst-nb", ".ipynb": "myst-nb"}
exclude_patterns = ["_build", "jupyter_execute"]

# The notebooks are stored without outputs, so run every one at build time.
# A notebook that raises fails the build -- the site never shows a traceback
# where an example result should be.
nb_execution_mode = "force"
nb_execution_timeout = 300
nb_execution_raise_on_error = True
nb_execution_show_tb = True
# Notebook kernels inherit this environment. dask_setup's INFO lines go to
# stderr and would otherwise put a dozen log boxes between each code cell and
# its result; warnings still show.
os.environ.setdefault("DASK_SETUP_LOG_LEVEL", "WARNING")

# GitHub-style slugs for "page.md#some-heading" links carried over from the wiki.
myst_heading_anchors = 4
myst_enable_extensions = ["colon_fence", "deflist"]

autodoc_member_order = "bysource"
autodoc_typehints = "description"
autodoc_default_options = {"members": True, "show-inheritance": True}
napoleon_google_docstring = True
napoleon_numpy_docstring = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "distributed": ("https://distributed.dask.org/en/stable/", None),
    "xarray": ("https://docs.xarray.dev/en/stable/", None),
}

html_theme = "sphinx_rtd_theme"
html_title = f"dask_setup {release}"
html_theme_options = {"navigation_depth": 3}
html_context = {
    "display_github": True,
    "github_user": "21centuryweather",
    "github_repo": "dask_setup",
    "github_version": "main",
    "conf_py_path": "/docs/",
}
