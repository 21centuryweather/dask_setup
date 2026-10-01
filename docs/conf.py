"""Sphinx configuration for the dask_setup documentation site."""

import tomllib
from pathlib import Path

_pyproject = tomllib.loads((Path(__file__).parents[1] / "pyproject.toml").read_text())

project = "dask_setup"
author = "Sam Green"
copyright = "2026, Sam Green"
release = _pyproject["project"]["version"]
version = release

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx_copybutton",
]

source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
exclude_patterns = ["_build"]

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
