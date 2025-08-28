"""Backward compatibility module.

This module maintains the original single-file API for backward compatibility.
The implementation has been refactored into a proper package structure under
src/dask_setup/, but this module re-exports the same function signature.

DEPRECATED: Import directly from 'dask_setup' package instead.
"""

import warnings

from .dask_setup import setup_dask_client as _setup_dask_client


def setup_dask_client(*args, **kwargs):
    """Backward compatibility wrapper for setup_dask_client.

    DEPRECATED: Import setup_dask_client directly from 'dask_setup' package instead.
    """
    warnings.warn(
        "Importing setup_dask_client from dask_setup.py is deprecated. "
        "Import from 'dask_setup' package instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _setup_dask_client(*args, **kwargs)
