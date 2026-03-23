"""JSON Schema for dask_setup profile YAML files.

Importing this module exposes :data:`PROFILE_SCHEMA` — a JSON Schema
(draft-07) dict that describes the structure of profile YAML files.
Editors that support JSON Schema (VS Code, PyCharm, etc.) can use this
schema for autocompletion and inline validation.

Example usage::

    from dask_setup.schema import PROFILE_SCHEMA
    import json
    print(json.dumps(PROFILE_SCHEMA, indent=2))

The schema is also accessible via the CLI::

    dask-setup schema
"""

from __future__ import annotations

import json
from pathlib import Path

_SCHEMA_FILE = Path(__file__).parent / "profile_schema.json"


def _load_schema() -> dict:
    with open(_SCHEMA_FILE) as f:
        return json.load(f)


#: The JSON Schema (draft-07) for ``dask_setup`` profile YAML files.
PROFILE_SCHEMA: dict = _load_schema()

__all__ = ["PROFILE_SCHEMA"]
