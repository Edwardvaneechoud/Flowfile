"""``manual_input`` settings normalisation.

The schema (``schemas.input_schema.RawData``) demands columnar layout
but LLMs consistently emit row-oriented data and dict-of-types columns.
These helpers translate the unambiguous cases at the executor seam so
the strict refusal path stays for genuinely-bad payloads.
"""

from __future__ import annotations

from typing import Any


def _normalize_manual_input_columns(columns: Any) -> Any:
    """Coerce LLM-natural ``columns`` shapes into the canonical
    list-of-objects form (``[{"name", "data_type"}, ...]``).

    Two recoverable shapes:

    * **Dict mapping name → dtype** (most common when the LLM thinks
      "named columns of types"): ``{"category": "String"}`` →
      ``[{"name": "category", "data_type": "String"}]``.
    * **List of bare strings**: ``["category"]`` → objects with
      default ``data_type="String"``.

    Returns ``columns`` unchanged when canonical or when we don't
    recognise the shape — the validator can still reject. Identity-
    preserving on no-op so the caller can detect changes via ``is``.
    """
    if isinstance(columns, dict):
        return [
            {
                "name": str(name),
                "data_type": str(dtype) if dtype is not None else "String",
            }
            for name, dtype in columns.items()
        ]
    if (
        isinstance(columns, list)
        and columns
        and all(isinstance(c, str) for c in columns)
    ):
        return [{"name": c, "data_type": "String"} for c in columns]
    return columns


def _normalize_manual_input_data(data: Any, columns: Any) -> Any:
    """Coerce LLM-natural ``data`` shapes into the canonical columnar
    list-of-lists form (``data[i]`` = all values for ``columns[i]``).

    The schema is columnar but LLMs frequently emit row-oriented or
    list-of-records shapes. We translate the unambiguous cases:

    * **List-of-records** (``[{col: val, ...}, ...]``): reshape using
      the column names as keys; missing keys become ``None``.
    * **Row-oriented list-of-lists** — only when
      ``len(data) != len(columns)`` AND every inner list has length
      ``len(columns)``. Transpose to columnar.

    Square shapes (``len(data) == len(columns)`` AND inner length ==
    ``len(columns)``) are left untouched: ambiguous, schema is
    canonical, trust the LLM's intent. Validator accepts either
    interpretation since both are structurally valid ``list[list]``.

    Identity-preserving on no-op.
    """
    if not isinstance(data, list) or not data:
        return data

    col_names: list[str] | None
    if (
        isinstance(columns, list)
        and columns
        and all(isinstance(c, dict) and "name" in c for c in columns)
    ):
        col_names = [str(c["name"]) for c in columns]
    elif (
        isinstance(columns, list)
        and columns
        and all(isinstance(c, str) for c in columns)
    ):
        col_names = list(columns)
    else:
        col_names = None

    if all(isinstance(row, dict) for row in data):
        if col_names:
            return [[row.get(name) for row in data] for name in col_names]
        keys = list(data[0].keys())
        return [[row.get(k) for row in data] for k in keys]

    if (
        col_names is not None
        and len(col_names) > 0
        and len(data) != len(col_names)
        and all(isinstance(row, list) and len(row) == len(col_names) for row in data)
    ):
        return [list(col) for col in zip(*data, strict=False)]

    return data


def _normalize_manual_input_args(tool_args: dict[str, Any]) -> dict[str, Any]:
    """Accept the LLM's natural ``raw_data_format`` shapes.

    The schema (``schemas.input_schema.RawData``) demands columnar:

      columns: [{name, data_type}, ...]
      data:    [<col0_values>, <col1_values>, ...]   # data[i] = column i

    LLMs consistently emit row-oriented data (one inner list per
    record) and — less often — ``columns`` as a dict-of-types, even
    though the in-prompt example is canonical. The JSON-Schema rendering
    of ``data: list[list]`` is unconstrained at the inner level
    (``items: {}``), so the schema doesn't teach the columnar invariant.
    Same posture as ``_coerce_connection_id_to_flat``: accept the LLM's
    natural emission rather than burning retry rounds on shape attrition.

    Three recoverable shapes (returns ``tool_args`` unchanged when
    nothing matches — preserves the strict refusal path for genuinely
    bad payloads):

    1. ``columns`` as ``{name: dtype}`` dict.
    2. ``data`` as row-oriented list-of-lists (when shape disambiguates).
    3. ``data`` as list-of-records.
    """
    raw = tool_args.get("raw_data_format")
    if not isinstance(raw, dict):
        return tool_args

    columns = raw.get("columns")
    data = raw.get("data")

    new_columns = _normalize_manual_input_columns(columns)
    new_data = _normalize_manual_input_data(data, new_columns)

    if new_columns is columns and new_data is data:
        return tool_args

    rebuilt_raw = dict(raw)
    if new_columns is not columns:
        rebuilt_raw["columns"] = new_columns
    if new_data is not data:
        rebuilt_raw["data"] = new_data
    rebuilt = dict(tool_args)
    rebuilt["raw_data_format"] = rebuilt_raw
    return rebuilt
