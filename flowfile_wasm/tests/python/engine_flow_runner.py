"""Run a flow through the real engine builders and print the resulting rows.

Reads a flow spec as JSON on stdin, applies `engine.build_*` in the given order,
and prints `@@@<json rows>`. This is the ground truth the generated plain-Python
and Polars scripts are checked against by the parity suites — the point
being that the comparison is against the code the browser actually runs, not
against a second reimplementation of it.

Usage: python engine_flow_runner.py < spec.json
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src" / "pyodide"))

import polars as pl  # noqa: E402

import engine  # noqa: E402


def _source_frame(node_id: int, settings: dict) -> pl.LazyFrame:
    """Build a manual-input frame by running the real executor, not a copy of it."""
    result = engine.execute_manual_input(node_id, "", settings)
    if not result.get("success"):
        raise RuntimeError(result.get("error"))
    return engine.get_lazyframe(node_id)


SINGLE_INPUT_BUILDERS = {
    "filter": engine.build_filter,
    "select": engine.build_select,
    "sort": engine.build_sort,
    "unique": engine.build_unique,
    "head": engine.build_head,
    "sample": engine.build_head,
    "record_id": engine.build_record_id,
    "dynamic_rename": engine.build_dynamic_rename,
    "group_by": engine.build_group_by,
    "formula": engine.build_formula,
    "unpivot": engine.build_unpivot,
}

TWO_INPUT_BUILDERS = {
    "join": engine.build_join,
    "cross_join": engine.build_cross_join,
}


def run(spec: dict) -> list[dict]:
    frames: dict[int, pl.LazyFrame] = {}
    for step in spec["steps"]:
        node_id = int(step["id"])
        node_type = step["type"]
        settings = step.get("settings", {})

        if node_type == "manual_input":
            frames[node_id] = _source_frame(node_id, settings)
        elif node_type == "dynamic_rename" and (settings.get("dynamic_rename_input") or {}).get(
            "rename_mode"
        ) == "first_row":
            # first_row needs the data, so it only exists in the executor.
            input_id = int(step["inputs"][0])
            engine.store_lazyframe(input_id, frames[input_id])
            result = engine.execute_dynamic_rename(node_id, input_id, settings)
            if not result.get("success"):
                raise RuntimeError(result.get("error"))
            frames[node_id] = engine.get_lazyframe(node_id)
        elif node_type in SINGLE_INPUT_BUILDERS:
            frames[node_id] = SINGLE_INPUT_BUILDERS[node_type](frames[int(step["inputs"][0])], settings)
        elif node_type in TWO_INPUT_BUILDERS:
            frames[node_id] = TWO_INPUT_BUILDERS[node_type](
                frames[int(step["left"])], frames[int(step["right"])], settings
            )
        elif node_type == "pivot":
            # Pivot has no build_* form: it collects, so it only exists as an
            # executor over engine state. Stage the input, run it, read it back.
            input_id = int(step["inputs"][0])
            engine.store_lazyframe(input_id, frames[input_id])
            result = engine.execute_pivot(node_id, input_id, settings)
            if not result.get("success"):
                raise RuntimeError(result.get("error"))
            frames[node_id] = engine.get_lazyframe(node_id)
        elif node_type == "union":
            frames[node_id] = engine.build_union([frames[int(i)] for i in step["inputs"]], settings)
        elif node_type in ("explore_data", "external_output"):
            frames[node_id] = frames[int(step["inputs"][0])]
        else:
            raise ValueError(f"engine_flow_runner has no case for node type {node_type!r}")

    return frames[int(spec["output"])].collect().to_dicts()


if __name__ == "__main__":
    print("@@@" + json.dumps(run(json.load(sys.stdin)), default=str))
