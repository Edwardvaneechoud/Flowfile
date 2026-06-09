"""Standalone ``flowfile_ctx`` shim shipped with exported Flowfile projects.

This module mirrors the ``flowfile_ctx`` API that notebook (Python script)
nodes use inside Flowfile's kernel containers, so exported notebook modules
run unchanged outside Flowfile. Inputs and outputs are exchanged in memory
as Polars LazyFrames instead of parquet files on a shared volume.

Differences from the kernel runtime:

- Artifacts are pickled to a project-local ``.artifacts/`` directory.
- ``get_shared_location`` resolves into a project-local ``.shared/`` directory.
- Log and display messages are printed to stdout.
- Global-artifact and catalog APIs require a running Flowfile server and
  raise ``NotImplementedError`` here.
"""

from __future__ import annotations

import pickle
import runpy
from pathlib import Path
from typing import Any, Literal

import polars as pl

_PROJECT_DIR = Path(__file__).resolve().parent
_ARTIFACTS_DIR = _PROJECT_DIR / ".artifacts"
_SHARED_DIR = _PROJECT_DIR / ".shared"

_current_context: dict[str, Any] | None = None


# ===== Pipeline-facing helper =====


def run_node(
    module_name: str,
    inputs: dict[str, list[pl.LazyFrame]],
    output_names: list[str],
    node_name: str = "",
) -> dict[str, pl.LazyFrame]:
    """Execute a notebook module with *inputs* available through this shim.

    Mirrors the kernel contract: the module reads its inputs via
    ``read_input``/``read_inputs`` and publishes results via
    ``publish_output``. When the module does not publish the primary
    (first) output, the first input frame is passed through unchanged.
    """
    global _current_context
    if _current_context is not None:
        raise RuntimeError("flowfile_ctx.run_node() does not support nested notebook execution")
    context: dict[str, Any] = {"inputs": inputs, "outputs": {}, "node_name": node_name or module_name}
    _current_context = context
    try:
        runpy.run_module(module_name, run_name="__main__")
    finally:
        _current_context = None

    published: dict[str, pl.LazyFrame] = context["outputs"]
    results: dict[str, pl.LazyFrame] = {}
    for index, name in enumerate(output_names):
        if name in published:
            results[name] = published[name]
        elif index == 0:
            results[name] = _first_input_frame(inputs)
        else:
            raise RuntimeError(
                f"Notebook node '{context['node_name']}' declared output '{name}' but did not publish it. "
                f"Published outputs: {sorted(published)}"
            )
    return results


def _first_input_frame(inputs: dict[str, list[pl.LazyFrame]]) -> pl.LazyFrame:
    frames = inputs.get("main") or next((f for f in inputs.values() if f), None)
    if not frames:
        return pl.LazyFrame()
    return frames[0]


# ===== Node-facing context helpers =====


def _get_context() -> dict[str, Any]:
    if _current_context is None:
        raise RuntimeError(
            "flowfile_ctx has no active node context. Notebook modules must be executed "
            "through flowfile_ctx.run_node() (see pipeline.py)."
        )
    return _current_context


def _check_input_available(inputs: dict[str, list[pl.LazyFrame]], name: str) -> list[pl.LazyFrame]:
    if name not in inputs or not inputs[name]:
        available = sorted(k for k, v in inputs.items() if v)
        if not available:
            raise RuntimeError("This notebook node has no inputs connected.")
        raise KeyError(f"Input '{name}' not found. Available inputs: {available}")
    return inputs[name]


def read_input(name: str = "main") -> pl.LazyFrame:
    """Return all input frames registered under *name* as a single LazyFrame.

    When multiple frames are registered under the same name (e.g. several
    upstream nodes), they are concatenated, matching the kernel behaviour.
    """
    inputs: dict[str, list[pl.LazyFrame]] = _get_context()["inputs"]
    frames = _check_input_available(inputs, name)
    if len(frames) == 1:
        return frames[0]
    return pl.concat(frames)


def read_first(name: str = "main") -> pl.LazyFrame:
    """Return only the first input frame registered under *name*."""
    inputs: dict[str, list[pl.LazyFrame]] = _get_context()["inputs"]
    frames = _check_input_available(inputs, name)
    return frames[0]


def read_inputs() -> dict[str, list[pl.LazyFrame]]:
    """Return all named inputs as a dict of LazyFrame lists."""
    inputs: dict[str, list[pl.LazyFrame]] = _get_context()["inputs"]
    return {name: list(frames) for name, frames in inputs.items()}


def publish_output(df: pl.LazyFrame | pl.DataFrame, name: str = "main") -> None:
    """Register *df* as the node output called *name*."""
    outputs: dict[str, pl.LazyFrame] = _get_context()["outputs"]
    outputs[name] = df.lazy()


# ===== Local artifact store (pickle-based) =====


def publish_artifact(name: str, obj: Any) -> None:
    """Pickle *obj* into the project-local ``.artifacts/`` directory."""
    _ARTIFACTS_DIR.mkdir(exist_ok=True)
    with open(_ARTIFACTS_DIR / f"{name}.pkl", "wb") as f:
        pickle.dump(obj, f)


def read_artifact(name: str) -> Any:
    """Load a previously published artifact."""
    path = _ARTIFACTS_DIR / f"{name}.pkl"
    if not path.exists():
        raise KeyError(f"Artifact '{name}' not found in {_ARTIFACTS_DIR}")
    with open(path, "rb") as f:
        return pickle.load(f)


def list_artifacts() -> list[dict[str, Any]]:
    """List artifacts published in this project."""
    if not _ARTIFACTS_DIR.exists():
        return []
    return [
        {"name": path.stem, "path": str(path), "size_bytes": path.stat().st_size}
        for path in sorted(_ARTIFACTS_DIR.glob("*.pkl"))
    ]


def delete_artifact(name: str) -> None:
    """Delete a previously published artifact (no-op when missing)."""
    path = _ARTIFACTS_DIR / f"{name}.pkl"
    if path.exists():
        path.unlink()


# ===== File utilities =====


def get_shared_location(filename: str) -> str:
    """Return an absolute path for *filename* inside the project-local ``.shared/`` directory.

    Parent directories are created automatically.
    """
    full_path = _SHARED_DIR / filename
    full_path.parent.mkdir(parents=True, exist_ok=True)
    return str(full_path)


# ===== Logging / display =====


def log(message: str, level: Literal["INFO", "WARNING", "ERROR"] = "INFO") -> None:
    """Print a log message to stdout (the kernel sends these to the Flowfile log viewer)."""
    print(f"[{level}] {message}")


def log_info(message: str) -> None:
    """Convenience wrapper: ``log(message, level="INFO")``."""
    log(message, level="INFO")


def log_warning(message: str) -> None:
    """Convenience wrapper: ``log(message, level="WARNING")``."""
    log(message, level="WARNING")


def log_error(message: str) -> None:
    """Convenience wrapper: ``log(message, level="ERROR")``."""
    log(message, level="ERROR")


def display(obj: Any, title: str = "") -> None:
    """Print *obj* to stdout (the kernel renders rich output in the notebook panel)."""
    if title:
        print(f"=== {title} ===")
    print(obj)


# ===== Server-backed APIs (unavailable in exported projects) =====


def _unsupported(api_name: str) -> Any:
    raise NotImplementedError(
        f"flowfile_ctx.{api_name}() requires a running Flowfile server and is not available in exported projects."
    )


def publish_global(*args: Any, **kwargs: Any) -> Any:
    """Global artifacts require a running Flowfile server."""
    return _unsupported("publish_global")


def get_global(*args: Any, **kwargs: Any) -> Any:
    """Global artifacts require a running Flowfile server."""
    return _unsupported("get_global")


def list_global_artifacts(*args: Any, **kwargs: Any) -> Any:
    """Global artifacts require a running Flowfile server."""
    return _unsupported("list_global_artifacts")


def delete_global_artifact(*args: Any, **kwargs: Any) -> Any:
    """Global artifacts require a running Flowfile server."""
    return _unsupported("delete_global_artifact")


def read_catalog_table(*args: Any, **kwargs: Any) -> Any:
    """Catalog tables require a running Flowfile server."""
    return _unsupported("read_catalog_table")


def write_catalog_table(*args: Any, **kwargs: Any) -> Any:
    """Catalog tables require a running Flowfile server."""
    return _unsupported("write_catalog_table")


def list_catalogs(*args: Any, **kwargs: Any) -> Any:
    """Catalog APIs require a running Flowfile server."""
    return _unsupported("list_catalogs")


def get_catalog(*args: Any, **kwargs: Any) -> Any:
    """Catalog APIs require a running Flowfile server."""
    return _unsupported("get_catalog")


def default_schema(*args: Any, **kwargs: Any) -> Any:
    """Catalog APIs require a running Flowfile server."""
    return _unsupported("default_schema")


def list_schemas(*args: Any, **kwargs: Any) -> Any:
    """Catalog APIs require a running Flowfile server."""
    return _unsupported("list_schemas")


def list_catalog_tables(*args: Any, **kwargs: Any) -> Any:
    """Catalog APIs require a running Flowfile server."""
    return _unsupported("list_catalog_tables")
