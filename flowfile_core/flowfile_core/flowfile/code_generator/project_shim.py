"""Standalone ``flowfile_ctx`` shim shipped with exported Flowfile projects.

This module mirrors the ``flowfile_ctx`` API that notebook (Python script)
nodes use inside Flowfile's kernel containers, so exported notebook code runs
unchanged outside Flowfile. Each notebook module wraps its code in a ``run()``
function that executes inside ``node_context()``; inputs and outputs are
exchanged in memory as Polars LazyFrames instead of parquet files on a shared
volume.

Differences from the kernel runtime:

- Artifacts are pickled to a project-local ``.artifacts/`` directory.
- ``get_shared_location`` resolves into a project-local ``.shared/`` directory.
- Log and display messages are printed to stdout.
- Global-artifact and catalog APIs require a running Flowfile server and
  raise ``NotImplementedError`` here.
"""

from __future__ import annotations

import pickle
from pathlib import Path
from typing import Any, Literal

import polars as pl

_PROJECT_DIR = Path(__file__).resolve().parent
_ARTIFACTS_DIR = _PROJECT_DIR / ".artifacts"
_SHARED_DIR = _PROJECT_DIR / ".shared"

_current_context: dict[str, Any] | None = None


# ===== Pipeline-facing helper =====


class NodeContext:
    """Execution context for one notebook node; create via :func:`node_context`.

    While the with-block is open, the node-facing helpers (``read_input``,
    ``publish_output``, ...) resolve against this context. The collected
    results stay available on the object after the block exits, so the
    notebook module's ``run()`` ends with ``return ctx.results()``.
    """

    def __init__(self, inputs: dict[str, list[pl.LazyFrame]], output_names: list[str], node_name: str = ""):
        self._inputs = inputs
        self._output_names = output_names
        self.node_name = node_name
        self._outputs: dict[str, pl.LazyFrame] = {}
        self._results: dict[str, pl.LazyFrame] | None = None

    def __enter__(self) -> NodeContext:
        global _current_context
        if _current_context is not None:
            raise RuntimeError("flowfile_ctx.node_context() does not support nested notebook execution")
        # Share self._outputs with the global context so publish_output() writes
        # stay readable on this object after the global is cleared.
        _current_context = {"inputs": self._inputs, "outputs": self._outputs, "node_name": self.node_name}
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        global _current_context
        _current_context = None
        # Only collect on a clean exit so a missing-output error never masks
        # an exception raised by the notebook code itself.
        if exc_type is None:
            self._results = self._collect_results()
        return False

    def _collect_results(self) -> dict[str, pl.LazyFrame]:
        results: dict[str, pl.LazyFrame] = {}
        for index, name in enumerate(self._output_names):
            if name in self._outputs:
                results[name] = self._outputs[name]
            elif index == 0:
                results[name] = _first_input_frame(self._inputs)
            else:
                # Mirror the Flowfile runtime: a declared secondary output the
                # notebook never published falls back to the primary result
                # (flow_graph silently skips missing outputs; downstream nodes
                # read the primary), so a flow that runs in Flowfile keeps
                # running once exported instead of raising here.
                primary = results.get(self._output_names[0])
                results[name] = primary if primary is not None else _first_input_frame(self._inputs)
        return results

    def results(self) -> dict[str, pl.LazyFrame]:
        """Return the node's outputs keyed by output name (after the with-block exits)."""
        if self._results is None:
            raise RuntimeError("node_context() results are only available after the with-block exits")
        return self._results


def node_context(
    inputs: dict[str, list[pl.LazyFrame]],
    output_names: list[str],
    node_name: str = "",
) -> NodeContext:
    """Open the context a notebook module's ``run()`` function executes inside.

    Mirrors the kernel contract: the notebook code reads its inputs via
    ``read_input``/``read_inputs`` and publishes results via
    ``publish_output``. When the code does not publish the primary (first)
    output, the first input frame is passed through unchanged; a declared
    secondary output that was never published falls back to the primary result
    (matching the Flowfile runtime, which silently skips missing outputs).
    """
    return NodeContext(inputs, output_names, node_name)


def _first_input_frame(inputs: dict[str, list[pl.LazyFrame]]) -> pl.LazyFrame:
    frames = inputs.get("main") or next((f for f in inputs.values() if f), None)
    if not frames:
        return pl.LazyFrame()
    return frames[0]


# ===== Node-facing context helpers =====


def _get_context() -> dict[str, Any]:
    if _current_context is None:
        raise RuntimeError(
            "flowfile_ctx has no active node context. Notebook code must run inside "
            "flowfile_ctx.node_context() — call the module's run() function (see pipeline.py)."
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
