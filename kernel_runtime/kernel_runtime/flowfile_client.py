from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import polars as pl

from kernel_runtime.artifact_store import ArtifactStore

_context: dict[str, Any] = {}


def _set_context(
    node_id: int,
    input_paths: dict[str, str],
    output_dir: str,
    artifact_store: ArtifactStore,
) -> None:
    _context["node_id"] = node_id
    _context["input_paths"] = input_paths
    _context["output_dir"] = output_dir
    _context["artifact_store"] = artifact_store


def _clear_context() -> None:
    _context.clear()


def _get_context_value(key: str) -> Any:
    if key not in _context:
        raise RuntimeError(f"flowfile context not initialized (missing '{key}'). This API is only available during /execute.")
    return _context[key]


def read_input(name: str = "main") -> pl.LazyFrame:
    input_paths: dict[str, str] = _get_context_value("input_paths")
    if name not in input_paths:
        available = list(input_paths.keys())
        raise KeyError(f"Input '{name}' not found. Available inputs: {available}")
    return pl.scan_parquet(input_paths[name])


def read_inputs() -> dict[str, pl.LazyFrame]:
    input_paths: dict[str, str] = _get_context_value("input_paths")
    return {name: pl.scan_parquet(path) for name, path in input_paths.items()}


def publish_output(df: pl.LazyFrame | pl.DataFrame, name: str = "main") -> None:
    output_dir = _get_context_value("output_dir")
    os.makedirs(output_dir, exist_ok=True)
    output_path = Path(output_dir) / f"{name}.parquet"
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    df.write_parquet(str(output_path))


def publish_artifact(name: str, obj: Any) -> None:
    store: ArtifactStore = _get_context_value("artifact_store")
    node_id: int = _get_context_value("node_id")
    store.publish(name, obj, node_id)


def read_artifact(name: str) -> Any:
    store: ArtifactStore = _get_context_value("artifact_store")
    return store.get(name)


def delete_artifact(name: str) -> None:
    store: ArtifactStore = _get_context_value("artifact_store")
    store.delete(name)


def list_artifacts() -> dict:
    store: ArtifactStore = _get_context_value("artifact_store")
    return store.list_all()
