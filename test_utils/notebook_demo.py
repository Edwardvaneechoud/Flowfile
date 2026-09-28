"""The showcase demo's seed data, loader and ``mood_emoji`` install, shared by the core corpus and the frame test.

Core imports stay inside the functions so importing ``test_utils`` never loads ``flowfile_core``.
"""

from __future__ import annotations

import importlib.util
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
DEMO_FILE = REPO_ROOT / "flowfile_frame" / "tests" / "fixtures" / "demo_catalog_pipeline.py"
MOOD_EMOJI = REPO_ROOT / "flowfile_core/tests/flowfile/community_nodes/fixture_registry/nodes/mood_emoji/node.py"

SALES = {
    "order_id": [1, 2, 3, 4, 5, 6],
    "region": ["N", "S", "N", "S", "N", "S"],
    "product": ["a", "b", "a", "c", "b", "a"],
    "category": ["x", "y", "x", "y", "x", "x"],
    "status": ["Completed"] * 5 + ["Cancelled"],
    "amount": [30.0, 50.0, 70.0, 20.0, 90.0, 40.0],
    "quantity": [1, 2, 3, 1, 2, 1],
    "order_date": pl.date_range(pl.date(2024, 1, 1), pl.date(2024, 6, 1), "1mo", eager=True).to_list(),
}
REGIONS = {"region": ["N", "S"], "manager": ["Ann", "Bob"], "target_sales": [100.0, 100.0]}


def load_demo():
    spec = importlib.util.spec_from_file_location("notebook_demo_catalog_pipeline", DEMO_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def storage_files() -> set[Path]:
    """Every file under the storage dir (catalog tables, flows, outputs), minus the catalog DB, logs and bytecode.

    Outside Docker the user-data dir is the home directory, so it is not scanned.
    """
    from shared.storage_config import storage

    return {
        path
        for path in Path(storage.base_directory).rglob("*")
        if path.is_file()
        and not any(part.endswith("logs") or part == "__pycache__" for part in path.parts)
        and ".db" not in path.name
    }


@contextmanager
def installed_mood_emoji() -> Iterator[None]:
    """Install the ``mood_emoji`` community fixture node into the registry, restoring the node store after."""
    from flowfile_core.configs import node_store
    from flowfile_core.flowfile.user_defined.registry import registry

    directory = registry.directory
    directory.mkdir(parents=True, exist_ok=True)
    node_file = directory / "mood_emoji.py"
    installed = not node_file.exists()
    saved = dict(node_store.node_dict), list(node_store.nodes_list), dict(node_store.CUSTOM_NODE_STORE._overrides)
    if installed:
        shutil.copyfile(MOOD_EMOJI, node_file)
        registry.load_file(node_file)
    try:
        yield
    finally:
        if installed:
            registry.remove_file(node_file)
            node_file.unlink()
            node_store.node_dict.clear()
            node_store.node_dict.update(saved[0])
            node_store.nodes_list[:] = saved[1]
            node_store.CUSTOM_NODE_STORE.clear()
            node_store.CUSTOM_NODE_STORE.update(saved[2])
