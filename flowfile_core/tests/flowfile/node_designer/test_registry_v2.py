"""
Tests for the file-backed custom node registry (registry v2):

- scans the nodes directory (never the icons subdir) — AST-only, no exec
- broken files stay registered visible-with-error, siblings unaffected
- duplicate node names: the later file lands in an error state naming the winner
- hot reload on save/delete keeps sys.modules and the store views in sync
- ensure_class execs lazily, caches classes and failures
"""
import sys
import threading
from pathlib import Path

import pytest

from flowfile_core.flowfile.user_defined.registry import (
    CustomNodeExecError,
    CustomNodeRegistry,
    compute_node_key,
    missing_custom_node_error,
    registry as singleton_registry,
)

NODE_TEMPLATE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class {class_name}(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "Testing"

    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Main", value_input=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def write_node_file(directory: Path, file_name: str, node_name: str, class_name: str = "TestNode") -> Path:
    path = directory / file_name
    path.write_text(NODE_TEMPLATE.format(class_name=class_name, node_name=node_name))
    return path


@pytest.fixture
def nodes_dir(tmp_path):
    directory = tmp_path / "user_defined_nodes"
    directory.mkdir()
    (directory / "icons").mkdir()
    return directory


@pytest.fixture
def local_registry(nodes_dir):
    return CustomNodeRegistry(directory=nodes_dir)


def test_compute_node_key_matches_item_contract():
    assert compute_node_key("My Test Node") == "my_test_node"


def test_scan_loads_nodes_directory_not_icons(nodes_dir, local_registry):
    write_node_file(nodes_dir, "good_node.py", "Good Node")
    write_node_file(nodes_dir / "icons", "sneaky_node.py", "Sneaky Node")

    entries = local_registry.scan()

    assert [e.file_name for e in entries] == ["good_node.py"]
    entry = local_registry.get("good_node")
    assert entry is not None
    assert not entry.is_broken
    assert entry.error is None
    assert entry.node_class is None  # scan is AST-only; exec happens at placement
    assert entry.class_name == "TestNode"
    assert len(entry.source_hash) == 64
    assert entry.template is not None and entry.template.item == "good_node"
    node_class = local_registry.ensure_class(entry)
    assert entry.node_class is node_class
    assert node_class().node_name == "Good Node"


def test_broken_file_visible_with_error_siblings_unaffected(nodes_dir, local_registry):
    write_node_file(nodes_dir, "good_node.py", "Good Node")
    (nodes_dir / "broken_node.py").write_text("def broken(:\n")

    entries = local_registry.scan()

    assert len(entries) == 2
    broken = local_registry.get_by_file("broken_node.py")
    assert broken is not None
    assert broken.is_broken
    assert "Syntax error" in broken.error
    assert not local_registry.get("good_node").is_broken
    assert [e.file_name for e in local_registry.all(include_broken=False)] == ["good_node.py"]


def test_no_node_class_is_error_not_crash(nodes_dir, local_registry):
    (nodes_dir / "empty_module.py").write_text("x = 1\n")
    entry = local_registry.load_file(nodes_dir / "empty_module.py")
    assert entry.is_broken
    assert "No CustomNodeBase subclass" in entry.error


def test_two_classes_in_one_file_is_error(nodes_dir, local_registry):
    code = NODE_TEMPLATE.format(class_name="NodeA", node_name="Node A") + (
        "\n\nclass NodeB(NodeA):\n    node_name: str = \"Node B\"\n"
    )
    (nodes_dir / "two_classes.py").write_text(code)
    entry = local_registry.load_file(nodes_dir / "two_classes.py")
    assert entry.is_broken
    assert "Multiple CustomNodeBase subclasses" in entry.error


def test_duplicate_node_name_later_file_errors_naming_winner(nodes_dir, local_registry):
    write_node_file(nodes_dir, "a_first.py", "Same Name")
    write_node_file(nodes_dir, "b_second.py", "Same Name")

    local_registry.scan()

    winner = local_registry.get_by_file("a_first.py")
    loser = local_registry.get_by_file("b_second.py")
    assert not winner.is_broken
    assert loser.is_broken
    assert "Duplicate node name 'Same Name'" in loser.error
    assert "a_first.py" in loser.error
    assert local_registry.get("same_name").file_name == "a_first.py"


def test_hot_reload_replaces_class_and_module(nodes_dir, local_registry):
    path = write_node_file(nodes_dir, "reload_node.py", "Reload Node")
    first = local_registry.load_file(path)
    assert local_registry.ensure_class(first)().node_name == "Reload Node"
    first_module = sys.modules.get("flowfile_udn_reload_node")
    assert first_module is not None

    path.write_text(path.read_text().replace("Reload Node", "Reloaded Node"))
    second = local_registry.load_file(path)

    assert local_registry.ensure_class(second)().node_name == "Reloaded Node"
    assert second.node_key == "reloaded_node"
    assert local_registry.get("reload_node") is None
    assert sys.modules.get("flowfile_udn_reload_node") is not first_module


def test_broken_rewrite_keeps_prior_key_with_error(nodes_dir, local_registry):
    path = write_node_file(nodes_dir, "flaky_node.py", "Flaky Node")
    local_registry.load_file(path)
    path.write_text("def broken(:\n")

    entry = local_registry.load_file(path)

    assert entry.is_broken
    assert entry.node_key == "flaky_node"  # flows referencing the key still find the error
    assert "Syntax error" in local_registry.get("flaky_node").error


def test_remove_file_unregisters(nodes_dir, local_registry):
    path = write_node_file(nodes_dir, "removable.py", "Removable Node")
    entry = local_registry.load_file(path)
    local_registry.ensure_class(entry)  # exec so the sys.modules cleanup below is meaningful
    assert local_registry.get("removable_node") is not None
    assert "flowfile_udn_removable" in sys.modules

    removed = local_registry.remove_file("removable.py")

    assert removed is not None
    assert local_registry.get("removable_node") is None
    assert local_registry.get_by_file("removable.py") is None
    assert "flowfile_udn_removable" not in sys.modules
    assert local_registry.remove_file("removable.py") is None


def test_missing_custom_node_error_prefers_registry_error(nodes_dir, local_registry, monkeypatch):
    (nodes_dir / "broken_node.py").write_text("def broken(:\n")
    local_registry.scan()

    assert "not installed" in missing_custom_node_error("never_seen_node")
    # the package re-exports the singleton, shadowing the submodule name
    registry_module = sys.modules["flowfile_core.flowfile.user_defined.registry"]
    monkeypatch.setattr(registry_module, "registry", local_registry)
    assert "Syntax error" in missing_custom_node_error("broken_node")


@pytest.fixture
def singleton_on_tmp_dir(nodes_dir):
    """Point the wired singleton registry at a temp dir, restoring all store views afterwards."""
    from flowfile_core.configs import node_store

    saved_store = dict(node_store.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store.node_dict)
    saved_list = list(node_store.nodes_list)
    saved_entries = dict(singleton_registry._entries)
    saved_directory = singleton_registry._directory
    singleton_registry._entries = {}
    singleton_registry._directory = nodes_dir
    try:
        yield node_store
    finally:
        singleton_registry._directory = saved_directory
        singleton_registry._entries = saved_entries
        node_store.CUSTOM_NODE_STORE.clear()
        node_store.CUSTOM_NODE_STORE.update(saved_store)
        node_store.node_dict.clear()
        node_store.node_dict.update(saved_dict)
        node_store.nodes_list[:] = saved_list


def test_store_view_parity_with_singleton(nodes_dir, singleton_on_tmp_dir):
    node_store = singleton_on_tmp_dir
    write_node_file(nodes_dir, "store_node.py", "Store Node")
    (nodes_dir / "broken_node.py").write_text("def broken(:\n")

    singleton_registry.scan()

    assert node_store.CUSTOM_NODE_STORE["store_node"] is singleton_registry.get("store_node").node_class
    assert "store_node" in node_store.node_dict
    assert any(t.item == "store_node" for t in node_store.nodes_list)
    # broken entries are in the registry but never in the store / palette
    assert singleton_registry.get_by_file("broken_node.py").error is not None
    assert "broken_node" not in node_store.CUSTOM_NODE_STORE
    assert not any(t.item == "broken_node" for t in node_store.nodes_list)

    singleton_registry.remove_file("store_node.py")
    assert "store_node" not in node_store.CUSTOM_NODE_STORE
    assert not any(t.item == "store_node" for t in node_store.nodes_list)


def test_hot_reload_updates_store_template(nodes_dir, singleton_on_tmp_dir):
    node_store = singleton_on_tmp_dir
    path = write_node_file(nodes_dir, "renamed.py", "Old Name")
    singleton_registry.load_file(path)
    assert "old_name" in node_store.CUSTOM_NODE_STORE

    path.write_text(path.read_text().replace("Old Name", "New Name"))
    singleton_registry.load_file(path)

    assert "old_name" not in node_store.CUSTOM_NODE_STORE
    assert "new_name" in node_store.CUSTOM_NODE_STORE
    assert "old_name" not in node_store.node_dict
    assert node_store.node_dict["new_name"].item == "new_name"


SENTINEL_TEMPLATE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase

with open(r"{sentinel}", "a") as f:
    f.write("exec\\n")


class SentinelNode(CustomNodeBase):
    node_name: str = "{node_name}"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''

IMPORT_BOMB_TEMPLATE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase

raise RuntimeError("boom at import")


class BombNode(CustomNodeBase):
    node_name: str = "{node_name}"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def test_scan_never_execs_node_modules(nodes_dir, local_registry, tmp_path):
    sentinel = tmp_path / "exec_log.txt"
    (nodes_dir / "sentinel_node.py").write_text(
        SENTINEL_TEMPLATE.format(sentinel=sentinel, node_name="Sentinel Node")
    )

    entries = local_registry.scan()

    assert not sentinel.exists()
    entry = entries[0]
    assert not entry.is_broken
    assert entry.template is not None and entry.template.item == "sentinel_node"
    assert entry.class_name == "SentinelNode"


def test_ensure_class_execs_exactly_once(nodes_dir, local_registry, tmp_path):
    sentinel = tmp_path / "exec_log.txt"
    path = nodes_dir / "sentinel_node.py"
    path.write_text(SENTINEL_TEMPLATE.format(sentinel=sentinel, node_name="Sentinel Node"))
    entry = local_registry.load_file(path)
    assert not sentinel.exists()

    first = local_registry.ensure_class(entry)
    second = local_registry.ensure_class(local_registry.get("sentinel_node"))

    assert first is second
    assert sentinel.read_text().count("exec") == 1


def test_ensure_class_thread_safe_single_exec(nodes_dir, local_registry, tmp_path):
    sentinel = tmp_path / "exec_log.txt"
    path = nodes_dir / "sentinel_node.py"
    path.write_text(SENTINEL_TEMPLATE.format(sentinel=sentinel, node_name="Sentinel Node"))
    local_registry.load_file(path)

    results = []

    def hit():
        results.append(local_registry.ensure_class(local_registry.get("sentinel_node")))

    threads = [threading.Thread(target=hit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len({id(cls) for cls in results}) == 1
    assert sentinel.read_text().count("exec") == 1


def test_exec_failure_cached_until_reload(nodes_dir, local_registry):
    path = nodes_dir / "bomb_node.py"
    path.write_text(IMPORT_BOMB_TEMPLATE.format(node_name="Bomb Node"))
    entry = local_registry.load_file(path)
    assert not entry.is_broken  # AST-healthy: import failures surface at placement, not scan

    with pytest.raises(CustomNodeExecError, match="boom at import"):
        local_registry.ensure_class(entry)
    assert entry.exec_error is not None
    assert entry.load_error == entry.exec_error
    with pytest.raises(CustomNodeExecError, match="boom at import"):
        local_registry.ensure_class(entry)

    # fixing the file + reloading (save/install/rescan) clears the cached failure
    path.write_text(NODE_TEMPLATE.format(class_name="BombNode", node_name="Bomb Node"))
    entry = local_registry.load_file(path)
    assert entry.exec_error is None
    assert local_registry.ensure_class(entry)().node_name == "Bomb Node"


def test_stale_file_reloaded_before_exec(nodes_dir, local_registry):
    path = write_node_file(nodes_dir, "stale_node.py", "Stale Node")
    entry = local_registry.load_file(path)
    path.write_text(NODE_TEMPLATE.format(class_name="TestNode", node_name="Fresh Node"))

    node_class = local_registry.ensure_class(entry)

    assert node_class().node_name == "Fresh Node"
    fresh = local_registry.get("fresh_node")
    assert fresh is not None and fresh.node_class is node_class
    assert local_registry.get("stale_node") is None


def test_store_get_returns_none_for_exec_broken(nodes_dir, singleton_on_tmp_dir):
    node_store = singleton_on_tmp_dir
    (nodes_dir / "bomb_node.py").write_text(IMPORT_BOMB_TEMPLATE.format(node_name="Bomb Node"))
    singleton_registry.scan()

    assert "bomb_node" in node_store.CUSTOM_NODE_STORE  # AST-healthy: browsable
    assert node_store.CUSTOM_NODE_STORE.get("bomb_node") is None  # exec fails -> None
    assert "boom at import" in singleton_registry.get("bomb_node").load_error
    # visible-with-error: the palette template stays; placement surfaces the error
    assert "bomb_node" in node_store.node_dict
