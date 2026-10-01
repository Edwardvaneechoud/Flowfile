"""
Tests for the file-backed custom node registry (registry v2):

- scans the nodes directory (never the icons subdir) — AST-only, no exec
- broken files stay registered visible-with-error, siblings unaffected
- duplicate node names: the later file lands in an error state naming the winner
- hot reload on save/delete keeps sys.modules and the store views in sync
- ensure_class execs lazily, caches classes and failures
"""
import os
import sys
import threading
import time
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
    saved_stamps = singleton_registry._stamps
    singleton_registry._entries = {}
    singleton_registry._directory = nodes_dir
    singleton_registry._stamps = None
    try:
        yield node_store
    finally:
        singleton_registry._directory = saved_directory
        singleton_registry._entries = saved_entries
        singleton_registry._stamps = saved_stamps
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


STACK_TEMPLATE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase


class StackNode(CustomNodeBase):
    node_name: str = "{node_name}"
    number_of_inputs: int = 3

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return pl.concat(inputs)
'''

STACK_KEY = "late_stack_node"


def _save_stack_flow(nodes_dir: Path, yaml_path: Path, flow_id: int, configure: bool = True) -> Path:
    """Save manual inputs 1-3 -> a three-input node file, then drop its entry: a server that never scanned it.

    ``configure=False`` leaves the node a bare promise, as a canvas drop that was never configured saves.
    """
    from flowfile_core.flowfile.flow_graph import add_connection
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas import input_schema, schemas

    path = nodes_dir / f"{STACK_KEY}.py"
    path.write_text(STACK_TEMPLATE.format(node_name="Late Stack Node"))
    node_class = singleton_registry.ensure_class(singleton_registry.load_file(path))
    handler = FlowfileHandler()
    handler.register_flow(
        schemas.FlowSettings(
            flow_id=flow_id, name="late_node_flow", path=".", execution_mode="Development", execution_location="local"
        )
    )
    flow = handler.get_flow(flow_id)
    for node_id in (1, 2, 3):
        flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type="manual_input"))
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow_id, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist([{"a": node_id}])
            )
        )
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=4, node_type=STACK_KEY))
    for node_id in (1, 2, 3):
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(node_id, 4))
    if configure:
        flow.add_user_defined_node(
            custom_node=node_class(),
            user_defined_node_settings=input_schema.UserDefinedNode(
                flow_id=flow_id, node_id=4, settings={}, is_user_defined=True
            ),
        )
    flow.save_flow(str(yaml_path))
    singleton_registry.remove_file(path)
    return path


def _import(yaml_path: Path):
    from flowfile_core.flowfile.handler import FlowfileHandler

    handler = FlowfileHandler()
    return handler.get_flow(handler.import_flow(yaml_path, register_session=False))


def test_imported_flow_resolves_node_file_written_after_scan(nodes_dir, singleton_on_tmp_dir, tmp_path):
    node_store = singleton_on_tmp_dir
    _save_stack_flow(nodes_dir, tmp_path / "late.yaml", flow_id=6301)
    assert singleton_registry.get(STACK_KEY) is None and STACK_KEY not in node_store.CUSTOM_NODE_STORE

    loaded = _import(tmp_path / "late.yaml")

    node = loaded.get_node(4)
    assert node.results.errors is None
    assert sorted(n.node_id for n in node.all_inputs) == [1, 2, 3]  # wiring read the real three-input template
    assert any(t.item == STACK_KEY for t in node_store.nodes_list)  # the palette caught up too
    result = loaded.run_graph()
    assert result.success, result
    assert sorted(node.get_resulting_data().data_frame.collect()["a"].to_list()) == [1, 2, 3]


def test_imported_flow_resolves_unconfigured_late_node(nodes_dir, singleton_on_tmp_dir, tmp_path):
    _save_stack_flow(nodes_dir, tmp_path / "unconfigured.yaml", flow_id=6305, configure=False)
    assert "setting_input: null" in (tmp_path / "unconfigured.yaml").read_text()  # only the type says it is custom

    loaded = _import(tmp_path / "unconfigured.yaml")

    assert sorted(n.node_id for n in loaded.get_node(4).all_inputs) == [1, 2, 3]


def test_copy_of_a_not_installed_node_resolves_once_its_file_appears(nodes_dir, singleton_on_tmp_dir, tmp_path):
    from flowfile_core.schemas import input_schema

    path = _save_stack_flow(nodes_dir, tmp_path / "copy.yaml", flow_id=6306)
    parked = path.rename(tmp_path / path.name)
    loaded = _import(tmp_path / "copy.yaml")
    source = loaded.get_node(4)
    assert source.results.errors == missing_custom_node_error(STACK_KEY)
    parked.rename(path)

    loaded.copy_node(
        input_schema.NodePromise(flow_id=loaded.flow_id, node_id=5, node_type=STACK_KEY),
        source.setting_input,
        STACK_KEY,
    )

    assert loaded.get_node(5).results.errors is None
    assert STACK_KEY in singleton_on_tmp_dir.CUSTOM_NODE_STORE


def test_node_promise_resolves_late_file_and_still_raises_for_unknown_type(nodes_dir, singleton_on_tmp_dir):
    from flowfile_core.flowfile.handler import FlowfileHandler
    from flowfile_core.schemas import input_schema, schemas

    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=6304, name="build", path=".", execution_mode="Development"))
    flow = handler.get_flow(6304)
    (nodes_dir / f"{STACK_KEY}.py").write_text(STACK_TEMPLATE.format(node_name="Late Stack Node"))

    flow.add_node_promise(input_schema.NodePromise(flow_id=6304, node_id=1, node_type=STACK_KEY, is_user_defined=True))

    assert flow.get_node(1).setting_input.is_user_defined is True
    with pytest.raises(Exception, match="^Node template no_such_late_node not found$"):  # unchanged for no file
        flow.add_node_promise(
            input_schema.NodePromise(flow_id=6304, node_id=2, node_type="no_such_late_node", is_user_defined=True)
        )


def test_flow_with_node_type_without_file_keeps_missing_error(nodes_dir, singleton_on_tmp_dir, tmp_path):
    _save_stack_flow(nodes_dir, tmp_path / "gone.yaml", flow_id=6302).unlink()

    loaded = _import(tmp_path / "gone.yaml")

    assert loaded.get_node(4).results.errors == f"Custom node '{STACK_KEY}' is not installed on this machine"
    assert singleton_registry.all() == []


def test_flow_referencing_ast_broken_late_file_surfaces_its_error(nodes_dir, singleton_on_tmp_dir, tmp_path):
    _save_stack_flow(nodes_dir, tmp_path / "broken.yaml", flow_id=6303).write_text("def broken(:\n")

    loaded = _import(tmp_path / "broken.yaml")

    assert loaded.get_node(4).results.errors.startswith(f"Custom node '{STACK_KEY}' failed to load: Syntax error")
    assert singleton_registry.get_by_file(f"{STACK_KEY}.py").is_broken


def test_refresh_loads_only_new_files_and_never_execs(nodes_dir, local_registry, tmp_path):
    from flowfile_core.flowfile.user_defined.mounts import add_mount

    known = local_registry.load_file(write_node_file(nodes_dir, "known_node.py", "Known Node"))
    mount_dir = tmp_path / "mounted_nodes"
    mount_dir.mkdir()
    add_mount(str(mount_dir), base_dir=nodes_dir)
    sentinel = tmp_path / "exec_log.txt"
    (nodes_dir / "sentinel_node.py").write_text(SENTINEL_TEMPLATE.format(sentinel=sentinel, node_name="Sentinel Node"))
    write_node_file(mount_dir, "mounted_late.py", "Mounted Late")
    (nodes_dir / "broken_late.py").write_text("def broken(:\n")

    loaded = local_registry.refresh()

    assert not sentinel.exists()
    assert sorted(e.file_name for e in loaded) == ["broken_late.py", "mounted_late.py", "sentinel_node.py"]
    assert local_registry.get("sentinel_node").node_class is None
    assert local_registry.get("mounted_late").mount_path == str(mount_dir)
    assert "Syntax error" in local_registry.get_by_file("broken_late.py").error
    assert local_registry.get("known_node") is known
    assert local_registry.refresh() == []  # nothing new: no reloads


def test_refresh_retries_broken_file_once_it_changes(nodes_dir, local_registry):
    path = nodes_dir / "fixed_later.py"
    path.write_text("def broken(:\n")
    local_registry.refresh()
    assert local_registry.get_by_file("fixed_later.py").is_broken

    path.write_text(NODE_TEMPLATE.format(class_name="FixedNode", node_name="Fixed Later"))
    os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 1))

    assert [e.node_key for e in local_registry.refresh()] == ["fixed_later"]
    assert not local_registry.get("fixed_later").is_broken


@pytest.fixture
def globbed(monkeypatch):
    """Every directory ``Path.glob`` lists during the test; a registry rescan is one glob per directory."""
    calls: list[Path] = []
    real_glob = Path.glob

    def glob(self, pattern, *args, **kwargs):
        calls.append(self)
        return real_glob(self, pattern, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", glob)
    return calls


def _backdate(*paths: Path) -> None:
    """Age mtimes past the recent-mtime window, so a refresh may trust them."""
    past = time.time() - 60
    for path in paths:
        os.utime(path, (past, past))


def test_refresh_skips_the_glob_until_a_directory_changes(nodes_dir, local_registry, tmp_path, globbed):
    from flowfile_core.flowfile.user_defined.mounts import add_mount, mounts_file_path

    mount_dir = tmp_path / "mounted_nodes"
    mount_dir.mkdir()
    add_mount(str(mount_dir), base_dir=nodes_dir)
    _backdate(nodes_dir, mounts_file_path(nodes_dir), mount_dir)
    local_registry.scan()
    globbed.clear()

    assert local_registry.refresh() == [] and local_registry.refresh() == []
    assert globbed == []

    write_node_file(nodes_dir, "late_node.py", "Late Node")
    assert [e.node_key for e in local_registry.refresh()] == ["late_node"]
    _backdate(nodes_dir)
    local_registry.refresh()
    write_node_file(mount_dir, "mounted_late.py", "Mounted Late")
    assert [e.node_key for e in local_registry.refresh()] == ["mounted_late"]


def test_refresh_does_not_trust_a_recent_directory_mtime(nodes_dir, local_registry):
    os.utime(nodes_dir)  # modified just now
    local_registry.scan()
    stamp = nodes_dir.stat().st_mtime_ns
    write_node_file(nodes_dir, "same_tick.py", "Same Tick")
    os.utime(nodes_dir, ns=(stamp, stamp))  # a coarse-mtime filesystem shows no change

    assert [e.node_key for e in local_registry.refresh()] == ["same_tick"]


def test_opening_and_undoing_a_flow_with_a_missing_node_rescan_at_most_once(
    nodes_dir, singleton_on_tmp_dir, tmp_path, globbed
):
    from flowfile_core.schemas import input_schema

    _save_stack_flow(nodes_dir, tmp_path / "missing.yaml", flow_id=6307).unlink()
    _backdate(nodes_dir)
    globbed.clear()

    loaded = _import(tmp_path / "missing.yaml")

    assert loaded.get_node(4).results.errors == missing_custom_node_error(STACK_KEY)
    assert globbed.count(nodes_dir) <= 1

    loaded.add_node_promise(input_schema.NodePromise(flow_id=loaded.flow_id, node_id=9, node_type="manual_input"))
    globbed.clear()

    assert loaded.undo().success
    assert loaded.get_node(9) is None and loaded.get_node(4).results.errors == missing_custom_node_error(STACK_KEY)
    assert globbed.count(nodes_dir) <= 1


def test_refresh_finds_a_file_whose_entry_was_removed_but_stayed_on_disk(nodes_dir, local_registry):
    path = write_node_file(nodes_dir, "kept_on_disk.py", "Kept On Disk")
    _backdate(nodes_dir)
    local_registry.scan()

    local_registry.remove_file(path)  # its delete could not unlink the file

    assert [e.node_key for e in local_registry.refresh()] == ["kept_on_disk"]


def test_a_slow_glob_does_not_make_a_recent_stamp_trusted(nodes_dir, local_registry, monkeypatch):
    from types import SimpleNamespace

    registry_module = sys.modules["flowfile_core.flowfile.user_defined.registry"]

    clock = [time.time_ns()]
    monkeypatch.setattr(registry_module, "time", SimpleNamespace(time_ns=lambda: clock[0]))
    real_glob = Path.glob

    def slow_glob(self, *args, **kwargs):
        clock[0] += 3_000_000_000  # the listing outlasts the recent-mtime window
        return real_glob(self, *args, **kwargs)

    monkeypatch.setattr(Path, "glob", slow_glob)
    stamp = clock[0] - 1_000_000_000
    os.utime(nodes_dir, ns=(stamp, stamp))
    local_registry.refresh()

    write_node_file(nodes_dir, "same_tick.py", "Same Tick")
    os.utime(nodes_dir, ns=(stamp, stamp))  # a coarse-mtime filesystem shows no change

    assert [e.node_key for e in local_registry.refresh()] == ["same_tick"]


def test_refresh_reads_mounts_json_only_when_it_changed(nodes_dir, local_registry, tmp_path, monkeypatch):
    registry_module = sys.modules["flowfile_core.flowfile.user_defined.registry"]
    from flowfile_core.flowfile.user_defined.mounts import add_mount, mounts_file_path

    mount_dir = tmp_path / "mounted_nodes"
    mount_dir.mkdir()
    add_mount(str(mount_dir), base_dir=nodes_dir)
    _backdate(nodes_dir, mounts_file_path(nodes_dir), mount_dir)
    local_registry.scan()
    reads = []
    real_load_mounts = registry_module.load_mounts

    def load_mounts(base_dir):
        reads.append(base_dir)
        return real_load_mounts(base_dir)

    monkeypatch.setattr(registry_module, "load_mounts", load_mounts)

    assert local_registry.refresh() == [] and local_registry.refresh() == []
    assert reads == []

    second = tmp_path / "second_mount"
    second.mkdir()
    write_node_file(second, "second_late.py", "Second Late")
    add_mount(str(second), base_dir=nodes_dir)  # rewrites mounts.json in place

    assert [e.node_key for e in local_registry.refresh()] == ["second_late"]
    assert reads == [nodes_dir]
