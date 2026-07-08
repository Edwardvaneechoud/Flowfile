"""Tests for custom-node mounts (workstream J):

- mount persistence (add/remove/load) in mounts.json next to the default dir
- validation: absolute, exists, is dir, not the default dir, no nesting
- registry scans default dir + mounts; entries carry their origin (mount_path)
- equal file names across mounts get distinct sys.modules names (no collision)
- get_by_file resolves default-dir files only; mounted nodes stay read-only there
- removing a mount drops its nodes from the registry on rescan
"""
import sys
from pathlib import Path

import pytest

from flowfile_core.flowfile.user_defined.mounts import (
    MountValidationError,
    add_mount,
    load_mounts,
    mounts_file_path,
    remove_mount,
    save_mounts,
)
from flowfile_core.flowfile.user_defined.registry import CustomNodeRegistry

NODE_TEMPLATE = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class {class_name}(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "{category}"

    settings_schema: NodeSettings = NodeSettings(
        main_section=Section(title="Main", value_input=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def write_node_file(directory: Path, file_name: str, node_name: str, class_name="TestNode", category="Testing") -> Path:
    path = directory / file_name
    path.write_text(NODE_TEMPLATE.format(class_name=class_name, node_name=node_name, category=category))
    return path


@pytest.fixture
def default_dir(tmp_path):
    directory = tmp_path / "user_defined_nodes"
    directory.mkdir()
    (directory / "icons").mkdir()
    return directory


@pytest.fixture
def extra_dir(tmp_path):
    directory = tmp_path / "elsewhere" / "my_nodes"
    directory.mkdir(parents=True)
    return directory


@pytest.fixture
def local_registry(default_dir):
    return CustomNodeRegistry(directory=default_dir)


# ---------------------------------------------------------------- persistence


def test_load_mounts_missing_file_is_empty(default_dir):
    assert load_mounts(default_dir) == []


def test_add_mount_persists_next_to_default_dir(default_dir, extra_dir):
    mount = add_mount(str(extra_dir), base_dir=default_dir)

    assert mount.path == str(extra_dir)
    assert mount.added_at
    assert mounts_file_path(default_dir) == default_dir / "mounts.json"
    assert mounts_file_path(default_dir).exists()
    loaded = load_mounts(default_dir)
    assert [m.path for m in loaded] == [str(extra_dir)]


def test_remove_mount_never_touches_files(default_dir, extra_dir):
    write_node_file(extra_dir, "kept.py", "Kept Node")
    add_mount(str(extra_dir), base_dir=default_dir)

    removed = remove_mount(str(extra_dir), base_dir=default_dir)

    assert removed is not None
    assert removed.path == str(extra_dir)
    assert load_mounts(default_dir) == []
    assert (extra_dir / "kept.py").exists()  # files are left on disk


def test_remove_unknown_mount_returns_none(default_dir, extra_dir):
    assert remove_mount(str(extra_dir), base_dir=default_dir) is None


def test_corrupt_mounts_file_degrades_to_empty(default_dir):
    mounts_file_path(default_dir).write_text("{ not json", encoding="utf-8")
    assert load_mounts(default_dir) == []


def test_non_list_mounts_file_degrades_to_empty(default_dir):
    mounts_file_path(default_dir).write_text('{"path": "x"}', encoding="utf-8")
    assert load_mounts(default_dir) == []


def test_save_mounts_round_trips(default_dir):
    from flowfile_core.flowfile.user_defined.mounts import NodeMount

    save_mounts([NodeMount(path="/a/b", added_at="2020-01-01T00:00:00+00:00")], base_dir=default_dir)
    loaded = load_mounts(default_dir)
    assert len(loaded) == 1
    assert loaded[0].path == "/a/b"


# ------------------------------------------------------------------ validation


def test_add_mount_rejects_relative_path(default_dir):
    with pytest.raises(MountValidationError, match="absolute"):
        add_mount("relative/dir", base_dir=default_dir)


def test_add_mount_rejects_missing_path(default_dir, tmp_path):
    with pytest.raises(MountValidationError, match="does not exist"):
        add_mount(str(tmp_path / "nope"), base_dir=default_dir)


def test_add_mount_rejects_file(default_dir, tmp_path):
    a_file = tmp_path / "a_file.py"
    a_file.write_text("x = 1\n")
    with pytest.raises(MountValidationError, match="Not a directory"):
        add_mount(str(a_file), base_dir=default_dir)


def test_add_mount_rejects_default_dir(default_dir):
    with pytest.raises(MountValidationError, match="already the default"):
        add_mount(str(default_dir), base_dir=default_dir)


def test_add_mount_rejects_nesting_inside_default(default_dir):
    nested = default_dir / "sub"
    nested.mkdir()
    with pytest.raises(MountValidationError, match="nest"):
        add_mount(str(nested), base_dir=default_dir)


def test_add_mount_rejects_parent_of_default(default_dir):
    with pytest.raises(MountValidationError, match="nest"):
        add_mount(str(default_dir.parent), base_dir=default_dir)


def test_add_mount_rejects_duplicate(default_dir, extra_dir):
    add_mount(str(extra_dir), base_dir=default_dir)
    with pytest.raises(MountValidationError, match="Already mounted"):
        add_mount(str(extra_dir), base_dir=default_dir)


def test_add_mount_rejects_nested_mounts(default_dir, extra_dir):
    add_mount(str(extra_dir), base_dir=default_dir)
    child = extra_dir / "child"
    child.mkdir()
    with pytest.raises(MountValidationError, match="nest with existing"):
        add_mount(str(child), base_dir=default_dir)


# -------------------------------------------------------------------- registry


def test_scan_covers_default_and_mounts(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "home_node.py", "Home Node")
    write_node_file(extra_dir, "mounted_node.py", "Mounted Node")
    add_mount(str(extra_dir), base_dir=default_dir)

    local_registry.scan()

    home = local_registry.get("home_node")
    mounted = local_registry.get("mounted_node")
    assert home is not None and home.node_class is not None
    assert mounted is not None and mounted.node_class is not None
    assert home.mount_path is None
    assert mounted.mount_path == str(extra_dir)


def test_mount_directories_reads_persisted_mounts(default_dir, extra_dir, local_registry):
    add_mount(str(extra_dir), base_dir=default_dir)
    assert local_registry.mount_directories() == [Path(extra_dir)]


def test_missing_mount_dir_is_skipped_not_fatal(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "home_node.py", "Home Node")
    add_mount(str(extra_dir), base_dir=default_dir)
    # remove the mounted directory after registering it
    for child in extra_dir.iterdir():
        child.unlink()
    extra_dir.rmdir()

    entries = local_registry.scan()

    assert [e.file_name for e in entries] == ["home_node.py"]


def test_same_file_name_across_sources_no_module_collision(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "shared.py", "Home Shared", class_name="HomeNode")
    write_node_file(extra_dir, "shared.py", "Mounted Shared", class_name="MountedNode")
    add_mount(str(extra_dir), base_dir=default_dir)

    local_registry.scan()

    home = local_registry.get("home_shared")
    mounted = local_registry.get("mounted_shared")
    assert home is not None and home.node_class is not None
    assert mounted is not None and mounted.node_class is not None
    assert home.module_name != mounted.module_name
    assert home.module_name in sys.modules
    assert mounted.module_name in sys.modules


def test_default_dir_wins_on_duplicate_name(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "home.py", "Same Name")
    write_node_file(extra_dir, "away.py", "Same Name")
    add_mount(str(extra_dir), base_dir=default_dir)

    local_registry.scan()

    winner = local_registry.get("same_name")
    assert winner.mount_path is None  # default dir is scanned first, so it wins
    loser = local_registry.get_by_file(str(extra_dir / "away.py"))
    assert loser is not None
    assert loser.node_class is None
    assert "Duplicate node name" in loser.error


def test_get_by_file_resolves_default_dir_only(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "home_node.py", "Home Node")
    write_node_file(extra_dir, "mounted_node.py", "Mounted Node")
    add_mount(str(extra_dir), base_dir=default_dir)

    local_registry.scan()

    assert local_registry.get_by_file("home_node.py") is not None
    # a mounted file is not resolvable by bare file name against the default dir
    # (this is what the default-dir-only save/delete routes rely on)
    assert local_registry.get_by_file("mounted_node.py") is None
    # ... but the entry does exist, keyed by its full path
    full = extra_dir / "mounted_node.py"
    assert any(e.file_path == full for e in local_registry.all())


def test_removing_mount_drops_its_nodes_on_rescan(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "home_node.py", "Home Node")
    write_node_file(extra_dir, "mounted_node.py", "Mounted Node")
    add_mount(str(extra_dir), base_dir=default_dir)
    local_registry.scan()
    mounted_module = local_registry.get("mounted_node").module_name
    assert mounted_module in sys.modules

    remove_mount(str(extra_dir), base_dir=default_dir)
    local_registry.scan()

    assert local_registry.get("mounted_node") is None
    assert local_registry.get("home_node") is not None
    # the mounted module is dropped from sys.modules on the rescan's remove pass
    assert mounted_module not in sys.modules


def test_broken_mounted_file_visible_with_error(default_dir, extra_dir, local_registry):
    write_node_file(default_dir, "home_node.py", "Home Node")
    (extra_dir / "broken.py").write_text("def broken(:\n")
    add_mount(str(extra_dir), base_dir=default_dir)

    local_registry.scan()

    broken = next(e for e in local_registry.all() if e.file_name == "broken.py")
    assert broken.node_class is None
    assert broken.mount_path == str(extra_dir)
    assert "Syntax error" in broken.error


def test_mount_module_name_is_deterministic(default_dir, extra_dir, local_registry):
    write_node_file(extra_dir, "mounted_node.py", "Mounted Node")
    add_mount(str(extra_dir), base_dir=default_dir)
    local_registry.scan()
    first = local_registry.get("mounted_node").module_name

    local_registry.scan()
    second = local_registry.get("mounted_node").module_name

    assert first == second
    assert first.startswith("flowfile_udn_m")
