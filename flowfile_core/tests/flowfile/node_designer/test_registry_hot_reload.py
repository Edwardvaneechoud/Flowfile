"""Hot-reload: ensure_class picks up on-disk edits (mtime/hash) without a rescan,
and the kernel bake defaults settings fields missing from a placed node's saved values."""
import os
import time
from pathlib import Path

import pytest

from flowfile_core.configs.settings import FLOWFILE_CUSTOM_NODE_HOT_RELOAD
from flowfile_core.flowfile.user_defined.kernel_codegen import generate_kernel_script
from flowfile_core.flowfile.user_defined.registry import CustomNodeExecError, CustomNodeRegistry

ONE_FIELD = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class ReloadNode(CustomNodeBase):
    node_name: str = "Reload Node"
    node_category: str = "{category}"

    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", val=TextInput(label="Value", default="{default}")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''

TWO_FIELD = '''
import polars as pl
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class ReloadNode(CustomNodeBase):
    node_name: str = "Reload Node"
    node_category: str = "Testing"

    settings_schema: NodeSettings = NodeSettings(
        main=Section(
            title="Main",
            val=TextInput(label="Value", default="a"),
            extra=TextInput(label="Extra", default="D"),
        ),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''


def bump_mtime(path: Path) -> None:
    """Deterministically move mtime well past any prior scan."""
    future = time.time() + 10
    os.utime(path, (future, future))


@pytest.fixture
def nodes_dir(tmp_path):
    directory = tmp_path / "user_defined_nodes"
    directory.mkdir()
    (directory / "icons").mkdir()
    return directory


@pytest.fixture
def local_registry(nodes_dir):
    return CustomNodeRegistry(directory=nodes_dir)


@pytest.fixture
def hot_reload_on():
    prev = bool(FLOWFILE_CUSTOM_NODE_HOT_RELOAD)
    FLOWFILE_CUSTOM_NODE_HOT_RELOAD.set(True)
    yield
    FLOWFILE_CUSTOM_NODE_HOT_RELOAD.set(prev)


def _write(path: Path, category: str = "V1", default: str = "a") -> None:
    path.write_text(ONE_FIELD.format(category=category, default=default))


def test_edit_between_calls_reloads_class(local_registry, nodes_dir, hot_reload_on):
    path = nodes_dir / "reload.py"
    _write(path, category="V1")
    local_registry.scan()
    cls1 = local_registry.ensure_class(local_registry.get("reload_node"))
    assert cls1().node_category == "V1"
    old_hash = local_registry.get("reload_node").source_hash

    _write(path, category="V2")
    bump_mtime(path)
    cls2 = local_registry.ensure_class(local_registry.get("reload_node"))

    assert cls2 is not cls1
    assert cls2().node_category == "V2"
    assert local_registry.get("reload_node").source_hash != old_hash


def test_same_mtime_content_edit_reloads_class(local_registry, nodes_dir, hot_reload_on):
    """An edit landing within mtime resolution must still reload (content hash).

    Trusting mtime alone once served a stale class whose missing
    predict_output_schema silently disabled hook wiring at placement.
    """
    path = nodes_dir / "reload.py"
    _write(path, category="V1")
    local_registry.scan()
    cls1 = local_registry.ensure_class(local_registry.get("reload_node"))
    stat = path.stat()

    _write(path, category="V2")
    os.utime(path, (stat.st_atime, stat.st_mtime))  # pin mtime back: same-tick rewrite
    cls2 = local_registry.ensure_class(local_registry.get("reload_node"))

    assert cls2 is not cls1
    assert cls2().node_category == "V2"


def test_unchanged_and_touch_do_not_reexec(local_registry, nodes_dir, hot_reload_on):
    path = nodes_dir / "reload.py"
    _write(path, category="V1")
    local_registry.scan()
    cls1 = local_registry.ensure_class(local_registry.get("reload_node"))

    # No edit: same object.
    assert local_registry.ensure_class(local_registry.get("reload_node")) is cls1
    # Byte-identical touch: mtime moves but content is unchanged -> no re-exec.
    bump_mtime(path)
    assert local_registry.ensure_class(local_registry.get("reload_node")) is cls1
    assert local_registry.get("reload_node").node_class is cls1


def test_deleted_file_serves_last_known_good(local_registry, nodes_dir, hot_reload_on):
    path = nodes_dir / "reload.py"
    _write(path, category="V1")
    local_registry.scan()
    cls1 = local_registry.ensure_class(local_registry.get("reload_node"))

    path.unlink()
    assert local_registry.ensure_class(local_registry.get("reload_node")) is cls1


def test_broken_edit_raises_then_recovers(local_registry, nodes_dir, hot_reload_on):
    path = nodes_dir / "reload.py"
    _write(path, category="V1")
    local_registry.scan()
    local_registry.ensure_class(local_registry.get("reload_node"))

    path.write_text("def broken(:\n")
    bump_mtime(path)
    with pytest.raises(CustomNodeExecError):
        local_registry.ensure_class(local_registry.get("reload_node"))

    _write(path, category="V3")
    bump_mtime(path)
    cls3 = local_registry.ensure_class(local_registry.get("reload_node"))
    assert cls3().node_category == "V3"


def test_flag_off_keeps_cached_class(local_registry, nodes_dir):
    prev = bool(FLOWFILE_CUSTOM_NODE_HOT_RELOAD)
    FLOWFILE_CUSTOM_NODE_HOT_RELOAD.set(False)
    try:
        path = nodes_dir / "reload.py"
        _write(path, category="V1")
        local_registry.scan()
        cls1 = local_registry.ensure_class(local_registry.get("reload_node"))

        _write(path, category="V2")
        bump_mtime(path)
        # Hot-reload disabled: the cached class is served, edits ignored until rescan.
        assert local_registry.ensure_class(local_registry.get("reload_node")) is cls1
        assert cls1().node_category == "V1"
    finally:
        FLOWFILE_CUSTOM_NODE_HOT_RELOAD.set(prev)


def test_worker_source_text_freshens_on_reload(local_registry, nodes_dir, hot_reload_on):
    path = nodes_dir / "reload.py"
    _write(path, category="V1")
    local_registry.scan()
    entry = local_registry.get("reload_node")
    local_registry.ensure_class(entry)
    old_source = entry.source_text

    _write(path, category="V2")
    bump_mtime(path)
    local_registry.ensure_class(local_registry.get("reload_node"))
    fresh = local_registry.get("reload_node")

    assert "V2" in fresh.source_text
    assert fresh.source_text != old_source


def test_kernel_bake_defaults_field_missing_from_saved(local_registry, nodes_dir, hot_reload_on):
    path = nodes_dir / "reload.py"
    path.write_text(TWO_FIELD)
    local_registry.scan()
    entry = local_registry.get("reload_node")
    cls = local_registry.ensure_class(entry)

    node = cls.from_settings({"main": {"val": "saved"}})  # predates 'extra'
    values = node._extract_settings_values()
    assert values["main"] == {"val": "saved", "extra": "D"}

    code = generate_kernel_script(
        node_source=entry.source_text,
        class_name=entry.class_name,
        settings_values=values,
        output_names=["main"],
        number_of_inputs=1,
    )
    assert '"extra"' in code
    assert "def __getattr__" in code  # proxy backstop for stray references
