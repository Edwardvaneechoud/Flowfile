"""``ff.custom_nodes``: list and place installed custom nodes by key, and install classes and files.

Every test writes node files into the (scratch) ``user_defined_nodes/`` directory and removes
them, and their registry entries, afterwards.
"""

import asyncio
import json
import os
import subprocess
import sys
import types
import warnings
from uuid import uuid4

import polars as pl
import pytest

import flowfile_frame as ff
from flowfile_core.configs import node_store
from flowfile_core.flowfile.user_defined.registry import registry
from flowfile_frame.custom_node import _INSTALLED_CLASSES
from shared.node_designer import CustomNodeBase, NodeSettings, NumericInput, Section

from .utils import is_docker_available

DATA = {"name": ["ann", "bob", "cy"], "amount": [1, 2, 3]}

UPPER_SOURCE = """import polars as pl

from flowfile import node_designer as nd


class UpperSettings(nd.NodeSettings):
    main: nd.Section = nd.Section(title="Main", column=nd.TextInput(label="Column", default="name"))


class InstallTestUpper(nd.CustomNodeBase):
    node_name: str = "Install Test Upper"
    node_category: str = "Testing"
    settings_schema: UpperSettings = UpperSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.col(self.settings_schema.main.column.value).str.to_uppercase())
"""

NEVER_BUILT_SOURCE = """from flowfile import node_designer as nd


class InstallTestNeverBuilt(nd.CustomNodeBase):
    node_name: str = "Install Test Never Built"

    def process(self, *inputs):
        raise RuntimeError("process() ran while building")
"""


class DoublerSettings(NodeSettings):
    main: Section = Section(title="Main", factor=NumericInput(label="Factor", default=2))


class InstallDoubler(CustomNodeBase):
    node_name: str = "Install Test Doubler"
    node_category: str = "Testing"
    settings_schema: DoublerSettings = DoublerSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        factor = self.settings_schema.main.factor.value
        return inputs[0].with_columns((pl.col("amount") * factor).alias("doubled"))


class InstallKernelScaler(CustomNodeBase):
    node_name: str = "Install Test Kernel Scaler"
    node_category: str = "Testing"
    environment: str = "kernel"
    settings_schema: DoublerSettings = DoublerSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        factor = self.settings_schema.main.factor.value
        return inputs[0].with_columns((pl.col("amount") * factor).alias("doubled"))

    def predict_output_schema(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(0).alias("doubled"))


class InstallUsesHelper(CustomNodeBase):
    node_name: str = "Install Test Uses Helper"

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(_bonus()).alias("bonus"))


def _bonus() -> int:
    return 10


@pytest.fixture
def nodes_dir():
    """The user-defined nodes directory; node files a test adds, their registry entries and templates go afterwards."""
    directory = registry.directory
    directory.mkdir(parents=True, exist_ok=True)
    before = set(directory.glob("*.py"))
    saved_overrides = dict(node_store.CUSTOM_NODE_STORE._overrides)
    saved_dict, saved_list = dict(node_store.node_dict), list(node_store.nodes_list)
    saved_installed = dict(_INSTALLED_CLASSES)
    yield directory
    for path in set(directory.glob("*.py")) - before:
        registry.remove_file(path)
        path.unlink()
    node_store.CUSTOM_NODE_STORE.clear()
    node_store.CUSTOM_NODE_STORE.update(saved_overrides)
    node_store.node_dict.clear()
    node_store.node_dict.update(saved_dict)
    node_store.nodes_list[:] = saved_list
    _INSTALLED_CLASSES.clear()
    _INSTALLED_CLASSES.update(saved_installed)


def _add_file(directory, name: str, source: str):
    path = directory / name
    path.write_text(source, encoding="utf-8")
    registry.load_file(path)
    return path


# discovery


def test_installed_file_is_listed_and_placed_by_key_name_and_attribute(nodes_dir):
    path = _add_file(nodes_dir, "install_test_upper.py", UPPER_SOURCE)

    infos = {info.key: info for info in ff.custom_nodes.list()}
    assert infos["install_test_upper"] == ff.CustomNodeInfo(
        key="install_test_upper",
        name="Install Test Upper",
        category="Testing",
        environment="local",
        inputs=1,
        outputs=["main"],
        file=path,
        error=None,
    )
    assert "install_test_upper" in ff.custom_nodes and "Install Test Upper" in ff.custom_nodes
    assert "install_test_upper" in list(ff.custom_nodes)
    assert len(ff.custom_nodes) == len(list(ff.custom_nodes))
    assert "install_test_upper" in dir(ff.custom_nodes) and "install" in dir(ff.custom_nodes)

    factory = ff.custom_nodes.install_test_upper
    assert isinstance(factory, ff.CustomNodeFactory)
    assert factory.node_type == ff.custom_nodes.get("Install Test Upper").node_type == "install_test_upper"
    assert ff.custom_nodes["install_test_upper"].node_class is factory.node_class
    out = factory(ff.from_dict(DATA), column="name")
    assert out.collect()["name"].to_list() == ["ANN", "BOB", "CY"]


def test_session_class_is_listed_without_a_file(nodes_dir):
    ff.CustomNode(InstallDoubler, ff.from_dict(DATA))
    info = next(info for info in ff.custom_nodes.list() if info.key == "install_test_doubler")
    assert (info.name, info.file, info.error, info.outputs) == ("Install Test Doubler", None, None, ["main"])
    assert ff.custom_nodes.install_test_doubler.node_class is InstallDoubler


def test_broken_files_are_listed_with_their_error_and_raise_when_placed(nodes_dir):
    _add_file(nodes_dir, "install_test_broken.py", "class Oops(:\n")
    import_error = UPPER_SOURCE.replace('"Install Test Upper"', '"Install Test Bad Import"').replace(
        "import polars as pl\n", "import polars as pl\nimport no_such_module_for_install_tests\n"
    )
    _add_file(nodes_dir, "install_test_bad_import.py", import_error)

    infos = {info.key: info for info in ff.custom_nodes.list()}
    assert infos["install_test_broken"].error.startswith("Syntax error at line 1")
    assert infos["install_test_broken"].file == nodes_dir / "install_test_broken.py"
    assert "install_test_broken" not in ff.custom_nodes
    assert "install_test_broken" not in dir(ff.custom_nodes)
    with pytest.raises(ff.NativeNodeError, match="'install_test_broken' failed to load: Syntax error at line 1"):
        ff.custom_nodes.get("install_test_broken")
    with pytest.raises(ff.NativeNodeError, match="failed to load: Syntax error"):
        _ = ff.custom_nodes.install_test_broken

    assert infos["install_test_bad_import"].error is None  # the scan does not execute the file
    with pytest.raises(ff.NativeNodeError, match="No module named 'no_such_module_for_install_tests'"):
        ff.custom_nodes["install_test_bad_import"]
    infos = {info.key: info for info in ff.custom_nodes.list()}
    assert "no_such_module_for_install_tests" in infos["install_test_bad_import"].error


def test_unknown_names_raise():
    assert getattr(ff.custom_nodes, "no_such_custom_node", None) is None
    with pytest.raises(AttributeError, match="'no_such_custom_node' is not installed"):
        _ = ff.custom_nodes.no_such_custom_node
    with pytest.raises(ff.NativeNodeError, match="'no_such_custom_node' is not installed"):
        ff.custom_nodes["No Such Custom Node"]
    with pytest.raises(ff.NativeNodeError, match=r"fl.Node\('filter'"):
        ff.custom_nodes.get("filter")
    assert "filter" not in ff.custom_nodes and 42 not in ff.custom_nodes


def test_installed_node_on_a_remote_graph_is_deferred_and_never_runs(nodes_dir):
    _add_file(nodes_dir, "install_test_never_built.py", NEVER_BUILT_SOURCE)
    source = ff.from_dict(DATA)
    source.flow_graph.flow_settings.execution_location = "remote"

    node = ff.custom_nodes.install_test_never_built.node(source)

    assert node.deferred is True and node.output._deferred is True
    assert node.node.deferred_until_run is True


# install


def test_install_class_writes_its_file_and_both_placements_work(nodes_dir):
    ff.CustomNode(InstallDoubler, ff.from_dict(DATA))  # registered for the session first

    path = ff.custom_nodes.install(InstallDoubler)

    assert path == nodes_dir / "install_test_doubler.py"
    text = path.read_text(encoding="utf-8")
    assert text.startswith("import polars as pl\nfrom shared.node_designer import CustomNodeBase, NodeSettings")
    assert "class DoublerSettings(NodeSettings):" in text and "class InstallDoubler(CustomNodeBase):" in text
    assert text.index("class DoublerSettings") < text.index("class InstallDoubler")
    for unrelated in ("import pytest", "import subprocess", "class InstallKernelScaler", "def _bonus", "UPPER_SOURCE"):
        assert unrelated not in text
    entry = registry.get("install_test_doubler")
    assert entry.file_path == path and entry.error is None

    by_class = ff.CustomNode(InstallDoubler, ff.from_dict(DATA), settings={"main": {"factor": 3}})
    by_key = ff.custom_nodes.install_test_doubler(ff.from_dict(DATA), factor=3)
    assert by_class.node_class is InstallDoubler
    assert ff.custom_nodes.install_test_doubler.node_class is not InstallDoubler  # the file's own class
    assert by_class.output.collect()["doubled"].to_list() == [3, 6, 9]
    assert by_key.collect()["doubled"].to_list() == [3, 6, 9]
    info = next(info for info in ff.custom_nodes.list() if info.key == "install_test_doubler")
    assert (info.file, info.error) == (path, None)


def test_install_refuses_an_existing_file_unless_overwrite(nodes_dir):
    path = ff.custom_nodes.install(InstallDoubler)
    path.write_text(path.read_text(encoding="utf-8") + "\n# edited\n", encoding="utf-8")
    with pytest.raises(ff.NativeNodeError, match="install_test_doubler.py already exists; pass overwrite=True"):
        ff.custom_nodes.install(InstallDoubler)
    assert path.read_text(encoding="utf-8").endswith("# edited\n")

    assert ff.custom_nodes.install(InstallDoubler, overwrite=True) == path
    assert not path.read_text(encoding="utf-8").endswith("# edited\n")


def test_install_refuses_a_class_reading_module_level_names(nodes_dir):
    with pytest.raises(ff.NativeNodeError, match=r"reads \['_bonus'\] from its module.*install\('path/to/node.py'\)"):
        ff.custom_nodes.install(InstallUsesHelper)
    assert not (nodes_dir / "install_test_uses_helper.py").exists()


def test_install_refuses_a_class_defined_in_a_function(nodes_dir):
    class InstallNested(CustomNodeBase):
        node_name: str = "Install Test Nested"

        def process(self, *inputs):
            return inputs[0]

    with pytest.raises(ff.NativeNodeError, match="defined inside a function or class"):
        ff.custom_nodes.install(InstallNested)


def test_install_of_a_class_from_a_console_raises_a_clear_error(nodes_dir, monkeypatch):
    monkeypatch.setitem(sys.modules, "__main__", types.ModuleType("__main__"))  # a console's __main__ has no file
    namespace = {"__name__": "__main__"}
    source = "from shared.node_designer import CustomNodeBase\n\nclass ConsoleNode(CustomNodeBase):\n    node_name: str = 'Install Test Console'\n\n    def process(self, *inputs):\n        return inputs[0]\n"
    exec(compile(source, "<input>", "exec"), namespace)
    with pytest.raises(ff.NativeNodeError, match="Cannot read the source of custom node class ConsoleNode"):
        ff.custom_nodes.install(namespace["ConsoleNode"])


def test_install_path_copies_the_file_under_its_node_key(nodes_dir, tmp_path):
    source_file = tmp_path / "my_upper_node.py"
    source_file.write_text(UPPER_SOURCE, encoding="utf-8")

    path = ff.custom_nodes.install(source_file)

    assert path == nodes_dir / "install_test_upper.py"
    assert path.read_text(encoding="utf-8") == UPPER_SOURCE
    assert ff.custom_nodes.install_test_upper(ff.from_dict(DATA)).collect()["name"].to_list() == ["ANN", "BOB", "CY"]
    assert ff.custom_nodes.install(str(source_file), overwrite=True) == path


def test_install_path_refusals(nodes_dir, tmp_path):
    broken = tmp_path / "broken.py"
    broken.write_text("class Oops(:\n", encoding="utf-8")
    with pytest.raises(ff.NativeNodeError, match="Cannot install custom node .*broken.py: Syntax error"):
        ff.custom_nodes.install(broken)
    with pytest.raises(ff.NativeNodeError, match="Cannot read custom node file"):
        ff.custom_nodes.install(tmp_path / "missing.py")
    with pytest.raises(ff.NativeNodeError, match="install takes a CustomNodeBase subclass or the path"):
        ff.custom_nodes.install(InstallDoubler())

    built_in = tmp_path / "filter_node.py"
    built_in.write_text(UPPER_SOURCE.replace('"Install Test Upper"', '"Filter"'), encoding="utf-8")
    with pytest.raises(ff.NativeNodeError, match="node key 'filter' of a built-in node"):
        ff.custom_nodes.install(built_in)

    _add_file(nodes_dir, "some_other_name.py", UPPER_SOURCE)
    copy = tmp_path / "upper_copy.py"
    copy.write_text(UPPER_SOURCE, encoding="utf-8")
    with pytest.raises(ff.NativeNodeError, match="'install_test_upper' is already installed from .*some_other_name.py"):
        ff.custom_nodes.install(copy)
    assert not (nodes_dir / "install_test_upper.py").exists()
    assert [p.name for p in nodes_dir.glob("*.py") if p.stem in ("filter", "broken")] == []


# session classes elsewhere


REOPEN_SCRIPT = """
import json, sys
from pathlib import Path

import flowfile_frame as ff
from flowfile_core.flowfile.manage.io_flowfile import open_flow

flow_path, node_file, node_id = Path(sys.argv[1]), sys.argv[2], int(sys.argv[3])
before = open_flow(flow_path)
installed = ff.custom_nodes.install(node_file)
after = open_flow(flow_path)
run = after.run_graph()
print(json.dumps({
    "error_before": before.get_node(node_id).results.errors,
    "installed": str(installed),
    "error_after": after.get_node(node_id).results.errors,
    "success": run.success,
    "doubled": after.get_node(node_id).get_resulting_data().collect()["doubled"].to_list(),
}))
"""


def test_flow_with_a_session_class_opens_elsewhere_as_a_placeholder_until_installed(nodes_dir, tmp_path):
    node = ff.CustomNode(InstallDoubler, ff.from_dict(DATA), settings={"main": {"factor": 3}})
    flow_path = tmp_path / "session_class_flow.yaml"
    node.output.save_graph(str(flow_path))
    node_file = ff.custom_nodes.install(InstallDoubler)
    other = tmp_path / "other_process"
    env = {
        **os.environ,
        "FLOWFILE_STORAGE_DIR": str(other / "storage"),
        "FLOWFILE_USER_DATA_DIR": str(other / "user"),
        "FLOWFILE_DB_PATH": str(other / "catalog.db"),
        "FLOWFILE_TELEMETRY": "0",
    }

    run = subprocess.run(
        [sys.executable, "-c", REOPEN_SCRIPT, str(flow_path), str(node_file), str(node.node_id)],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )

    assert run.returncode == 0, run.stderr[-3000:]
    result = json.loads(run.stdout.splitlines()[-1])
    assert result["error_before"] == "Custom node 'install_test_doubler' is not installed on this machine"
    assert result["installed"] == str(other / "storage" / "user_defined_nodes" / "install_test_doubler.py")
    assert result["error_after"] is None
    assert result["success"] is True
    assert result["doubled"] == [3, 6, 9]


def test_register_flow_warns_about_a_session_class_but_not_an_installed_one(nodes_dir):
    schema = ff.CatalogReference(f"CustomNodes_{uuid4().hex[:8]}", auto_create=True).schema("flows", auto_create=True)
    out = ff.CustomNode(InstallDoubler, ff.from_dict(DATA)).output
    with pytest.warns(UserWarning, match=r"install_test_doubler \(node \d+\).*fl\.custom_nodes\.install"):
        ff.register_flow(out.flow_graph, name="session class", schema=schema)

    ff.custom_nodes.install(InstallDoubler)
    installed = ff.CustomNode(InstallDoubler, ff.from_dict(DATA)).output
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        ff.register_flow(installed.flow_graph, name="installed class", schema=schema)
    assert not [w for w in caught if "fl.custom_nodes.install" in str(w.message)]


# kernel end-to-end (Docker)


KERNEL_E2E_ID = f"ff-custom-node-e2e-{uuid4().hex[:8]}"


@pytest.fixture
def e2e_kernel(monkeypatch, tmp_path):
    """A kernel of this test's own, on a manager of its own; deleted afterwards, and nothing else touched.

    Skips without Docker or without a kernel image present locally: it never pulls or builds one.
    """
    if not is_docker_available():
        pytest.skip("Docker is not available")
    import flowfile_core.kernel as kernel_module
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import ImageFlavour, KernelConfig

    # This manager's registry lacks the developer's kernels, so its startup GC must not reap their containers.
    monkeypatch.setenv("FLOWFILE_KERNEL_GC", "0")
    manager = KernelManager(shared_volume_path=str(tmp_path))
    if manager.resolve_local_image(ImageFlavour.BASE) is None:
        pytest.skip("No kernel image present locally; this test never pulls or builds one")
    monkeypatch.setattr(kernel_module, "_manager", manager)
    asyncio.run(manager.create_kernel(KernelConfig(id=KERNEL_E2E_ID, name="custom node e2e"), user_id=1))
    try:
        yield KERNEL_E2E_ID
    finally:
        asyncio.run(manager.delete_kernel(KERNEL_E2E_ID))


@pytest.mark.kernel
def test_installed_kernel_node_runs_on_a_kernel_with_its_parameter_resolved(nodes_dir, e2e_kernel):
    ff.custom_nodes.install(InstallKernelScaler)
    source = ff.from_dict(DATA)
    factor = ff.add_flow_parameter(source, ff.Parameter("factor", default="4"))

    node = ff.custom_nodes.install_test_kernel_scaler.node(source, factor=factor, kernel=e2e_kernel)

    assert node.deferred is True
    assert node.node.setting_input.settings == {"main": {"factor": "${factor}"}}
    assert node.output.columns == ["name", "amount", "doubled"]
    assert node.output.collect().to_dicts() == [
        {"name": "ann", "amount": 1, "doubled": 4},
        {"name": "bob", "amount": 2, "doubled": 8},
        {"name": "cy", "amount": 3, "doubled": 12},
    ]
