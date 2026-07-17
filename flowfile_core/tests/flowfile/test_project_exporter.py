"""Tests for the project export: multi-file FlowFrame project generation.

Covers manifest structure, verbatim notebook module emission, the local
flowfile_ctx shim, custom-node module emission, zip packaging, and an
end-to-end execution of an exported project.
"""

import ast
import io
import subprocess
import sys
import zipfile
from pathlib import Path

import polars as pl
import pytest

from flowfile_core.configs.node_store import add_to_custom_node_store
from flowfile_core.flowfile.code_generator import project_shim
from flowfile_core.flowfile.code_generator.project_exporter import (
    FlowGraphToProjectConverter,
    _insert_flowfile_ctx_import,
    export_flow_to_project,
    project_to_zip_bytes,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput
from flowfile_core.schemas import input_schema, schemas, transform_schema
from flowfile_core.schemas.output_model import ProjectExportManifest


@pytest.fixture(autouse=True)
def _restore_custom_node_registry():
    """Snapshot/restore the global node registries so file-backed custom nodes
    registered in a test never leak into sibling tests."""
    from flowfile_core.configs import node_store as node_store_mod

    saved_store = dict(node_store_mod.CUSTOM_NODE_STORE)
    saved_dict = dict(node_store_mod.node_dict)
    saved_list = list(node_store_mod.nodes_list)
    try:
        yield
    finally:
        node_store_mod.CUSTOM_NODE_STORE.clear()
        node_store_mod.CUSTOM_NODE_STORE.update(saved_store)
        node_store_mod.node_dict.clear()
        node_store_mod.node_dict.update(saved_dict)
        node_store_mod.nodes_list[:] = saved_list


def _write_node_module(tmp_path: Path, stem: str, source: str):
    """Write a custom-node module to a temp file and import it (file-backed so the
    exporter can read the class source via ``inspect.getfile``)."""
    import importlib
    import sys

    (tmp_path / f"{stem}.py").write_text(source)
    sys.path.insert(0, str(tmp_path))
    try:
        return importlib.import_module(stem)
    finally:
        sys.path.remove(str(tmp_path))


# A custom node whose process() logs via the kernel-injected flowfile_ctx global.
# The exported code must bind flowfile_ctx (ship the shim + insert the import) or
# this raises NameError when run standalone.
_CTX_NODE_SOURCE = '''
import polars as pl

from flowfile import node_designer as nd


class CtxLogger(nd.CustomNodeBase):
    node_name: str = "Ctx Logger"
    node_category: str = "Transform"
    number_of_inputs: int = 1
    number_of_outputs: int = 1

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        flowfile_ctx.log_info("ctx node ran")
        return inputs[0].with_columns(pl.lit(1).alias("k"))
'''


def create_flow_settings(flow_id: int = 1) -> schemas.FlowSettings:
    return schemas.FlowSettings(
        flow_id=flow_id,
        execution_mode="Performance",
        execution_location="local",
        path="/tmp/test_flow",
    )


def create_basic_flow(flow_id: int = 1, name: str = "test_flow") -> FlowGraph:
    return FlowGraph(flow_settings=create_flow_settings(flow_id), name=name)


def add_sample_input(flow: FlowGraph, node_id: int = 1, node_reference: str | None = None) -> None:
    flow.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow.flow_id,
            node_id=node_id,
            node_reference=node_reference,
            raw_data_format=input_schema.RawData(
                columns=[
                    input_schema.MinimalFieldInfo(name="id", data_type="Integer"),
                    input_schema.MinimalFieldInfo(name="age", data_type="Integer"),
                ],
                data=[[1, 2, 3], [25, 30, 35]],
            ),
        )
    )


def add_notebook_node(
    flow: FlowGraph,
    node_id: int,
    depending_on_ids: list[int],
    *,
    cells: list[str] | None = None,
    code: str = "",
    output_names: list[str] | None = None,
    description: str = "",
) -> None:
    notebook_cells = None
    if cells is not None:
        notebook_cells = [input_schema.NotebookCell(id=f"cell-{i}", code=c) for i, c in enumerate(cells)]
    flow.add_python_script(
        input_schema.NodePythonScript(
            flow_id=flow.flow_id,
            node_id=node_id,
            depending_on_ids=depending_on_ids,
            description=description,
            python_script_input=input_schema.PythonScriptInput(code=code, cells=notebook_cells),
            output_names=output_names or ["main"],
        )
    )


def get_file(manifest: ProjectExportManifest, path: str) -> str:
    for file in manifest.files:
        if file.path == path:
            return file.content
    raise AssertionError(f"File {path!r} not in manifest: {[f.path for f in manifest.files]}")


def file_paths(manifest: ProjectExportManifest) -> set[str]:
    return {file.path for file in manifest.files}


def write_project(manifest: ProjectExportManifest, target: Path) -> Path:
    project_dir = target / manifest.project_name
    for file in manifest.files:
        path = project_dir / file.path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(file.content, encoding="utf-8")
    return project_dir


# ---------------------------------------------------------------------------
# Manifest structure
# ---------------------------------------------------------------------------


def test_project_manifest_without_notebooks():
    flow = create_basic_flow(name="Plain Flow")
    add_sample_input(flow, node_id=1)
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=2,
            depending_on_id=1,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[age]>26"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert manifest.project_name == "plain_flow"
    assert {"pipeline.py", "main.py", "pyproject.toml", "README.md"} <= file_paths(manifest)
    # No notebook nodes -> no shim, no notebooks package
    assert "flowfile_ctx.py" not in file_paths(manifest)
    assert not any(path.startswith("notebooks/") for path in file_paths(manifest))
    assert "import flowfile_ctx" not in get_file(manifest, "pipeline.py")
    assert manifest.warnings == []


def test_project_scaffolding_contents():
    flow = create_basic_flow(name="My Flow")
    add_sample_input(flow, node_id=1)

    manifest = export_flow_to_project(flow)

    pyproject = get_file(manifest, "pyproject.toml")
    assert 'name = "my-flow"' in pyproject
    assert "flowfile" in pyproject
    assert "polars" in pyproject

    readme = get_file(manifest, "README.md")
    assert "My Flow" in readme
    assert "manual_input" in readme

    main_py = get_file(manifest, "main.py")
    assert "from pipeline import run_etl_pipeline" in main_py

    pipeline = get_file(manifest, "pipeline.py")
    assert "def run_etl_pipeline():" in pipeline


def test_pyproject_name_and_description_sanitized():
    """Digit-leading flow names must not yield a leading-dash (invalid) package name,
    and quotes in the flow name must not break the TOML description string."""
    flow = create_basic_flow(name='2024 "Q1" sales')
    add_sample_input(flow, node_id=1)

    manifest = export_flow_to_project(flow)

    pyproject = get_file(manifest, "pyproject.toml")
    assert 'name = "2024-q1-sales"' in pyproject
    assert '\\"Q1\\"' in pyproject


def test_project_manifest_with_notebook():
    flow = create_basic_flow(name="Notebook Flow")
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        description="Clean data",
        cells=["df = flowfile_ctx.read_input()", "flowfile_ctx.publish_output(df)"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert {"flowfile_ctx.py", "notebooks/__init__.py", "notebooks/node_02_clean_data.py"} <= file_paths(manifest)
    pipeline = get_file(manifest, "pipeline.py")
    assert "from notebooks import node_02_clean_data" in pipeline
    assert "_nb_2_outputs = node_02_clean_data.run(" in pipeline
    # The pipeline calls the module's run() function; only notebook modules touch the shim
    assert "import flowfile_ctx" not in pipeline
    # The shipped shim is the real module source
    assert get_file(manifest, "flowfile_ctx.py") == Path(project_shim.__file__).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Notebook module emission
# ---------------------------------------------------------------------------


def get_node_source(module: str) -> str:
    """Extract the verbatim _NODE_SOURCE constant from a generated notebook module."""
    for stmt in ast.parse(module).body:
        if isinstance(stmt, ast.Assign) and getattr(stmt.targets[0], "id", None) == "_NODE_SOURCE":
            return ast.literal_eval(stmt.value)
    raise AssertionError("_NODE_SOURCE not found in generated module")


def test_notebook_module_keeps_cells_verbatim():
    cell_1 = "import polars as pl\n\ndf = flowfile_ctx.read_input()"
    cell_2 = "result = df.with_columns((pl.col('age') * 2).alias('age2'))\nflowfile_ctx.publish_output(result)"
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], cells=[cell_1, cell_2])
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_02_python_script.py")
    assert "import flowfile_ctx" in module
    assert "def run(df_1: pl.LazyFrame) -> dict[str, pl.LazyFrame]:" in module
    assert "with flowfile_ctx.node_context(" in module
    assert "return ctx.results()" in module
    # The user code is preserved byte-for-byte in _NODE_SOURCE, cells joined
    # with # %% markers, and exec'd inside run().
    assert get_node_source(module) == f"# %%\n{cell_1}\n\n# %%\n{cell_2}"
    assert "exec(" in module


def test_notebook_module_falls_back_to_code_without_cells():
    code = "df = flowfile_ctx.read_input()\nflowfile_ctx.publish_output(df.head(1))"
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], code=code)
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_02_python_script.py")
    assert get_node_source(module) == code
    assert "# %%" not in get_node_source(module)


def test_notebook_multiline_string_fidelity(monkeypatch):
    """Multi-line string literals in notebook code keep their exact value in the export."""
    sql = 'SELECT *\n\nFROM "orders"\n    WHERE x > 1'
    cell = (
        "import polars as pl\n"
        "\n"
        f'query = """{sql}"""\n'
        'flowfile_ctx.publish_output(pl.LazyFrame({"query": [query]}))'
    )
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], cells=[cell])
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)
    module_source = get_file(manifest, "notebooks/node_02_python_script.py")

    # Run the generated module in-process with the shim standing in for flowfile_ctx.
    monkeypatch.setitem(sys.modules, "flowfile_ctx", project_shim)
    namespace = {}
    exec(compile(module_source, "node_02_python_script.py", "exec"), namespace)
    result = namespace["run"](pl.LazyFrame({"a": [1]}))
    assert result["main"].collect()["query"][0] == sql


def test_notebook_description_with_newline_and_quotes_compiles():
    """Descriptions with newlines / triple quotes must not break generated code."""
    description = 'Cleans the data\nand has """tricky""" content'
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        description=description,
        cells=["flowfile_ctx.publish_output(flowfile_ctx.read_input())"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_02_cleans_the_data_and_has_tricky_content.py")
    compile(module, "module.py", "exec")
    pipeline = get_file(manifest, "pipeline.py")
    compile(pipeline, "pipeline.py", "exec")
    # The comment keeps only the first line of the description.
    assert "# Notebook node 2: Cleans the data\n" in pipeline


def test_notebook_inputs_use_node_reference():
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1, node_reference="orders")
    add_notebook_node(flow, node_id=2, depending_on_ids=[1], cells=["df = flowfile_ctx.read_input('orders')"])
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    pipeline = get_file(manifest, "pipeline.py")
    assert "node_02_python_script.run(orders=orders.data)" in pipeline
    module = get_file(manifest, "notebooks/node_02_python_script.py")
    assert "def run(orders: pl.LazyFrame) -> dict[str, pl.LazyFrame]:" in module
    assert '"orders": [orders]' in module
    assert '"main": [orders]' in module


def test_notebook_multi_output_feeds_downstream_nodes():
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        cells=["flowfile_ctx.publish_output(flowfile_ctx.read_input(), name='main')"],
        output_names=["main", "rejected"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[age]>26"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3, output_handle="output-1"))

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_02_python_script.py")
    assert "output_names=['main', 'rejected']" in module
    pipeline = get_file(manifest, "pipeline.py")
    assert 'df_2_rejected = ff.FlowFrame(_nb_2_outputs["rejected"])' in pipeline
    # The downstream filter must consume the output-1 variable
    assert "filtered = df_2_rejected.filter" in pipeline


def test_notebook_zero_inputs():
    flow = create_basic_flow()
    add_notebook_node(
        flow,
        node_id=1,
        depending_on_ids=[],
        cells=["import polars as pl\n\nflowfile_ctx.publish_output(pl.LazyFrame({'a': [1]}))"],
    )

    manifest = export_flow_to_project(flow)

    module = get_file(manifest, "notebooks/node_01_python_script.py")
    assert "def run() -> dict[str, pl.LazyFrame]:" in module
    assert "inputs={}" in module
    pipeline = get_file(manifest, "pipeline.py")
    assert "node_01_python_script.run()" in pipeline


def test_notebook_with_server_only_api_adds_warning():
    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        cells=["model = flowfile_ctx.get_global('my_model')"],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert any("get_global" in warning for warning in manifest.warnings)
    readme = get_file(manifest, "README.md")
    assert "Limitations" in readme


# ---------------------------------------------------------------------------
# Custom (user-defined) nodes
# ---------------------------------------------------------------------------


@pytest.fixture
def MarkerColumnNode():
    """A custom node that adds a marker column."""

    class MarkerColumnSettings(NodeSettings):
        config: Section = Section(
            title="Configuration",
            column_name=TextInput(label="Column Name", default="marker"),
        )

    class MarkerColumn(CustomNodeBase):
        node_name: str = "Marker Column"
        node_category: str = "Transform"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: MarkerColumnSettings = MarkerColumnSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            col_name = self.settings_schema.config.column_name.value
            return inputs[0].with_columns(pl.lit(True).alias(col_name))

    return MarkerColumn


def test_custom_node_exported_as_module(MarkerColumnNode):
    add_to_custom_node_store(MarkerColumnNode)

    flow = create_basic_flow()
    add_sample_input(flow, node_id=1)
    node_settings = input_schema.UserDefinedNode(
        flow_id=1,
        node_id=2,
        settings={"config": {"column_name": "is_processed"}},
        is_user_defined=True,
    )
    flow.add_user_defined_node(
        custom_node=MarkerColumnNode.from_settings(node_settings.settings),
        user_defined_node_settings=node_settings,
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

    manifest = export_flow_to_project(flow)

    assert {"custom_nodes/__init__.py", "custom_nodes/marker_column.py"} <= file_paths(manifest)
    assert "class MarkerColumn" in get_file(manifest, "custom_nodes/marker_column.py")
    pipeline = get_file(manifest, "pipeline.py")
    assert "from custom_nodes.marker_column import MarkerColumn" in pipeline
    # The class must not be inlined in the pipeline itself
    assert "class MarkerColumn" not in pipeline
    assert "_custom_node_2.process(" in pipeline


def test_custom_node_module_name_collision_disambiguated():
    """Two distinct custom-node classes whose names collapse to the same module
    stem get distinct modules, and each import points at its own class."""

    class MarkerColumn(CustomNodeBase):
        node_name: str = "Marker A"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

    class Marker_Column(CustomNodeBase):  # noqa: N801 - intentional stem collision with MarkerColumn
        node_name: str = "Marker B"
        number_of_inputs: int = 1
        number_of_outputs: int = 1

    converter = FlowGraphToProjectConverter(create_basic_flow())
    assert converter._register_custom_node_source(object(), MarkerColumn) is True
    assert converter._register_custom_node_source(object(), Marker_Column) is True

    custom_modules = {path for path in converter.module_files if path.startswith("custom_nodes/")}
    assert custom_modules == {"custom_nodes/marker_column.py", "custom_nodes/marker_column_2.py"}
    assert "from custom_nodes.marker_column import MarkerColumn" in converter.imports
    assert "from custom_nodes.marker_column_2 import Marker_Column" in converter.imports

    # The same class registered twice reuses its module (no spurious _3).
    assert converter._register_custom_node_source(object(), MarkerColumn) is True
    assert len({p for p in converter.module_files if p.startswith("custom_nodes/")}) == 2


# ---------------------------------------------------------------------------
# Shim behaviour
# ---------------------------------------------------------------------------


@pytest.fixture
def shim_context():
    """Activate a shim node context and clean it up afterwards."""

    def activate(inputs: dict[str, list[pl.LazyFrame]]):
        project_shim._current_context = {"inputs": inputs, "outputs": {}, "node_name": "test"}
        return project_shim._current_context

    yield activate
    project_shim._current_context = None


def test_shim_read_input_concatenates_frames(shim_context):
    frame_a = pl.LazyFrame({"a": [1]})
    frame_b = pl.LazyFrame({"a": [2]})
    shim_context({"main": [frame_a, frame_b]})
    assert project_shim.read_input().collect().to_series().to_list() == [1, 2]


def test_shim_read_input_unknown_name_lists_available(shim_context):
    shim_context({"orders": [pl.LazyFrame({"a": [1]})], "main": [pl.LazyFrame({"a": [1]})]})
    with pytest.raises(KeyError, match="orders"):
        project_shim.read_input("customers")


def test_shim_requires_active_context():
    with pytest.raises(RuntimeError, match="node_context"):
        project_shim.read_input()


def test_shim_artifact_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(project_shim, "_ARTIFACTS_DIR", tmp_path / ".artifacts")
    project_shim.publish_artifact("model", {"weights": [1, 2, 3]})
    assert project_shim.read_artifact("model") == {"weights": [1, 2, 3]}
    assert [a["name"] for a in project_shim.list_artifacts()] == ["model"]
    # Mirrors the kernel ArtifactStore: duplicate publish raises ValueError
    with pytest.raises(ValueError, match="already exists"):
        project_shim.publish_artifact("model", {"weights": [4]})
    project_shim.delete_artifact("model")
    assert project_shim.list_artifacts() == []
    with pytest.raises(KeyError):
        project_shim.read_artifact("model")
    # Mirrors the kernel ArtifactStore: deleting a missing artifact raises KeyError
    with pytest.raises(KeyError, match="not found"):
        project_shim.delete_artifact("model")


def test_shim_server_only_apis_raise():
    with pytest.raises(NotImplementedError, match="publish_global"):
        project_shim.publish_global("name", object())
    with pytest.raises(NotImplementedError, match="read_catalog_table"):
        project_shim.read_catalog_table("table")


def test_shim_covers_kernel_public_api():
    """Every public function of the kernel flowfile_ctx client must exist in the shim
    (implemented, or stubbed via the server-only NotImplementedError wrappers), so
    exported notebook code never hits an AttributeError that the in-app run wouldn't.

    The kernel client is parsed (not imported) — kernel_runtime has its own
    dependency set and is not importable from the core test environment.
    """
    repo_root = Path(__file__).resolve()
    while not (repo_root / "kernel_runtime").is_dir():
        repo_root = repo_root.parent
        assert repo_root != repo_root.parent, "could not locate the repo root"
    client_path = repo_root / "kernel_runtime" / "kernel_runtime" / "flowfile_client.py"
    kernel_funcs = {
        stmt.name
        for stmt in ast.parse(client_path.read_text(encoding="utf-8")).body
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)) and not stmt.name.startswith("_")
    }
    shim_funcs = {name for name in dir(project_shim) if not name.startswith("_")}
    missing = kernel_funcs - shim_funcs
    assert not missing, f"kernel flowfile_ctx APIs missing from project_shim: {sorted(missing)}"


def test_shim_node_context_collects_outputs_and_falls_back():
    frame = pl.LazyFrame({"a": [1, 2]})

    with project_shim.node_context({"main": [frame]}, ["main"]) as ctx:
        df = project_shim.read_input()
        project_shim.publish_output(df.with_columns(b=pl.lit(2)))
    assert ctx.results()["main"].collect().columns == ["a", "b"]

    # Nothing published -> the primary output falls back to the first input frame
    with project_shim.node_context({"main": [frame]}, ["main"]) as ctx:
        pass
    assert ctx.results()["main"] is frame

    # A declared secondary output that was never published falls back to the
    # primary result (mirrors the Flowfile runtime), instead of raising.
    with project_shim.node_context({"main": [frame]}, ["main", "rejected"]) as ctx:
        project_shim.publish_output(project_shim.read_input().with_columns(c=pl.lit(1)))
    primary = ctx.results()["main"]
    assert primary.collect().columns == ["a", "c"]
    assert ctx.results()["rejected"] is primary


def test_shim_node_context_guards():
    frame = pl.LazyFrame({"a": [1]})

    # Nested contexts are rejected before the global context is touched
    with project_shim.node_context({"main": [frame]}, ["main"]):
        with pytest.raises(RuntimeError, match="nested"):
            with project_shim.node_context({"main": [frame]}, ["main"]):
                pass

    # Results are only available after the with-block exits
    pending = project_shim.node_context({"main": [frame]}, ["main"])
    with pytest.raises(RuntimeError, match="after the with-block"):
        pending.results()

    # Exceptions in the notebook code propagate (not masked by missing-output
    # collection) and the global context is cleared
    with pytest.raises(ValueError, match="boom"):
        with project_shim.node_context({"main": [frame]}, ["main"]):
            raise ValueError("boom")
    assert project_shim._current_context is None


# ---------------------------------------------------------------------------
# Packaging & execution
# ---------------------------------------------------------------------------


def test_zip_round_trip():
    flow = create_basic_flow(name="Zip Flow")
    add_sample_input(flow, node_id=1)

    manifest = export_flow_to_project(flow)
    zip_bytes = project_to_zip_bytes(manifest)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        names = archive.namelist()
        assert all(name.startswith("zip_flow/") for name in names)
        assert "zip_flow/pipeline.py" in names
        unpacked = archive.read("zip_flow/pipeline.py").decode("utf-8")
    assert unpacked == get_file(manifest, "pipeline.py")


def test_project_executes_end_to_end(tmp_path):
    """Write an exported project (with a notebook node) to disk and run it."""
    flow = create_basic_flow(name="E2E Flow")
    add_sample_input(flow, node_id=1)
    add_notebook_node(
        flow,
        node_id=2,
        depending_on_ids=[1],
        description="Double the age",
        cells=[
            "import polars as pl\n\ndf = flowfile_ctx.read_input()",
            "result = df.with_columns((pl.col('age') * 2).alias('age_doubled'))\n"
            "flowfile_ctx.log_info('doubled the age column')\n"
            "flowfile_ctx.publish_output(result)",
        ],
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_filter(
        input_schema.NodeFilter(
            flow_id=1,
            node_id=3,
            depending_on_id=2,
            filter_input=transform_schema.FilterInput(filter_type="advanced", advanced_filter="[age_doubled]>50"),
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))

    manifest = export_flow_to_project(flow)
    project_dir = write_project(manifest, tmp_path)

    result = subprocess.run(
        [sys.executable, "main.py"], cwd=project_dir, capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    assert "doubled the age column" in result.stdout
    assert "age_doubled" in result.stdout


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# ==================== run_flow (subflow) project export ====================


def _register_flow_file(path: Path, name: str) -> int:
    import uuid as _uuid

    from flowfile_core.database import models as db_models
    from flowfile_core.database.connection import get_db_context

    with get_db_context() as db:
        reg = db_models.FlowRegistration(flow_uuid=str(_uuid.uuid4()), name=name, flow_path=str(path), owner_id=1)
        db.add(reg)
        db.commit()
        db.refresh(reg)
        return reg.id


def _build_head_subflow(tmp_path: Path) -> dict:
    """flow_input 'customers' (4 sample rows) -> head(${limit}) -> flow_outputs result/row_count."""
    flow = create_basic_flow(flow_id=31, name="head_subflow")
    flow.flow_settings.parameters = [
        schemas.FlowParameter(name="limit", default_value="10", type="integer")
    ]
    flow.add_flow_input(
        input_schema.NodeFlowInput(
            flow_id=flow.flow_id,
            node_id=1,
            input_name="customers",
            raw_data_format=input_schema.RawData.from_pylist(
                [{"name": f"c{i}", "rank": i} for i in range(1, 5)]
            ),
        )
    )
    flow.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=flow.flow_id,
            node_id=2,
            polars_code_input=transform_schema.PolarsCodeInput(
                polars_code="output_df = input_df.head(${limit})"
            ),
            depending_on_ids=[1],
        )
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=flow.flow_id, node_id=3, output_name="result", depending_on_id=2)
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
    flow.add_record_count(input_schema.NodeRecordCount(flow_id=flow.flow_id, node_id=4, depending_on_id=2))
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 4))
    flow.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=flow.flow_id, node_id=5, output_name="row_count", depending_on_id=4)
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(4, 5))

    path = tmp_path / "head_subflow.yaml"
    flow.save_flow(str(path))
    return {"path": path, "registration_id": _register_flow_file(path, "head_subflow")}


def _keyed_connect(flow: FlowGraph, from_id: int, to_id: int, target_handle: str) -> None:
    add_connection(
        flow,
        input_schema.NodeConnection(
            input_connection=input_schema.NodeInputConnection(node_id=to_id, connection_class=target_handle),
            output_connection=input_schema.NodeOutputConnection(node_id=from_id, connection_class="output-0"),
        ),
    )


def _run_flow_settings(flow: FlowGraph, registration_id: int, **overrides) -> input_schema.NodeRunFlow:
    defaults = dict(
        flow_id=flow.flow_id,
        node_id=9,
        user_id=1,
        flow_reference=input_schema.SubflowReference(registration_id=registration_id),
        input_slots=["customers"],
        output_slots=["result", "row_count"],
        parameter_specs=[schemas.FlowParameter(name="limit", default_value="10", type="integer")],
    )
    defaults.update(overrides)
    return input_schema.NodeRunFlow(**defaults)


def _manifest_file(manifest: ProjectExportManifest, path: str) -> str:
    return next(f.content for f in manifest.files if f.path == path)


class TestRunFlowProjectExport:
    def test_subflow_module_and_call_site(self, tmp_path):
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=41, name="parent_flow")
        add_sample_input(flow, node_id=1)
        flow.add_run_flow(
            _run_flow_settings(
                flow,
                sub["registration_id"],
                parameter_bindings=[
                    input_schema.RunFlowParameterBinding(
                        parameter_name="limit", source="constant", constant_value="3"
                    )
                ],
            )
        )
        _keyed_connect(flow, 1, 9, "input-1")

        manifest = export_flow_to_project(flow)
        paths = {f.path for f in manifest.files}
        assert "subflows/head_subflow.py" in paths
        assert "subflows/__init__.py" in paths

        module = _manifest_file(manifest, "subflows/head_subflow.py")
        assert "def run(customers: ff.FlowFrame | None = None, *, limit: int = 10) -> dict[str, ff.FlowFrame]:" in module
        assert ".head(limit)" in module  # sentinel resolved to the kwarg
        assert '"result":' in module and '"row_count":' in module
        assert "customers if customers is not None else" in module  # FlowFrame arg used directly
        ast.parse(module)

        pipeline = _manifest_file(manifest, "pipeline.py")
        assert "from subflows import head_subflow" in pipeline
        # boundary naming may rename vars (df_1 -> source, df_9 -> df); assert shape, not names
        assert "_sf_9_outputs = head_subflow.run(customers=" in pipeline
        assert ", limit=3)" in pipeline
        # subflow modules speak FlowFrame end-to-end: no .data / FlowFrame() hops
        assert '= _sf_9_outputs["result"]' in pipeline
        assert '= _sf_9_outputs["row_count"]' in pipeline
        assert 'ff.FlowFrame(_sf_9_outputs' not in pipeline
        ast.parse(pipeline)

    def test_iterate_mode_emits_loop_with_metadata(self, tmp_path):
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=42, name="parent_iterate")
        add_sample_input(flow, node_id=1)
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow.flow_id,
                node_id=2,
                raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}, {"limit": 2}]),
            )
        )
        flow.add_run_flow(
            _run_flow_settings(
                flow,
                sub["registration_id"],
                iteration_mode="iterate",
                append_run_metadata=True,
                parameter_bindings=[
                    input_schema.RunFlowParameterBinding(
                        parameter_name="limit", source="column", column_name="limit"
                    )
                ],
            )
        )
        _keyed_connect(flow, 1, 9, "input-1")
        _keyed_connect(flow, 2, 9, "input-0")

        pipeline = _manifest_file(export_flow_to_project(flow), "pipeline.py")
        assert "_sf_9_param_rows = " in pipeline and ".collect().to_dicts()" in pipeline
        assert "head_subflow.run(customers=" in pipeline
        assert "limit=_sf_9_row['limit'])" in pipeline
        assert "ff.concat([" in pipeline
        assert 'how="diagonal_relaxed"' in pipeline
        assert 'ff.lit(_sf_9_row[\'limit\']).cast(ff.Int64).alias("param_limit")' in pipeline
        assert 'ff.lit(_sf_9_i).cast(ff.UInt32).alias("run_index")' in pipeline
        assert ".data" not in pipeline  # subflow boundary stays at the FlowFrame level
        ast.parse(pipeline)

    def test_first_value_column_binding(self, tmp_path):
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=43, name="parent_first")
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow.flow_id,
                node_id=2,
                raw_data_format=input_schema.RawData.from_pylist([{"limit": 2}]),
            )
        )
        flow.add_run_flow(
            _run_flow_settings(
                flow,
                sub["registration_id"],
                parameter_bindings=[
                    input_schema.RunFlowParameterBinding(
                        parameter_name="limit", source="column", column_name="limit"
                    )
                ],
            )
        )
        _keyed_connect(flow, 2, 9, "input-0")

        pipeline = _manifest_file(export_flow_to_project(flow), "pipeline.py")
        assert "_sf_9_params = " in pipeline and ".head(1).collect()" in pipeline
        assert "limit=_sf_9_params['limit'][0]" in pipeline
        assert "run_index" not in pipeline
        ast.parse(pipeline)

    def test_column_name_with_quote_first_value_is_valid_python(self, tmp_path):
        """A column name containing a double quote must go through repr() so the
        generated first-value call site stays valid Python. Raw interpolation
        produced _sf_9_params["a"b"][0], a SyntaxError (and a code-injection seam)."""
        col = 'a"b'
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=51, name="parent_quote_first")
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow.flow_id,
                node_id=2,
                raw_data_format=input_schema.RawData.from_pylist([{"limit": 2}]),
            )
        )
        flow.add_run_flow(
            _run_flow_settings(
                flow,
                sub["registration_id"],
                parameter_bindings=[
                    input_schema.RunFlowParameterBinding(
                        parameter_name="limit", source="column", column_name=col
                    )
                ],
            )
        )
        _keyed_connect(flow, 2, 9, "input-0")

        pipeline = _manifest_file(export_flow_to_project(flow), "pipeline.py")
        assert f"limit=_sf_9_params[{col!r}][0]" in pipeline
        ast.parse(pipeline)  # unescaped column name would raise SyntaxError here

    def test_column_name_with_quote_iterate_is_valid_python(self, tmp_path):
        """Iterate-mode loop kwargs and metadata exprs also repr() the column name."""
        col = 'a"b'
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=52, name="parent_quote_iter")
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow.flow_id,
                node_id=2,
                raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}, {"limit": 2}]),
            )
        )
        flow.add_run_flow(
            _run_flow_settings(
                flow,
                sub["registration_id"],
                iteration_mode="iterate",
                append_run_metadata=True,
                parameter_bindings=[
                    input_schema.RunFlowParameterBinding(
                        parameter_name="limit", source="column", column_name=col
                    )
                ],
            )
        )
        _keyed_connect(flow, 2, 9, "input-0")

        pipeline = _manifest_file(export_flow_to_project(flow), "pipeline.py")
        assert f"limit=_sf_9_row[{col!r}]" in pipeline          # loop kwarg
        assert f"ff.lit(_sf_9_row[{col!r}]).cast(" in pipeline  # metadata value_expr
        ast.parse(pipeline)

    def test_module_reuse_across_two_nodes(self, tmp_path):
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=44, name="parent_two_nodes")
        flow.add_run_flow(_run_flow_settings(flow, sub["registration_id"], node_id=9))
        flow.add_run_flow(_run_flow_settings(flow, sub["registration_id"], node_id=10))

        manifest = export_flow_to_project(flow)
        module_paths = [f.path for f in manifest.files if f.path.startswith("subflows/") and f.path != "subflows/__init__.py"]
        assert module_paths == ["subflows/head_subflow.py"]
        pipeline = _manifest_file(manifest, "pipeline.py")
        assert "_sf_9_outputs = head_subflow.run(" in pipeline
        assert "_sf_10_outputs = head_subflow.run(" in pipeline

    def test_circular_reference_reports_unsupported(self, tmp_path):
        from flowfile_core.flowfile.code_generator.code_generator import UnsupportedNodeError
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        path = tmp_path / "self_ref_export.yaml"
        path.write_text("placeholder")
        registration_id = _register_flow_file(path, "self_ref_export")
        flow = create_basic_flow(flow_id=45, name="self_ref_export")
        flow.add_run_flow(
            input_schema.NodeRunFlow(
                flow_id=flow.flow_id,
                node_id=9,
                user_id=1,
                flow_reference=input_schema.SubflowReference(registration_id=registration_id),
                input_slots=[],
                output_slots=[],
            )
        )
        flow.save_flow(str(path))

        reloaded = open_flow(path)
        with pytest.raises(UnsupportedNodeError, match="[Cc]ircular"):
            export_flow_to_project(reloaded)

    def test_pipeline_parameters_become_kwargs(self, tmp_path):
        flow = create_basic_flow(flow_id=46, name="param_pipeline")
        flow.flow_settings.parameters = [
            schemas.FlowParameter(name="n", default_value="2", type="integer"),
            schemas.FlowParameter(name="label", default_value="x y", type="string"),
        ]
        add_sample_input(flow, node_id=1)
        flow.add_polars_code(
            input_schema.NodePolarsCode(
                flow_id=flow.flow_id,
                node_id=2,
                polars_code_input=transform_schema.PolarsCodeInput(
                    polars_code='output_df = input_df.head(${n}).with_columns(pl.lit("v ${label}").alias("tag"))'
                ),
                depending_on_ids=[1],
            )
        )
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))

        pipeline = _manifest_file(export_flow_to_project(flow), "pipeline.py")
        assert "def run_etl_pipeline(*, n: int = 2, label: str = 'x y'):" in pipeline
        assert ".head(n)" in pipeline
        assert 'f"v {label}"' in pipeline
        assert "${" not in pipeline
        ast.parse(pipeline)

    def test_plain_exports_still_unsupported(self, tmp_path):
        from flowfile_core.flowfile.code_generator.code_generator import (
            FlowGraphToFlowFrameConverter,
            FlowGraphToPolarsConverter,
        )

        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=47, name="plain_export")
        flow.add_run_flow(_run_flow_settings(flow, sub["registration_id"]))
        for converter_cls in (FlowGraphToFlowFrameConverter, FlowGraphToPolarsConverter):
            converter = converter_cls(flow)
            try:
                converter.convert()
            except Exception:
                pass
            assert any(node_type == "run_flow" for _, node_type, _ in converter.unsupported_nodes)

    def test_project_with_subflow_executes_end_to_end(self, tmp_path):
        sub = _build_head_subflow(tmp_path)
        flow = create_basic_flow(flow_id=48, name="e2e_subflow_parent")
        add_sample_input(flow, node_id=1)  # 3 rows: id/age
        flow.add_manual_input(
            input_schema.NodeManualInput(
                flow_id=flow.flow_id,
                node_id=2,
                raw_data_format=input_schema.RawData.from_pylist([{"limit": 1}, {"limit": 2}]),
            )
        )
        flow.add_run_flow(
            _run_flow_settings(
                flow,
                sub["registration_id"],
                iteration_mode="iterate",
                append_run_metadata=True,
                parameter_bindings=[
                    input_schema.RunFlowParameterBinding(
                        parameter_name="limit", source="column", column_name="limit"
                    )
                ],
            )
        )
        _keyed_connect(flow, 1, 9, "input-1")
        _keyed_connect(flow, 2, 9, "input-0")

        manifest = export_flow_to_project(flow)
        project_dir = write_project(manifest, tmp_path)
        result = subprocess.run(
            [sys.executable, "main.py"], cwd=project_dir, capture_output=True, text=True, timeout=300
        )
        assert result.returncode == 0, result.stderr
        # head(1) + head(2) of the 3-row parent frame -> 3 rows with metadata columns
        assert "param_limit" in result.stdout
        assert "run_index" in result.stdout


# ==================== flowfile_ctx binding in exported custom-node code ====================


def _build_ctx_node_subflow(tmp_path: Path, node_cls) -> dict:
    """flow_input 'customers' -> ctx custom node -> flow_output 'result'.

    The custom node lives ONLY inside this subflow, so the parent project ships
    flowfile_ctx.py only if _needs_flowfile_ctx propagates up from the child.
    """
    flow = create_basic_flow(flow_id=33, name="ctx_subflow")
    flow.add_flow_input(
        input_schema.NodeFlowInput(
            flow_id=flow.flow_id,
            node_id=1,
            input_name="customers",
            raw_data_format=input_schema.RawData.from_pylist([{"name": "a"}, {"name": "b"}]),
        )
    )
    node_settings = input_schema.UserDefinedNode(flow_id=flow.flow_id, node_id=2, settings={}, is_user_defined=True)
    flow.add_user_defined_node(
        custom_node=node_cls.from_settings({}),
        user_defined_node_settings=node_settings,
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
    flow.add_flow_output(
        input_schema.NodeFlowOutput(flow_id=flow.flow_id, node_id=3, output_name="result", depending_on_id=2)
    )
    add_connection(flow, input_schema.NodeConnection.create_from_simple_input(2, 3))
    path = tmp_path / "ctx_subflow.yaml"
    flow.save_flow(str(path))
    return {"path": path, "registration_id": _register_flow_file(path, "ctx_subflow")}


def _ctx_custom_module(manifest: ProjectExportManifest) -> str:
    """The shipped custom_nodes/<mod>.py content (exactly one in these tests)."""
    path = next(
        p
        for p in file_paths(manifest)
        if p.startswith("custom_nodes/") and p.endswith(".py") and not p.endswith("__init__.py")
    )
    return get_file(manifest, path)


def test_flow_input_sample_expression_uses_public_api(tmp_path):
    """Bug 1: the flow_input sample expression must not import flowfile_core."""
    sub = _build_head_subflow(tmp_path)
    flow = create_basic_flow(flow_id=51, name="sample_public_api_parent")
    add_sample_input(flow, node_id=1)
    flow.add_run_flow(_run_flow_settings(flow, sub["registration_id"]))
    _keyed_connect(flow, 1, 9, "input-1")

    manifest = export_flow_to_project(flow)
    module = get_file(manifest, "subflows/head_subflow.py")
    assert "flowfile_core" not in module
    assert "ff.from_raw_data(" in module
    assert "RawData(" not in module
    ast.parse(module)


def test_custom_node_flowfile_ctx_executes_end_to_end(tmp_path):
    """Bug 2: an exported project with a flowfile_ctx-using custom node runs standalone."""
    mod = _write_node_module(tmp_path, "ctx_e2e_node", _CTX_NODE_SOURCE)
    try:
        add_to_custom_node_store(mod.CtxLogger)
        flow = create_basic_flow(name="ctx_e2e")
        add_sample_input(flow, node_id=1)
        node_settings = input_schema.UserDefinedNode(flow_id=1, node_id=2, settings={}, is_user_defined=True)
        flow.add_user_defined_node(
            custom_node=mod.CtxLogger.from_settings({}),
            user_defined_node_settings=node_settings,
        )
        add_connection(flow, input_schema.NodeConnection.create_from_simple_input(1, 2))
        manifest = export_flow_to_project(flow)
    finally:
        sys.modules.pop("ctx_e2e_node", None)

    assert "flowfile_ctx.py" in file_paths(manifest)
    assert "import flowfile_ctx" in _ctx_custom_module(manifest)

    project_dir = write_project(manifest, tmp_path)
    result = subprocess.run(
        [sys.executable, "main.py"], cwd=project_dir, capture_output=True, text=True, timeout=300
    )
    assert result.returncode == 0, result.stderr
    assert "ctx node ran" in result.stdout


def test_insert_flowfile_ctx_import_handles_decorated_first_statement():
    """A decorated first top-level class/def has lineno at the keyword, not the
    decorator; the import must go ABOVE the decorator or the module is a SyntaxError."""
    # Decorated class as the first statement.
    out = _insert_flowfile_ctx_import(
        "@nd.register\nclass N(nd.CustomNodeBase):\n    def process(self, m):\n        return m\n"
    )
    ast.parse(out)  # would raise if spliced between @nd.register and `class`
    lines = out.splitlines()
    assert lines.index("import flowfile_ctx") < next(i for i, line in enumerate(lines) if line.startswith("@"))

    # Stacked decorators after a docstring + __future__ import: stay after __future__,
    # still above the first decorator.
    out2 = _insert_flowfile_ctx_import(
        '"""doc"""\nfrom __future__ import annotations\n\n@deco_a\n@deco_b\nclass N:\n    pass\n'
    )
    ast.parse(out2)
    l2 = out2.splitlines()
    assert l2.index("from __future__ import annotations") < l2.index("import flowfile_ctx")
    assert l2.index("import flowfile_ctx") < next(i for i, line in enumerate(l2) if line.startswith("@"))


def test_subflow_custom_node_ships_flowfile_ctx_shim(tmp_path):
    """Bug 2 regression guard: _needs_flowfile_ctx must propagate up from a subflow so
    a custom node living ONLY inside the subflow still ships flowfile_ctx.py."""
    mod = _write_node_module(tmp_path, "ctx_subflow_node", _CTX_NODE_SOURCE)
    try:
        add_to_custom_node_store(mod.CtxLogger)
        sub = _build_ctx_node_subflow(tmp_path, mod.CtxLogger)
        flow = create_basic_flow(flow_id=52, name="ctx_subflow_parent")
        add_sample_input(flow, node_id=1)
        flow.add_run_flow(
            _run_flow_settings(flow, sub["registration_id"], output_slots=["result"], parameter_specs=[])
        )
        _keyed_connect(flow, 1, 9, "input-1")
        manifest = export_flow_to_project(flow)
    finally:
        sys.modules.pop("ctx_subflow_node", None)

    assert "flowfile_ctx.py" in file_paths(manifest)
    assert "import flowfile_ctx" in _ctx_custom_module(manifest)
    ast.parse(get_file(manifest, "subflows/ctx_subflow.py"))
