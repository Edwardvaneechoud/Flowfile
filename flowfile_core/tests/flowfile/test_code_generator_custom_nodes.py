"""
Tests for code generation of custom/user-defined nodes.
Tests that generated code includes proper imports, settings classes, and correct process invocations.
"""

import polars as pl
import pytest

from flowfile_core.configs.node_store import add_to_custom_node_store
from flowfile_core.flowfile.code_generator.code_generator import (
    export_flow_to_flowframe,
    export_flow_to_polars,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.node_designer import (
    ColumnSelector,
    CustomNodeBase,
    NodeSettings,
    Section,
    TextInput,
)
from flowfile_core.schemas import input_schema, schemas


@pytest.fixture(autouse=True)
def _restore_custom_node_registry():
    """Snapshot the global node registries and restore them after each test so a
    custom node registered via add_to_custom_node_store never leaks into sibling
    tests (which mutates CUSTOM_NODE_STORE plus node_dict and nodes_list)."""
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


def create_flowfile_handler() -> FlowfileHandler:
    handler = FlowfileHandler()
    return handler


def create_graph(flow_id: int = 1, execution_mode: str = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=flow_id,
        name=f'flow_{flow_id}',
        path='.',
        execution_mode=execution_mode
    ))
    return handler.get_flow(flow_id)


def add_manual_input(graph: FlowGraph, data: list[dict], node_id: int = 1) -> FlowGraph:
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type='manual_input'
    )
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=node_id,
        raw_data_format=input_schema.RawData.from_pylist(data)
    )
    graph.add_manual_input(input_file)
    return graph


def add_custom_node_to_graph(
        graph: FlowGraph,
        custom_node_class: type,
        node_id: int,
        settings: dict,
        output_names: list[str] | None = None,
) -> FlowGraph:
    """Helper to add a custom node to a graph with settings."""
    node_promise = input_schema.NodePromise(
        flow_id=graph.flow_id,
        node_id=node_id,
        node_type=custom_node_class().item,
        is_user_defined=True
    )
    graph.add_node_promise(node_promise)
    user_defined_node = custom_node_class.from_settings(settings)
    node_settings = input_schema.UserDefinedNode(
        flow_id=graph.flow_id,
        node_id=node_id,
        settings=settings,
        output_names=output_names or ["main"],
        is_user_defined=True
    )
    graph.add_user_defined_node(
        custom_node=user_defined_node,
        user_defined_node_settings=node_settings
    )
    return graph


def connect(graph: FlowGraph, from_id: int, to_id: int, input_handle: str = "input-0") -> None:
    """Connect two nodes on a specific input handle (multi-input custom nodes use input-1/2/...)."""
    conn = input_schema.NodeConnection.create_from_simple_input(from_id, to_id)
    conn.input_connection.connection_class = input_handle
    add_connection(graph, conn)


def add_record_count(graph: FlowGraph, node_id: int, depends_on: int) -> FlowGraph:
    """Wire a downstream record_count node — a FlowFrame-only op (emits ff.len())."""
    graph.add_record_count(
        input_schema.NodeRecordCount(flow_id=graph.flow_id, node_id=node_id, depending_on_id=depends_on)
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(depends_on, node_id))
    return graph


@pytest.fixture
def AddColumnNode():
    """A custom node that adds a column with a fixed value."""

    class AddColumnSettings(NodeSettings):
        config: Section = Section(
            title="Configuration",
            column_name=TextInput(
                label="New Column Name",
                default="new_column",
            ),
            fixed_value=TextInput(
                label="Fixed Value",
                default="default_value",
            ),
        )

    class AddColumn(CustomNodeBase):
        node_name: str = "Add Column"
        node_category: str = "Transform"
        title: str = "Add Fixed Column"
        intro: str = "Adds a new column with a fixed value"
        number_of_inputs: int = 1
        number_of_outputs: int = 1
        settings_schema: AddColumnSettings = AddColumnSettings()

        def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
            lf = inputs[0]
            col_name = self.settings_schema.config.column_name.value
            value = self.settings_schema.config.fixed_value.value
            return lf.with_columns(pl.lit(value).alias(col_name))

    return AddColumn


class TestCustomNodeCodeGeneration:
    """Integration tests for generating code with custom nodes."""

    @pytest.mark.parametrize(
        "export_func", [export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"]
    )
    def test_generated_code_includes_custom_node_imports(self, AddColumnNode, export_func):
        """Test that generated code includes necessary imports."""
        add_to_custom_node_store(AddColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_func(graph)

        # Canonical re-added imports: nd alias plus the public node_designer SDK
        # symbols. Neither the internal shared.* nor core.node_designer spelling
        # leaks into user-facing exported code.
        assert "from flowfile import node_designer as nd" in code
        assert "from flowfile.node_designer import (" in code
        assert "CustomNodeBase" in code
        assert "from shared.node_designer import" not in code
        assert "from flowfile_core.flowfile.node_designer import" not in code

    def test_generated_code_no_unnecessary_collect_lazy(self, AddColumnNode):
        """Test that LazyFrame-based process methods don't have unnecessary collect/lazy."""
        add_to_custom_node_store(AddColumnNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)

        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # Since AddColumn.process accepts LazyFrame and returns LazyFrame,
        # there should be no .collect() or .lazy() in the process call
        lines = code.split('\n')
        process_lines = [l for l in lines if '_custom_node_' in l and '.process(' in l]

        for line in process_lines:
            assert '.collect()' not in line, f"Should not have .collect() for LazyFrame process: {line}"
            assert '.lazy()' not in line, f"Should not have .lazy() for LazyFrame process: {line}"


def _write_node_module(tmp_path, stem: str, source: str):
    """Write a custom-node module to a temp file, import it, register it.

    File-backed because the export path reads the class's source via
    ``inspect.getfile`` (which fails for classes defined in exec'd strings).
    """
    import importlib
    import sys

    path = tmp_path / f"{stem}.py"
    path.write_text(source)
    sys.path.insert(0, str(tmp_path))
    try:
        mod = importlib.import_module(stem)
    finally:
        sys.path.remove(str(tmp_path))
    return mod


class TestReturnNormalization:
    """The exported call passes lazy inputs and isinstance-normalizes the return."""

    def test_lazy_in_no_collect_isinstance_normalize_out(self, AddColumnNode):
        add_to_custom_node_store(AddColumnNode)
        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)
        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)

        # No .collect() on the way in; the return is normalized via isinstance.
        process_line = next(l for l in code.split("\n") if "_out_2 = " in l and ".process(" in l)
        assert ".collect()" not in process_line
        assert any(
            "isinstance(_out_2, pl.DataFrame)" in l and "_out_2.lazy()" in l for l in code.split("\n")
        )


class TestFlowFrameConverter:
    """FlowFrame export wraps a custom node's polars output back into a FlowFrame and
    bridges FlowFrame inputs down to polars for the process() contract."""

    def test_output_wrapped_and_input_bridged(self, AddColumnNode):
        add_to_custom_node_store(AddColumnNode)
        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)
        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_flowframe(graph)

        process_line = next(l for l in code.split("\n") if "_out_2 = " in l and ".process(" in l)
        # Input bridged to the underlying polars LazyFrame via .data (no eager collect).
        assert ".data" in process_line
        assert ".collect()" not in process_line
        # Output re-wrapped as a FlowFrame (not the polars isinstance/.lazy() form).
        assert any("ff.FlowFrame(_out_2)" in l for l in code.split("\n"))
        assert "isinstance(_out_2, pl.DataFrame)" not in code

    def test_custom_node_executes_with_downstream_ff_op(self, AddColumnNode):
        """Regression guard: the custom-node output feeds a FlowFrame-only op
        (record_count -> ff.len()), which only runs if the output is a FlowFrame."""
        add_to_custom_node_store(AddColumnNode)
        graph = create_graph()
        add_manual_input(graph, [{"Column 1": "test"}], node_id=1)
        settings = {"config": {"column_name": "new_col", "fixed_value": "hello"}}
        add_custom_node_to_graph(graph, AddColumnNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
        add_record_count(graph, node_id=3, depends_on=2)

        code = export_flow_to_flowframe(graph)

        ns: dict = {}
        exec(compile(code, "<gen>", "exec"), ns)
        result = ns["run_etl_pipeline"]()
        collected = result.collect() if hasattr(result, "collect") else result
        assert collected.columns == ["number_of_records"]
        assert collected["number_of_records"].to_list() == [1]


class TestSdkImportIsConditional:
    """The bare-symbol SDK import is emitted only when the node body needs it."""

    def test_nd_style_node_omits_bare_import(self, tmp_path):
        source = '''
import polars as pl

from flowfile import node_designer as nd


class NdNode(nd.CustomNodeBase):
    node_name: str = "Nd Node"
    node_category: str = "Transform"
    number_of_inputs: int = 1

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''
        mod = _write_node_module(tmp_path, "nd_style_mod", source)
        try:
            add_to_custom_node_store(mod.NdNode)
            graph = create_graph()
            add_manual_input(graph, [{"x": 1}], node_id=1)
            add_custom_node_to_graph(graph, mod.NdNode, node_id=2, settings={})
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            code = export_flow_to_polars(graph)
        finally:
            import sys

            sys.modules.pop("nd_style_mod", None)

        # Body only uses nd.* → the bare-symbol import would be dead, so it is dropped.
        assert "from flowfile import node_designer as nd" in code
        assert "from flowfile.node_designer import (" not in code

    def test_bare_symbol_node_emits_bare_import(self, tmp_path):
        source = '''
import polars as pl

from flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class BareSettings(NodeSettings):
    cfg: Section = Section(title="C", lab=TextInput(label="L", default="x"))


class BareNode(CustomNodeBase):
    node_name: str = "Bare Node"
    node_category: str = "Transform"
    number_of_inputs: int = 1
    settings_schema: BareSettings = BareSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''
        mod = _write_node_module(tmp_path, "bare_symbol_mod", source)
        try:
            add_to_custom_node_store(mod.BareNode)
            graph = create_graph()
            add_manual_input(graph, [{"x": 1}], node_id=1)
            add_custom_node_to_graph(graph, mod.BareNode, node_id=2, settings={})
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            code = export_flow_to_polars(graph)
        finally:
            import sys

            sys.modules.pop("bare_symbol_mod", None)

        # Body uses unqualified CustomNodeBase/Section/... → the bare import is needed.
        assert "from flowfile.node_designer import (" in code


class TestMultiInputOrdering:
    """Multi-input custom nodes emit every input in connected-input order."""

    def test_two_inputs_both_emitted_in_order(self, tmp_path):
        source = '''
import polars as pl

from flowfile import node_designer as nd


class TwoInputNode(nd.CustomNodeBase):
    node_name: str = "Two Input Node"
    node_category: str = "Transform"
    number_of_inputs: int = 2

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        a, b = inputs
        return a.join(b, how="cross")
'''
        mod = _write_node_module(tmp_path, "two_input_mod", source)
        try:
            add_to_custom_node_store(mod.TwoInputNode)
            graph = create_graph()
            add_manual_input(graph, [{"a": 1}], node_id=1)
            add_manual_input(graph, [{"b": 2}], node_id=3)
            add_custom_node_to_graph(graph, mod.TwoInputNode, node_id=2, settings={})
            connect(graph, 1, 2, "input-1")
            connect(graph, 3, 2, "input-2")

            code = export_flow_to_polars(graph)
        finally:
            import sys

            sys.modules.pop("two_input_mod", None)

        process_line = next(l for l in code.split("\n") if "_out_2 = " in l and ".process(" in l)
        # Two positional args (the old code dropped non-"main" inputs entirely).
        args = process_line.split(".process(")[1].rsplit(")", 1)[0]
        assert args.count(",") == 1, process_line

        # The generated script runs and produces the cross join.
        ns: dict = {}
        exec(compile(code, "<gen>", "exec"), ns)
        result = ns["run_etl_pipeline"]()
        collected = result.collect() if hasattr(result, "collect") else result
        assert set(collected.columns) == {"a", "b"}
        assert collected.height == 1


class TestMultiOutputHandles:
    """Multi-output dict returns unpack into one var per output handle."""

    def test_multi_output_unpacks_and_runs(self, tmp_path):
        source = '''
import polars as pl

from flowfile import node_designer as nd


class SplitNode(nd.CustomNodeBase):
    node_name: str = "Split Node"
    node_category: str = "Transform"
    number_of_outputs: int = 2
    output_names: list = ["hi", "lo"]

    def process(self, *inputs: pl.LazyFrame):
        lf = inputs[0]
        return {"hi": lf.filter(pl.col("v") > 1), "lo": lf.filter(pl.col("v") <= 1)}
'''
        mod = _write_node_module(tmp_path, "split_mod", source)
        try:
            add_to_custom_node_store(mod.SplitNode)
            graph = create_graph()
            add_manual_input(graph, [{"v": 0}, {"v": 1}, {"v": 2}], node_id=1)
            add_custom_node_to_graph(
                graph, mod.SplitNode, node_id=2, settings={}, output_names=["hi", "lo"]
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            code = export_flow_to_polars(graph)
        finally:
            import sys

            sys.modules.pop("split_mod", None)

        # One process call, one var per declared output handle. Dict keys are
        # emitted via repr (single-quoted).
        assert "_outs_2 = " in code
        assert "_outs_2['hi']" in code
        assert "_outs_2['lo']" in code

        ns: dict = {}
        exec(compile(code, "<gen>", "exec"), ns)
        ns["run_etl_pipeline"]()  # must not raise


class TestExtraImportCarry:
    """A node's own non-designer imports survive; designer imports are dropped/re-added."""

    def test_non_polars_import_carried(self, tmp_path):
        source = '''
import math

import polars as pl

from flowfile import node_designer as nd


class MathNode(nd.CustomNodeBase):
    node_name: str = "Math Carry Node"
    node_category: str = "Transform"
    number_of_inputs: int = 1

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(math.pi).alias("pi"))
'''
        mod = _write_node_module(tmp_path, "math_carry_mod", source)
        try:
            add_to_custom_node_store(mod.MathNode)
            graph = create_graph()
            add_manual_input(graph, [{"x": 1}], node_id=1)
            add_custom_node_to_graph(graph, mod.MathNode, node_id=2, settings={})
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            code = export_flow_to_polars(graph)
        finally:
            import sys

            sys.modules.pop("math_carry_mod", None)

        assert "import math" in code
        # The node's own designer import spelling is not carried verbatim.
        assert code.count("node_designer") >= 1


class TestIsolatedEnvHeader:
    """Kernel-environment nodes export with a pip-install header comment."""

    def test_dependencies_listed_in_header(self, tmp_path):
        source = '''
import polars as pl

from flowfile import node_designer as nd


class KernelSettings(nd.NodeSettings):
    cfg: nd.Section = nd.Section(title="C", lab=nd.TextInput(label="L", default="x"))


class KernelNode(nd.CustomNodeBase):
    node_name: str = "Kernel Env Node"
    node_category: str = "Transform"
    environment: str = "kernel"
    dependencies: list = ["scikit-learn", "polars-ds"]
    settings_schema: KernelSettings = KernelSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0].with_columns(pl.lit(1).alias("k"))
'''
        mod = _write_node_module(tmp_path, "kernel_env_mod", source)
        try:
            add_to_custom_node_store(mod.KernelNode)
            graph = create_graph()
            add_manual_input(graph, [{"x": 1}], node_id=1)
            node_promise = input_schema.NodePromise(
                flow_id=graph.flow_id, node_id=2, node_type=mod.KernelNode().item, is_user_defined=True
            )
            graph.add_node_promise(node_promise)
            node_settings = input_schema.UserDefinedNode(
                flow_id=graph.flow_id, node_id=2, settings={}, kernel_id="k1", is_user_defined=True
            )
            graph.add_user_defined_node(
                custom_node=mod.KernelNode.from_settings({}),
                user_defined_node_settings=node_settings,
            )
            add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

            code = export_flow_to_polars(graph)
        finally:
            import sys

            sys.modules.pop("kernel_env_mod", None)

        assert "# Requires: pip install scikit-learn polars-ds" in code


def test_custom_node_preserves_non_polars_imports(tmp_path):
    """A custom node's own runtime imports (e.g. ``import math``) survive into the export."""
    import importlib
    import sys

    module_source = '''
import math

import polars as pl

from flowfile_core.flowfile.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class MathSettings(NodeSettings):
    config: Section = Section(
        title="Configuration",
        column_name=TextInput(label="Column", default="pi"),
    )


class MathNode(CustomNodeBase):
    node_name: str = "Math Node"
    node_category: str = "Transform"
    title: str = "Math Node"
    intro: str = "Adds a pi column"
    number_of_inputs: int = 1
    number_of_outputs: int = 1
    settings_schema: MathSettings = MathSettings()

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        lf = inputs[0]
        col_name = self.settings_schema.config.column_name.value
        return lf.with_columns(pl.lit(math.pi).alias(col_name))
'''
    module_path = tmp_path / "math_custom_node_mod.py"
    module_path.write_text(module_source)
    sys.path.insert(0, str(tmp_path))
    try:
        mod = importlib.import_module("math_custom_node_mod")
        add_to_custom_node_store(mod.MathNode)

        graph = create_graph()
        add_manual_input(graph, [{"Column 1": 1}], node_id=1)
        settings = {"config": {"column_name": "pi"}}
        add_custom_node_to_graph(graph, mod.MathNode, node_id=2, settings=settings)
        add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

        code = export_flow_to_polars(graph)
    finally:
        sys.path.remove(str(tmp_path))
        sys.modules.pop("math_custom_node_mod", None)

    assert "import math" in code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
