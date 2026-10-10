"""The FlowFrame export's ``ff`` dialect: placeholders, per-statement emissions and native flow ports."""

import ast

import pytest

from flowfile_core.flowfile.code_generator import FlowGraphToFlowFrameConverter
from flowfile_core.schemas import input_schema, schemas
from tests.flowfile.test_project_exporter import (
    _add_rename_select,
    _connect,
    add_notebook_node,
    add_sample_input,
    create_basic_flow,
)


def _flow_with_an_unconfigured_node():
    """1 source -> 2 -> 3 selects (one fused statement), 4 promise -> 5 select, 6 explore_data off 3."""
    flow = create_basic_flow(flow_id=501, name="placeholders")
    add_sample_input(flow, node_id=1)
    _add_rename_select(flow, node_id=2, depending_on_id=1, old="age", new="years")
    _add_rename_select(flow, node_id=3, depending_on_id=2, old="id", new="key")
    flow.add_node_promise(input_schema.NodePromise(flow_id=flow.flow_id, node_id=4, node_type="filter"))
    _connect(flow, 3, 4)
    _add_rename_select(flow, node_id=5, depending_on_id=4, old="key", new="k")
    flow.add_explore_data(input_schema.NodeExploreData(flow_id=flow.flow_id, node_id=6, depending_on_id=3))
    _connect(flow, 3, 6)
    return flow


def test_placeholders_bind_downstream_and_carry_their_reason():
    converter = FlowGraphToFlowFrameConverter(_flow_with_an_unconfigured_node(), placeholders=True)
    code = converter.convert()

    ast.parse(code)
    emitted = [(em.node_ids, em.placeholder_reason) for em in converter.emissions()]
    assert emitted == [
        ([1, 2, 3], None),
        ([4], "not configured yet"),
        ([5], "downstream of node 4, which is not editable as code"),
        ([6], None),
    ]
    placeholder = converter.emissions()[1].code
    assert placeholder == "filtered = ff.canvas_node(4, selected_1)  # Filter data: not configured yet"
    assert converter.emissions()[3].code == "ff.explore(selected_1)"


def test_placeholders_are_opt_in():
    code = FlowGraphToFlowFrameConverter(_flow_with_an_unconfigured_node()).convert()

    assert "canvas_node" not in code


def test_emissions_resolve_parameters_and_expose_the_wrapper_parts():
    flow = create_basic_flow(flow_id=502, name="params")
    flow.flow_settings.parameters = [schemas.FlowParameter(name="label", default_value="x", type="string")]
    add_sample_input(flow, node_id=1)
    _add_rename_select(flow, node_id=2, depending_on_id=1, old="age", new="age_${label}")
    converter = FlowGraphToFlowFrameConverter(flow)

    code = converter.convert()

    assert "def run_etl_pipeline(*, label: str = 'x'):" in code
    assert [em.node_ids for em in converter.emissions()] == [[1, 2]]
    assert 'ff.col("age").alias(f"age_{label}")' in converter.emissions()[0].code
    assert converter.import_lines() == ["import flowfile as ff"]
    assert [p.name for p in converter.parameters()] == ["label"]


def test_flow_ports_export_as_native_classes_and_run():
    flow = create_basic_flow(flow_id=503, name="ports")
    flow.add_flow_input(
        input_schema.NodeFlowInput(
            flow_id=flow.flow_id,
            node_id=1,
            input_name="customers",
            raw_data_format=input_schema.RawData.from_pylist([{"a": 1}, {"a": 2}]),
        )
    )
    _add_rename_select(flow, node_id=2, depending_on_id=1, old="a", new="b")
    flow.add_flow_output(input_schema.NodeFlowOutput(flow_id=flow.flow_id, node_id=3, output_name="result"))
    _connect(flow, 2, 3)

    code = FlowGraphToFlowFrameConverter(flow).convert()

    assert "flow = ff.create_flow_graph()" in code
    assert "ff.FlowInput(" in code and "flow_graph=flow," in code
    assert 'sample=pl.DataFrame({"a": [1, 2]}, schema={"a": ff.Int64}, strict=False),' in code
    assert '.to_flow_output("result")' in code
    namespace: dict = {}
    exec(code, namespace)
    assert namespace["run_etl_pipeline"]().collect()["b"].to_list() == [1, 2]


@pytest.mark.parametrize("placeholders", [False, True])
def test_python_script_exports_as_a_native_script(placeholders):
    flow = create_basic_flow(flow_id=504, name="script")
    add_sample_input(flow, node_id=1)
    cells = ["df = flowfile_ctx.read_inputs()['main'][0]", "flowfile_ctx.publish_output(df)"]
    add_notebook_node(flow, 2, [1], cells=cells)
    _connect(flow, 1, 2)

    code = FlowGraphToFlowFrameConverter(flow, placeholders=placeholders).convert()

    assert "scripted = ff.PythonScript(\n        source,\n        cells=[" in code
    assert "(\"cell-1\", \"flowfile_ctx.publish_output(df)\")," in code


DRAWER_TEMPLATE = (
    "import polars as pl\n\ndf = flowfile_ctx.read_input()\n\n# Your transformation here\n\n"
    "flowfile_ctx.publish_output(df.count())\n"
)
DRAWER_TEMPLATE_CELL = '''\
@ff.python_script(kernel="lite", description="")
def _script_2():
    import polars as pl

    df = flowfile_ctx.read_input()

    # Your transformation here

    flowfile_ctx.publish_output(df.count())


python_script_2 = _script_2(source_1)'''


def _script_flow(cells: list[str], *, inputs: int = 1, outputs: list[str] | None = None, flow_id: int = 510):
    """``inputs`` sample sources into one Python Script (the last node) holding ``cells`` as the drawer stores them."""
    flow = create_basic_flow(flow_id=flow_id, name="drawer_script")
    sources = list(range(1, inputs + 1))
    for node_id in sources:
        add_sample_input(flow, node_id=node_id)
    script = input_schema.PythonScriptInput(
        code="\n\n".join(cell for cell in cells if cell),
        kernel_id="lite",
        cells=[input_schema.NotebookCell(id=f"cell-{i}", code=cell) for i, cell in enumerate(cells)],
    )
    flow.add_python_script(
        input_schema.NodePythonScript(
            flow_id=flow.flow_id,
            node_id=inputs + 1,
            depending_on_ids=sources,
            python_script_input=script,
            output_names=outputs or ["main"],
        )
    )
    for node_id in sources:
        _connect(flow, node_id, inputs + 1)
    return flow


def _emitted_cells(code: str) -> list[list[str]]:
    """The cell texts each ``ff.PythonScript(cells=...)`` call in ``code`` evaluates to."""
    calls = [node for node in ast.walk(ast.parse(code)) if isinstance(node, ast.Call)]
    scripts = [node for node in calls if ast.unparse(node.func) == "ff.PythonScript"]
    return [[text for _, text in ast.literal_eval(kw.value)] for node in scripts for kw in node.keywords if kw.arg == "cells"]


@pytest.mark.parametrize("placeholders", [False, True])
def test_a_multi_line_cell_exports_as_the_stored_text(placeholders):
    """The wrapper's indent does not reach into a cell's text, and a script is not fused into the call below it."""
    cells = [
        "import polars as pl\n\ndf = flowfile_ctx.read_input()\n\nflowfile_ctx.publish_output(df)\n",
        'text = """a\n   \nb"""',
        "flowfile_ctx.publish_output(\n    df\n)",
        "n = 1",
    ]
    flow = _script_flow(cells, flow_id=516)
    _add_rename_select(flow, node_id=3, depending_on_id=2, old="age", new="years")
    converter = FlowGraphToFlowFrameConverter(flow, placeholders=placeholders)

    assert _emitted_cells(converter.convert()) == [cells]
    script = converter.emissions()[1]
    assert script.node_ids == [2] and _emitted_cells(script.code) == [cells]


def _script_statement(flow) -> str:
    """The script's statement as the notebook render asks for it (decorated when its cells regenerate)."""
    converter = FlowGraphToFlowFrameConverter(flow, placeholders=True, deterministic_names=True, decorated_scripts=True)
    converter.convert()
    return converter.emissions(verbatim_refs=True)[-1].code


def test_a_drawer_script_is_a_function_without_a_return_only_where_the_export_asks_for_it():
    """The wrapped export indents its body into ``run_etl_pipeline``, where a decorated ``def`` is not
    module-level, so it keeps the cells; the notebook render writes the body as written."""
    flow = _script_flow([DRAWER_TEMPLATE])
    for placeholders in (False, True):
        wrapped = FlowGraphToFlowFrameConverter(flow, placeholders=placeholders).convert()
        assert "ff.PythonScript(" in wrapped and "@ff.python_script" not in wrapped
    assert _script_statement(flow) == DRAWER_TEMPLATE_CELL


def test_a_drawer_script_is_decorated_where_inspect_cannot_read_compiled_text(monkeypatch):
    """A PyInstaller build patches ``inspect.getsourcefile`` to map a relative ``co_filename`` into the bundle, so
    ``inspect.getsource`` fails for anything compiled from a string; the render regenerates from text and stays
    decorated (the installed 0.22.0 app wrote every drawer script as ``ff.PythonScript``)."""
    import inspect
    import os

    bundle = "/Applications/Flowfile.app/Contents/Resources/binaries/_internal"
    original = inspect.getsourcefile

    def frozen_getsourcefile(obj):
        filename = os.path.normpath(inspect.getfile(obj))
        if not os.path.isabs(filename):
            return os.path.normpath(os.path.join(bundle, filename))
        return filename if filename.startswith(bundle) else original(obj)

    monkeypatch.setattr(inspect, "getsourcefile", frozen_getsourcefile)
    assert _script_statement(_script_flow([DRAWER_TEMPLATE], flow_id=517)) == DRAWER_TEMPLATE_CELL


def test_a_drawer_scripts_frames_go_in_the_call():
    cells = ["left, right = flowfile_ctx.read_inputs()['main']\n", "", "flowfile_ctx.publish_output(left, 'a')\n"]
    two = _script_statement(_script_flow(cells, inputs=2, outputs=["a", "b"], flow_id=511))
    assert two.startswith('@ff.python_script(kernel="lite", outputs=["a", "b"], description="")\ndef _script_3():\n')
    assert "\n    # %%\n    flowfile_ctx.publish_output(left, 'a')\n" in two
    assert two.endswith("\n\n\npython_script_3 = _script_3.node(source_1, source_2)")

    made = "import polars as pl\nflowfile_ctx.publish_output(pl.DataFrame({'a': [1]}))"
    none = _script_statement(_script_flow([made], inputs=0, flow_id=512))
    assert none.endswith("\n\n\npython_script_1 = _script_1()")


_PUBLISH = "flowfile_ctx.publish_output(flowfile_ctx.read_input())"


def _ladder(rungs: int) -> str:
    return "n = 0\nif n < 0:\n    pass\n" + "".join(f"elif n == {i}:\n    pass\n" for i in range(rungs)) + _PUBLISH


KEPT_AS_CELLS = {
    "multi_line_string": f'text = """a\nb"""\n{_PUBLISH}',
    "multi_line_f_string": f'n = 1\ntext = f"""a\n{{n}}"""\n{_PUBLISH}',
    "cell_marker_inside_a_cell": f"n = 1\n# %%\n{_PUBLISH}",
    "module_used_without_an_import": "flowfile_ctx.publish_output(flowfile_ctx.read_input().filter(pl.col('age') > 1))",
    "top_level_return": f"{_PUBLISH}\nreturn",
    "top_level_return_of_a_frame": "return flowfile_ctx.read_input()",
    "whitespace_only_line": f"n = 1\n   \n{_PUBLISH}",
    "star_import": f"from math import *\n{_PUBLISH}",
    "no_publish": "df = flowfile_ctx.read_input()",
    "outside_name": f"n = not_defined_anywhere\n{_PUBLISH}",
    "marker_comment": f"# flowfile: outputs\n{_PUBLISH}",
    "carriage_return": f"n = 1\rm = 2\n{_PUBLISH}",
    "deeper_than_a_cell_reads": _ladder(98),
}


@pytest.mark.parametrize("name", sorted(KEPT_AS_CELLS))
def test_a_script_that_would_not_come_back_the_same_keeps_its_cells(name):
    statement = _script_statement(_script_flow([KEPT_AS_CELLS[name]], flow_id=513))
    assert statement.startswith("python_script_2 = ff.PythonScript(") and "@ff.python_script" not in statement


def test_the_cases_kept_as_cells_are_one_step_from_a_script_that_is_decorated():
    for cell in (f"n = 1\n\n{_PUBLISH}", f"from math import floor\n{_PUBLISH}", f"total = sum([1])\n{_PUBLISH}", _ladder(5)):
        assert _script_statement(_script_flow([cell], flow_id=514)).startswith("@ff.python_script("), cell


def test_a_script_using_a_builtin_a_node_is_named_after_keeps_its_cells():
    """A notebook binds a node reference as a variable, which the function's body would read instead of the builtin."""
    from flowfile_frame.native import set_node_reference

    flow = _script_flow([f"total = sum([1])\n{_PUBLISH}"], flow_id=515)
    assert _script_statement(flow).startswith("@ff.python_script(")
    set_node_reference(flow, 1, "sum")
    statement = _script_statement(flow)
    assert statement.startswith("python_script_2 = ff.PythonScript(\n    sum,") and "@ff.python_script" not in statement


# visual groups: declared as ff.FlowGroup, every grouped node's statement ends in .add_to_group(...)


def _grouped_graph():
    """Nested groups over a fused chain, a described join, a gate and a tuple-assigning split."""
    import flowfile_frame as ff

    outer = ff.FlowGroup("Clean data", color="blue")
    inner = ff.FlowGroup("Inner step", parent_group=outer)
    source = ff.from_dict({"id": [1, 2, 3], "x": [1, 2, 3]})
    selected = source.filter(ff.col("x") > 1).add_to_group(outer).select(["id", "x"]).add_to_group(inner)
    names = ff.from_dict({"id": [1, 2, 3], "y": ["p", "q", "r"]}, flow_graph=source.flow_graph)
    joined = selected.join(names, on="id", description="Add y").add_to_group(outer)
    gate = ff.Gate(joined, formula="[x] > 1").add_to_group(outer)
    computed = gate.then.with_columns(ff.lit(1).alias("one")).add_to_group(inner)
    train, _test = computed.random_split({"train": 50, "test": 50}, seed=1)
    train.add_to_group(outer)
    return computed.flow_graph


def _group_shape(graph):
    by_id = {g.id: g for g in graph._groups.values()}
    return sorted(
        (
            g.name,
            g.color,
            by_id[g.parent_group_id].name if g.parent_group_id else None,
            len(graph._member_node_ids(g.id)),
        )
        for g in by_id.values()
    )


def test_groups_export_as_flow_groups_and_rebuild_on_exec():
    graph = _grouped_graph()
    converter = FlowGraphToFlowFrameConverter(graph)
    code = converter.convert()

    assert converter.group_lines() == [
        'clean_data = ff.FlowGroup("Clean data", color="blue")',
        'inner_step = ff.FlowGroup("Inner step", parent_group=clean_data)',
    ]
    assert code.index("clean_data = ff.FlowGroup") < code.index("inner_step = ff.FlowGroup") < code.index(".filter(")
    assert '.filter(ff.col("x") > 1).add_to_group(clean_data)' in code
    assert "]).add_to_group(inner_step)" in code  # the fused chain tags every link
    assert 'description="Add y").add_to_group(clean_data)' in code  # description inside the call, the group after
    assert "else_output=True).add_to_group(clean_data)" in code  # a native node joins a group too
    assert "\n    train.add_to_group(clean_data)\n" in code  # a tuple assignment gets its own line
    assert "_group_" not in code and converter.warnings == []

    namespace: dict = {}
    exec(code, namespace)
    rebuilt = namespace["run_etl_pipeline"]().flow_graph
    assert (
        _group_shape(rebuilt)
        == _group_shape(graph)
        == [("Clean data", "blue", None, 4), ("Inner step", None, "Clean data", 2)]
    )


def test_group_names_never_shadow_node_names_or_builtins():
    import flowfile_frame as ff

    source = ff.from_dict({"a": [1, 2]})
    # the described filter is a boundary bound to ``filtered``, which the group of that name must not shadow
    filtered = source.filter(ff.col("a") > 1, description="keep").add_to_group(ff.FlowGroup("filtered"))
    filtered.select(["a"]).add_to_group(ff.FlowGroup("list")).sort("a").add_to_group(ff.FlowGroup("1 / 2 %"))
    converter = FlowGraphToFlowFrameConverter(filtered.flow_graph)
    code = converter.convert()
    assert converter.group_lines() == [
        'filtered_2 = ff.FlowGroup("filtered")',
        'list_2 = ff.FlowGroup("list")',
        'group_1_2 = ff.FlowGroup("1 / 2 %")',
    ]
    assert "\n    filtered = (\n" in code and 'description="keep").add_to_group(filtered_2)' in code
    namespace: dict = {}
    exec(code, namespace)
    assert sorted(g.name for g in namespace["run_etl_pipeline"]().flow_graph._groups.values()) == [
        "1 / 2 %",
        "filtered",
        "list",
    ]


def test_the_polars_export_ignores_groups():
    from flowfile_core.flowfile.code_generator import FlowGraphToPolarsConverter

    code = FlowGraphToPolarsConverter(_grouped_graph()).convert()
    assert "FlowGroup" not in code and "add_to_group" not in code
