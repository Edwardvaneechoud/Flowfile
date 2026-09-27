"""The FlowFrame export's ``fl`` dialect: placeholders, per-statement emissions and native flow ports."""

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
        ([6], "explore data is interactive only"),
    ]
    placeholder = converter.emissions()[1].code
    assert placeholder == "filtered = fl.canvas_node(4, selected_1)  # Filter data: not configured yet"


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
    assert 'fl.col("age").alias(f"age_{label}")' in converter.emissions()[0].code
    assert converter.import_lines() == ["import flowfile as fl"]
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

    assert "flow = fl.create_flow_graph()" in code
    assert "fl.FlowInput(" in code and "flow_graph=flow," in code
    assert 'sample=pl.DataFrame({"a": [1, 2]}, schema={"a": fl.Int64}, strict=False),' in code
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

    assert "scripted = fl.PythonScript(\n        source,\n        cells=[" in code
    assert "(\"cell-1\", \"flowfile_ctx.publish_output(df)\")," in code
