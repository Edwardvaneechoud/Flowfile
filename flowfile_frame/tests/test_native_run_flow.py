"""``ff.FlowInput`` / ``to_flow_output`` / ``ff.register_flow`` / ``ff.flow_ref`` / ``ff.RunFlow``.

Child flows are registered for real (YAML under the session's temp storage dir plus a catalog
row in the shared test DB), so every test uses its own catalog and flow names. The parent's
run_flow outputs are deferred: building never runs the child, ``collect()`` does.
"""

import inspect
import tempfile
from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.catalog import AmbiguousFlowError, FlowExistsError, FlowNotFoundError, NamespaceNotFoundError
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.flowfile import subflow
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_frame import run_flow
from flowfile_frame.run_flow import FlowRef
from shared.storage_config import storage

from .native_helpers import core_node, round_trip

ORDERS = {"id": [1, 2, 3, 4], "amount": [10.0, -5.0, 70.0, 120.0]}
ORDER_SCHEMA = {"id": ff.Int64, "amount": ff.Float64}


def _unique(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


@pytest.fixture
def schema() -> ff.SchemaReference:
    return ff.CatalogReference(_unique("RunFlowCat"), auto_create=True).schema("flows", auto_create=True)


def _clean_orders_child() -> ff.FlowFrame:
    """orders -> amount > ${min_amount} -> 'kept'; orders -> amount > 0 -> 'positive'."""
    child = ff.create_flow_graph()
    ff.add_flow_parameter(child, ff.Parameter("min_amount", default=0, type="integer"))
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child)
    settings = {"filter_input": {"mode": "advanced", "advanced_filter": "[amount] > ${min_amount}"}}
    ff.Node("filter", raw, settings=settings, deferred=True).output.to_flow_output("kept")
    raw.filter(ff.col("amount") > 0).to_flow_output("positive")
    return raw


def _registered_child(schema: ff.SchemaReference) -> FlowRef:
    return schema.register_flow(_clean_orders_child(), name=_unique("clean"))


# FlowInput / to_flow_output


def test_flow_input_is_typed_by_schema_and_holds_no_rows():
    nested = {"id": ff.Int64, "tags": ff.List(ff.String), "point": ff.Struct({"x": ff.Float64})}
    frame = ff.FlowInput("orders", schema=nested, description="incoming orders")
    node = frame.flow_graph.get_node(frame.node_id)
    assert node.node_type == "flow_input"
    assert node.setting_input.input_name == "orders"
    assert node.setting_input.description == "incoming orders"
    assert [c.data_type for c in node.setting_input.raw_data_format.columns] == [
        "Int64",
        "List(String)",
        "Struct({'x': Float64})",
    ]
    assert frame.collect().schema == pl.Schema(nested)
    assert frame.collect().height == 0


def test_flow_input_schema_forms_agree():
    as_dict = ff.FlowInput("a", schema=ORDER_SCHEMA)
    as_schema = ff.FlowInput("a", schema=pl.Schema(ORDER_SCHEMA))
    as_pairs = ff.FlowInput("a", schema=list(ORDER_SCHEMA.items()))
    assert as_dict.collect().schema == as_schema.collect().schema == as_pairs.collect().schema


def test_flow_input_sample_is_the_standalone_data():
    frame = ff.FlowInput("orders", sample={"id": [1], "amount": [2.5]})
    assert_frame_equal(frame.collect(), pl.DataFrame({"id": [1], "amount": [2.5]}))


def test_flow_input_errors():
    graph = ff.create_flow_graph()
    ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=graph)
    with pytest.raises(ff.NativeNodeError, match="already used"):
        ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=graph)
    assert len(graph.nodes) == 1
    with pytest.raises(ff.NativeNodeError, match="not both"):
        ff.FlowInput("x", schema=ORDER_SCHEMA, sample={"id": [1]})
    with pytest.raises(ff.NativeNodeError, match="Invalid flow input name"):
        ff.FlowInput("1bad", schema=ORDER_SCHEMA)


@pytest.mark.parametrize("name", ["params", "name", "inputs", "schema", "flow_graph"])
def test_flow_input_refuses_a_run_flow_keyword_as_its_name(name):
    graph = ff.create_flow_graph()
    with pytest.raises(ff.NativeNodeError, match=f"is a RunFlow keyword.*e.g. '{name}_input'"):
        ff.FlowInput(name, schema=ORDER_SCHEMA, flow_graph=graph)
    assert len(graph.nodes) == 0


def test_the_reserved_input_names_are_the_run_flow_keywords():
    signature = inspect.signature(ff.RunFlow.__init__)
    named = {p.name for p in signature.parameters.values() if p.kind is not inspect.Parameter.VAR_KEYWORD}
    assert run_flow._RUN_FLOW_KEYWORDS == named


def test_to_flow_output_returns_the_same_frame_and_places_a_sink():
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA)
    clean = raw.filter(ff.col("amount") > 0)
    assert clean.to_flow_output("orders_clean", description="clean orders") is clean
    sinks = [n for n in raw.flow_graph.nodes if n.node_type == "flow_output"]
    assert len(sinks) == 1
    assert sinks[0].setting_input.output_name == "orders_clean"
    assert sinks[0].setting_input.description == "clean orders"
    assert [n.node_id for n in sinks[0].all_inputs] == [clean.node_id]
    with pytest.raises(ff.NativeNodeError, match="already used"):
        raw.to_flow_output("orders_clean")


def _sink_settings(frame: ff.FlowFrame):
    (sink,) = [n for n in frame.flow_graph.nodes if n.node_type == "flow_output"]
    return sink.setting_input


def test_to_flow_output_takes_a_declared_flow_output():
    declared = ff.FlowOutput("orders_clean", description="clean orders")
    as_string = ff.FlowInput("orders", schema=ORDER_SCHEMA).to_flow_output("orders_clean", description="clean orders")
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA)
    assert raw.to_flow_output(declared) is raw
    for frame in (as_string, raw):
        settings = _sink_settings(frame)
        assert (settings.output_name, settings.description) == ("orders_clean", "clean orders")
    overridden = ff.FlowInput("orders", schema=ORDER_SCHEMA).to_flow_output(declared, description="mine")
    assert _sink_settings(overridden).description == "mine"


def test_flow_output_is_equal_and_hashable_by_name():
    declared = ff.FlowOutput("orders_clean", description="clean orders")
    assert repr(declared) == "FlowOutput('orders_clean')"
    assert (declared.name, declared.description) == ("orders_clean", "clean orders")
    assert declared == ff.FlowOutput("orders_clean") and declared != ff.FlowOutput("other")
    assert declared != "orders_clean"
    assert {declared: 1}[ff.FlowOutput("orders_clean")] == 1
    with pytest.raises(ff.NativeNodeError, match="non-empty name"):
        ff.FlowOutput(" ")


# register_flow / flow_ref


def test_register_flow_writes_an_absolute_file_and_is_idempotent(schema):
    child = _clean_orders_child()
    name = _unique("clean")
    ref = ff.register_flow(child, name=name, schema=schema)
    path = Path(ref.flow_path)
    assert path.is_absolute() and path.is_file()
    assert path.is_relative_to(storage.python_editor_flows_directory.resolve())
    assert ref.name == name and ref.schema == schema and ref.flow_uuid
    assert ref.namespace_full_name == f"{schema.catalog.name}.flows"
    assert child.flow_graph.flow_settings.source_registration_id == ref.registration_id

    again = schema.register_flow(child, name=name)
    assert (again.registration_id, again.flow_uuid, again.flow_path) == (
        ref.registration_id,
        ref.flow_uuid,
        ref.flow_path,
    )
    rerun = schema.register_flow(_clean_orders_child(), name=name)
    assert rerun == ref


def test_register_flow_moves_a_graph_saved_elsewhere_to_the_registered_file(schema):
    child = _clean_orders_child()
    elsewhere = Path(tempfile.mkdtemp()) / "scratch_copy.yaml"
    child.save_graph(str(elsewhere))
    name = _unique("moved")
    ref = ff.register_flow(child, name=name, schema=schema)
    settings = child.flow_graph.flow_settings
    assert settings.path == ref.flow_path != str(elsewhere)
    assert settings.name == name
    assert open_flow(Path(ref.flow_path)).flow_settings.name == name


def test_register_flow_defaults_to_the_python_editor_schema():
    ff.CatalogReference("General", auto_create=True)
    ref = ff.register_flow(_clean_orders_child(), name=_unique("default_schema"))
    assert ref.namespace_full_name == "General.Python Editor"


def test_register_flow_refuses_a_flow_saved_elsewhere_unless_overwrite(schema):
    name = _unique("designer")
    elsewhere = Path(tempfile.mkdtemp()) / "designer_flow.yaml"
    designer = _clean_orders_child().flow_graph
    designer.save_flow(str(elsewhere))
    from flowfile_frame.catalog import register_flow_with_catalog

    registration_id = register_flow_with_catalog(designer, name=name, schema=schema, flow_path=str(elsewhere))
    with pytest.raises(ff.NativeNodeError, match="pass overwrite=True") as info:
        schema.register_flow(_clean_orders_child(), name=name)
    assert isinstance(info.value.__cause__, FlowExistsError)
    replaced = schema.register_flow(_clean_orders_child(), name=name, overwrite=True)
    assert replaced.registration_id == registration_id
    assert Path(replaced.flow_path) == elsewhere


def test_flow_ref_by_name_uuid_id_and_namespace_forms(schema):
    ref = _registered_child(schema)
    full_name = f"{schema.catalog.name}.{schema.name}"
    assert ff.flow_ref(full_name, ref.name) == ref
    assert ff.flow_ref(schema, ref.name) == ref
    assert ff.flow_ref(name=ref.name) == ref
    assert ff.flow_ref(uuid=ref.flow_uuid) == ref
    assert ff.flow_ref(registration_id=ref.registration_id) == ref
    assert ff.flow_ref(full_name, ref.name, uuid=ref.flow_uuid, registration_id=ref.registration_id) == ref
    assert ref.to_subflow_reference().model_dump() == {
        "registration_id": ref.registration_id,
        "flow_uuid": ref.flow_uuid,
        "flow_path": ref.flow_path,
        "namespace": full_name,
        "name": ref.name,
    }
    with pytest.raises(AttributeError, match="immutable"):
        ref.name = "other"


def test_flow_ref_errors(schema):
    ref = _registered_child(schema)
    full_name = f"{schema.catalog.name}.{schema.name}"
    missing = _unique("missing")
    with pytest.raises(ff.NativeNodeError, match=f"No flow named '{missing}' in '{full_name}'") as info:
        ff.flow_ref(schema, missing)
    assert isinstance(info.value.__cause__, FlowNotFoundError)
    with pytest.raises(ff.NativeNodeError, match=f"No flow named '{missing}' in any schema") as info:
        ff.flow_ref(name=missing)
    assert isinstance(info.value.__cause__, FlowNotFoundError)
    with pytest.raises(ff.NativeNodeError, match="No flow is registered with uuid") as info:
        ff.flow_ref(uuid=str(uuid4()))
    assert isinstance(info.value.__cause__, FlowNotFoundError)
    with pytest.raises(ff.NativeNodeError, match=f"No flow registration has id {10**9}") as info:
        ff.flow_ref(registration_id=10**9)
    assert isinstance(info.value.__cause__, FlowNotFoundError)
    with pytest.raises(ff.NativeNodeError, match=f"no catalog or schema '{schema.catalog.name}.nope'") as info:
        ff.flow_ref(f"{schema.catalog.name}.nope", ref.name)
    assert isinstance(info.value.__cause__, NamespaceNotFoundError)
    catalog_only = f"directly in catalog '{schema.catalog.name}'.*pass '{schema.catalog.name}.<schema>'"
    with pytest.raises(ff.NativeNodeError, match=catalog_only):
        ff.flow_ref(schema.catalog.name, ref.name)
    with pytest.raises(ff.NativeNodeError, match="not 'other'"):
        ff.flow_ref(uuid=ref.flow_uuid, name="other")
    other_schema = schema.catalog.schema("other", auto_create=True)
    with pytest.raises(ff.NativeNodeError, match="is not in"):
        ff.flow_ref(other_schema, uuid=ref.flow_uuid)
    with pytest.raises(ff.NativeNodeError, match="needs a flow name"):
        ff.flow_ref(schema)


def test_same_name_in_two_schemas_is_ambiguous_without_a_namespace(schema):
    name = _unique("twice")
    first = schema.register_flow(_clean_orders_child(), name=name)
    second = schema.catalog.schema("other", auto_create=True).register_flow(_clean_orders_child(), name=name)
    with pytest.raises(ff.NativeNodeError, match="pass its schema first") as info:
        ff.flow_ref(name=name)
    for ref in (first, second):
        assert f"{ref.namespace_full_name}.{name} (id={ref.registration_id})" in str(info.value)
    cause = info.value.__cause__
    assert isinstance(cause, AmbiguousFlowError)
    assert {c["id"] for c in cause.candidates} == {first.registration_id, second.registration_id}
    namespaces = {c["namespace_name"] for c in cause.candidates}
    assert namespaces == {first.namespace_full_name, second.namespace_full_name}
    assert ff.flow_ref(second.schema, name) == second


def test_flow_ref_schema_reads_tables_from_the_same_handle(schema):
    ref = _registered_child(schema)
    assert ref.schema == schema
    ref.schema.write_table(ff.from_dict(ORDERS), "orders")
    assert_frame_equal(ref.schema.read_table("orders").collect(), pl.DataFrame(ORDERS))


# RunFlow


def test_run_flow_settings_mirror_the_child_interface(schema):
    ref = _registered_child(schema)
    orders = ff.from_dict(ORDERS)
    run = ff.RunFlow(ref, orders=orders, params={"min_amount": 50}, description="clean the orders")
    settings = core_node(run).setting_input
    assert settings.flow_reference.registration_id == ref.registration_id
    assert settings.flow_reference.flow_uuid == ref.flow_uuid
    assert (settings.flow_reference.namespace, settings.flow_reference.name) == (ref.namespace_full_name, ref.name)
    assert settings.input_slots == ["orders"]
    assert settings.output_slots == ["kept", "positive"]
    assert [p.name for p in settings.parameter_specs] == ["min_amount"]
    assert [b.model_dump() for b in settings.parameter_bindings] == [
        {"parameter_name": "min_amount", "source": "constant", "constant_value": "50", "column_name": None}
    ]
    assert settings.iteration_mode == "first_value"
    assert settings.user_id is not None
    assert settings.description == "clean the orders"
    assert run.outputs == ["kept", "positive"]
    assert core_node(run).node_inputs.keyed_inputs["input-1"].node_id == orders.node_id


def test_run_flow_does_not_run_the_child_at_build(schema, tmp_path):
    child = _clean_orders_child()
    marker = tmp_path / "child_ran.csv"
    child.write_csv(str(marker))
    ref = schema.register_flow(child, name=_unique("lazy"))
    marker.unlink()  # written while the child itself was built

    run = ff.RunFlow(ref, orders=ff.from_dict(ORDERS))
    node = core_node(run)
    assert node.deferred_until_run is True
    assert node.node_stats.has_run_with_current_setup is False
    assert node.results.resulting_data.number_of_records == 0
    assert run["kept"]._deferred and run["positive"]._deferred
    assert run["kept"].data.collect().height == 0
    assert run["kept"].data.collect_schema() == pl.Schema(ORDER_SCHEMA)
    run["kept"].select("id").with_columns(ff.col("id") * 2)
    sinks = [n for n in child.flow_graph.nodes if n.node_type == "flow_output"]
    assert sinks and not any(n.node_stats.has_run_with_current_setup for n in sinks)
    assert not marker.exists()

    run["kept"].collect()
    assert_frame_equal(pl.read_csv(marker), pl.DataFrame(ORDERS))


def test_collect_runs_the_child_like_the_run_flow_node_does(schema):
    ref = _registered_child(schema)
    orders = ff.from_dict(ORDERS)
    run = ff.RunFlow(ref, orders=orders, params={"min_amount": 50})
    kept = run["kept"].filter(ff.col("id") > 0)

    direct = subflow.execute_run_flow_node(
        run.flow_graph, core_node(run).setting_input, None, (FlowDataEngine(pl.DataFrame(ORDERS)),)
    ).by_handle()
    expected_kept = pl.DataFrame(ORDERS).filter(pl.col("amount") > 50)
    assert_frame_equal(direct["output-0"].data_frame.collect(), expected_kept)

    assert_frame_equal(kept.collect(), expected_kept)
    assert_frame_equal(run["positive"].collect(), pl.DataFrame(ORDERS).filter(pl.col("amount") > 0))
    assert core_node(run).deferred_until_run is False


def test_run_resolves_the_bound_parameter_value_not_the_default(schema):
    min_amount = ff.Parameter("min_amount", default=0, type="integer")
    child = ff.create_flow_graph()
    ff.add_flow_parameter(child, min_amount)
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child)
    raw.filter(ff.col("amount") >= min_amount).to_flow_output("orders_clean")
    ref = schema.register_flow(child, name=_unique("param_clean"))

    expected = pl.DataFrame(ORDERS).filter(pl.col("amount") >= 25)
    for key in (min_amount, "min_amount"):
        run = ff.RunFlow(ref, orders=ff.from_dict(ORDERS), params={key: 25})
        assert [b.constant_value for b in core_node(run).setting_input.parameter_bindings] == ["25"]
        assert_frame_equal(run.output.collect(), expected)


def test_a_parameter_value_forwards_a_parent_parameter(schema):
    min_amount = ff.Parameter("min_amount", default=0, type="integer")
    child = ff.create_flow_graph()
    ff.add_flow_parameter(child, min_amount)
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child)
    raw.filter(ff.col("amount") >= min_amount).to_flow_output("orders_clean")
    ref = schema.register_flow(child, name=_unique("forward"))

    threshold = ff.Parameter("threshold", default=25, type="integer")
    for value in (threshold, "${threshold}"):
        parent = ff.from_dict(ORDERS)
        ff.add_flow_parameter(parent, threshold)
        run = ff.RunFlow(ref, orders=parent, params={"min_amount": value})
        assert [b.constant_value for b in core_node(run).setting_input.parameter_bindings] == ["${threshold}"]
        assert_frame_equal(run.output.collect(), pl.DataFrame(ORDERS).filter(pl.col("amount") >= 25))
        ff.set_flow_parameter(parent, threshold, 100)
        assert run.output.collect()["id"].to_list() == [4]

    with pytest.raises(ff.NativeNodeError, match=r"undeclared flow parameter\(s\) \['threshold'\]"):
        ff.RunFlow(ref, orders=ff.from_dict(ORDERS), params={"min_amount": threshold})


def test_a_collected_output_joined_across_graphs_does_not_rerun_the_child(schema, tmp_path):
    """After a run the node is no longer flagged deferred; the merge must still carry its result."""
    written = tmp_path / "child_ran.csv"
    child = ff.create_flow_graph()
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child)
    raw.to_flow_output("orders_out")
    raw.write_csv(str(written))
    ref = schema.register_flow(child, name=_unique("side_effect"))
    run = ff.RunFlow(ref, orders=ff.from_dict(ORDERS))
    assert run.output.collect().height == 4
    written.unlink()

    joined = run.output.join(ff.from_dict({"id": [1, 3], "tag": ["a", "c"]}), on="id")

    assert not written.exists()
    assert joined.columns == ["id", "amount", "tag"]
    assert sorted(joined.collect()["id"].to_list()) == [1, 3]
    assert written.exists()


def test_a_declared_output_names_the_child_sink_and_reads_the_run_output(schema):
    orders_clean = ff.FlowOutput("orders_clean")
    child = ff.create_flow_graph()
    raw = ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child)
    raw.filter(ff.col("amount") >= 10).to_flow_output(orders_clean)
    ref = schema.register_flow(child, name=_unique("declared"))

    run = ff.RunFlow(ref, orders=ff.from_dict(ORDERS))
    frames = [run[orders_clean], run.get_output(orders_clean), run["orders_clean"]]
    assert {(frame.node_id, frame.output_handle) for frame in frames} == {(run.node_id, "output-0")}
    expected = pl.DataFrame(ORDERS).filter(pl.col("amount") >= 10)
    assert_frame_equal(run.get_output(orders_clean).collect(), expected)
    with pytest.raises(ff.NativeNodeError, match=r"no output 'nope': \['orders_clean'\]"):
        run.get_output(ff.FlowOutput("nope"))


def test_column_binding_iterates_and_appends_run_metadata(schema):
    ref = _registered_child(schema)
    thresholds = ff.from_dict({"threshold": [0, 50, 100]})
    run = ff.RunFlow(
        ref,
        orders=ff.from_dict(ORDERS),
        params={"min_amount": ff.col("threshold")},
        param_frame=thresholds,
        iterate=True,
    )
    settings = core_node(run).setting_input
    assert settings.iteration_mode == "iterate"
    assert settings.parameter_bindings[0].source == "column"
    assert settings.parameter_bindings[0].column_name == "threshold"
    assert core_node(run).node_inputs.keyed_inputs["input-0"].node_id == thresholds.node_id
    assert run["kept"].data.collect_schema().names() == ["id", "amount", "param_min_amount", "run_index"]

    result = run["kept"].collect()
    assert result.columns == ["id", "amount", "param_min_amount", "run_index"]
    assert result["run_index"].to_list() == [1, 1, 1, 2, 2, 3]
    assert result["param_min_amount"].to_list() == [0, 0, 0, 50, 50, 100]
    assert result["id"].to_list() == [1, 3, 4, 3, 4, 4]

    plain = ff.RunFlow(
        ref,
        orders=ff.from_dict(ORDERS),
        params={"min_amount": ff.col("threshold")},
        param_frame=ff.from_dict({"threshold": [0, 50, 100]}),
        iterate=True,
        append_metadata=False,
    )
    assert plain["kept"].collect().columns == ["id", "amount"]


def test_iterate_needs_a_column_binding(schema):
    ref = _registered_child(schema)
    parent = ff.create_flow_graph()
    orders = ff.from_dict(ORDERS, flow_graph=parent)
    for params in ({}, {"min_amount": 5}):
        with pytest.raises(ff.NativeNodeError, match="iterate=True .* needs a parameter bound to one of its columns"):
            ff.RunFlow(ref, orders=orders, params=params, iterate=True)
    assert [n.node_type for n in parent.nodes] == ["manual_input"]


def test_slot_order_is_the_order_ports_were_created_in(schema):
    child = ff.create_flow_graph()
    second = ff.FlowInput("b", schema={"v": ff.Int64}, flow_graph=child)
    first = ff.FlowInput("a", schema={"v": ff.Int64}, flow_graph=child)
    second.to_flow_output("from_b")
    first.to_flow_output("from_a")
    ref = schema.register_flow(child, name=_unique("ports"))

    a, b = ff.from_dict({"v": [1, 10]}), ff.from_dict({"v": [2, 20]})
    run = ff.RunFlow(ref, a=a, b=b)
    settings = core_node(run).setting_input
    assert settings.input_slots == ["b", "a"]
    assert settings.output_slots == ["from_b", "from_a"]
    keyed = core_node(run).node_inputs.keyed_inputs
    assert {handle: node.node_id for handle, node in keyed.items()} == {"input-1": b.node_id, "input-2": a.node_id}
    assert run["from_a"].collect()["v"].to_list() == [1, 10]
    assert run["from_b"].collect()["v"].to_list() == [2, 20]
    with pytest.raises(ff.NativeNodeError, match="pick one"):
        _ = run.output


def test_run_flow_argument_errors(schema):
    ref = _registered_child(schema)
    orders = ff.from_dict(ORDERS)
    with pytest.raises(ff.NativeNodeError, match=r"no input\(s\) \['order'\]; its inputs are \['orders'\]"):
        ff.RunFlow(ref, order=orders)
    with pytest.raises(ff.NativeNodeError, match="must be FlowFrames"):
        ff.RunFlow(ref, orders=pl.DataFrame(ORDERS))
    with pytest.raises(ff.NativeNodeError, match="no parameter"):
        ff.RunFlow(ref, orders=orders, params={"limit": 1})
    with pytest.raises(ff.NativeNodeError, match="not a valid integer"):
        ff.RunFlow(ref, orders=orders, params={"min_amount": "many"})
    with pytest.raises(ff.NativeNodeError, match="param_frame is only read by column bindings"):
        ff.RunFlow(ref, orders=orders, params={"min_amount": 5}, param_frame=ff.from_dict({"t": [1]}))
    with pytest.raises(ff.NativeNodeError, match="as param_frame="):
        ff.RunFlow(ref, orders=orders, params={"min_amount": ff.col("t")})
    with pytest.raises(ff.NativeNodeError, match="has no column 'missing'"):
        ff.RunFlow(ref, params={"min_amount": ff.col("missing")}, param_frame=ff.from_dict({"t": [1]}))
    with pytest.raises(ff.NativeNodeError, match="plain column"):
        ff.RunFlow(ref, params={"min_amount": ff.col("t") + 1}, param_frame=ff.from_dict({"t": [1]}))
    with pytest.raises(ff.NativeNodeError, match="needs name="):
        ff.RunFlow(ff.create_flow_graph())
    only_for_graphs = "only apply when flow is a FlowGraph or a FlowFrame to register"
    for kwargs, given in (
        ({"name": "x"}, "name="),
        ({"schema": schema}, "schema="),
        ({"overwrite": True}, "overwrite="),
    ):
        with pytest.raises(ff.NativeNodeError, match=f"{only_for_graphs}; drop {given} for a FlowRef"):
            ff.RunFlow(ref, **kwargs)


def test_run_flow_takes_an_id_or_registers_a_graph(schema):
    ref = _registered_child(schema)
    by_id = ff.RunFlow(ref.registration_id, orders=ff.from_dict(ORDERS))
    assert by_id.flow == ref

    ff.CatalogReference("General", auto_create=True)
    name = _unique("inline")
    inline = ff.RunFlow(_clean_orders_child().flow_graph, name=name, orders=ff.from_dict(ORDERS))
    assert inline.flow.name == name
    assert ff.RunFlow(_clean_orders_child(), name=name).flow == inline.flow


def test_run_flow_registers_a_graph_in_a_schema_and_can_overwrite(schema):
    name = _unique("designer")
    in_schema = ff.RunFlow(_clean_orders_child(), name=_unique("in_schema"), schema=schema)
    assert in_schema.flow.schema == schema

    elsewhere = Path(tempfile.mkdtemp()) / "designer_flow.yaml"
    designer = _clean_orders_child().flow_graph
    designer.save_flow(str(elsewhere))
    from flowfile_frame.catalog import register_flow_with_catalog

    registration_id = register_flow_with_catalog(designer, name=name, schema=schema, flow_path=str(elsewhere))
    with pytest.raises(ff.NativeNodeError, match="pass overwrite=True") as info:
        ff.RunFlow(_clean_orders_child(), name=name, schema=schema)
    assert isinstance(info.value.__cause__, FlowExistsError)
    replaced = ff.RunFlow(_clean_orders_child(), name=name, schema=schema, overwrite=True, orders=ff.from_dict(ORDERS))
    assert replaced.flow.registration_id == registration_id
    assert Path(replaced.flow.flow_path) == elsewhere
    assert replaced["positive"].collect()["id"].to_list() == [1, 3, 4]


def _keyword_named_child(schema: ff.SchemaReference) -> FlowRef:
    """A child whose inputs are called ``params`` and ``orders``, as a flow built in the designer can be."""
    child = ff.create_flow_graph()
    raw_data = {"columns": [{"name": "v", "data_type": "Int64"}], "data": [[]]}
    port = ff.Node("flow_input", settings={"input_name": "params", "raw_data_format": raw_data}, flow_graph=child)
    port.output.to_flow_output("from_params")
    ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child).to_flow_output("from_orders")
    return schema.register_flow(child, name=_unique("keyword_ports"))


def test_inputs_mapping_feeds_an_input_named_like_a_keyword(schema):
    ref = _keyword_named_child(schema)
    values, orders = ff.from_dict({"v": [7, 8]}), ff.from_dict(ORDERS)
    run = ff.RunFlow(ref, inputs={"params": values}, orders=orders)
    keyed = core_node(run).node_inputs.keyed_inputs
    assert {handle: node.node_id for handle, node in keyed.items()} == {
        "input-1": values.node_id,
        "input-2": orders.node_id,
    }
    assert run["from_params"].collect()["v"].to_list() == [7, 8]
    assert_frame_equal(run["from_orders"].collect(), pl.DataFrame(ORDERS))

    both = ff.RunFlow(ref, inputs={"params": values, "orders": orders})
    assert core_node(both).node_inputs.keyed_inputs.keys() == keyed.keys()


def test_inputs_mapping_errors(schema):
    ref = _keyword_named_child(schema)
    orders = ff.from_dict(ORDERS)
    with pytest.raises(ff.NativeNodeError, match=r"\['orders'\] are given both in inputs= and as keywords"):
        ff.RunFlow(ref, inputs={"orders": orders}, orders=orders)
    with pytest.raises(ff.NativeNodeError, match="inputs= maps child input names to FlowFrames, got list"):
        ff.RunFlow(ref, inputs=[orders])
    with pytest.raises(ff.NativeNodeError, match=r"no input\(s\) \['nope'\]"):
        ff.RunFlow(ref, inputs={"nope": orders})
    with pytest.raises(ff.NativeNodeError, match="must be FlowFrames"):
        ff.RunFlow(ref, inputs={"params": pl.DataFrame({"v": [1]})})


def test_run_flow_nodes_do_not_share_parameter_specs(schema):
    ref = _registered_child(schema)
    first, second = (ff.RunFlow(ref, orders=ff.from_dict(ORDERS)) for _ in range(2))
    cached = subflow.get_subflow_interface(Path(ref.flow_path)).parameters[0]
    specs = [core_node(run).setting_input.parameter_specs[0] for run in (first, second)]
    assert len({id(spec) for spec in (*specs, cached)}) == 3
    specs[0].default_value = "99"
    assert specs[1].default_value == cached.default_value == "0"


def test_child_without_outputs_exposes_a_run_summary(schema):
    child = ff.create_flow_graph()
    ff.add_flow_parameter(child, ff.Parameter("region", default="eu"))
    ff.FlowInput("orders", schema=ORDER_SCHEMA, flow_graph=child)
    ref = schema.register_flow(child, name=_unique("no_outputs"))

    run = ff.RunFlow(ref, params={"region": "us"}, flow_graph=ff.create_flow_graph())
    assert run.outputs == ["main"]
    summary = pl.Schema({"run_index": pl.Int64, "success": pl.Boolean, "param_region": pl.String})
    assert run.output.data.collect_schema() == summary
    assert_frame_equal(
        run.output.collect(), pl.DataFrame({"run_index": [1], "success": [True], "param_region": ["us"]})
    )
    assert run.output.select("param_region").collect()["param_region"].to_list() == ["us"]

    reopened, _ = round_trip(run.output, "no_outputs_parent.yaml")
    canvas = reopened.get_node(run.node_id).get_predicted_schema()
    assert pl.Schema({c.column_name: c.get_polars_type().pl_datatype for c in canvas}) == summary


def test_run_flow_follows_a_child_registered_again_under_the_same_name(schema):
    ref = _registered_child(schema)
    run = ff.RunFlow(ref, orders=ff.from_dict(ORDERS), params={"min_amount": 50})
    with get_db_context() as db:
        db.query(FlowRegistration).filter_by(id=ref.registration_id).delete()
        db.commit()
    again = schema.register_flow(_clean_orders_child(), name=ref.name)
    assert again.flow_uuid != ref.flow_uuid

    assert_frame_equal(run["kept"].collect(), pl.DataFrame(ORDERS).filter(pl.col("amount") > 50))


def test_round_trip_keeps_a_keyed_edge_from_output_1(schema):
    ref = _registered_child(schema)
    _, small = ff.from_dict(ORDERS).filter_split(ff.col("amount") > 50)
    run = ff.RunFlow(ref, orders=small)
    assert core_node(run).node_inputs.keyed_source_handles == {"input-1": "output-1"}
    reopened, _ = round_trip(run["positive"], "parent.yaml")
    node = reopened.get_node(run.node_id)
    assert node.node_type == "run_flow"
    assert node.node_inputs.keyed_source_handles == {"input-1": "output-1"}
    assert node.node_inputs.keyed_inputs["input-1"].node_type == "filter"

    reopened.run_graph()
    result = node.get_output("output-0").data_frame
    expected = pl.DataFrame(ORDERS).filter(pl.col("amount") <= 50, pl.col("amount") > 0)
    assert_frame_equal(result.collect() if isinstance(result, pl.LazyFrame) else result, expected)
