"""Deferred frames: a node seeded with typed zero-row outputs is never executed while the graph is
built, only by ``run_graph()`` — which ``collect()`` on a deferred frame triggers.

These tests seed a node by hand with ``native.seed_deferred_node`` and wrap it with
``FlowFrame(..., deferred=True)``, the same frames the native node classes produce.
"""

from uuid import uuid4

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile_frame as ff
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.init_db import create_default_catalog_namespace
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.flow_graph import add_connection
from flowfile_core.flowfile.param_types import FlowParameter
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame.flow_frame import FlowFrame
from flowfile_frame.native import NativeNodeError, seed_deferred_node
from flowfile_frame.utils import generate_node_id

DATA = {"a": [1, 2, 3], "g": ["x", "x", "y"]}
TEN_X = "output_df = input_df.with_columns((pl.col('a') * 10).alias('a10'))"
EXPECTED = pl.DataFrame({"a": [1, 2, 3], "g": ["x", "x", "y"], "a10": [10, 20, 30]})


def _add_polars_code_node(graph, parent_id: int, code: str) -> int:
    """Place a polars_code node below ``parent_id`` with core calls (promise, connect, settings)."""
    node_id = generate_node_id()
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=node_id, node_type="polars_code"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(parent_id, node_id))
    graph.add_polars_code(
        input_schema.NodePolarsCode(
            flow_id=graph.flow_id,
            node_id=node_id,
            polars_code_input=transform_schema.PolarsCodeInput(polars_code=code),
            depending_on_ids=[parent_id],
            is_setup=True,
        )
    )
    return node_id


def _defer(graph, node_id: int, parent_id: int, schemas=None) -> FlowFrame:
    """Seed ``node_id`` (with its predicted schema unless given) and wrap output-0 as a deferred frame."""
    node = graph.get_node(node_id)
    if schemas is None:
        schemas = {"output-0": node.get_predicted_schema() or []}
    seed_deferred_node(node, schemas)
    return FlowFrame(
        node.results.resulting_data.data_frame,
        flow_graph=graph,
        node_id=node_id,
        parent_node_id=parent_id,
        deferred=True,
    )


def _deferred_frame():
    """A manual input feeding a seeded polars_code node; returns (source, deferred frame, node, seed)."""
    source = ff.from_dict(DATA)
    node_id = _add_polars_code_node(source.flow_graph, source.node_id, TEN_X)
    frame = _defer(source.flow_graph, node_id, source.node_id)
    node = frame.flow_graph.get_node(frame.node_id)
    return source, frame, node, node.results.resulting_data


def _assert_not_executed(frame: FlowFrame, node_id: int, seed) -> None:
    node = frame.flow_graph.get_node(node_id)
    assert node.deferred_until_run is True
    assert node.results.resulting_data is seed


def _handle_into(target: FlowFrame, source_id: int) -> str:
    return target.flow_graph.get_node(target.node_id)._input_output_handles[source_id]


# building on a deferred frame (I1)


def test_seed_is_typed_and_empty():
    _, frame, node, _ = _deferred_frame()
    assert frame._deferred is True
    assert frame.columns == ["a", "g", "a10"]
    assert frame.data.collect().height == 0
    assert node.deferred_until_run is True


def test_fluent_chain_never_executes_the_seeded_node():
    _, frame, node, seed = _deferred_frame()
    filtered = frame.filter(ff.col("a") > 1)
    added = filtered.with_columns((ff.col("a") * 2).alias("b"))
    selected = added.select("a", "b", "a10")
    ordered = selected.sort("a")
    grouped = frame.group_by("g").agg(ff.col("a10").sum())

    for child in (filtered, added, selected, ordered, grouped):
        assert child._deferred is True
    assert ordered.columns == ["a", "b", "a10"]
    assert ordered.schema["b"] == pl.Int64
    assert grouped.columns == ["g", "a10"]
    _assert_not_executed(frame, node.node_id, seed)


def test_collect_runs_the_graph_and_returns_real_rows():
    _, frame, node, _ = _deferred_frame()
    ordered = frame.filter(ff.col("a") > 1).sort("a")

    assert_frame_equal(ordered.collect(), EXPECTED.filter(pl.col("a") > 1))
    assert node.deferred_until_run is False
    assert_frame_equal(frame.collect(), EXPECTED)


def test_describe_runs_the_graph():
    _, frame, _, _ = _deferred_frame()
    described = frame.describe()
    assert described.filter(pl.col("statistic") == "count")["a10"].item() == 3


def test_non_deferred_frames_keep_plain_collect():
    df = ff.from_dict(DATA)
    assert df._deferred is False
    assert df.filter(ff.col("a") > 1)._deferred is False
    assert_frame_equal(df.collect(), pl.DataFrame(DATA))


# cross-graph merges carry the seed (I2)


def test_join_with_another_graph_carries_the_seed():
    _, frame, _, seed = _deferred_frame()
    other = ff.from_dict({"a": [1, 2, 3], "label": ["one", "two", "three"]})
    old_graph = frame.flow_graph

    joined = frame.join(other, on="a")

    assert joined.flow_graph is not old_graph and frame.flow_graph is joined.flow_graph
    assert joined._deferred is True
    _assert_not_executed(frame, frame.node_id, seed)
    assert joined.columns == ["a", "g", "a10", "label"]
    assert_frame_equal(joined.collect().sort("a"), EXPECTED.with_columns(label=pl.Series(["one", "two", "three"])))


def test_split_fail_branch_keeps_its_handle_across_a_merge():
    _, frame, _, seed = _deferred_frame()
    _, fail = frame.filter_split(ff.col("a") > 1)
    split_id = fail.node_id
    other = ff.from_dict({"a": [1, 2], "label": ["one", "two"]})

    joined = fail.join(other, on="a")

    assert fail.node_id != split_id  # remapped onto the merged graph
    assert _handle_into(joined, fail.node_id) == "output-1"
    _assert_not_executed(joined, joined.flow_graph.get_node(fail.node_id).node_inputs.main_inputs[0].node_id, seed)
    assert joined.collect().to_dicts() == [{"a": 1, "g": "x", "a10": 10, "label": "one"}]


def test_split_fail_branch_concat_across_graphs_keeps_output_1():
    _, frame, _, seed = _deferred_frame()
    _, fail = frame.filter_split(ff.col("a") > 1)
    other = ff.from_dict({"a": [7, 8], "g": ["z", "z"], "a10": [70, 80]})

    combined = ff.concat([other, fail])

    assert combined._deferred is True
    assert _handle_into(combined, fail.node_id) == "output-1"
    _assert_not_executed(combined, combined.flow_graph.get_node(fail.node_id).node_inputs.main_inputs[0].node_id, seed)
    assert sorted(combined.collect()["a"].to_list()) == [1, 7, 8]


# output handles on non-deferred frames (I5) and translated wiring errors (I4)


def _split():
    df = ff.from_dict(DATA)
    passed, failed = df.filter_split(ff.col("a") > 1)
    other = df.select("a", (ff.col("a") * 100).alias("hundred"))
    return df, passed, failed, other


def test_join_reads_the_fail_branch():
    _, _, failed, other = _split()
    joined = failed.join(other, on="a")
    assert _handle_into(joined, failed.node_id) == "output-1"
    assert joined.collect().to_dicts() == [{"a": 1, "g": "x", "hundred": 100}]


def test_polars_code_join_reads_the_fail_branch():
    _, _, failed, other = _split()
    joined = failed.join(other, on="a", suffix="_other")
    assert _handle_into(joined, failed.node_id) == "output-1"
    assert joined.collect().to_dicts() == [{"a": 1, "g": "x", "hundred": 100}]


def test_fuzzy_join_reads_the_fail_branch():
    left = ff.from_dict({"id": [1, 2, 3], "street": ["123 Main St", "456 Elm St", "789 Maple Ave"]})
    _, failed = left.filter_split(ff.col("id") > 1)
    right = ff.from_dict({"id": [1, 2], "street": ["123 Main Street", "456 Elm Street"]})
    joined = failed.fuzzy_join(right, [ff.FuzzyMapping("street", threshold_score=40)])
    assert _handle_into(joined, failed.node_id) == "output-1"
    assert joined.collect()["street"].to_list() == ["123 Main St"]


def test_concat_reads_the_fail_branch():
    df, _, failed, _ = _split()
    combined = ff.concat([df, failed])
    assert _handle_into(combined, failed.node_id) == "output-1"
    assert sorted(combined.collect()["a"].to_list()) == [1, 1, 2, 3]


def test_wait_for_reads_the_dependency_handle():
    df, _, failed, _ = _split()
    waited = df.wait_for(failed)
    node = waited.flow_graph.get_node(waited.node_id)
    assert node.node_inputs.right_input.node_id == failed.node_id
    assert node._input_output_handles[failed.node_id] == "output-1"


def test_wait_for_merges_graphs():
    df = ff.from_dict(DATA)
    other = ff.from_dict({"b": [1]})
    waited = df.wait_for(other)
    assert other.flow_graph is waited.flow_graph
    assert_frame_equal(waited.collect(), pl.DataFrame(DATA))


def test_both_exits_of_one_node_into_a_join_raise_native_error():
    _, passed, failed, _ = _split()
    with pytest.raises(NativeNodeError, match="single output handle"):
        passed.join(failed, on="a")


def test_both_exits_of_one_node_into_a_concat_raise_native_error():
    _, passed, failed, _ = _split()
    with pytest.raises(NativeNodeError, match="single output handle"):
        ff.concat([passed, failed])


# writers below a deferred frame (I1 at build, I3 at collect)


def test_write_parquet_waits_for_collect(tmp_path):
    _, frame, node, seed = _deferred_frame()
    path = tmp_path / "out.parquet"

    written = frame.write_parquet(path)

    writer = written.flow_graph.get_node(written.node_id)
    assert written._deferred is True and writer.deferred_until_run is True
    assert not path.exists()
    _assert_not_executed(frame, node.node_id, seed)
    assert_frame_equal(written.collect(), EXPECTED)
    assert_frame_equal(pl.read_parquet(path), EXPECTED)


def test_write_csv_waits_for_run_graph(tmp_path):
    _, frame, _, _ = _deferred_frame()
    path = tmp_path / "out.csv"

    frame.filter(ff.col("a") > 1).write_csv(path)

    assert not path.exists()
    assert frame.flow_graph.run_graph().success
    assert_frame_equal(pl.read_csv(path), EXPECTED.filter(pl.col("a") > 1))


def test_train_model_below_a_deferred_frame_is_seeded_not_run():
    _, frame, node, seed = _deferred_frame()
    trained = frame.train_model(target="a10", features=["a"])
    assert trained._deferred is True
    assert trained.flow_graph.get_node(trained.node_id).deferred_until_run is True
    _assert_not_executed(frame, node.node_id, seed)


def test_write_catalog_table_waits_for_collect():
    _, frame, node, seed = _deferred_frame()
    with get_db_context() as db:
        create_default_catalog_namespace(db)  # catalog tests earlier in the session wipe the seeded schemas
    schema = ff.default_schema()
    name = f"deferred_writer_{uuid4().hex[:8]}"

    written = frame.write_catalog_table(name, schema=schema)
    try:
        assert written._deferred is True
        assert written.flow_graph.get_node(written.node_id).deferred_until_run is True
        assert name not in [t.name for t in schema.list_tables()]
        _assert_not_executed(frame, node.node_id, seed)

        assert_frame_equal(written.collect(), EXPECTED)
        assert_frame_equal(ff.read_catalog_table(name, schema=schema).collect().sort("a"), EXPECTED)
    finally:
        for table in schema.list_tables():
            if table.name == name:
                with get_db_context() as db:
                    CatalogService(SQLAlchemyCatalogRepository(db)).delete_table(table.id, delete_file=True)


def test_apply_model_with_a_deferred_upstream_is_seeded_not_run():
    source, frame, node, seed = _deferred_frame()
    trained = frame.train_model(target="a10", features=["a"])

    scored = source.apply_model(trained)

    assert source._deferred is False
    assert scored._deferred is True
    assert scored.flow_graph.get_node(scored.node_id).deferred_until_run is True
    assert scored.flow_graph.get_node(trained.node_id).deferred_until_run is True
    assert scored.columns == ["a", "g", "prediction"]
    _assert_not_executed(frame, node.node_id, seed)


def test_sink_and_inspect_refuse_deferred_frames(tmp_path):
    _, frame, _, _ = _deferred_frame()
    node_count = len(frame.flow_graph.nodes)

    with pytest.raises(NativeNodeError, match="sink_parquet runs when it is built"):
        frame.sink_parquet(str(tmp_path / "x.parquet"))
    with pytest.raises(NativeNodeError, match="inspect runs when it is built"):
        frame.inspect()

    assert len(frame.flow_graph.nodes) == node_count
    assert list(tmp_path.iterdir()) == []


def test_sink_parquet_on_a_plain_frame_still_writes(tmp_path):
    path = tmp_path / "plain.parquet"
    ff.from_dict(DATA).sink_parquet(str(path))
    assert_frame_equal(pl.read_parquet(path), pl.DataFrame(DATA))


@pytest.mark.parametrize(
    "write",
    [
        lambda f, p: f.write_parquet(p / "x.parquet", statistics=True),
        lambda f, p: f.write_csv(p / "x.csv", include_header=True),
        lambda f, p: f.write_ipc(p / "x.arrow", compat_level=None),
        lambda f, p: f.write_excel(p / "x.xlsx", autofit=True),
    ],
)
def test_polars_code_writer_fallbacks_refuse_deferred_frames(write, tmp_path):
    _, frame, _, _ = _deferred_frame()
    with pytest.raises(NativeNodeError, match="placeholder rows"):
        write(frame, tmp_path)
    assert list(tmp_path.iterdir()) == []


# fallbacks that would bake the placeholder into the node


def test_polars_code_serialize_fallback_refuses_deferred_frames():
    _, frame, node, seed = _deferred_frame()
    expr = ff.col("a") * 2
    expr.convertable_to_code = False
    with pytest.raises(NativeNodeError, match="deferred frame"):
        frame.sort(expr)
    _assert_not_executed(frame, node.node_id, seed)


def test_lazy_method_short_circuit_refuses_deferred_frames():
    _, frame, _, _ = _deferred_frame()
    expr = ff.col("a") * 2
    expr.convertable_to_code = False
    with pytest.raises(NativeNodeError, match="deferred frame"):
        frame.fill_null(expr)


# gates, failures and stale seeds on collect (I3)


def _gate_exits(env: str):
    """A parameter gate (env == prod, with an else exit) below the seeded node; both exits deferred."""
    _, frame, node, _ = _deferred_frame()
    graph = frame.flow_graph
    graph.flow_settings.parameters.append(FlowParameter(name="env", default_value=env, type="string"))
    gate_id = generate_node_id()
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=gate_id, node_type="gate"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(node.node_id, gate_id))
    graph.add_gate(
        input_schema.NodeGate(
            flow_id=graph.flow_id,
            node_id=gate_id,
            depending_on_id=node.node_id,
            is_setup=True,
            else_output=True,
            gate_input=transform_schema.GateInput(parameter="env", operator="equals", value="prod"),
        )
    )
    gate = graph.get_node(gate_id)
    then, otherwise = (
        FlowFrame(
            gate.get_output(handle).data_frame,
            flow_graph=graph,
            node_id=gate_id,
            parent_node_id=node.node_id,
            output_handle=handle,
            deferred=True,
        )
        for handle in ("output-0", "output-1")
    )
    return then, otherwise


@pytest.mark.parametrize("env", ["prod", "dev"])
def test_gate_exits_collect_live_rows_and_empty_dead_side(env):
    then, otherwise = _gate_exits(env)
    live, dead = (then, otherwise) if env == "prod" else (otherwise, then)

    assert_frame_equal(live.collect(), EXPECTED)
    dead_rows = dead.collect()
    assert dead_rows.height == 0 and dead_rows.columns == ["a", "g", "a10"]


@pytest.mark.parametrize("env", ["prod", "dev"])
def test_node_below_a_closed_gate_exit_collects_empty(env):
    then, otherwise = _gate_exits(env)
    below_then = then.select("a")
    below_otherwise = otherwise.select("a")
    live, dead = (below_then, below_otherwise) if env == "prod" else (below_otherwise, below_then)

    assert live.collect()["a"].to_list() == [1, 2, 3]
    dead_rows = dead.collect()
    assert dead_rows.height == 0 and dead_rows.columns == ["a"]


def test_failure_on_an_unrelated_branch_does_not_fail_collect():
    source, frame, _, _ = _deferred_frame()
    _add_polars_code_node(source.flow_graph, source.node_id, "output_df = name_that_does_not_exist")

    assert_frame_equal(frame.select("a").collect(), EXPECTED.select("a"))
    assert frame.flow_graph.latest_run_info.success is False


def test_failed_ancestor_raises_native_error():
    source = ff.from_dict(DATA)
    node_id = _add_polars_code_node(source.flow_graph, source.node_id, "output_df = name_that_does_not_exist")
    columns = source.flow_graph.get_node(source.node_id).get_predicted_schema()
    child = _defer(source.flow_graph, node_id, source.node_id, {"output-0": columns}).select("a")

    with pytest.raises(NativeNodeError, match=f"node {node_id}:"):
        child.collect()


def test_unconfigured_ancestor_raises_native_error():
    source = ff.from_dict(DATA)
    graph = source.flow_graph
    join_id = generate_node_id()
    graph.add_node_promise(input_schema.NodePromise(flow_id=graph.flow_id, node_id=join_id, node_type="join"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(source.node_id, join_id))
    graph.add_join(
        input_schema.NodeJoin(
            flow_id=graph.flow_id,
            node_id=join_id,
            depending_on_ids=[source.node_id],
            is_setup=True,
            join_input=transform_schema.JoinInput(
                join_mapping=[transform_schema.JoinMap(left_col="a", right_col="a")],
                left_select=[transform_schema.SelectInput(old_name="a", keep=True)],
                right_select=[transform_schema.SelectInput(old_name="a", keep=True)],
                how="inner",
            ),
        )
    )
    assert graph.get_node(join_id).is_correct is False  # only its left input is wired
    columns = graph.get_node(source.node_id).get_predicted_schema()
    child = _defer(graph, join_id, source.node_id, {"output-0": columns}).select("a")

    with pytest.raises(NativeNodeError, match=f"node {join_id}: not configured"):
        child.collect()


@pytest.mark.parametrize("change", [lambda f: f.set_group("late group"), lambda f: f.cache()])
def test_settings_change_after_build_raises_native_error(change):
    _, frame, node, _ = _deferred_frame()
    change(frame)
    with pytest.raises(NativeNodeError, match=f"node {node.node_id} lost its deferred placeholder"):
        frame.select("a")


def test_real_run_replaces_a_stale_multi_output_seed():
    source = ff.from_dict(DATA)
    passed, _ = source.filter_split(ff.col("a") > 1)
    graph, split_id = source.flow_graph, passed.node_id
    real = graph.get_node(split_id).schema_for_handle("output-1")
    stale = [*real, FlowfileColumn.from_input("seed_only", "String")]
    split = graph.get_node(split_id)
    seed_deferred_node(split, {"output-0": list(real), "output-1": stale})
    fail = FlowFrame(
        split.get_output("output-1").data_frame,
        flow_graph=graph,
        node_id=split_id,
        parent_node_id=source.node_id,
        output_handle="output-1",
        deferred=True,
    )
    assert "seed_only" in fail.columns

    assert fail.collect().to_dicts() == [{"a": 1, "g": "x"}]
    assert [c.name for c in split.schema_for_handle("output-1")] == ["a", "g"]
