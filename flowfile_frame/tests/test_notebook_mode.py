"""Notebook build mode: one session graph, nothing runs, writes or registers while a cell builds."""

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile as fl
import flowfile_core.kernel as kernel_package
import flowfile_frame as ff
from flowfile_frame import catalog, notebook
from flowfile_frame._identity import current_user_id
from flowfile_frame.cloud_storage import secret_manager
from flowfile_frame.database import connection_manager
from flowfile_frame.native import NativeNodeError

from .native_helpers import core_node

DATA = {"a": [1, 2, 3], "g": ["x", "x", "y"]}
SCRIPT_REFUSAL = "run it from a script"


@pytest.fixture
def mode():
    with notebook.notebook_mode() as active:
        yield active


def test_mode_context_and_implicit_graph(mode):
    assert notebook.current() is mode
    frame = ff.from_dict(DATA)
    assert frame.flow_graph is mode.graph
    assert ff.create_flow_graph() is not mode.graph


def test_mode_exit_restores_everything():
    real_manager = kernel_package.KernelManager
    with notebook.notebook_mode() as active:
        graph = active.graph
        assert kernel_package.KernelManager is not real_manager
    assert notebook.current() is None
    assert kernel_package.KernelManager is real_manager
    assert "run_graph" not in graph.__dict__
    assert ff.from_dict(DATA).flow_graph is not graph


def test_modes_do_not_nest(mode):
    with pytest.raises(NativeNodeError, match="already active"):
        notebook.enter()


def test_writer_on_an_ungated_frame_writes_nothing_in_the_mode(mode, tmp_path):
    path = tmp_path / "out.csv"
    written = ff.from_dict(DATA).write_csv(str(path))
    assert not path.exists()
    assert written._deferred is True
    assert core_node(written).node_type == "output"


def test_writer_outside_the_mode_still_writes_at_build(tmp_path):
    path = tmp_path / "out.csv"
    ff.from_dict(DATA).write_csv(str(path))
    assert_frame_equal(pl.read_csv(path), pl.DataFrame(DATA))


@pytest.mark.parametrize(
    "write, typed",
    [
        (lambda f, p: f.write_parquet(p / "x.parquet", statistics=True), "write_parquet"),
        (lambda f, p: f.write_csv(p / "x.csv", include_header=True), "write_csv"),
        (lambda f, p: f.write_ipc(p / "x.arrow", compat_level=None), "write_ipc"),
        (lambda f, p: f.write_ndjson(p / "x.ndjson", maintain_order=True), "write_ndjson"),
        (lambda f, p: f.write_excel(p / "x.xlsx", autofit=True), "write_excel"),
    ],
)
def test_polars_code_writer_fallbacks_refuse_naming_the_typed_writer(mode, write, typed, tmp_path):
    frame = ff.from_dict(DATA)
    node_count = len(mode.graph.nodes)
    with pytest.raises(NativeNodeError, match=rf"typed writer instead: {typed}\(path\)"):
        write(frame, tmp_path)
    assert len(mode.graph.nodes) == node_count
    assert list(tmp_path.iterdir()) == []


def test_sink_refuses_in_the_mode(mode, tmp_path):
    with pytest.raises(NativeNodeError, match="write_\\* method"):
        ff.from_dict(DATA).sink_parquet(str(tmp_path / "x.parquet"))
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "call",
    [
        lambda g: ff.register_flow(g, name="nb_refused"),
        lambda g: ff.RunFlow(g, name="nb_refused"),
        lambda g: ff.custom_nodes.install("does_not_exist.py"),
        lambda g: ff.create_database_connection("nb", database_type="sqlite", database=":memory:"),
        lambda g: ff.create_database_connection_if_not_exists("nb", database_type="sqlite", database=":memory:"),
        lambda g: ff.del_database_connection("nb"),
        lambda g: ff.create_cloud_storage_connection(None),
        lambda g: ff.create_cloud_storage_connection_if_not_exists(None),
        lambda g: ff.del_cloud_storage_connection("nb"),
        lambda g: fl.open_graph_in_editor(g),
    ],
    ids=[
        "register_flow",
        "run_flow_graph",
        "custom_nodes_install",
        "create_database_connection",
        "create_database_connection_if_not_exists",
        "del_database_connection",
        "create_cloud_storage_connection",
        "create_cloud_storage_connection_if_not_exists",
        "del_cloud_storage_connection",
        "open_graph_in_editor",
    ],
)
def test_side_effect_calls_are_refused(mode, call):
    ff.from_dict(DATA)
    with pytest.raises(NativeNodeError, match=SCRIPT_REFUSAL):
        call(mode.graph)


def test_session_graph_run_graph_is_refused(mode):
    ff.from_dict(DATA)
    with pytest.raises(NativeNodeError, match="use Run on canvas"):
        mode.graph.run_graph()


def test_new_source_joined_to_a_session_frame_keeps_every_node_id(mode):
    orders = ff.from_dict({"id": [1, 2], "customer_id": [10, 20]})
    filtered = orders.filter(ff.col("id") > 0)
    before = {n.node_id: n.node_type for n in mode.graph.nodes}
    customers = ff.from_dict({"customer_id": [10, 20], "name": ["Ann", "Bob"]})
    joined = filtered.join(customers, on="customer_id")
    after = {n.node_id: n.node_type for n in mode.graph.nodes}
    assert {k: after[k] for k in before} == before
    assert (orders.node_id, filtered.node_id) == tuple(sorted(before))
    assert joined.flow_graph is mode.graph
    assert joined.collect()["name"].to_list() == ["Ann", "Bob"]


def test_merge_with_another_graph_is_refused(mode):
    session_frame = ff.from_dict(DATA)
    foreign = ff.from_dict(DATA, flow_graph=ff.create_flow_graph())
    with pytest.raises(NativeNodeError, match="drop the explicit flow_graph="):
        session_frame.join(foreign, on="a")


def test_node_ids_skip_ids_already_on_an_adopted_graph():
    graph = ff.create_flow_graph()
    seeded = ff.from_dict(DATA, flow_graph=graph)
    ff.utils.set_node_id(0)
    with notebook.notebook_mode(graph):
        new = ff.from_dict(DATA)
    assert new.flow_graph is graph
    assert new.node_id > seeded.node_id
    assert len(graph.nodes) == 2


def test_parameter_declared_after_a_source_resolves(mode):
    orders = ff.from_dict({"id": [1, 2, 3], "amount": [10, 25, 70]})
    ff.add_flow_parameter(mode.graph, ff.Parameter("min_amount", default=20, type="integer"))
    kept = orders.filter(ff.col("amount") >= ff.lit("${min_amount}").cast(ff.Int64))
    assert kept.collect()["id"].to_list() == [2, 3]


def test_add_flow_parameter_upserts_on_the_session_graph(mode):
    ff.add_flow_parameter(mode.graph, ff.Parameter("region", default="EU"))
    ff.add_flow_parameter(mode.graph, ff.Parameter("region", default="US"))
    declared = [(p.name, p.default_value) for p in mode.graph.flow_settings.parameters]
    assert declared == [("region", "US")]


def test_add_flow_parameter_still_refuses_a_duplicate_on_other_graphs(mode):
    other = ff.create_flow_graph()
    ff.add_flow_parameter(other, ff.Parameter("region", default="EU"))
    with pytest.raises(NativeNodeError, match="already declared"):
        ff.add_flow_parameter(other, ff.Parameter("region", default="US"))


def test_collect_on_a_deferred_frame_refuses_and_a_lazy_safe_collect_works(mode):
    source = ff.from_dict(DATA)
    live = source.filter(ff.col("a") > 1)
    assert_frame_equal(live.collect(), pl.DataFrame({"a": [2, 3], "g": ["x", "y"]}))
    script = ff.PythonScript(source, code="x = 1")
    assert script.output._deferred is True
    with pytest.raises(NativeNodeError, match="use Run on canvas"):
        script.output.collect()


def test_polars_code_is_seeded_with_its_predicted_schema(mode):
    frame = ff.from_dict(DATA).with_columns(ff.col("a").cum_sum().alias("running"))
    node = core_node(frame)
    assert node.node_type == "polars_code"
    assert frame._deferred is True
    assert node.deferred_until_run is True
    assert frame.collect_schema().names() == ["a", "g", "running"]


def test_fluent_polars_code_is_seeded_not_run_in_the_mode(mode):
    frame = ff.from_dict(DATA).polars_code("input_df.with_columns(pl.col('a').cum_sum().alias('running'))")
    node = core_node(frame)
    assert node.node_type == "polars_code" and node.deferred_until_run is True
    assert frame._deferred is True
    assert frame.collect_schema().names() == ["a", "g", "running"]


def test_fluent_native_nodes_stay_lazy_safe_in_the_mode(mode):
    source = ff.from_dict({"g": ["x", "x"], "a": [1.0, 3.0], "b": [2.0, 4.0]})
    numbered = source.with_row_index("n", 1, group_by=["g"])
    long = numbered.unpivot(["a", "b"], index=["n", "g"])
    spread = long.group_by("g").agg(ff.col("value").std())
    assert [core_node(f).node_type for f in (numbered, long, spread)] == ["record_id", "unpivot", "group_by"]
    assert not spread._deferred
    assert spread.collect()["value"].round(4).to_list() == [1.291]


def test_explicit_deferred_false_is_overridden(mode, tmp_path):
    path = tmp_path / "eager.csv"
    frame = ff.from_dict(DATA)
    from flowfile_core.schemas import input_schema

    settings = input_schema.OutputSettings(
        file_type="csv", name=path.name, directory=str(path), table_settings=input_schema.OutputCsvTable()
    )
    settings.set_absolute_filepath()
    node = ff.Node("output", frame, settings={"output_settings": settings}, deferred=False)
    assert node.deferred is True
    assert not path.exists()


def test_kernel_manager_sentinel_keeps_docker_out(mode):
    initialized_before = kernel_package.get_kernel_manager_if_initialized()
    script = ff.PythonScript(ff.from_dict(DATA), code="x = 1", kernel="ml-kernel")
    assert core_node(script.output).node_type == "python_script"
    assert kernel_package.get_kernel_manager_if_initialized() is initialized_before
    if initialized_before is None:
        with pytest.raises(NativeNodeError, match="kernel nodes run on the canvas: use Run on canvas"):
            kernel_package.get_kernel_manager()
        assert kernel_package.get_kernel_manager_if_initialized() is None


def test_identity_hook(monkeypatch):
    monkeypatch.delenv("FLOWFILE_SESSION_USER_ID", raising=False)
    assert current_user_id() == 1
    monkeypatch.setenv("FLOWFILE_SESSION_USER_ID", "7")
    assert current_user_id() == 7
    assert connection_manager.get_current_user_id() == 7
    with notebook.notebook_mode(user_id=5):
        assert current_user_id() == 5
        assert catalog.get_current_user_id() == 5
        assert connection_manager.get_current_user_id() == 5
        assert secret_manager.get_current_user_id() == 5
        node = ff.Node("sample", ff.from_dict(DATA), settings={"sample_size": 1})
        assert node.node.setting_input.user_id == 5
    assert current_user_id() == 7
