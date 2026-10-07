"""Notebook build mode: one session graph, nothing runs, writes or registers while a cell builds."""

import os

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import flowfile
import flowfile_core.kernel as kernel_package
import flowfile_frame as ff
from flowfile_core.configs import node_store
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.schemas import input_schema, transform_schema
from flowfile_frame import catalog, catalog_reference, kafka, native, notebook, rest_api
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
        assert kernel_package.kernel_manager_refusal.get() is not None
        assert kernel_package.KernelManager is real_manager
    assert notebook.current() is None
    assert kernel_package.kernel_manager_refusal.get() is None
    assert kernel_package.KernelManager is real_manager
    assert "run_graph" not in graph.__dict__
    assert ff.from_dict(DATA).flow_graph is not graph


def test_modes_do_not_nest(mode):
    loggers = set(FlowLogger._instances)
    with pytest.raises(NativeNodeError, match="already active"):
        notebook.enter()
    assert set(FlowLogger._instances) == loggers


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
        lambda g: ff.register_flow_with_catalog(g, name="nb_refused"),
        lambda g: ff.RunFlow(g, name="nb_refused"),
        lambda g: ff.custom_nodes.install("does_not_exist.py"),
        lambda g: ff.create_database_connection("nb", database_type="sqlite", database=":memory:"),
        lambda g: ff.create_database_connection_if_not_exists("nb", database_type="sqlite", database=":memory:"),
        lambda g: ff.del_database_connection("nb"),
        lambda g: ff.create_cloud_storage_connection(None),
        lambda g: ff.create_cloud_storage_connection_if_not_exists(None),
        lambda g: ff.del_cloud_storage_connection("nb"),
        lambda g: flowfile.open_graph_in_editor(g),
    ],
    ids=[
        "register_flow",
        "register_flow_with_catalog",
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


def test_a_sync_seeds_a_polars_code_method_from_the_frames_own_plan(monkeypatch):
    from flowfile_core.flowfile.flow_data_engine.polars_code_parser import PolarsCodeParser

    monkeypatch.setattr(PolarsCodeParser, "get_executable", lambda *_: pytest.fail("polars code ran"))
    with notebook.notebook_mode(user_id=1, sync=True) as sync:
        source = ff.from_dict(DATA)
        running = source.with_columns(ff.col("a").cum_sum().alias("running"))
        last = source.tail(1)
        unplanned = source.select(ff.numeric())
        assert [core_node(f).node_type for f in (running, last, unplanned)] == ["polars_code"] * 3
        assert running.collect_schema().names() == ["a", "g", "running"]
        assert last.collect_schema().names() == ["a", "g"]
        assert unplanned.collect_schema().names() == []
        assert sync.column_less == {unplanned.node_id}


def test_a_sync_renames_a_frame_without_known_columns_through_polars_code():
    with notebook.notebook_mode(user_id=1, sync=True):
        source = ff.from_dict(DATA)
        unplanned = source.select(ff.numeric())
        below = unplanned.with_columns(ff.lit(1).alias("one"))
        known = source.rename({"a": "b"})
        renames = [unplanned.rename({"a": "b"}), below.rename({"a": "b"}, strict=False)]

        assert core_node(known).node_type == "select"
        assert [core_node(f).node_type for f in renames] == ["polars_code", "polars_code"]
        assert [core_node(f).setting_input.polars_code_input.polars_code for f in renames] == [
            "output_df = input_df.rename({'a': 'b'})",
            "output_df = input_df.rename({'a': 'b'}, strict=False)",
        ]


def test_fluent_polars_code_is_seeded_not_run_in_the_mode(mode):
    frame = ff.from_dict(DATA).polars_code("input_df.with_columns(pl.col('a').cum_sum().alias('running'))")
    node = core_node(frame)
    assert node.node_type == "polars_code" and node.deferred_until_run is True
    assert frame._deferred is True
    assert frame.collect_schema().names() == ["a", "g", "running"]


@pytest.mark.parametrize(
    "call",
    [
        lambda f: f.tail(f),
        lambda f: f.shift(1, fill_value=f),
        lambda f: f.drop_nulls(subset=["a", f]),
        lambda f: f.cast({"a": f}),
        lambda f: f.explode(f),
        lambda f: f.explode("a", f),
    ],
    ids=["positional", "keyword", "in_a_list", "in_a_dict", "explode", "explode_more_columns"],
)
def test_a_polars_code_method_takes_no_frame_argument_in_the_mode(mode, call):
    frame = ff.from_dict(DATA)
    node_count = len(mode.graph.nodes)
    with pytest.raises(NativeNodeError, match="takes no frame as an argument"):
        call(frame)
    assert len(mode.graph.nodes) == node_count


def test_a_polars_code_method_binds_its_arguments_to_polars_in_the_mode(mode):
    frame = ff.from_dict(DATA)
    node_count = len(mode.graph.nodes)
    with pytest.raises(TypeError, match=r"tail\(\) got an unexpected keyword argument 'nope'"):
        frame.tail(nope=1)
    with pytest.raises(TypeError, match=r"slice\(\) missing a required argument: 'offset'"):
        frame.slice()
    assert len(mode.graph.nodes) == node_count
    assert core_node(frame.tail(2, description="Last two")).setting_input.description == "Last two"


def test_an_argument_without_a_code_form_is_refused_in_the_mode(mode):
    frame = ff.from_dict(DATA)
    node_count = len(mode.graph.nodes)
    expr = ff.col("a") * 2
    expr.convertable_to_code = False
    with pytest.raises(NativeNodeError, match="no code form"):
        frame.fill_null(expr)
    assert len(mode.graph.nodes) == node_count


def test_text_that_reads_like_a_lambda_stays_code_in_the_mode(mode):
    filled = ff.from_dict(DATA).fill_null("<lambda> at 0x0")
    node = core_node(filled)
    assert node.node_type == "polars_code"
    assert node.setting_input.polars_code_input.polars_code == "output_df = input_df.fill_null('<lambda> at 0x0')"


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


def test_get_kernel_manager_refuses_in_the_mode_even_when_one_is_cached(mode, monkeypatch):
    from flowfile_core.flowfile import flow_graph as flow_graph_module

    initialized_before = kernel_package.get_kernel_manager_if_initialized()
    script = ff.PythonScript(ff.from_dict(DATA), code="x = 1", kernel="ml-kernel")
    assert core_node(script.output).node_type == "python_script"
    assert kernel_package.get_kernel_manager_if_initialized() is initialized_before
    monkeypatch.setattr(kernel_package, "_manager", object())
    for get_kernel_manager in (kernel_package.get_kernel_manager, flow_graph_module.get_kernel_manager):
        with pytest.raises(NativeNodeError, match="kernel nodes run on the canvas: use Run on canvas"):
            get_kernel_manager()


def test_identity_hook():
    hooks = (
        catalog.get_current_user_id,
        catalog_reference._get_current_user_id,
        connection_manager.get_current_user_id,
        kafka.get_current_user_id,
        rest_api.get_current_user_id,
        secret_manager.get_current_user_id,
    )
    assert current_user_id() == 1
    assert [hook() for hook in hooks] == [1] * len(hooks)
    with notebook.notebook_mode():
        assert current_user_id() == 1
    with notebook.notebook_mode(user_id=5):
        assert current_user_id() == 5
        assert [hook() for hook in hooks] == [5] * len(hooks)
        node = ff.Node("sample", ff.from_dict(DATA), settings={"sample_size": 1})
        assert node.node.setting_input.user_id == 5
    assert current_user_id() == 1
    assert [hook() for hook in hooks] == [1] * len(hooks)


BUILT_IN_A_SYNC = {
    "cross_join", "data_cleansing", "dynamic_rename", "explode_hierarchy", "filter", "flow_input", "formula", "gate",
    "graph_solver", "group_by", "join", "manual_input", "multi_field_formula", "record_count", "record_id", "sample",
    "select", "sort", "sql_query", "text_to_rows", "union", "unique", "unpivot", "wait_for", "window_functions",
}  # fmt: skip


def test_every_built_in_node_type_is_held_or_built_in_a_sync():
    built_in = {name for name, template in node_store.node_dict.items() if not template.custom_node}
    with notebook.notebook_mode(user_id=1, sync=True):
        built = {name for name in built_in if not native.notebook_defers(name)}
    assert built == BUILT_IN_A_SYNC


def test_a_sync_holds_what_reads_by_its_settings_and_a_plain_mode_does_not():
    first_row = input_schema.NodeDynamicRename(
        flow_id=1, node_id=1, dynamic_rename_input=transform_schema.DynamicRenameInput(rename_mode="first_row")
    )
    reads = input_schema.NodeSqlQuery(
        flow_id=1, node_id=2, sql_query_input=transform_schema.SqlQueryInput(sql_code="SELECT * FROM read_csv('x.csv')")
    )
    plain = input_schema.NodeSqlQuery(
        flow_id=1, node_id=3, sql_query_input=transform_schema.SqlQueryInput(sql_code="SELECT * FROM input_1")
    )
    counts_nulls = input_schema.NodeDataCleansing(
        flow_id=1, node_id=4, cleansing_input=transform_schema.DataCleansingInput(remove_null_columns=True)
    )
    trims = input_schema.NodeDataCleansing(
        flow_id=1, node_id=5, cleansing_input=transform_schema.DataCleansingInput(trim_whitespace=True)
    )
    reading = [("dynamic_rename", first_row), ("sql_query", reads), ("data_cleansing", counts_nulls)]
    with notebook.notebook_mode(user_id=1, sync=True):
        assert all(native.notebook_defers(t, s) for t, s in reading)
        assert not native.notebook_defers("sql_query", plain)
        assert not native.notebook_defers("data_cleansing", trims)
    with notebook.notebook_mode(user_id=1):
        assert not any(native.notebook_defers(t, s) for t, s in reading)
        assert not native.notebook_defers("read") and not native.notebook_defers("fuzzy_match")


def test_a_plain_mode_reads_a_local_file_and_a_sync_probes_its_header_only(tmp_path):
    path = tmp_path / "rows.csv"
    pl.DataFrame(DATA).write_csv(path)
    with notebook.notebook_mode(user_id=1):
        live = ff.read_csv(str(path))
        assert not live._deferred and live.collect().height == 3
    with notebook.notebook_mode(user_id=1, sync=True) as sync:
        held = ff.read_csv(str(path))
        listed = ff.list_files(str(tmp_path))
        assert held._deferred and core_node(held).deferred_until_run
        assert held.collect_schema().names() == ["a", "g"]
        assert listed._deferred and "file_path" in listed.collect_schema().names()
        assert held.data.collect().height == 0
        assert sync.column_less == set()


def test_a_read_csv_polars_fallback_is_a_polars_code_source_seeded_without_columns(tmp_path):
    path = tmp_path / "rows.csv"
    pl.DataFrame(DATA).write_csv(path)
    for sync in (False, True):
        with notebook.notebook_mode(user_id=1, sync=sync):
            rows = ff.read_csv(str(path), n_rows=1)
            assert core_node(rows).node_type == "polars_code"
            assert rows._deferred and core_node(rows).deferred_until_run
            assert rows.collect_schema().names() == []


@pytest.mark.parametrize(
    ("sql", "held"),
    [
        ("SELECT * FROM read_csv('x.csv')", True),
        ("WITH scan_results(a) AS (SELECT a FROM input_1) SELECT * FROM scan_results", False),
        ("SELECT * FROM input_1 -- was read_csv('x.csv')", False),
        ("SELECT * FROM input_1 /* scan_parquet('y') */", False),
        ("SELECT * FROM input_1 /* a /* read_csv('x.csv') */ b */", False),
        ("SELECT * FROM `read_csv`('x.csv')", True),
        ("SELECT * FROM `READ_PARQUET`('x.parquet')", True),
        ('SELECT * FROM "Read_Json"(\'x.json\')', True),
        ("SELECT * FROM read_ipc\n  ('x.arrow')", True),
        ("SELECT * FROM read_csv -- c\n('x.csv')", True),
        ("SELECT * FROM read_csv/* a /* b */ c */('x.csv')", True),
        ("SELECT * FROM `read_csv`.`x`('x.csv')", True),
        ("SELECT '--' AS z, * FROM read_csv('x.csv')", True),
        ("SELECT \"/*\".* FROM read_csv('x.csv') AS \"/*\" -- */", True),
        ("SELECT $$--$$ AS z, * FROM read_csv('x.csv')", True),
        ("SELECT E'\\'--' AS z, * FROM read_csv('x.csv')", True),
        ("SELECT * FROM `read_csv('x.csv')", True),
        ("SELECT * FROM ${source}", True),
    ],
)
def test_a_sync_holds_sql_by_the_canvas_table_function_gate(sql, held):
    settings = input_schema.NodeSqlQuery(
        flow_id=1, node_id=1, sql_query_input=transform_schema.SqlQueryInput(sql_code=sql)
    )
    with notebook.notebook_mode(user_id=1, sync=True):
        assert native.notebook_defers("sql_query", settings) is held


def test_a_native_node_is_held_by_its_settings_in_a_sync():
    first_row = {"dynamic_rename_input": {"rename_mode": "first_row"}}
    reads = {"sql_query_input": {"sql_code": "SELECT * FROM read_csv('x.csv')"}}
    with notebook.notebook_mode(user_id=1, sync=True):
        source = ff.from_dict(DATA)
        renamed = ff.Node("dynamic_rename", source, settings=first_row)
        assert renamed.output._deferred and core_node(renamed).deferred_until_run
        with pytest.raises(NativeNodeError, match="SQL table functions are not allowed"):
            ff.Node("sql_query", source, settings=reads)


def test_a_user_less_mode_checks_a_placement_as_the_user_its_settings_carry():
    ff.create_database_connection_if_not_exists("notebook_user_less", database_type="sqlite", database=":memory:")
    settings = {
        "database_settings": {
            "connection_mode": "reference",
            "database_connection_name": "notebook_user_less",
            "table_name": "orders",
        },
        "fields": [{"name": "x", "data_type": "Int64"}],
    }
    with notebook.notebook_mode() as mode:
        reader = ff.Node("database_reader", settings=settings)
        assert reader.node.setting_input.user_id == 1 and mode.refusals == []
        assert reader.output._deferred and reader.output.collect_schema().names() == ["x"]


def test_paths_kept_as_written_still_glob_a_folder_and_join_a_name(tmp_path):
    from flowfile_core.schemas.input_schema import ReceivedTable
    from shared.path_utils import expand_glob_pattern

    folder = tmp_path / "sales"
    folder.mkdir()
    (folder / "a.csv").write_text("x\n1\n")
    with notebook.paths_as_written():
        directory = ReceivedTable(path=str(folder), file_type="csv", scan_mode="directory")
        assert expand_glob_pattern(directory.abs_file_path) == [str(folder / "a.csv")]
        assert ReceivedTable(path=str(folder), name="a.csv", file_type="csv").abs_file_path == str(folder / "a.csv")
        windows = ReceivedTable(path=r"C:\data", name=r"C:\data\a.csv", file_type="csv")
        assert windows.path == r"C:\data" and windows.abs_file_path == os.path.join(r"C:\data", "a.csv")


def test_scan_mode_asks_core_in_a_kernel_session(tmp_path):
    """Under ``paths_as_written`` a bare path is probed through the ``is_directory`` lookup when one is installed (a
    kernel session: the kernel sees no folder of this machine), else on this filesystem; the syntax rules come first."""
    from flowfile_core.notebook.lookup import metadata_lookup
    from flowfile_frame.flow_frame_methods import _resolve_scan_mode

    folder = tmp_path / "sales"
    folder.mkdir()
    asked: list[tuple[str, dict]] = []

    def hook(kind, args):
        asked.append((kind, args))
        return args["path"] == r"C:\data"

    with notebook.paths_as_written():
        assert _resolve_scan_mode(str(folder)) == "directory", "without a hook the folder is probed here"
        token = metadata_lookup.set(hook)
        try:
            assert _resolve_scan_mode(r"C:\data") == "directory"
            assert _resolve_scan_mode(r"C:\data\a.csv") == "single_file"
            assert _resolve_scan_mode("C:\\other\\") == "directory"
        finally:
            metadata_lookup.reset(token)
    assert asked == [("is_directory", {"path": r"C:\data"}), ("is_directory", {"path": r"C:\data\a.csv"})]
    assert _resolve_scan_mode(str(folder)) == "directory" and _resolve_scan_mode(r"C:\data") == "single_file"


def test_a_schema_resolver_seeds_a_deferred_node_without_columns(mode):
    """A kernel session sets ``schema_resolver``; a deferred node whose seed has no columns takes its answer, one
    whose seed has columns (declared, or a script's passthrough) is never asked, and a node the resolver does not
    know keeps its empty seed."""
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn

    asked: list[int] = []

    def resolver(node):
        asked.append(node.node_id)
        if node.setting_input.rest_api_settings.url.endswith("/known"):
            return {"output-0": [FlowfileColumn.from_input("predicted", "Int64")]}
        return None

    mode.schema_resolver = resolver
    known = ff.read_api("https://example.test/known")
    assert known.columns == ["predicted"]
    unknown = ff.read_api("https://example.test/other")
    assert unknown.columns == []
    script = ff.PythonScript(ff.from_dict(DATA), code="x = 1", kernel="other")
    assert script.output.columns == ["a", "g"]
    declared = ff.PythonScript(ff.from_dict(DATA), code="x = 1", kernel="other", schemas={"main": {"n": ff.Int64}})
    assert declared.output.columns == ["n"]
    assert asked == [known.node_id, unknown.node_id], "only a seed without columns is asked for"


def test_a_sync_never_asks_the_schema_resolver():
    with notebook.notebook_mode(sync=True) as mode:
        mode.schema_resolver = lambda node: pytest.fail("a sync asked the schema resolver")
        assert ff.read_api("https://example.test/known").columns == []


SECRET_NODE = """
import polars as pl
from shared.node_designer import CustomNodeBase, NodeSettings, Section, SecretSelector


class NotebookSecretReader(CustomNodeBase):
    node_name: str = "Notebook Secret Reader"
    node_category: str = "Testing"
    settings_schema: NodeSettings = NodeSettings(auth=Section(title="Auth", token=SecretSelector(label="Token")))

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
"""


@pytest.fixture
def installed_secret_node():
    """``notebook_secret_reader`` installed in the registry for the test, the node store restored afterwards."""
    from flowfile_core.flowfile.user_defined.registry import registry

    registry.directory.mkdir(parents=True, exist_ok=True)
    node_file = registry.directory / "notebook_secret_reader.py"
    saved = dict(node_store.node_dict), list(node_store.nodes_list), dict(node_store.CUSTOM_NODE_STORE._overrides)
    node_file.write_text(SECRET_NODE)
    registry.load_file(node_file)
    try:
        yield "notebook_secret_reader"
    finally:
        registry.remove_file(node_file)
        node_file.unlink()
        node_store.node_dict.clear()
        node_store.node_dict.update(saved[0])
        node_store.nodes_list[:] = saved[1]
        node_store.CUSTOM_NODE_STORE.clear()
        node_store.CUSTOM_NODE_STORE.update(saved[2])


def test_notebook_mode_defers_a_cloud_reader_and_a_custom_node_that_selects_a_secret(installed_secret_node):
    """Both resolve a stored connection or secret when built, which a kernel session cannot: they are seeded and
    left to the canvas (a held run in a kernel session). In a script the custom node still builds."""
    with notebook.notebook_mode() as mode:
        cloud = ff.read_from_cloud_storage("/etc/hosts", file_format="csv")
        assert cloud._deferred is True and mode.graph.get_node(cloud.node_id).deferred_until_run is True
        node = ff.CustomNode(installed_secret_node, ff.from_dict(DATA), settings={"auth": {"token": "no_such"}})
        assert node.deferred is True
    built = ff.CustomNode(installed_secret_node, ff.from_dict(DATA), settings={"auth": {"token": "no_such"}})
    assert built.deferred is False, "a script builds it as before"


def test_a_kernel_session_leaves_connection_sources_to_core(mode, monkeypatch):
    """With a ``schema_resolver`` set and the lookup hook installed (a kernel session), a database reader's placement
    check goes to core and its seed comes from the resolver: its schema callback never runs here."""
    from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
    from flowfile_core.flowfile.flow_node.flow_node import FlowNode
    from flowfile_frame import _metadata

    asked: list[dict] = []

    def transport(body):
        asked.append(body)
        return {"result": None}

    predicted: list[str] = []
    original = FlowNode.get_predicted_schema

    def spied(self, *args, **kwargs):
        predicted.append(self.node_type)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FlowNode, "get_predicted_schema", spied)
    mode.schema_resolver = lambda node: {"output-0": [FlowfileColumn.from_input("x", "Int64")]}
    with _metadata.installed(0, transport):
        rows = ff.read_database("some_connection", table_name="t")
    assert rows.columns == ["x"] and rows._deferred is True
    assert "database_reader" not in predicted
    assert [(body["flow_id"], body["kind"], body["args"]["settings_type"]) for body in asked] == [
        (0, "placement_refusal", "NodeDatabaseReader")
    ]
    node = mode.graph.get_node(rows.node_id)
    assert native._predicts_in_core(node) is True
    mode.schema_resolver = None
    assert native._predicts_in_core(node) is False, "a script or a sync predicts as before"


def test_metadata_lookups_answer_from_the_installed_hook_and_read_the_catalog_without_one():
    from flowfile_frame import _metadata

    calls: list[tuple[str, dict]] = []
    answers = {
        "namespaces": [{"id": 5, "name": "Remote", "parent_id": None}],
        "catalog_table": {"file_path": "s3://bucket/catalog/t", "table_type": "physical", "serialized_lf": None,
                          "is_optimized": False},
    }  # fmt: skip

    def transport(body):
        assert body["flow_id"] == 42
        calls.append((body["kind"], body["args"]))
        return {"result": answers[body["kind"]]}

    with _metadata.installed(42, transport) as hook:
        assert _metadata.namespaces(None) == [_metadata.Namespace(5, "Remote", None)]
        assert _metadata.namespaces(None)[0].name == "Remote" and len(calls) == 1, "memoised for the op"
        hook("catalog_table", {"node_id": 9, "catalog_table_id": 3})
        assert [kind for kind, _ in calls] == ["namespaces", "catalog_table"]
    assert isinstance(_metadata.default_namespace_id(), int)
    assert "General" in {ns.name for ns in _metadata.namespaces(None)}
