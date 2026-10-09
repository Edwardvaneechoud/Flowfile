"""Simple build in ``code`` mode (``ai/local_model/code_build.py``).

The model writes FlowFrame code; core pre-scans it (``ast`` only), interprets it through the notebook's exec-free
clean run, and turns the save-format payload into the ``{nodes, edges}`` spec the JSON path already stages. Covered
here: the extractor, the pre-scan refusals (one per family), the dialect block against the allowlist, the prompt
budget, the payload adapter, the end-to-end path through the **real** interpreter with a stub provider (linear, join,
the repair round, the double failure, the answer escape hatch), the no-exec contract on hostile code, the route's
``mode="code"`` shape, and that a non-admin in docker mode may use it.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.ai import diff
from flowfile_core.ai import generate_routes as generate_routes_module
from flowfile_core.ai.context.budget import estimate_tokens
from flowfile_core.ai.local_model import code_build, oneshot
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.notebook import allowlist
from flowfile_core.notebook.runner import install_notebook_runner
from flowfile_core.schemas import schemas
from tests.notebook.test_contract import recording_builtins

IMPORT = "import flowfile as ff"
LINEAR = (
    f"{IMPORT}\n"
    'orders = ff.read_csv("orders.csv")\n'
    'paid = orders.filter(ff.col("status") == "paid")\n'
    'totals = paid.group_by(["city"]).agg(ff.col("amount").sum().alias("total"))\n'
    'ranked = totals.sort(["total"], descending=[True])\n'
)
JOIN = (
    f"{IMPORT}\n"
    'orders = ff.read_csv("orders.csv")\n'
    'customers = ff.read_excel("customers.xlsx")\n'
    'joined = orders.join(customers, left_on="customer_id", right_on="id", how="left")\n'
    'picked = joined.select(["order_id", "name", "amount"])\n'
)


def fenced(code: str, lang: str = "python") -> str:
    return f"Here you go:\n```{lang}\n{code}\n```\n"


class _Reply:
    def __init__(self, content: str) -> None:
        self.content = content


class StubProvider:
    """Answers each ``chat`` with the next scripted reply and records the messages it saw."""

    def __init__(self, *replies: str) -> None:
        self.replies = list(replies)
        self.calls: list[list[Any]] = []

    async def chat(self, messages, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append(list(messages))
        return _Reply(self.replies.pop(0))


@pytest.fixture(autouse=True)
def _runner_and_diff_store() -> Iterator[None]:
    install_notebook_runner()
    diff.clear_for_tests()
    yield
    diff.clear_for_tests()


def _empty_flow(flow_id: int = 1) -> FlowGraph:
    return FlowGraph(
        flow_settings=schemas.FlowSettings(
            flow_id=flow_id, execution_mode="Performance", execution_location="local", path="/tmp/test_code_build"
        ),
        name="code_build_test",
    )


def _generate(provider: StubProvider, flow: FlowGraph | None = None, request: str = "build it") -> dict:
    flow = flow or _empty_flow()
    return asyncio.run(
        code_build.generate_code_flow(
            provider=provider, flow=flow, flow_id=flow.flow_id, user_id=1, user_request=request
        )
    )


# extract_code                                                                #


def test_extract_code_takes_the_python_fence():
    assert code_build.extract_code(fenced(LINEAR)) == LINEAR.strip()
    assert code_build.extract_code(fenced(LINEAR, "py")) == LINEAR.strip()


def test_extract_code_takes_an_untagged_fence_that_imports_flowfile():
    assert code_build.extract_code(fenced(LINEAR, "")) == LINEAR.strip()


def test_extract_code_takes_bare_code_and_strips_think_blocks():
    assert code_build.extract_code(LINEAR) == LINEAR.strip()
    assert code_build.extract_code(f"<think>let me think {{}}</think>\n{fenced(LINEAR)}") == LINEAR.strip()


def test_extract_code_takes_a_fence_the_reply_never_closed():
    cut = '```python\nimport flowfile as ff\norders = ff.read_csv("orders.csv")\npaid = orders.filter(ff.col("a"'
    assert code_build.extract_code(cut) == cut.split("\n", 1)[1]
    assert code_build.extract_code("```json\n{\"answer\": \"hi\"}\n```\n```python\nx = 1") == "x = 1"
    assert oneshot.extract_answer(cut) is not None  # what the answer path would have shown


def test_extract_code_is_none_for_answers_and_prose():
    assert code_build.extract_code('```json\n{"answer": "hi"}\n```') is None
    assert code_build.extract_code("I build flows from a description.") is None
    assert code_build.extract_code("") is None
    assert code_build.extract_code("<think>only thoughts</think>") is None


def test_ensure_import_prepends_the_flowfile_import_once():
    assert code_build.ensure_import('x = ff.read_csv("a.csv")') == f'{IMPORT}\nx = ff.read_csv("a.csv")'
    assert code_build.ensure_import(LINEAR) == LINEAR


# The dialect block and the prompt                                           #


def test_every_advertised_name_is_allowlisted_and_not_refused():
    for kind, names in code_build.advertised_names().items():
        for name in names:
            if kind == "ff":
                assert allowlist.FL_VERDICTS[name][0] == allowlist.ALLOW, name
                assert name not in code_build.REFUSED_FF_NAMES, name
            else:
                assert name in allowlist.ALLOWLIST[kind], (kind, name)
                assert name not in code_build.REFUSED_METHODS, (kind, name)


def test_dialect_block_offers_the_frame_methods_and_no_refused_call():
    block = code_build.render_dialect_block()
    assert "ff.read_csv" in block and "- .join(" in block and ".str.to_uppercase()" in block
    for refused in ("- .polars_code", "- .sql(", "ff.read_database", "- .write_csv", "- .collect"):
        assert refused not in block


def test_system_prompt_fits_a_small_context():
    prompt = code_build.system_prompt()
    assert "```python" in prompt and "## Available calls" in prompt
    assert code_build.render_dialect_block() in prompt
    assert estimate_tokens(prompt) <= 2500


# prescan                                                                     #


@pytest.mark.parametrize(
    ("code", "line", "needle"),
    [
        (f"{IMPORT}\nimport os", 2, "import os"),
        (f"{IMPORT}\nfrom flowfile import col", 2, "from ... import"),
        (f"{IMPORT}\ndef build():\n    return 1", 2, "FunctionDef"),
        (f"{IMPORT}\nf = lambda x: x", 2, "Lambda"),
        (f"{IMPORT}\nfor c in ['a']:\n    x = c", 2, "For"),
        (f"{IMPORT}\nif True:\n    x = 1", 2, "If"),
        (f"{IMPORT}\ncols = [ff.col(c) for c in ['a']]", 2, "ListComp"),
        (f"{IMPORT}\nprint(1)", 2, "print(...)"),
        (f"{IMPORT}\nx = __import__('os')", 2, "__import__"),
        (f"{IMPORT}\nx = ff.read_database('c')", 2, "stored connection"),
        (f"{IMPORT}\nx = ff.read_catalog_table('t')", 2, "stored connection"),
        (f"{IMPORT}\nx = ff.write_catalog_table", 2, "writes data"),
        (f"{IMPORT}\nx = ff.polars_code('1')", 2, "code that would run"),
        (f"{IMPORT}\nx = ff.PythonScript", 2, "code that would run"),
        (f"{IMPORT}\nx = ff.RunFlow", 2, "not available"),
        (f"{IMPORT}\na = ff.read_csv('a')\nu = ff.concat([a, a])", 3, "not available"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv').collect()", 2, "runs the flow"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv').write_csv('b.csv')", 2, "writes data"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv').sink_parquet('b')", 2, "writes data"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv').sql('select 1')", 2, "code that would run"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv').polars_code('x')", 2, "code that would run"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv').tail(2)", 2, "Polars Code node"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv')._repr_str", 2, "._repr_str"),
        (f"{IMPORT}\nx = ff.read_csv('a.csv'", 2, "invalid Python"),
    ],
)
def test_prescan_refuses_with_the_line(code: str, line: int, needle: str):
    with pytest.raises(code_build.CodeBuildRefusal) as excinfo:
        code_build.prescan(code)
    assert excinfo.value.line == line
    assert needle in excinfo.value.message


def test_prescan_accepts_the_dialect():
    assert code_build.prescan(LINEAR) is None
    assert code_build.prescan(JOIN) is None
    assert code_build.prescan("import flowfile as ff\nimport polars as pl\nimport datetime\nx = ff.col('a')") is None


# The clean run and the payload adapter                                      #


def test_clean_run_payload_becomes_a_spec_with_wiring_and_without_identity_keys():
    result = code_build.interpret_script(JOIN, user_id=1, flow_id=1, ceiling=0)
    assert result.error is None, result.error
    spec = code_build.spec_from_flowfile_data(result.flowfile_data)
    assert [n["type"] for n in spec["nodes"]] == ["read", "read", "join", "select"]
    read_a, read_b, join, select = spec["nodes"]
    assert spec["edges"] == [
        {"source": read_a["id"], "target": join["id"]},
        {"source": read_b["id"], "target": join["id"]},
        {"source": join["id"], "target": select["id"]},
    ]
    for node in spec["nodes"]:
        assert not set(node["settings"]) & {"flow_id", "node_id", "pos_x", "pos_y", "depending_on_id"}
    assert join["settings"]["join_input"]["join_mapping"] == [{"left_col": "customer_id", "right_col": "id"}]
    # The frame makes a bare file name absolute under core's cwd; a file that
    # is not there goes back to what the user typed.
    assert read_a["settings"]["received_file"]["path"] == "orders.csv"


def test_a_polars_code_node_from_a_frame_fallback_is_refused_by_the_adapter():
    code = f'{IMPORT}\nx = ff.read_csv("a.csv").filter(ff.col("name").str.contains("foo"))'
    assert code_build.prescan(code) is None  # ``contains`` is a legitimate expression method
    result = code_build.interpret_script(code, user_id=1, flow_id=1, ceiling=0)
    assert result.error is None
    with pytest.raises(code_build.CodeBuildRefusal) as excinfo:
        code_build.spec_from_flowfile_data(result.flowfile_data)
    assert "Polars Code node" in excinfo.value.message


# generate_code_flow through the real interpreter                            #


def test_generate_code_flow_builds_a_linear_flow():
    flow = _empty_flow()
    result = _generate(StubProvider(fenced(LINEAR)), flow)
    assert result["op_count"] == 4, result["warnings"]
    assert [c["type"] for c in result["created"]] == ["read", "filter", "group_by", "sort"]
    assert result["code"] == LINEAR.strip()
    assert result["answer"] is None
    assert result["rationale"] == "Generated flow (code mode)"
    assert result["warnings"] == []
    graph_diff = diff.get_diff(result["diff_id"])
    assert graph_diff is not None
    assert result["diff_payload"]["diff_id"] == result["diff_id"]
    applied = diff.apply_diff(flow, graph_diff)
    assert len(applied.applied_node_ids) == 4
    assert {n.node_type for n in flow.nodes} == {"read", "filter", "group_by", "sort"}


def test_generate_code_flow_wires_a_join_and_numbers_above_the_canvas():
    flow = _empty_flow()
    first = _generate(StubProvider(fenced(LINEAR)), flow)
    diff.apply_diff(flow, diff.get_diff(first["diff_id"]))
    result = _generate(StubProvider(fenced(JOIN)), flow)
    graph_diff = diff.get_diff(result["diff_id"])
    join = next(a for a in graph_diff.additions if a.node_type == "join")
    reads = [a.settings["node_id"] for a in graph_diff.additions if a.node_type == "read"]
    assert join.insertion_context.upstream_node_ids == [reads[0]]
    assert join.insertion_context.right_input_node_id == reads[1]
    assert min(a.settings["node_id"] for a in graph_diff.additions) > max(n.node_id for n in flow.nodes)
    diff.apply_diff(flow, graph_diff)
    assert len(flow.nodes) == 8


def test_generate_code_flow_adds_the_missing_import():
    result = _generate(StubProvider(fenced('orders = ff.read_csv("orders.csv")')))
    assert result["op_count"] == 1
    assert result["code"].startswith(IMPORT)


def test_generate_code_flow_repairs_once_with_the_line():
    provider = StubProvider(fenced(f'{IMPORT}\norders = ff.read_json("orders.json")'), fenced(LINEAR))
    result = _generate(provider)
    assert result["op_count"] == 4
    assert len(provider.calls) == 2
    repair = provider.calls[1]
    assert [m.role for m in repair] == ["system", "user", "assistant", "user"]
    assert "line 2" in repair[-1].content and "ff.read_json" in repair[-1].content
    assert "needs a kernel" not in repair[-1].content


def test_generate_code_flow_gives_up_after_the_repair_round():
    provider = StubProvider(
        fenced(f'{IMPORT}\norders = ff.read_json("orders.json")'),
        fenced(f'{IMPORT}\norders = ff.read_csv("orders.csv").collect()'),
    )
    with pytest.raises(code_build.CodeBuildError) as excinfo:
        _generate(provider)
    assert excinfo.value.line == 2
    assert "runs the flow" in excinfo.value.message
    assert ".collect()" in excinfo.value.code
    assert len(provider.calls) == 2
    assert diff.list_diffs_for_tests() == [] if hasattr(diff, "list_diffs_for_tests") else True


def test_generate_code_flow_returns_the_models_answer():
    result = _generate(StubProvider('```json\n{"answer": "Describe a pipeline and I will build it."}\n```'))
    assert result["answer"] == "Describe a pipeline and I will build it."
    assert result["diff_id"] is None and result["code"] is None and result["op_count"] == 0


def test_generate_flow_delegates_code_mode():
    flow = _empty_flow()
    result = asyncio.run(
        oneshot.generate_flow(
            provider=StubProvider(fenced(LINEAR)),
            flow=flow,
            flow_id=1,
            user_id=1,
            user_request="x",
            mode="code",
        )
    )
    assert result["code"] == LINEAR.strip() and result["op_count"] == 4


# The no-exec contract                                                       #


def test_hostile_code_never_reaches_the_clean_run_or_exec(monkeypatch):
    hostile = f'{IMPORT}\nimport os\nx = os.system("echo pwned")'
    hostile_2 = f'{IMPORT}\nx = __import__("os").system("echo pwned")'

    def never_reached():
        raise AssertionError("the clean run must not run on code the pre-scan refuses")

    monkeypatch.setattr(code_build, "get_clean_runner", never_reached)
    with recording_builtins() as calls:
        with pytest.raises(code_build.CodeBuildError) as excinfo:
            _generate(StubProvider(fenced(hostile), fenced(hostile_2)))
    assert excinfo.value.line == 2
    leaked = [
        (c.builtin, c.caller)
        for c in calls
        if not c.ast_only and isinstance(c.source, str) and "echo pwned" in c.source
    ]
    assert leaked == []
    assert not [c for c in calls if not c.ast_only and not isinstance(c.source, str) and "pwned" in repr(c.source)]


# The route                                                                   #


@pytest.fixture
def registered_flow() -> Iterator[FlowGraph]:
    flow = _empty_flow(flow_id=9801)
    flow_file_handler._flows[flow.flow_id] = flow
    for user_id in (1, 2):
        flow_file_handler._register_user_session(user_id, flow.flow_id)
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)
        for user_id in (1, 2):
            flow_file_handler._unregister_user_session(user_id, flow.flow_id)


@pytest.fixture
def client_for():
    def _client(user: PydanticUser) -> TestClient:
        main.app.dependency_overrides[get_current_active_user] = lambda: user
        return TestClient(main.app)

    try:
        yield _client
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _post(client: TestClient, flow_id: int, **body: Any):
    payload = {"flow_id": flow_id, "user_request": "read orders.csv", "provider": "anthropic", **body}
    return client.post("/ai/generate", json=payload)


def test_route_code_mode_returns_the_code(registered_flow, client_for, monkeypatch):
    monkeypatch.setattr(generate_routes_module, "get_configured_provider", lambda *a, **k: StubProvider(fenced(LINEAR)))
    response = _post(client_for(PydanticUser(id=1, username="u")), registered_flow.flow_id, mode="code")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["op_count"] == 4 and body["code"] == LINEAR.strip() and body["answer"] is None


def test_route_code_mode_failure_carries_line_and_code(registered_flow, client_for, monkeypatch):
    bad = fenced(f'{IMPORT}\nx = ff.read_csv("a.csv").collect()')
    monkeypatch.setattr(generate_routes_module, "get_configured_provider", lambda *a, **k: StubProvider(bad, bad))
    response = _post(client_for(PydanticUser(id=1, username="u")), registered_flow.flow_id, mode="code")
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert detail["kind"] == "code" and detail["line"] == 2 and ".collect()" in detail["code"]
    assert "runs the flow" in detail["message"]


def test_route_default_mode_is_still_the_json_path(registered_flow, client_for, monkeypatch):
    spec = '{"nodes": [{"id": "a", "type": "manual_input", "settings": {"raw_data_format": {"columns": [{"name": "x", "data_type": "Int64"}], "data": [[1]]}}}], "edges": []}'
    monkeypatch.setattr(generate_routes_module, "get_configured_provider", lambda *a, **k: StubProvider(spec))
    response = _post(client_for(PydanticUser(id=1, username="u")), registered_flow.flow_id)
    assert response.status_code == 200, response.text
    assert response.json()["rationale"] == "Generated flow (simple mode)"


def test_route_refuses_a_flow_open_only_in_another_session(registered_flow, client_for, monkeypatch):
    monkeypatch.setattr(generate_routes_module, "get_configured_provider", lambda *a, **k: StubProvider(fenced(LINEAR)))
    response = _post(client_for(PydanticUser(id=3, username="other")), registered_flow.flow_id, mode="code")
    assert response.status_code == 422 and "not found" in response.json()["detail"]


def test_route_code_mode_is_open_to_a_non_admin_in_docker_mode(registered_flow, client_for, monkeypatch):
    """The notebook's plan/push are admin-only outside electron; Simple build is not, because the pre-scan keeps
    every grant-less catalog lookup out of reach."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setattr(generate_routes_module, "get_configured_provider", lambda *a, **k: StubProvider(fenced(JOIN)))
    user = PydanticUser(id=2, username="plain", is_admin=False)
    response = _post(client_for(user), registered_flow.flow_id, mode="code")
    assert response.status_code == 200, response.text
    assert response.json()["op_count"] == 4


# Continuing the flow on the canvas                                           #

TABLE = (
    f"{IMPORT}\n"
    'names = ff.from_raw_data({"columns": [{"name": "name", "data_type": "String"}], '
    '"data": [["edward", "courtney", "hans"]]})\n'
)


def _flow_with_table() -> FlowGraph:
    flow = _empty_flow()
    first = _generate(StubProvider(fenced(TABLE)), flow)
    diff.apply_diff(flow, diff.get_diff(first["diff_id"]))
    assert [n.node_type for n in flow.nodes] == ["manual_input"]
    return flow


def test_canvas_context_is_empty_for_an_empty_canvas():
    context = code_build.canvas_context(_empty_flow())
    assert context.block == "" and context.cells == [] and context.provenance == {}
    assert code_build.user_message("hi", context) == "hi"


def test_canvas_context_renders_the_flow_with_column_hints():
    flow = _flow_with_table()
    context = code_build.canvas_context(flow)
    assert context.block.startswith(IMPORT)
    assert "source_1 = ff.from_raw_data(" in context.block
    assert context.block.rstrip().endswith("# columns: name")
    assert context.provenance == {"cell-1": [("manual_input", 1)]}
    assert [cell_id for cell_id, _ in context.cells] == ["imports", "cell-1"]
    message = code_build.user_message("keep only hans", context)
    assert message.startswith("## Current flow\n```python\n" + IMPORT)
    assert message.endswith("```\n## Request\nkeep only hans")


def test_canvas_context_keeps_the_newest_steps_under_the_budget(monkeypatch):
    flow = _flow_with_table()
    monkeypatch.setattr(code_build, "CONTEXT_CHAR_BUDGET", 10)
    context = code_build.canvas_context(flow)
    assert context.block.splitlines()[:2] == [IMPORT, "# ... earlier steps omitted ..."]
    assert len(context.cells) == 2  # the clean run still gets every cell


def test_generate_code_flow_continues_from_a_live_node():
    flow = _flow_with_table()
    provider = StubProvider(fenced('only_hans = source_1.filter(ff.col("name") == "hans")'))
    result = _generate(provider, flow, "keep only hans")
    assert provider.calls[0][1].content.startswith("## Current flow")
    assert [c["type"] for c in result["created"]] == ["filter"]
    graph_diff = diff.get_diff(result["diff_id"])
    (addition,) = graph_diff.additions
    assert addition.insertion_context.upstream_node_ids == [1]
    assert addition.insertion_context.pos_x > flow.get_node(1).setting_input.pos_x
    diff.apply_diff(flow, graph_diff)
    assert [(n.node_id, n.node_type) for n in flow.nodes] == [(1, "manual_input"), (2, "filter")]
    assert [i.node_id for i in flow.get_node(2).node_inputs.main_inputs] == [1]


def test_generate_code_flow_joins_a_new_read_onto_a_live_node():
    flow = _flow_with_table()
    code = 'ages = ff.read_csv("ages.csv")\nwith_ages = source_1.join(ages, left_on="name", right_on="name", how="left")'
    result = _generate(StubProvider(fenced(code)), flow, "join ages.csv on name")
    graph_diff = diff.get_diff(result["diff_id"])
    read, join = graph_diff.additions
    assert (read.node_type, join.node_type) == ("read", "join")
    assert join.insertion_context.upstream_node_ids == [1]
    assert join.insertion_context.right_input_node_id == read.settings["node_id"]
    # A new root never lands on a live node: it goes below the canvas.
    live = flow.get_node(1).setting_input
    assert (read.insertion_context.pos_x, read.insertion_context.pos_y) != (live.pos_x, live.pos_y)
    assert read.insertion_context.pos_y > live.pos_y
    diff.apply_diff(flow, graph_diff)
    assert flow.get_node(join.settings["node_id"]).node_inputs.right_input.node_id == read.settings["node_id"]


def test_generate_code_flow_refuses_a_script_that_adds_nothing():
    flow = _flow_with_table()
    with pytest.raises(code_build.CodeBuildError) as excinfo:
        _generate(StubProvider(fenced("x = 1"), fenced("y = 2")), flow, "nothing")
    assert "added no new step" in excinfo.value.message


def test_generate_code_flow_without_context_still_builds_from_scratch(monkeypatch):
    """A canvas the exporter cannot render falls back to the plain build."""
    from flowfile_core.notebook import render as render_module

    def broken(flow):
        raise RuntimeError("no export for this node")

    monkeypatch.setattr(render_module, "render", broken)
    flow = _flow_with_table()
    assert code_build.canvas_context(flow) == code_build.CanvasContext()
    provider = StubProvider(fenced(LINEAR))
    result = _generate(provider, flow, "read orders.csv")
    assert provider.calls[0][1].content == "read orders.csv"
    assert [c["type"] for c in result["created"]] == ["read", "filter", "group_by", "sort"]


def test_generate_code_flow_retries_without_context_when_a_canvas_cell_fails(monkeypatch):
    flow = _flow_with_table()
    broken = code_build.CanvasContext(cells=[("imports", IMPORT), ("cell-1", "source_1 = no_such_name")], block="x")
    monkeypatch.setattr(code_build, "canvas_context", lambda flow: broken)
    provider = StubProvider(fenced('only_hans = source_1.filter(ff.col("name") == "hans")'), fenced(LINEAR))
    result = _generate(provider, flow, "keep only hans")
    assert provider.calls[0][1].content.startswith("## Current flow")
    assert provider.calls[1][1].content == "keep only hans"
    assert [c["type"] for c in result["created"]] == ["read", "filter", "group_by", "sort"]


def test_generate_code_flow_repairs_a_reply_without_a_script():
    provider = StubProvider("Sure! Here is the flow:\n```python\n```", fenced(LINEAR))
    result = _generate(provider)
    assert result["op_count"] == 4 and len(provider.calls) == 2
    assert "no ```python block" in provider.calls[1][-1].content


def test_canvas_context_is_withheld_for_a_stored_resource_node_where_sharing_is_enabled(monkeypatch):
    flow = _flow_with_table()
    monkeypatch.setattr(code_build.sharing, "sharing_enabled", lambda: True)
    assert code_build.canvas_context(flow).cells  # a table reaches nothing
    monkeypatch.setattr(code_build, "STORED_RESOURCE_NODE_TYPES", frozenset({"manual_input"}))
    assert code_build.stored_resource_nodes(flow) == ["manual_input:1"]
    assert code_build.canvas_context(flow) == code_build.CanvasContext()
    monkeypatch.setattr(code_build.sharing, "sharing_enabled", lambda: False)
    assert code_build.canvas_context(flow).cells


def test_split_second_frame_is_wired_from_its_own_handle():
    flow = _empty_flow()
    code = (
        f"{IMPORT}\n"
        'orders = ff.read_csv("orders.csv")\n'
        'big, small = orders.filter_split(ff.col("amount") > 100)\n'
        'ranked = small.sort(["amount"], descending=[True])\n'
    )
    result = _generate(StubProvider(fenced(code)), flow)
    graph_diff = diff.get_diff(result["diff_id"])
    sort = next(a for a in graph_diff.additions if a.node_type == "sort")
    split = next(a for a in graph_diff.additions if a.node_type == "filter")  # a split is a two-exit filter
    assert sort.insertion_context.upstream_node_ids == []
    (connection,) = graph_diff.connections_added
    assert connection.connection["output_connection"]["node_id"] == split.settings["node_id"]
    assert connection.connection["output_connection"]["connection_class"] == "output-1"
    assert connection.connection["input_connection"]["node_id"] == sort.settings["node_id"]
    diff.apply_diff(flow, graph_diff)
    sort_node = flow.get_node(sort.settings["node_id"])
    assert [i.node_id for i in sort_node.node_inputs.main_inputs] == [split.settings["node_id"]]
    assert flow.get_node(split.settings["node_id"]).node_information.output_handles == ["output-1"]


def test_staged_nodes_never_cover_each_other_or_a_live_node():
    spec = {
        "nodes": [{"id": i, "type": "read", "settings": {}} for i in ("a", "b")]
        + [{"id": i, "type": "filter", "settings": {}} for i in ("a1", "a2", "b1")],
        "edges": [{"source": "a", "target": "a1"}, {"source": "a", "target": "a2"}, {"source": "b", "target": "b1"}],
    }
    planned = oneshot._plan_insertions(spec, 1)
    positions = [(p.pos_x, p.pos_y) for p in planned]
    assert len(set(positions)) == len(positions)
    flow = _flow_with_table()
    first = _generate(StubProvider(fenced('a = source_1.filter(ff.col("name") == "a")')), flow, "a")
    diff.apply_diff(flow, diff.get_diff(first["diff_id"]))
    second = _generate(StubProvider(fenced('b = source_1.filter(ff.col("name") == "b")')), flow, "b")
    (addition,) = diff.get_diff(second["diff_id"]).additions
    live = {(n.setting_input.pos_x, n.setting_input.pos_y) for n in flow.nodes}
    assert (addition.insertion_context.pos_x, addition.insertion_context.pos_y) not in live
    assert addition.settings["node_id"] == flow.node_id_ceiling + 1


def test_strip_echoed_drops_the_restated_context_lines_only():
    flow = _flow_with_table()
    context = code_build.canvas_context(flow)
    table_line = context.block.splitlines()[1]
    assert table_line.endswith("# columns: name")
    echoed = (
        f"{IMPORT}\n"
        f"{code_build._HINT_RE.sub('', table_line)}\n"
        f"{table_line}\n"
        'only_hans = source_1.filter(ff.col("name") == "hans")\n'
    )
    assert code_build.strip_echoed(echoed, context) == (
        f'{IMPORT}\nonly_hans = source_1.filter(ff.col("name") == "hans")'
    )
    assert code_build.strip_echoed(LINEAR, code_build.CanvasContext()) == LINEAR


def test_generate_code_flow_does_not_duplicate_an_echoed_table():
    flow = _flow_with_table()
    context = code_build.canvas_context(flow)
    echoed = context.block + '\nonly_hans = source_1.filter(ff.col("name") == "hans")'
    result = _generate(StubProvider(fenced(echoed)), flow, "keep only hans")
    assert [c["type"] for c in result["created"]] == ["filter"]
    assert "from_raw_data" not in result["code"]
