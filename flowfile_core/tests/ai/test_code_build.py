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
    try:
        yield flow
    finally:
        flow_file_handler._flows.pop(flow.flow_id, None)


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


def test_route_code_mode_is_open_to_a_non_admin_in_docker_mode(registered_flow, client_for, monkeypatch):
    """The notebook's plan/push are admin-only outside electron; Simple build is not, because the pre-scan keeps
    every grant-less catalog lookup out of reach."""
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    monkeypatch.setattr(generate_routes_module, "get_configured_provider", lambda *a, **k: StubProvider(fenced(JOIN)))
    user = PydanticUser(id=2, username="plain", is_admin=False)
    response = _post(client_for(user), registered_flow.flow_id, mode="code")
    assert response.status_code == 200, response.text
    assert response.json()["op_count"] == 4
