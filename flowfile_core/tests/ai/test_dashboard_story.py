"""Tests for ``POST /ai/dashboard_story``: the evidence, the story's checks, the model loop and the route."""

from __future__ import annotations

import ast
import asyncio
import importlib
import json
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import dashboard_routes
from flowfile_core.ai import dashboard_story as ds
from flowfile_core.ai.chart_request import ChartRequest
from flowfile_core.ai.dashboard_plan import PlannedKpi, PlannedRelation
from flowfile_core.ai.dashboard_story import Evidence, StoryChart, StoryFailure, check_story, write_story
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable
from flowfile_core.schemas.catalog_schema import SqlQueryResult
from tests.ai.test_chart_request import ScriptedProvider, _scheduler

BAR = {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}}
SCATTER = {"mark": "point", "x": {"field": "discount"}, "y": {"field": "quantity"}, "aggregate": False}


def _aggregate(groups: list[str], measures: list[tuple[str, str]], sort: bool = False) -> dict[str, Any]:
    query = {
        "op": "aggregate",
        "groupBy": groups,
        "measures": [{"field": f, "agg": a, "asFieldKey": f"{f}_{a}"} for f, a in measures],
    }
    workflow: list[dict[str, Any]] = [{"type": "view", "query": [query]}]
    if sort:
        workflow.append({"type": "sort", "by": [f"{measures[0][0]}_{measures[0][1]}"], "sort": "descending"})
    return {"workflow": workflow}


RAW = {"workflow": [{"type": "view", "query": [{"op": "raw", "fields": ["discount", "quantity"]}]}]}
BY_REGION = _aggregate(["region"], [("revenue", "sum")])


def _chart(chart_id: str, title: str, chart: dict = BAR, payload: dict | None = BY_REGION, **extra: Any) -> StoryChart:
    return StoryChart(id=chart_id, table_id=1, title=title, chart=ChartRequest(**chart), payload=payload, **extra)


CHARTS = [
    _chart("c1", "Revenue by region", purpose="Which regions lead."),
    _chart("c2", "Discount against quantity", SCATTER, RAW),
    _chart("c3", "Revenue by category"),
]


# Evidence as text.


def test_only_an_aggregate_query_is_read() -> None:
    assert ds.aggregate_query(BY_REGION)["groupBy"] == ["region"]
    assert ds.aggregate_query(RAW) is None
    assert ds.aggregate_query(None) is None
    assert ds.aggregate_query({"workflow": "nope"}) is None


def test_rows_are_ordered_by_their_groups_with_the_extremes_worked_out() -> None:
    rows = [
        {"region": "West", "revenue_sum": 19800.0},
        {"region": "North", "revenue_sum": 6100.123456},
        {"region": "East", "revenue_sum": 9000.0},
    ]
    assert ds.render_rows(BY_REGION, rows, 3) == "\n".join(
        [
            "Result, all 3 rows:",
            "revenue_sum: highest 19800.0 at region=West; lowest 6100.1235 at region=North; "
            "total over these rows 34900.1235",
            "region | revenue_sum",
            "East | 9000.0",
            "North | 6100.1235",
            "West | 19800.0",
        ]
    )


def test_a_sorted_query_keeps_its_order_and_a_mean_has_no_total() -> None:
    payload = _aggregate(["region"], [("revenue", "mean")], sort=True)
    rows = [{"region": "West", "revenue_mean": 9.0}, {"region": "East", "revenue_mean": 4.0}]
    text = ds.render_rows(payload, rows, 2)
    assert text.index("West | 9.0") < text.index("East | 4.0")
    assert "total over" not in text


def test_a_long_result_is_shown_as_evenly_spaced_rows_first_and_last_included() -> None:
    payload = _aggregate(["day"], [("revenue", "sum")])
    rows = [{"day": f"2025-{i:03d}", "revenue_sum": i} for i in range(365, 0, -1)]
    lines = ds.render_rows(payload, rows, 365).split("\n")
    assert lines[0] == f"Result, {ds.EVIDENCE_ROWS} evenly spaced rows of the 365 read:"
    assert lines[1].startswith("revenue_sum: highest 365 at day=2025-365; lowest 1 at day=2025-001; total")
    assert (lines[3], lines[-1], len(lines)) == ("2025-001 | 1", "2025-365 | 365", 3 + ds.EVIDENCE_ROWS)


def test_a_truncated_result_says_how_many_rows_there_are() -> None:
    rows = [{"region": f"r{i:04d}", "revenue_sum": i} for i in range(ds.EVIDENCE_FETCH_ROWS)]
    head = ds.render_rows(BY_REGION, rows, 5000).split("\n")[0]
    assert head == f"Result, {ds.EVIDENCE_ROWS} evenly spaced rows of the 400 read (the result has 5000):"


def test_values_are_masked_cut_and_the_columns_capped() -> None:
    payload = _aggregate(["contact"], [(f"m{i}", "sum") for i in range(10)])
    row = {"contact": "ann@example.com", **{f"m{i}_sum": i for i in range(10)}}
    lines = ds.render_rows(payload, [row, {**row, "contact": "x" * 80}], 2).split("\n")
    header = next(line for line in lines if line.startswith("contact | "))
    assert header.count("|") == ds.EVIDENCE_COLUMNS - 1
    assert "ann@example.com" not in "\n".join(lines)
    assert max(len(cell) for cell in lines[-1].split(" | ")) <= 40


def test_relation_words_and_its_query() -> None:
    assert ds.relation_words(0.04, 1200) == "Pearson r = 0.04 over 1200 rows: no linear relation"
    assert ds.relation_words(-0.25, 500) == "Pearson r = -0.25 over 500 rows: a weak negative linear relation"
    assert ds.relation_words(0.45, 500) == "Pearson r = 0.45 over 500 rows: a moderate positive linear relation"
    assert ds.relation_words(0.81, 500) == "Pearson r = 0.81 over 500 rows: a strong positive linear relation"
    assert ds.relation_words(0.9, 4).startswith("too few rows with both values (4)")
    assert ds.relation_words(None, 50).startswith("no correlation could be computed over 50 rows")
    assert ds.relation_words(float("nan"), 50).startswith("no correlation could be computed")
    table = SimpleNamespace(name="orders", full_table_name="sales.orders", qualified_name="main.sales.orders")
    assert ds.relation_sql(table, PlannedRelation(x="discount", y='qty "net"')) == (
        'SELECT CORR("discount", "qty ""net""") AS r, COUNT(*) AS n FROM "main.sales.orders" '
        'WHERE "discount" IS NOT NULL AND "qty ""net""" IS NOT NULL'
    )
    assert 'FROM "orders"' in ds.relation_sql(SimpleNamespace(name="orders"), PlannedRelation(x="a", y="b"))


def test_a_raw_scatter_of_two_number_columns_is_a_relation() -> None:
    numeric = frozenset({"discount", "quantity", "revenue"})
    assert ds.scatter_relation(ChartRequest(**SCATTER), numeric).model_dump() == {"x": "discount", "y": "quantity"}
    assert ds.scatter_relation(ChartRequest(**BAR), numeric) is None
    aggregated = {**SCATTER, "y": {"field": "quantity", "aggregate": "sum"}}
    assert ds.scatter_relation(ChartRequest(**aggregated), numeric) is None
    assert ds.scatter_relation(ChartRequest(**SCATTER), frozenset({"discount"})) is None


# The story's checks.


def _story(**changes: Any) -> dict[str, Any]:
    story = {
        "title": "Sales",
        "summary": "Sales are **48,210**.",
        "charts": [
            {"id": "c1", "title": "Revenue by region", "finding": "West leads.", "width": "full"},
            {"id": "c2", "finding": "No relation."},
        ],
        "left_out": [{"id": "c3", "reason": "No rows."}],
    }
    return {**story, **changes}


def test_a_valid_story_keeps_order_titles_findings_and_widths() -> None:
    story, problems, warnings = check_story(_story(), CHARTS, "Fallback")
    assert (problems, warnings) == ([], [])
    assert story.model_dump() == {
        "title": "Sales",
        "summary_md": "Sales are **48,210**.",
        "charts": [
            {"id": "c1", "title": "Revenue by region", "finding": "West leads.", "width": "full"},
            {"id": "c2", "title": "Discount against quantity", "finding": "No relation.", "width": "half"},
        ],
        "left_out": [{"id": "c3", "reason": "No rows."}],
    }


def test_a_chart_the_story_forgot_is_placed_last_with_a_warning() -> None:
    story, problems, warnings = check_story(_story(left_out=[], title=None), CHARTS, "Fallback")
    assert problems == []
    assert (story.title, [c.id for c in story.charts]) == ("Fallback", ["c1", "c2", "c3"])
    assert story.charts[2].finding is None
    assert warnings == ['The story did not mention "Revenue by category"; it was placed last without a finding.']


def test_unknown_ids_are_problems_and_are_dropped() -> None:
    raw = _story(charts=[{"id": "c9"}, {"id": "c1"}, "c2", {"id": "c1"}], left_out=[{"id": "zz"}, "c3", {"id": "c1"}])
    story, problems, _ = check_story(raw, CHARTS, "Fallback")
    assert problems == [
        'charts[0].id "c9" is not a chart. Charts: c1, c2, c3',
        'left_out[0].id "zz" is not a chart. Charts: c1, c2, c3',
    ]
    assert [c.id for c in story.charts] == ["c1", "c2"]
    assert [(o.id, o.reason) for o in story.left_out] == [("c3", "Left out by the AI.")]


@pytest.mark.parametrize("raw", [[], "story", {"charts": [{"id": "c1"}]}, {"summary": "  "}])
def test_an_answer_without_a_summary_is_refused(raw: Any) -> None:
    story, problems, _ = check_story(raw, CHARTS, "Fallback")
    assert story is None and problems


def test_both_examples_in_the_prompt_pass_the_checks() -> None:
    prompt = ds.system_prompt()
    assert "<<EXAMPLE" not in prompt
    charts = [_chart(f"c{i}", f"Chart {i}") for i in range(1, 5)]
    decoder = json.JSONDecoder()
    start = 0
    for expected_left_out in (["c4"], []):
        start = prompt.index('{\n "title": "Sales performance and drivers"', start)
        answer, length = decoder.raw_decode(prompt[start:])
        start += length
        story, problems, warnings = check_story(answer, charts, "Fallback")
        assert (problems, warnings) == ([], [])
        assert [o.id for o in story.left_out] == expected_left_out


# The model loop, with a scripted provider.

KPIS = [PlannedKpi(id="k1", table_id=1, label="Total revenue", field="revenue", agg="sum")]
EVIDENCE = [
    Evidence("k1", "kpi", "measured", text="48210.5"),
    Evidence("c1", "chart", "measured", text="Result, all 2 rows:\nregion | revenue_sum\nWest | 19800.0"),
    Evidence("c2", "chart", "not_measured", detail="the chart reads raw rows, which are not sent"),
    Evidence("c2", "relation", "measured", text='Relation of "discount" and "quantity": no relation.', detail="none"),
    Evidence("c3", "chart", "not_measured", detail="the chart's query failed"),
]


async def _write(provider: ScriptedProvider, **kwargs: Any):
    return await write_story(
        prompt=kwargs.pop("prompt", "How are sales doing?"),
        title="Sales",
        kpis=kwargs.pop("kpis", KPIS),
        charts=kwargs.pop("charts", CHARTS),
        sources={1: "orders"},
        evidence=kwargs.pop("evidence", EVIDENCE),
        provider=provider,
        user_id=7,
        scheduler=_scheduler(),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_the_story_is_written_from_the_evidence_on_the_dashboard_surface() -> None:
    provider = ScriptedProvider(json.dumps(_story()))
    resp = await _write(provider, failed=[StoryFailure(id="c4", title="Returns", reason="no chart")])
    assert (resp.degraded, resp.grounded, resp.attempts, resp.warnings) == (False, True, 1, [])
    assert [c.id for c in resp.story.charts] == ["c1", "c2"]
    assert [(e.id, e.kind, e.status) for e in resp.evidence] == [
        ("k1", "kpi", "measured"),
        ("c1", "chart", "measured"),
        ("c2", "chart", "not_measured"),
        ("c2", "relation", "measured"),
        ("c3", "chart", "not_measured"),
    ]
    [call] = provider.calls
    assert (call["surface"], call["user_id"], call["response_format"]) == ("dashboard", 7, {"type": "json_object"})
    user = call["messages"][1].content
    assert '- k1 "Total revenue" (sum of revenue on "orders") = 48210.5' in user
    assert '### c1 "Revenue by region" (source "orders")\nTests: Which regions lead.\nChart: {"mark": "bar"' in user
    assert "Evidence:\nResult, all 2 rows:\nregion | revenue_sum\nWest | 19800.0" in user
    assert 'Evidence:\nRelation of "discount" and "quantity": no relation.' in user
    assert "Evidence: not measured (the chart's query failed)." in user
    assert '- "Returns": no chart' in user
    assert "Data values are not shared" not in user


@pytest.mark.asyncio
async def test_withheld_values_mean_no_evidence_and_an_ungrounded_story() -> None:
    provider = ScriptedProvider(json.dumps(_story()))
    resp = await _write(provider, evidence=ds.withheld_evidence(KPIS, CHARTS))
    assert (resp.degraded, resp.grounded) == (False, False)
    assert {e.status for e in resp.evidence} == {"withheld"}
    user = provider.calls[0]["messages"][1].content
    assert "Data values are not shared. Nothing was measured." in user
    assert "Evidence:" not in user.replace("## Evidence", "")
    assert '- k1 "Total revenue" (sum of revenue on "orders")\n' in user


@pytest.mark.asyncio
async def test_evidence_that_all_failed_is_not_called_withheld() -> None:
    failed = [Evidence(c.id, "chart", "not_measured", detail="the chart's query failed") for c in CHARTS]
    provider = ScriptedProvider(json.dumps(_story()))
    resp = await _write(provider, evidence=failed, kpis=[])
    assert resp.grounded is False
    assert "The data could not be read. Nothing was measured." in provider.calls[0]["messages"][1].content


@pytest.mark.asyncio
async def test_problems_go_back_once_and_the_fixed_story_is_taken() -> None:
    bad = json.dumps(_story(charts=[{"id": "c9"}, {"id": "c1"}, {"id": "c2"}]))
    provider = ScriptedProvider(bad, json.dumps(_story()))
    resp = await _write(provider)
    assert (resp.degraded, resp.attempts, resp.warnings) == (False, 2, [])
    retry = provider.calls[1]["messages"]
    assert [m.role for m in retry] == ["system", "user", "assistant", "user"]
    assert retry[3].content.startswith('The story has 1 problem:\n- charts[0].id "c9" is not a chart.')


@pytest.mark.asyncio
@pytest.mark.parametrize("second", ["not json", ""])
async def test_a_failed_retry_keeps_the_first_usable_story(second: str) -> None:
    bad = json.dumps(_story(charts=[{"id": "c9"}, {"id": "c1"}, {"id": "c2"}]))
    resp = await _write(ScriptedProvider(bad, second))
    assert (resp.degraded, resp.attempts) == (False, 2)
    assert [c.id for c in resp.story.charts] == ["c1", "c2"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "reason"),
    [
        (ScriptedProvider("nope", "still nope"), "parse_error"),
        (ScriptedProvider("", ""), "empty_response"),
        (ScriptedProvider(json.dumps({"charts": []}), json.dumps({"charts": []})), "invalid_story"),
        (ScriptedProvider("[]", "[]"), "validation_error"),
        (ScriptedProvider(error=RuntimeError("boom")), "provider_error"),
        (ScriptedProvider("x", delay=0.2), "timeout"),
    ],
)
async def test_soft_failures_carry_their_reason_and_still_report_the_evidence(
    provider: ScriptedProvider, reason: str
) -> None:
    resp = await _write(provider, timeout=0.05 if reason == "timeout" else 5.0)
    assert (resp.degraded, resp.reason, resp.story) == (True, reason, None)
    assert reason in ds.REASONS
    assert (resp.grounded, len(resp.evidence)) == (True, len(EVIDENCE))


@pytest.mark.asyncio
async def test_no_charts_and_a_blank_prompt_make_no_call() -> None:
    provider = ScriptedProvider()
    assert (await _write(provider, charts=[], evidence=[])).reason == "no_charts"
    assert (await _write(provider, prompt=" ")).reason == "empty_prompt"
    assert provider.calls == []


# The route: evidence read through the catalog, as the caller.

SCHEMA = [
    {"name": "region", "dtype": "String"},
    {"name": "revenue", "dtype": "Float64"},
    {"name": "discount", "dtype": "Float64"},
    {"name": "quantity", "dtype": "Int64"},
]


@pytest.fixture
def table_id() -> Iterator[int]:
    with get_db_context() as db:
        ns = CatalogNamespace(name="DashboardStoryNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        table = CatalogTable(
            name="story_orders",
            namespace_id=ns.id,
            owner_id=1,
            file_path="/tmp/flowfile-test/story_orders",
            storage_format="delta",
            schema_json=json.dumps(SCHEMA),
            table_type="physical",
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        ids = (table.id, ns.id)
    yield ids[0]
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id == ids[0]).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id == ids[1]).delete()
        db.commit()


class FakeCompute:
    """Stands in for the worker's chart query and the catalog's SQL; records what each was asked."""

    def __init__(self, *, delay: float = 0.0, query_error: Exception | None = None) -> None:
        self.payloads: list[dict] = []
        self.sql: list[str] = []
        self.delay = delay
        self.query_error = query_error

    def query(self, worker_source, payload, max_rows, *, worker_url):
        self.payloads.append(payload)
        if self.delay:
            time.sleep(self.delay)
        if self.query_error is not None:
            raise self.query_error
        aggregate = payload["workflow"][0]["query"][0]
        if aggregate["groupBy"]:
            rows = [{"region": "West", "revenue_sum": 19800.0}, {"region": "ann@example.com", "revenue_sum": 6100.0}]
        else:
            rows = [{"value": 48210.5}]
        return {"rows": rows, "total_rows": len(rows), "truncated": False, "elapsed_ms": 1.0, "cache_hit": False}

    def execute_sql(self, query, max_rows=10_000, user_id=None):
        self.sql.append(query)
        return SqlQueryResult(columns=["r", "n"], rows=[[0.0412, 1200]], total_rows=1)


@pytest.fixture
def compute() -> Iterator[FakeCompute]:
    fake = FakeCompute()
    with (
        patch("flowfile_core.catalog.service.trigger_visualize_query", side_effect=lambda *a, **k: fake.query(*a, **k)),
        patch("flowfile_core.catalog.service.CatalogService.execute_sql_query", fake.execute_sql),
    ):
        yield fake


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    main.app.dependency_overrides[get_current_active_user] = lambda: PydanticUser(id=1, username="local_user")
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _use_provider(monkeypatch: pytest.MonkeyPatch, provider: ScriptedProvider) -> None:
    monkeypatch.setattr(dashboard_routes, "_resolve_provider", lambda *_a, **_kw: provider)


def _body(table_id: int, **changes: Any) -> dict[str, Any]:
    body = {
        "table_ids": [table_id],
        "prompt": "How are sales doing?",
        "title": "Sales",
        "kpis": [{"id": "k1", "table_id": table_id, "label": "Total revenue", "field": "revenue", "agg": "sum"}],
        "charts": [
            {"id": "c1", "table_id": table_id, "title": "Revenue by region", "chart": BAR, "payload": BY_REGION},
            {"id": "c2", "table_id": table_id, "title": "Discount against quantity", "chart": SCATTER, "payload": RAW},
            {"id": "c3", "table_id": table_id, "title": "Revenue by category", "chart": BAR, "payload": None},
        ],
    }
    return {**body, **changes}


def test_route_reads_the_evidence_and_writes_the_story(
    authed_client: TestClient, table_id: int, compute: FakeCompute, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(json.dumps(_story()))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/dashboard_story", json=_body(table_id))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["degraded"], body["grounded"], body["attempts"]) == (False, True, 1)
    assert [c["id"] for c in body["story"]["charts"]] == ["c1", "c2"]
    assert body["evidence"] == [
        {"id": "k1", "kind": "kpi", "status": "measured", "detail": None},
        {"id": "c1", "kind": "chart", "status": "measured", "detail": "2 rows read"},
        {
            "id": "c2",
            "kind": "chart",
            "status": "not_measured",
            "detail": "the chart reads raw rows, which are not sent",
        },
        {
            "id": "c2",
            "kind": "relation",
            "status": "measured",
            "detail": "Pearson r = 0.04 over 1200 rows: no linear relation",
        },
        {
            "id": "c3",
            "kind": "chart",
            "status": "not_measured",
            "detail": "the chart reads raw rows, which are not sent",
        },
    ]
    assert RAW not in compute.payloads and len(compute.payloads) == 2
    [sql] = compute.sql
    assert sql.startswith('SELECT CORR("discount", "quantity") AS r, COUNT(*) AS n FROM "')
    user = provider.calls[0]["messages"][1].content
    assert '- k1 "Total revenue" (sum of revenue on "story_orders") = 48210.5' in user
    assert "revenue_sum: highest 19800.0 at region=West" in user
    assert 'Relation of "discount" and "quantity": Pearson r = 0.04 over 1200 rows: no linear relation.' in user
    assert "ann@example.com" not in user


@pytest.mark.parametrize(("send_values", "env"), [(False, "true"), (True, "false")])
def test_route_reads_nothing_when_the_user_or_the_server_withholds_values(
    authed_client: TestClient,
    table_id: int,
    compute: FakeCompute,
    monkeypatch: pytest.MonkeyPatch,
    send_values: bool,
    env: str,
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_SEND_DATA", env)
    provider = ScriptedProvider(json.dumps(_story()))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/dashboard_story", json=_body(table_id, send_values=send_values))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["degraded"], body["grounded"]) == (False, False)
    assert {e["status"] for e in body["evidence"]} == {"withheld"}
    assert (compute.payloads, compute.sql) == ([], [])
    user = provider.calls[0]["messages"][1].content
    assert "Data values are not shared. Nothing was measured." in user
    assert "48210.5" not in user and "West" not in user


def test_route_marks_a_failed_query_not_measured_and_still_writes(
    authed_client: TestClient, table_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(json.dumps(_story()))
    _use_provider(monkeypatch, provider)
    with (
        patch("flowfile_core.catalog.service.trigger_visualize_query", side_effect=RuntimeError("worker down")),
        patch(
            "flowfile_core.catalog.service.CatalogService.execute_sql_query",
            lambda *_a, **_k: SqlQueryResult(error="No catalog tables available"),
        ),
    ):
        resp = authed_client.post("/ai/dashboard_story", json=_body(table_id))
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["degraded"], body["grounded"]) == (False, False)
    assert {e["status"] for e in body["evidence"]} == {"not_measured"}
    assert "The data could not be read." in provider.calls[0]["messages"][1].content


def test_route_does_not_measure_columns_the_table_does_not_have(
    authed_client: TestClient, table_id: int, compute: FakeCompute, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use_provider(monkeypatch, ScriptedProvider(json.dumps(_story())))
    body = _body(
        table_id,
        kpis=[
            {"id": "k1", "table_id": table_id, "label": "Bad", "field": "nope", "agg": "sum"},
            {"id": "k2", "table_id": table_id, "label": "Text", "field": "region", "agg": "mean"},
        ],
    )
    body["charts"][1]["relation"] = {"x": "discount", "y": "region"}
    notes = {(e["id"], e["kind"]): e for e in authed_client.post("/ai/dashboard_story", json=body).json()["evidence"]}
    assert notes[("k1", "kpi")]["detail"] == '"nope" is not a column of "story_orders"'
    assert notes[("k2", "kpi")]["detail"] == '"mean" needs a number column'
    assert notes[("c2", "relation")]["detail"] == "the relation does not name two number columns"
    assert compute.sql == []


def test_route_refuses_before_any_read_or_model_call(
    authed_client: TestClient, table_id: int, compute: FakeCompute, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider()
    _use_provider(monkeypatch, provider)
    stray = _body(table_id)
    stray["charts"][0]["table_id"] = table_id + 1
    assert authed_client.post("/ai/dashboard_story", json=stray).status_code == 422
    twice = _body(table_id)
    twice["charts"][1]["id"] = "c1"
    assert authed_client.post("/ai/dashboard_story", json=twice).status_code == 422
    assert authed_client.post("/ai/dashboard_story", json=_body(987654321, kpis=[], charts=[])).status_code == 404
    unknown = authed_client.post("/ai/dashboard_story", json=_body(table_id, provider="imaginary"))
    assert unknown.status_code == 404
    bad_chart = _body(table_id)
    bad_chart["charts"][0]["chart"] = {"mark": "bar", "surprise": 1}
    assert authed_client.post("/ai/dashboard_story", json=bad_chart).status_code == 422
    assert (provider.calls, compute.payloads, compute.sql) == ([], [], [])


def test_route_refuses_a_table_the_caller_may_not_read(
    table_id: int, compute: FakeCompute, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    provider = ScriptedProvider(json.dumps(_story()))
    _use_provider(monkeypatch, provider)
    main.app.dependency_overrides[get_current_active_user] = lambda: PydanticUser(
        id=987654, username="stranger", is_admin=False
    )
    try:
        resp = TestClient(main.app).post("/ai/dashboard_story", json=_body(table_id))
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
    assert resp.status_code == 403
    assert (provider.calls, compute.payloads, compute.sql) == ([], [], [])


def test_route_without_charts_is_a_soft_failure(
    authed_client: TestClient, table_id: int, compute: FakeCompute, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider()
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/dashboard_story", json=_body(table_id, charts=[]))
    assert resp.status_code == 200
    assert (resp.json()["reason"], provider.calls, compute.payloads) == ("no_charts", [], [])


@pytest.mark.asyncio
async def test_evidence_past_its_budget_is_not_measured(table_id: int) -> None:
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository

    slow = FakeCompute(delay=0.5)
    with get_db_context() as db:
        table = CatalogService(SQLAlchemyCatalogRepository(db)).get_table(table_id, 1)
    charts = [StoryChart(id="c1", table_id=table_id, title="T", chart=ChartRequest(**BAR), payload=BY_REGION)]
    with patch("flowfile_core.catalog.service.trigger_visualize_query", side_effect=slow.query):
        found = await ds.gather_evidence(
            tables={table_id: table}, user=PydanticUser(id=1, username="local_user"), kpis=[], charts=charts, budget=0.1
        )
        await asyncio.sleep(0.6)
    assert [(e.id, e.kind, e.status, e.detail) for e in found] == [
        ("c1", "chart", "not_measured", "not read within 0.1 s")
    ]


def test_lazy_litellm_contract() -> None:
    module = importlib.import_module("flowfile_core.ai.dashboard_story")
    for node in ast.parse(Path(module.__file__).read_text()).body:
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n == "litellm" or n.startswith("litellm.") for n in names)


def test_openapi_schema_lists_the_route() -> None:
    assert "/ai/dashboard_story" in main.app.openapi()["paths"]
