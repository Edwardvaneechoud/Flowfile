"""Tests for ``POST /ai/plan_dashboard``: the plan's checks, the model loop and the route."""

from __future__ import annotations

import ast
import importlib
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import chart_profile, dashboard_routes, node_codegen_routes
from flowfile_core.ai import dashboard_plan as dp
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.chart_request import ColumnInfo
from flowfile_core.ai.dashboard_plan import PlanSource, check_plan, plan_dashboard
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable
from tests.ai.test_chart_request import ScriptedProvider, _scheduler

ORDERS = PlanSource(
    key="orders",
    table_id=11,
    columns=(
        ColumnInfo("order_date", "date"),
        ColumnInfo("region", "string", "dimension"),
        ColumnInfo("customer_id", "integer", "dimension"),
        ColumnInfo("revenue", "float", "measure"),
        ColumnInfo("discount", "float", "measure"),
        ColumnInfo("quantity", "integer", "measure"),
    ),
    profile_text='Table "orders": 6 columns.',
)
CUSTOMERS = PlanSource(
    key="customers",
    table_id=12,
    columns=(ColumnInfo("customer_id", "integer", "dimension"), ColumnInfo("segment", "string", "dimension")),
    profile_text='Table "customers": 2 columns.',
)
SOURCES = [ORDERS, CUSTOMERS]

TREND = {
    "source": "orders",
    "title": "Revenue by month",
    "chart": "Line chart of the sum of revenue per month of order_date.",
    "tests": "Whether revenue grows.",
}
SCATTER = {
    "source": "orders",
    "title": "Discount against quantity",
    "chart": "Scatter plot of discount against quantity.",
    "tests": "Whether discounts go with larger orders.",
    "relation": ["discount", "quantity"],
}
SEGMENTS = {
    "source": "customers",
    "title": "Customers by segment",
    "chart": "Bar chart of the count of rows by segment.",
    "tests": "How large each segment is.",
}
TOTAL = {"source": "orders", "label": "Total revenue", "field": "revenue", "agg": "sum"}
ROWS = {"source": "orders", "label": "Orders", "agg": "count"}


def _plan(checks: list[Any], kpis: list[Any] | None = None, title: str | None = "Sales") -> dict[str, Any]:
    return {"title": title, "kpis": kpis or [], "checks": checks}


# The checks.


def test_a_valid_plan_gets_core_ids_and_table_ids() -> None:
    plan, problems, warnings = check_plan(_plan([TREND, SCATTER, SEGMENTS], [TOTAL, ROWS]), SOURCES)
    assert (problems, warnings) == ([], [])
    assert plan.model_dump() == {
        "title": "Sales",
        "filters": [],
        "kpis": [
            {"id": "k1", "table_id": 11, "label": "Total revenue", "field": "revenue", "agg": "sum"},
            {"id": "k2", "table_id": 11, "label": "Orders", "field": None, "agg": "count"},
        ],
        "checks": [
            {
                "id": "c1",
                "table_id": 11,
                "title": "Revenue by month",
                "brief": "Line chart of the sum of revenue per month of order_date.",
                "purpose": "Whether revenue grows.",
                "relation": None,
            },
            {
                "id": "c2",
                "table_id": 11,
                "title": "Discount against quantity",
                "brief": "Scatter plot of discount against quantity.",
                "purpose": "Whether discounts go with larger orders.",
                "relation": {"x": "discount", "y": "quantity"},
            },
            {
                "id": "c3",
                "table_id": 12,
                "title": "Customers by segment",
                "brief": "Bar chart of the count of rows by segment.",
                "purpose": "How large each segment is.",
                "relation": None,
            },
        ],
    }


def test_unambiguous_shapes_are_repaired() -> None:
    raw = _plan(
        [{"table": "Orders", "title": "Trend", "brief": "Line of revenue per month."}],
        [
            {"source": "ORDERS", "label": "Revenue", "field": "sum(Revenue)"},
            {"source": "orders", "field": "*", "agg": "count"},
            {"source": "orders", "field": "customer_id", "aggregate": "nunique"},
            {"source": "orders", "field": "revenue", "agg": "avg"},
        ],
    )
    plan, problems, _ = check_plan(raw, SOURCES)
    assert problems == []
    assert plan.checks[0].purpose == "Trend"
    assert [(k.label, k.field, k.agg) for k in plan.kpis] == [
        ("Revenue", "revenue", "sum"),
        ("Rows", None, "count"),
        ("distinctCount of customer_id", "customer_id", "distinctCount"),
        ("mean of revenue", "revenue", "mean"),
    ]


def test_a_single_source_needs_no_source_name() -> None:
    plan, problems, _ = check_plan(_plan([{"title": "Trend", "chart": "Line of revenue."}]), [ORDERS])
    assert problems == []
    assert plan.checks[0].table_id == 11


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ({**TREND, "source": "sales"}, 'checks[0].source "sales" is not a source. Sources: "orders", "customers"'),
        ({**TREND, "title": " "}, "checks[0].title is missing: give the chart a short title."),
        ({"source": "orders", "title": "T"}, "checks[0].chart is missing"),
        ("a chart", "checks[0] must be an object"),
    ],
)
def test_a_bad_check_is_a_problem_and_no_plan(item: Any, expected: str) -> None:
    plan, problems, _ = check_plan(_plan([item]), SOURCES)
    assert plan is None
    assert len(problems) == 1 and expected in problems[0]


@pytest.mark.parametrize(
    ("kpi", "expected"),
    [
        ({"source": "orders", "field": "revenue"}, "kpis[0].agg must be one of"),
        ({"source": "orders", "agg": "sum"}, 'kpis[0].field is missing: "sum" needs a column'),
        ({"source": "orders", "field": "revnue", "agg": "sum"}, 'kpis[0].field "revnue" is not a column. Closest'),
        ({"source": "orders", "field": "region", "agg": "mean"}, '"mean" needs a number column, and "region" is text'),
        ({"source": "nowhere", "agg": "count"}, 'kpis[0].source "nowhere" is not a source'),
    ],
)
def test_a_bad_kpi_is_a_problem_and_the_checks_stay(kpi: Any, expected: str) -> None:
    plan, problems, _ = check_plan(_plan([TREND], [kpi, ROWS]), SOURCES)
    assert [k.id for k in plan.kpis] == ["k1"] and plan.kpis[0].agg == "count"
    assert len(problems) == 1 and expected in problems[0]


@pytest.mark.parametrize(
    "relation",
    [["discount", "region"], ["discount", "discount"], ["discount", "nope"], ["discount"], "discount vs quantity"],
)
def test_a_relation_that_cannot_be_measured_is_dropped_with_a_warning(relation: Any) -> None:
    plan, problems, warnings = check_plan(_plan([{**SCATTER, "relation": relation}]), SOURCES)
    assert problems == []
    assert plan.checks[0].relation is None
    assert len(warnings) == 1 and "it is not measured" in warnings[0]


def test_a_relation_given_as_an_object_and_in_another_case_is_kept() -> None:
    plan, _, warnings = check_plan(_plan([{**SCATTER, "relation": {"x": "Discount", "y": "QUANTITY"}}]), SOURCES)
    assert warnings == []
    assert plan.checks[0].relation.model_dump() == {"x": "discount", "y": "quantity"}


def test_items_over_the_caps_are_trimmed_with_a_warning() -> None:
    checks = [{**TREND, "title": f"Chart {i}"} for i in range(dp.MAX_CHECKS + 2)]
    plan, problems, warnings = check_plan(_plan(checks, [ROWS] * (dp.MAX_KPIS + 1)), SOURCES)
    assert problems == []
    assert (len(plan.checks), len(plan.kpis)) == (dp.MAX_CHECKS, dp.MAX_KPIS)
    assert warnings == [
        f"The plan had {dp.MAX_CHECKS + 2} charts; the first {dp.MAX_CHECKS} were kept.",
        f"The plan had {dp.MAX_KPIS + 1} KPIs; the first {dp.MAX_KPIS} were kept.",
    ]


def test_filters_take_their_kind_from_the_column() -> None:
    raw = {
        **_plan([TREND, SEGMENTS]),
        "filters": [
            {"source": "orders", "field": "Order_Date", "label": "Order date"},
            {"source": "orders", "field": "region"},
            {"source": "orders", "field": "quantity"},
            {"source": "orders", "column": "customer_id", "kind": "categorical"},
        ],
    }
    plan, problems, warnings = check_plan(raw, SOURCES)
    assert problems == []
    assert [f.model_dump() for f in plan.filters] == [
        {"id": "f1", "table_id": 11, "field": "order_date", "kind": "date_range", "label": "Order date"},
        {"id": "f2", "table_id": 11, "field": "region", "kind": "categorical", "label": None},
        {"id": "f3", "table_id": 11, "field": "quantity", "kind": "numeric_range", "label": None},
    ]
    assert warnings == [f"The plan had 4 filters; the first {dp.MAX_FILTERS} were kept."]


def test_a_filter_kind_the_column_cannot_take_becomes_the_columns_own() -> None:
    raw = {**_plan([TREND]), "filters": [{"source": "orders", "field": "region", "kind": "date_range"}, "revenue"]}
    plan, problems, _ = check_plan(raw, [ORDERS])
    assert problems == []
    assert [(f.field, f.kind) for f in plan.filters] == [("region", "categorical"), ("revenue", "numeric_range")]


def test_a_repeated_filter_is_kept_once_and_one_nothing_reads_is_left_out() -> None:
    raw = {
        **_plan([TREND]),
        "filters": [
            {"source": "orders", "field": "region"},
            {"source": "orders", "field": "REGION"},
            {"source": "customers", "field": "segment"},
        ],
    }
    plan, problems, warnings = check_plan(raw, SOURCES)
    assert problems == []
    assert [(f.table_id, f.field) for f in plan.filters] == [(11, "region")]
    assert warnings == ['The filter on "segment" was left out: nothing on the page reads "customers".']


@pytest.mark.parametrize(
    ("item", "expected"),
    [
        ({"source": "orders", "field": "regoin"}, 'filters[0].field "regoin" is not a column. Closest: "region".'),
        ({"source": "orders"}, "filters[0].field is missing"),
        ({"source": "nowhere", "field": "region"}, 'filters[0].source "nowhere" is not a source'),
        (7, "filters[0] must be an object"),
    ],
)
def test_a_bad_filter_is_a_problem_and_the_rest_of_the_plan_stays(item: Any, expected: str) -> None:
    plan, problems, _ = check_plan({**_plan([TREND], [ROWS]), "filters": [item]}, SOURCES)
    assert (len(plan.checks), len(plan.kpis), plan.filters) == (1, 1, [])
    assert len(problems) == 1 and expected in problems[0]


def test_a_column_that_cannot_be_charted_cannot_be_filtered_by() -> None:
    blobs = PlanSource(
        key="docs",
        table_id=5,
        columns=(ColumnInfo("title", "string"), ColumnInfo("payload", "other")),
        profile_text="",
    )
    check = {"title": "Docs", "chart": "Bar of the count of rows by title."}
    plan, problems, _ = check_plan({**_plan([check]), "filters": [{"field": "payload"}]}, [blobs])
    assert plan.filters == []
    assert problems == ['filters[0]: "payload" cannot be filtered by; pick a text, number or date column.']


@pytest.mark.parametrize("raw", [[], "plan", {"title": "x"}, {"title": "x", "checks": []}])
def test_an_answer_without_checks_is_refused(raw: Any) -> None:
    plan, problems, _ = check_plan(raw, SOURCES)
    assert plan is None and len(problems) == 1


# The prompt's parts.


def test_source_keys_are_distinct() -> None:
    assert dp.source_keys(["sales", "Sales", "orders", "sales"]) == ["sales", "Sales_2", "orders", "sales_3"]


def test_possible_links_come_from_shared_names_and_types() -> None:
    assert dp.possible_links(SOURCES) == ['- "customer_id" (a whole number) is in "orders" and "customers".']


def test_a_wide_profile_keeps_names_and_types_of_the_columns_past_the_limit() -> None:
    lines = ['Table "wide": 3 rows, 4 columns.', "Columns (name: type, role; profile):"]
    lines += [f'- "c{i}": float, measure; 3 non-null, 3 distinct; min 1, max 9' for i in range(4)]
    text = "\n".join(lines)
    assert dp.compact_profile(text, detailed=4) == text
    assert dp.compact_profile(text, detailed=2) == "\n".join(
        [*lines[:4], 'More columns (name: type, role): "c2": float, measure | "c3": float, measure']
    )


def test_the_user_turn_lists_sources_links_and_the_question() -> None:
    text = dp.user_message(sources=SOURCES, prompt="How are sales doing?")
    assert text.index('### Source "orders"') < text.index('### Source "customers"') < text.index("## Possible links")
    assert "## Question\n\nHow are sales doing?" in text
    assert "## Possible links" not in dp.user_message(sources=[ORDERS], prompt="x")


def test_every_example_in_the_prompt_passes_the_checks() -> None:
    """The prompt's own answers are the contract the model copies: each must be accepted as written."""
    prompt = dp.system_prompt()
    orders = PlanSource(
        key="orders",
        table_id=1,
        columns=(
            ColumnInfo("order_date", "date"),
            ColumnInfo("region", "string"),
            ColumnInfo("category", "string"),
            ColumnInfo("revenue", "float"),
            ColumnInfo("quantity", "integer"),
            ColumnInfo("discount", "float"),
        ),
        profile_text="",
    )
    customers = PlanSource(
        key="customers",
        table_id=2,
        columns=(
            ColumnInfo("customer_id", "integer"),
            ColumnInfo("country", "string"),
            ColumnInfo("segment", "string"),
            ColumnInfo("signup_date", "date"),
        ),
        profile_text="",
    )
    tickets = PlanSource(
        key="tickets",
        table_id=3,
        columns=(
            ColumnInfo("ticket_id", "integer"),
            ColumnInfo("customer_id", "integer"),
            ColumnInfo("opened_at", "datetime"),
            ColumnInfo("priority", "string"),
            ColumnInfo("hours_to_resolve", "float"),
        ),
        profile_text="",
    )
    stores = PlanSource(
        key="stores",
        table_id=4,
        columns=(
            ColumnInfo("store_id", "integer"),
            ColumnInfo("city", "string"),
            ColumnInfo("lat", "float"),
            ColumnInfo("lon", "float"),
            ColumnInfo("revenue", "float"),
            ColumnInfo("warehouse_lat", "float"),
            ColumnInfo("warehouse_lon", "float"),
            ColumnInfo("km_to_warehouse", "float"),
        ),
        profile_text="",
    )
    decoder = json.JSONDecoder()
    answers = []
    for marker in ("Example 1.", "Example 2.", "Example 3.", "Example 4.", "Example 5."):
        start = prompt.index('{"title"', prompt.index(marker))
        answers.append(decoder.raw_decode(prompt[start:])[0])
    for answer, sources in zip(answers[:2], ([orders], [customers, tickets]), strict=True):
        plan, problems, warnings = check_plan(answer, sources)
        assert (problems, warnings) == ([], [])
        assert len(plan.checks) == 4
    assert dp.read_answer(json.dumps(answers[2]), [orders]).reason == "no_plan"
    plan, problems, warnings = check_plan(answers[3], [orders])
    assert (problems, warnings) == ([], [])
    assert [(f.field, f.kind, f.label) for f in plan.filters] == [
        ("order_date", "date_range", "Order date"),
        ("region", "categorical", "Region"),
    ]
    plan, problems, warnings = check_plan(answers[4], [stores])
    assert (problems, warnings) == ([], [])
    assert len(plan.checks) == 3


# The model loop, with a scripted provider.


async def _run(provider: ScriptedProvider, **kwargs: Any):
    return await plan_dashboard(
        prompt=kwargs.pop("prompt", "How are sales doing?"),
        sources=SOURCES,
        provider=provider,
        user_id=7,
        scheduler=_scheduler(),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_a_valid_plan_is_returned_in_json_mode_on_the_dashboard_surface() -> None:
    provider = ScriptedProvider(json.dumps(_plan([TREND, SEGMENTS], [TOTAL])))
    resp = await _run(provider)
    assert (resp.degraded, resp.attempts, resp.warnings) == (False, 1, [])
    assert [c.id for c in resp.plan.checks] == ["c1", "c2"]
    [call] = provider.calls
    assert call["response_format"] == {"type": "json_object"}
    assert (call["surface"], call["user_id"]) == ("dashboard", 7)
    assert [m.role for m in call["messages"]] == ["system", "user"]


@pytest.mark.asyncio
async def test_problems_go_back_once_and_the_fixed_plan_is_taken() -> None:
    bad = json.dumps(_plan([TREND, {**SEGMENTS, "source": "segments"}]))
    provider = ScriptedProvider(bad, json.dumps(_plan([TREND, SEGMENTS])))
    resp = await _run(provider)
    assert (resp.degraded, resp.attempts, resp.warnings) == (False, 2, [])
    assert len(resp.plan.checks) == 2
    retry = provider.calls[1]["messages"]
    assert [m.role for m in retry] == ["system", "user", "assistant", "user"]
    assert retry[2].content == bad
    assert retry[3].content.startswith('The plan has 1 problem:\n- checks[1].source "segments" is not a source.')


@pytest.mark.asyncio
async def test_the_last_answer_keeps_its_valid_items_and_names_the_rest() -> None:
    bad = json.dumps(_plan([TREND, {**SEGMENTS, "source": "segments"}]))
    resp = await _run(ScriptedProvider(bad, bad))
    assert (resp.degraded, resp.attempts) == (False, 2)
    assert [c.title for c in resp.plan.checks] == ["Revenue by month"]
    assert resp.warnings == [
        'Left out of the plan: checks[1].source "segments" is not a source. Sources: "orders", "customers"'
    ]


@pytest.mark.asyncio
async def test_a_plan_with_no_valid_check_after_the_retry_is_degraded() -> None:
    bad = json.dumps(_plan([{**TREND, "source": "nowhere"}]))
    resp = await _run(ScriptedProvider(bad, bad))
    assert (resp.degraded, resp.reason, resp.attempts, resp.plan) == (True, "invalid_plan", 2, None)
    assert resp.message.startswith("The plan has 1 problem:")


@pytest.mark.asyncio
async def test_a_failed_retry_falls_back_to_the_first_answers_valid_items() -> None:
    bad = json.dumps(_plan([TREND, {**SEGMENTS, "source": "segments"}]))
    resp = await _run(ScriptedProvider(bad, "not json"))
    assert (resp.degraded, resp.attempts) == (False, 2)
    assert [c.title for c in resp.plan.checks] == ["Revenue by month"]


@pytest.mark.asyncio
async def test_a_decline_is_not_retried() -> None:
    decline = json.dumps({"title": None, "kpis": [], "checks": [], "explanation": "The tables hold sales."})
    provider = ScriptedProvider(decline)
    resp = await _run(provider)
    assert (resp.degraded, resp.reason, resp.explanation) == (True, "no_plan", "The tables hold sales.")
    assert len(provider.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("provider", "reason"),
    [
        (ScriptedProvider("nope", "still nope"), "parse_error"),
        (ScriptedProvider("", ""), "empty_response"),
        (ScriptedProvider(error=RuntimeError("boom")), "provider_error"),
        (ScriptedProvider("x", delay=0.2), "timeout"),
    ],
)
async def test_soft_failures_carry_their_reason(provider: ScriptedProvider, reason: str) -> None:
    resp = await _run(provider, timeout=0.05 if reason == "timeout" else 5.0)
    assert (resp.degraded, resp.reason, resp.plan) == (True, reason, None)
    assert reason in dp.REASONS


@pytest.mark.asyncio
async def test_a_blank_prompt_and_unchartable_sources_make_no_call() -> None:
    provider = ScriptedProvider()
    assert (await _run(provider, prompt="  ")).reason == "empty_prompt"
    empty = PlanSource(key="blobs", table_id=1, columns=(ColumnInfo("payload", "other"),), profile_text="")
    resp = await plan_dashboard(prompt="x", sources=[empty], provider=provider, scheduler=_scheduler())
    assert resp.reason == "no_columns"
    assert provider.calls == []


# The route.


@pytest.fixture(autouse=True)
def _fresh_profile_cache() -> Iterator[None]:
    chart_profile.clear_cache()
    yield
    chart_profile.clear_cache()


SCHEMAS = {
    "plan_orders": [
        {"name": "region", "dtype": "String"},
        {"name": "revenue", "dtype": "Float64"},
        {"name": "customer_id", "dtype": "Int64"},
    ],
    "plan_customers": [{"name": "customer_id", "dtype": "Int64"}, {"name": "segment", "dtype": "String"}],
}


@pytest.fixture
def table_ids() -> Iterator[dict[str, int]]:
    with get_db_context() as db:
        ns = CatalogNamespace(name="DashboardPlanNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        ids: dict[str, int] = {}
        for name, schema in SCHEMAS.items():
            table = CatalogTable(
                name=name,
                namespace_id=ns.id,
                owner_id=1,
                file_path=f"/tmp/flowfile-test/{name}",
                storage_format="delta",
                schema_json=json.dumps(schema),
                table_type="physical",
            )
            db.add(table)
            db.commit()
            db.refresh(table)
            ids[name] = table.id
        ns_id = ns.id
    yield ids
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id.in_(ids.values())).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id == ns_id).delete()
        db.commit()


def _fake_fields(worker_source, *, worker_url):
    names = {c["name"] for schema in SCHEMAS.values() for c in schema}
    return {"fields": [{"fid": n, "analyticType": "measure" if n == "revenue" else "dimension"} for n in names]}


def _fake_query(worker_source, payload, max_rows, *, worker_url):
    query = payload["workflow"][0]["query"][0]
    if query["op"] == "raw":
        rows = [dict.fromkeys(query["fields"], "Zebra")]
    elif query["groupBy"]:
        rows = [{query["groupBy"][0]: "Zebra"}]
    else:
        rows = [{m["asFieldKey"]: 3 for m in query["measures"]}]
    return {"rows": rows, "total_rows": len(rows), "truncated": False, "elapsed_ms": 1.0, "cache_hit": False}


@pytest.fixture
def worker() -> Iterator[None]:
    with (
        patch("flowfile_core.catalog.service.trigger_visualize_fields", side_effect=_fake_fields),
        patch("flowfile_core.catalog.service.trigger_visualize_query", side_effect=_fake_query),
    ):
        yield


@pytest.fixture
def authed_client() -> Iterator[TestClient]:
    main.app.dependency_overrides[get_current_active_user] = lambda: PydanticUser(id=1, username="local_user")
    try:
        yield TestClient(main.app)
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def _use_provider(monkeypatch: pytest.MonkeyPatch, provider: ScriptedProvider) -> None:
    monkeypatch.setattr(dashboard_routes, "_resolve_provider", lambda *_a, **_kw: provider)


def _route_plan() -> str:
    return json.dumps(
        _plan(
            [
                {
                    "source": "plan_orders",
                    "title": "Revenue by region",
                    "chart": "Bar of the sum of revenue by region.",
                    "tests": "Which regions lead.",
                },
                {
                    "source": "plan_customers",
                    "title": "Customers by segment",
                    "chart": "Bar of the count of rows by segment.",
                    "tests": "How large each segment is.",
                },
            ],
            [{"source": "plan_orders", "label": "Revenue", "field": "revenue", "agg": "sum"}],
        )
        | {"filters": [{"source": "plan_orders", "field": "region", "label": "Region"}]}
    )


def test_route_plans_over_every_selected_table(
    authed_client: TestClient, table_ids: dict[str, int], worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_route_plan())
    _use_provider(monkeypatch, provider)
    orders, customers = table_ids["plan_orders"], table_ids["plan_customers"]
    resp = authed_client.post(
        "/ai/plan_dashboard", json={"table_ids": [orders, customers, orders], "prompt": "How are sales doing?"}
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert (body["degraded"], body["warnings"], body["attempts"]) == (False, [], 1)
    assert (body["provider"], body["model"]) == ("fake", "fake-model")
    assert [(c["id"], c["table_id"]) for c in body["plan"]["checks"]] == [("c1", orders), ("c2", customers)]
    assert body["plan"]["kpis"] == [
        {"id": "k1", "table_id": orders, "label": "Revenue", "field": "revenue", "agg": "sum"}
    ]
    assert body["plan"]["filters"] == [
        {"id": "f1", "table_id": orders, "field": "region", "kind": "categorical", "label": "Region"}
    ]
    user = provider.calls[0]["messages"][1].content
    assert user.count("### Source ") == 2
    assert 'Table "plan_orders": 3 rows, 3 columns.' in user
    assert '- "customer_id" (a whole number) is in "plan_orders" and "plan_customers".' in user
    assert "Zebra" in user


@pytest.mark.parametrize(("send_values", "env"), [(False, "true"), (True, "false")])
def test_route_sends_no_data_values_when_the_user_or_the_server_says_no(
    authed_client: TestClient,
    table_ids: dict[str, int],
    worker: None,
    monkeypatch: pytest.MonkeyPatch,
    send_values: bool,
    env: str,
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_SEND_DATA", env)
    provider = ScriptedProvider(_route_plan())
    _use_provider(monkeypatch, provider)
    resp = authed_client.post(
        "/ai/plan_dashboard",
        json={"table_ids": list(table_ids.values()), "prompt": "How are sales doing?", "send_values": send_values},
    )
    assert resp.status_code == 200, resp.text
    user = provider.calls[0]["messages"][1].content
    assert user.count("Data values are not shared for this table") == 2
    assert "Zebra" not in user and "distinct" not in user


def test_route_warns_per_table_when_a_profile_fails(
    authed_client: TestClient, table_ids: dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_route_plan())
    _use_provider(monkeypatch, provider)
    with patch("flowfile_core.catalog.service.trigger_visualize_fields", side_effect=RuntimeError("worker down")):
        resp = authed_client.post(
            "/ai/plan_dashboard", json={"table_ids": list(table_ids.values()), "prompt": "How are sales doing?"}
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["degraded"] is False
    assert [w.split(":")[0] for w in body["warnings"]] == ['"plan_orders"', '"plan_customers"']


def test_route_refuses_before_any_model_call(
    authed_client: TestClient, table_ids: dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider()
    _use_provider(monkeypatch, provider)
    known = table_ids["plan_orders"]
    missing = authed_client.post("/ai/plan_dashboard", json={"table_ids": [known, 987654321], "prompt": "x"})
    assert (missing.status_code, missing.json()["detail"]) == (404, "Catalog table not found")
    unknown = authed_client.post(
        "/ai/plan_dashboard", json={"table_ids": [known], "prompt": "x", "provider": "imaginary"}
    )
    assert unknown.status_code == 404
    for body in (
        {"table_ids": [], "prompt": "x"},
        {"table_ids": list(range(1, dp.MAX_SOURCES + 2)), "prompt": "x"},
        {"table_ids": [known], "prompt": ""},
        {"table_ids": [known], "prompt": "x" * 2001},
    ):
        assert authed_client.post("/ai/plan_dashboard", json=body).status_code == 422
    assert provider.calls == []


def test_route_refuses_a_table_the_caller_may_not_read(
    table_ids: dict[str, int], worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    provider = ScriptedProvider(_route_plan())
    _use_provider(monkeypatch, provider)
    stranger = PydanticUser(id=987654, username="stranger", is_admin=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: stranger
    try:
        resp = TestClient(main.app).post(
            "/ai/plan_dashboard", json={"table_ids": list(table_ids.values()), "prompt": "x"}
        )
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
    assert resp.status_code == 403
    assert provider.calls == []


def test_route_409_without_a_provider_and_503_when_ai_is_off(
    authed_client: TestClient, table_ids: dict[str, int], monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    body = {"table_ids": list(table_ids.values()), "prompt": "x"}
    monkeypatch.setattr(node_codegen_routes, "get_configured_provider", _raise)
    assert authed_client.post("/ai/plan_dashboard", json=body).status_code == 409
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        assert authed_client.post("/ai/plan_dashboard", json=body).status_code == 503
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)


@pytest.mark.parametrize("module_name", ["flowfile_core.ai.dashboard_plan", "flowfile_core.ai.dashboard_routes"])
def test_lazy_litellm_contract(module_name: str) -> None:
    module = importlib.import_module(module_name)
    for node in ast.parse(Path(module.__file__).read_text()).body:
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n == "litellm" or n.startswith("litellm.") for n in names), module_name
    if not any(name == "litellm" or name.startswith("litellm.") for name in sys.modules):
        sys.modules.pop(module_name, None)
        importlib.import_module(module_name)
        assert "litellm" not in sys.modules


def test_openapi_schema_lists_the_route() -> None:
    assert "/ai/plan_dashboard" in main.app.openapi()["paths"]
