"""Tests for ``POST /ai/generate_chart``: the chart request rules, the engine and the route.

The model loop with a scripted provider (valid, repaired, retried, given up, timeout, provider
error, unparsable, follow-ups), the route's status codes and access refusal, the prompt log
surface and the lazy-litellm contract. The rules' verdicts are in ``test_chart_request_cases.py``.
"""

from __future__ import annotations

import ast
import asyncio
import importlib
import json
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import chart_profile, chart_routes, node_codegen_routes
from flowfile_core.ai import chart_request as cr
from flowfile_core.ai.byok import ProviderNotConfiguredError
from flowfile_core.ai.chart_request import (
    ColumnInfo,
    generate_chart,
    review_chart,
)
from flowfile_core.ai.providers.base import ChatResponse
from flowfile_core.ai.scheduler import RateLimitScheduler
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.configs import settings as core_settings
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import CatalogNamespace, CatalogTable

COLUMNS = [
    ColumnInfo("order_date", "date"),
    ColumnInfo("created_at", "datetime"),
    ColumnInfo("date_str", "string"),
    ColumnInfo("region", "string", "dimension", ("East", "North", "South", "West")),
    ColumnInfo("category", "string", "dimension"),
    ColumnInfo("revenue", "float", "measure"),
    ColumnInfo("price", "float", "measure"),
    ColumnInfo("quantity", "integer", "dimension"),
    ColumnInfo("is_active", "boolean", "dimension"),
    ColumnInfo("tags", "other"),
]

BAR = {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}}


# The model loop, with a scripted provider.


class ScriptedProvider:
    """Answers each ``chat`` with the next reply (or raises/sleeps) and records every call."""

    name = "fake"
    model = "fake-model"

    def __init__(self, *replies: str, delay: float = 0.0, error: BaseException | None = None) -> None:
        self.replies = list(replies)
        self.delay = delay
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def chat(self, messages, tools=None, max_tokens=None, response_format=None, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append({"messages": list(messages), "response_format": response_format, **kwargs})
        if self.error is not None:
            raise self.error
        if self.delay:
            await asyncio.sleep(self.delay)
        return ChatResponse(model=self.model, content=self.replies.pop(0))


def _scheduler() -> RateLimitScheduler:
    return RateLimitScheduler(time_source=lambda: 0.0, sleep=lambda *_a, **_k: asyncio.sleep(0))


def _answer(chart: Any, title: str = "Revenue by region", explanation: str = "Total per region.") -> str:
    return json.dumps({"chart": chart, "title": title, "explanation": explanation})


async def _generate(provider: ScriptedProvider, **kwargs: Any):
    return await generate_chart(
        prompt=kwargs.pop("prompt", "revenue by region"),
        columns=COLUMNS,
        profile_text='Table "sales": 10 columns.',
        provider=provider,
        user_id=7,
        scheduler=_scheduler(),
        **kwargs,
    )


@pytest.mark.asyncio
async def test_valid_answer_is_returned_with_json_mode_and_the_surface() -> None:
    provider = ScriptedProvider(_answer(BAR))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert resp.chart.model_dump() == BAR
    assert resp.title == "Revenue by region"
    assert resp.explanation == "Total per region."
    [call] = provider.calls
    assert call["response_format"] == {"type": "json_object"}
    assert call["surface"] == "chart"
    assert call["user_id"] == 7
    assert (resp.attempts, resp.repairs) == (1, [])
    assert [m.role for m in call["messages"]] == ["system", "user"]
    assert "## Request\n\nrevenue by region" in call["messages"][1].content


@pytest.mark.asyncio
async def test_repairable_answer_is_repaired_without_a_retry() -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "Region", "y": "sum(revenue)"}))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert resp.chart.model_dump() == BAR
    assert len(provider.calls) == 1
    assert (resp.attempts, resp.repairs) == (1, ["shorthand", "column_case"])


@pytest.mark.asyncio
async def test_rejected_answer_is_fixed_by_the_retry() -> None:
    bad = _answer({**BAR, "limit": 5})
    provider = ScriptedProvider(bad, _answer({**BAR, "sort": "descending", "limit": 5}))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert resp.chart.model_dump() == {**BAR, "sort": "descending", "limit": 5}
    assert (resp.attempts, resp.repairs) == (2, [])
    second = provider.calls[1]["messages"]
    assert [m.role for m in second] == ["system", "user", "assistant", "user"]
    assert second[2].content == bad
    assert 'limit needs sort: "descending" for the top 5' in second[3].content


@pytest.mark.asyncio
async def test_unparsable_first_answer_is_retried() -> None:
    provider = ScriptedProvider("Sure! Here is a chart.", _answer(BAR))
    resp = await _generate(provider)
    assert resp.degraded is False
    assert "not valid JSON" in provider.calls[1]["messages"][3].content


@pytest.mark.asyncio
async def test_answer_with_prose_around_the_object_is_parsed() -> None:
    provider = ScriptedProvider("```json\n" + _answer(BAR) + "\n```")
    assert (await _generate(provider)).chart.model_dump() == BAR
    provider = ScriptedProvider("Here you go: " + _answer(BAR) + " Enjoy.")
    assert (await _generate(provider)).chart.model_dump() == BAR


@pytest.mark.asyncio
async def test_still_failing_after_the_retry_gives_up_with_the_message() -> None:
    bad = _answer({**BAR, "limit": 5})
    provider = ScriptedProvider(bad, bad)
    resp = await _generate(provider)
    assert resp.degraded is True
    assert resp.reason == "invalid_chart"
    assert resp.chart is None
    assert resp.message.startswith("The chart request has 1 problem:")
    assert len(provider.calls) == 2
    assert (resp.attempts, resp.repairs) == (2, [])


@pytest.mark.asyncio
async def test_repairs_are_those_of_the_accepted_answer_only() -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "Region", "y": "revenue", "limit": 5}), _answer(BAR))
    resp = await _generate(provider)
    assert (resp.attempts, resp.repairs) == (2, [])


@pytest.mark.asyncio
async def test_unparsable_twice_is_a_parse_error() -> None:
    resp = await _generate(ScriptedProvider("no json", "still none"))
    assert (resp.degraded, resp.reason) == (True, "parse_error")


@pytest.mark.asyncio
async def test_empty_answer_twice_is_an_empty_response() -> None:
    resp = await _generate(ScriptedProvider("", " "))
    assert (resp.degraded, resp.reason) == (True, "empty_response")


@pytest.mark.asyncio
async def test_answer_without_a_chart_key_twice_is_a_validation_error() -> None:
    resp = await _generate(ScriptedProvider('{"foo": 1}', "[1, 2]"))
    assert (resp.degraded, resp.reason) == (True, "validation_error")


@pytest.mark.asyncio
async def test_declined_request_is_no_chart_without_a_retry() -> None:
    provider = ScriptedProvider(json.dumps({"chart": None, "title": None, "explanation": "This table has no weather."}))
    resp = await _generate(provider, prompt="weather tomorrow")
    assert (resp.degraded, resp.reason) == (True, "no_chart")
    assert resp.explanation == "This table has no weather."
    assert len(provider.calls) == 1
    assert (resp.attempts, resp.repairs) == (1, [])


@pytest.mark.asyncio
async def test_timeout_is_degraded() -> None:
    resp = await _generate(ScriptedProvider(_answer(BAR), delay=1.0), timeout=0.05)
    assert (resp.degraded, resp.reason) == (True, "timeout")
    assert resp.attempts == 1


@pytest.mark.asyncio
async def test_provider_error_is_degraded_without_its_text() -> None:
    resp = await _generate(ScriptedProvider(error=RuntimeError("secret-key-sk-123 rejected")))
    assert (resp.degraded, resp.reason) == (True, "provider_error")
    assert resp.message == "The AI provider call failed (RuntimeError)."
    assert resp.attempts == 1


@pytest.mark.asyncio
async def test_empty_prompt_is_degraded_without_a_call() -> None:
    provider = ScriptedProvider()
    resp = await _generate(provider, prompt="   ")
    assert (resp.degraded, resp.reason) == (True, "empty_prompt")
    assert provider.calls == []
    assert (resp.attempts, resp.repairs) == (0, [])


@pytest.mark.asyncio
async def test_follow_up_sends_the_valid_current_chart() -> None:
    line = {**BAR, "mark": "line", "x": {"field": "order_date", "timeUnit": "month"}}
    current = {**line, "mark": "bar"}
    provider = ScriptedProvider(_answer(line, title="Monthly revenue"))
    resp = await _generate(provider, prompt="make it a line", current_chart=current)
    assert resp.degraded is False
    assert resp.warnings == []
    assert resp.chart.model_dump() == line
    assert resp.repairs == []
    user = provider.calls[0]["messages"][1].content
    assert "## Current chart\n\n" + json.dumps(current) in user


@pytest.mark.asyncio
async def test_follow_up_with_an_invalid_current_chart_goes_on_without_it() -> None:
    provider = ScriptedProvider(_answer(BAR))
    resp = await _generate(provider, prompt="only 2025", current_chart={**BAR, "x": {"field": "nope"}})
    assert resp.degraded is False
    [warning] = resp.warnings
    assert warning.startswith("The current chart could not be used for a change (")
    assert 'x.field "nope" is not a column.' in warning
    assert "## Current chart\n\nNone: build a new chart." in provider.calls[0]["messages"][1].content


@pytest.mark.asyncio
async def test_follow_up_with_an_oversized_current_chart_goes_on_without_it() -> None:
    values = ["v" * 40] * (cr.MAX_CURRENT_CHART_CHARS // 40)
    current = {**BAR, "filters": [{"field": "region", "oneOf": values}]}
    provider = ScriptedProvider(_answer(BAR))
    with patch.object(cr, "review_chart", wraps=cr.review_chart) as review:
        resp = await _generate(provider, prompt="only 2025", current_chart=current)
    assert [call.args[0] for call in review.call_args_list] == [BAR]
    assert resp.degraded is False
    assert resp.warnings == [
        f"The current chart could not be used for a change (It is larger than {cr.MAX_CURRENT_CHART_CHARS} "
        "characters.); the chart was built from the prompt alone."
    ]
    assert "## Current chart\n\nNone: build a new chart." in provider.calls[0]["messages"][1].content


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("sum(revenue)", {"field": "revenue", "aggregate": "sum"}),
        (" Sum ( revenue ) ", {"field": "revenue", "aggregate": "sum"}),
        ("sum(\n  revenue\n)", {"field": "revenue", "aggregate": "sum"}),
        ("avg('price')", {"field": "price", "aggregate": "mean"}),
        ("sum(`revenue`)", {"field": "revenue", "aggregate": "sum"}),
        ("count(*)", {"field": "*", "aggregate": "count"}),
        ("count()", {"field": "*", "aggregate": "count"}),
        ("sum()", {"field": "sum()"}),
        ("nope(revenue)", {"field": "nope(revenue)"}),
        ("sum(revenue) per region", {"field": "sum(revenue) per region"}),
        (" revenue ", {"field": "revenue"}),
    ],
)
def test_shorthand_reference_forms(text: str, expected: dict[str, str]) -> None:
    assert cr.ref_from_string(text, [c.name for c in COLUMNS]) == expected


def test_a_column_named_like_a_shorthand_wins() -> None:
    assert cr.ref_from_string("sum(revenue)", ["sum(revenue)", "revenue"]) == {"field": "sum(revenue)"}


_WORST_CASES = {
    "unclosed_then_spaces": "A(" + " " * 200_000,
    "closed_then_spaces": "A(" + ")" + " " * 200_000 + "x",
    "many_closing_parens": "A(" + ") " * 100_000 + "x",
    "leading_spaces": " " * 200_000 + "(",
    "spaces_inside": "sum(" + " " * 200_000 + "revenue)",
}


@pytest.mark.parametrize("case", sorted(_WORST_CASES))
def test_shorthand_parsing_is_linear_in_the_text(case: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """The pattern alone, without the length guard: the old one took minutes on a few thousand spaces.

    Parametrized by name: pytest puts the test id in an environment variable, and Windows
    refuses one longer than 32,767 characters.
    """
    text = _WORST_CASES[case]
    monkeypatch.setattr(cr, "_MAX_SHORTHAND_LEN", len(text))
    started = time.perf_counter()
    cr.ref_from_string(text, [])
    assert time.perf_counter() - started < 1.0


def test_long_text_is_never_read_as_a_shorthand() -> None:
    long = "sum(" + "x" * cr._MAX_SHORTHAND_LEN + ")"
    assert cr.ref_from_string(long, []) == {"field": long}
    fits = "sum(" + "x" * (cr._MAX_SHORTHAND_LEN - 5) + ")"
    assert cr.ref_from_string(fits, []) == {"field": "x" * (cr._MAX_SHORTHAND_LEN - 5), "aggregate": "sum"}


def test_a_filter_number_too_large_for_a_float_is_a_problem_not_an_error() -> None:
    huge = 10**400
    assert cr._coerce(huge, "float") == (False, huge)
    chart, problems = review_chart({**BAR, "filters": [{"field": "revenue", "oneOf": [huge]}]}, COLUMNS)
    assert chart is None
    assert problems == ['"revenue" holds numbers; use numbers such as 9.5.']


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("9007199254740993", (True, 9007199254740993)),
        (" 42 ", (True, 42)),
        ("3.0", (True, 3)),
        ("1e3", (True, 1000)),
        ("2.5", (False, "2.5")),
        ("many", (False, "many")),
    ],
)
def test_integer_filter_text_keeps_every_digit(value: str, expected: tuple[bool, Any]) -> None:
    assert cr._coerce(value, "integer") == expected


def test_sort_is_checked_on_the_axis_the_browser_sorts() -> None:
    month = {"field": "order_date", "timeUnit": "month"}
    heat = {"mark": "rect", "color": {"field": "revenue", "aggregate": "sum"}, "sort": "descending"}
    chart, problems = review_chart({**heat, "x": month, "y": {"field": "region"}}, COLUMNS)
    assert problems == [] and chart is not None
    chart, problems = review_chart({**heat, "x": {"field": "region"}, "y": month}, COLUMNS)
    assert chart is None
    assert problems == ["A time or number axis is always in order; remove sort and limit, or use dateRange."]


_GEO_COLUMNS = [*COLUMNS, ColumnInfo("lat", "float"), ColumnInfo("lon", "float")]
_COORDINATES = {"latitude": {"field": "lat"}, "longitude": {"field": "lon"}}


def test_a_map_plots_raw_rows_and_drops_aggregates() -> None:
    repairs: list[str] = []
    raw = {
        "mark": "map",
        **_COORDINATES,
        "size": {"field": "revenue", "aggregate": "sum"},
        "color": {"field": "region"},
    }
    chart, problems = review_chart(raw, _GEO_COLUMNS, repairs)
    assert problems == [] and chart is not None
    assert chart.aggregate is False
    assert chart.size == cr.FieldRef(field="revenue")
    assert repairs == ["map_raw"]


def test_coordinates_alone_on_another_mark_make_a_map() -> None:
    repairs: list[str] = []
    chart, problems = review_chart({"mark": "point", **_COORDINATES, "aggregate": False}, _GEO_COLUMNS, repairs)
    assert problems == [] and chart is not None
    assert chart.mark == "map"
    assert repairs == ["map_mark"]


def test_coordinates_beside_axes_are_dropped() -> None:
    repairs: list[str] = []
    chart, problems = review_chart({**BAR, **_COORDINATES}, _GEO_COLUMNS, repairs)
    assert problems == [] and chart is not None
    assert (chart.mark, chart.latitude, chart.longitude) == ("bar", None, None)
    assert repairs == ["geo_dropped"]


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"longitude": None}, "A map takes latitude and longitude"),
        ({"latitude": {"field": "region"}}, "A map takes latitude and longitude"),
        ({"latitude": {"field": "lat", "bin": 10}}, "A map takes latitude and longitude"),
        ({"x": {"field": "lon"}}, "A map takes latitude and longitude"),
        ({"size": {"field": "region"}}, "On a map, size takes one plain number column."),
        ({"sort": "descending"}, "A map shows every row"),
        ({"limit": 5}, "A map shows every row"),
    ],
)
def test_a_map_that_cannot_be_drawn_says_why(change: dict[str, Any], problem: str) -> None:
    raw = {k: v for k, v in {"mark": "map", **_COORDINATES, "aggregate": False, **change}.items() if v is not None}
    chart, problems = review_chart(raw, _GEO_COLUMNS)
    assert chart is None
    assert any(p.startswith(problem) for p in problems), problems


def test_reason_vocabulary_is_stable() -> None:
    assert cr.REASONS == (
        "empty_prompt",
        "no_columns",
        "timeout",
        "provider_error",
        "empty_response",
        "parse_error",
        "validation_error",
        "no_chart",
        "invalid_chart",
    )


def test_system_prompt_carries_the_worked_examples() -> None:
    prompt = cr.system_prompt()
    for request in (
        "revenue by region",
        "monthly revenue trend",
        "top 5 products by revenue",
        "share of revenue per category as a pie",
        "price against quantity",
        "histogram of price",
        "revenue by category in the West during 2024",
        "make it a line",
        "only 2025",
        "where are the orders, on a map sized by revenue",
    ):
        assert f"Request: {request}\n" in prompt
    examples = [json.loads(block) for block in _example_answers(prompt)]
    assert len(examples) >= 9
    for example in examples:
        if example["chart"] is None:
            continue
        chart, problems = review_chart(example["chart"], _SALES_COLUMNS)
        assert problems == [], (example, problems)
        assert chart.model_dump(mode="json") == example["chart"], "a worked example must need no repair"


_SALES_COLUMNS = [
    ColumnInfo("order_date", "date"),
    ColumnInfo("region", "string", values=("East", "North", "South", "West")),
    ColumnInfo("category", "string"),
    ColumnInfo("product", "string"),
    ColumnInfo("revenue", "float"),
    ColumnInfo("quantity", "integer"),
    ColumnInfo("price", "float"),
    ColumnInfo("is_return", "boolean"),
    ColumnInfo("store_lat", "float"),
    ColumnInfo("store_lon", "float"),
]


def _example_answers(prompt: str) -> list[str]:
    """Each worked answer: the lines after a ``Request:`` line up to the blank line."""
    blocks: list[str] = []
    lines = prompt.splitlines()
    for i, line in enumerate(lines):
        if line.startswith("Request: "):
            body = []
            for nxt in lines[i + 1 :]:
                if not nxt.strip():
                    break
                body.append(nxt)
            blocks.append("\n".join(body))
    return blocks


# The route.


@pytest.fixture(autouse=True)
def _fresh_profile_cache() -> Iterator[None]:
    chart_profile.clear_cache()
    yield
    chart_profile.clear_cache()


def _make_table() -> tuple[int, int]:
    with get_db_context() as db:
        ns = CatalogNamespace(name="ChartAiNs", parent_id=None, level=0, owner_id=1)
        db.add(ns)
        db.commit()
        db.refresh(ns)
        table = CatalogTable(
            name="ai_sales",
            namespace_id=ns.id,
            owner_id=1,
            file_path="/tmp/flowfile-test/ai_sales",
            storage_format="delta",
            schema_json=json.dumps(
                [
                    {"name": "region", "dtype": "String"},
                    {"name": "revenue", "dtype": "Float64"},
                    {"name": "order_date", "dtype": "Date"},
                ]
            ),
            table_type="physical",
        )
        db.add(table)
        db.commit()
        db.refresh(table)
        return table.id, ns.id


@pytest.fixture
def table_id() -> Iterator[int]:
    tid, ns_id = _make_table()
    yield tid
    with get_db_context() as db:
        db.query(CatalogTable).filter(CatalogTable.id == tid).delete()
        db.query(CatalogNamespace).filter(CatalogNamespace.id == ns_id).delete()
        db.commit()


def _fake_fields(worker_source, *, worker_url):
    return {
        "fields": [
            {"fid": "region", "analyticType": "dimension"},
            {"fid": "revenue", "analyticType": "measure"},
            {"fid": "order_date", "analyticType": "dimension"},
        ]
    }


def _fake_query(worker_source, payload, max_rows, *, worker_url):
    query = payload["workflow"][0]["query"][0]
    if query["op"] == "raw":
        rows = [{"region": "West", "revenue": 10.5, "order_date": "2025-01-02"}]
    elif query["groupBy"]:
        rows = [{"region": v} for v in ("West", "East")]
    else:
        rows = [
            {
                "__rows": 2,
                "c0_nonnull": 2,
                "c0_distinct": 2,
                "c1_nonnull": 2,
                "c1_distinct": 2,
                "c1_min": 1.0,
                "c1_max": 10.5,
                "c2_nonnull": 2,
                "c2_distinct": 2,
                "c2_min": "2025-01-01",
                "c2_max": "2025-01-02",
            }
        ]
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
    monkeypatch.setattr(chart_routes, "_resolve_provider", lambda *_a, **_kw: provider)


def test_route_generates_a_chart_from_the_profile(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "chart": {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}},
        "title": "Revenue by region",
        "explanation": "Total per region.",
        "warnings": [],
        "degraded": False,
        "reason": None,
        "message": None,
        "attempts": 1,
        "repairs": ["shorthand"],
        "provider": "fake",
        "model": "fake-model",
    }
    user = provider.calls[0]["messages"][1].content
    assert 'Table "ai_sales": 2 rows, 3 columns.' in user
    assert '- "region": string, dimension; 2 non-null, 2 distinct; values: "East", "West"' in user


def test_route_follow_up_with_a_current_chart(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    line = {
        "mark": "line",
        "x": {"field": "order_date", "timeUnit": "month"},
        "y": {"field": "revenue", "aggregate": "sum"},
    }
    provider = ScriptedProvider(_answer(line))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post(
        "/ai/generate_chart",
        json={"table_id": table_id, "prompt": "make it a line", "current_chart": {**line, "mark": "bar"}},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["chart"] == line
    assert '"mark": "bar"' in provider.calls[0]["messages"][1].content


@pytest.mark.parametrize(("send_values", "env"), [(False, "true"), (True, "false")])
def test_route_sends_no_data_values_when_the_user_or_the_server_says_no(
    authed_client: TestClient,
    table_id: int,
    worker: None,
    monkeypatch: pytest.MonkeyPatch,
    send_values: bool,
    env: str,
) -> None:
    monkeypatch.setenv("FLOWFILE_AI_SEND_DATA", env)
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    _use_provider(monkeypatch, provider)
    resp = authed_client.post(
        "/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region", "send_values": send_values}
    )
    assert resp.status_code == 200, resp.text
    user = provider.calls[0]["messages"][1].content
    assert "Data values are not shared for this table" in user
    assert '- "region": string, dimension\n' in user
    assert "West" not in user and "distinct" not in user
    assert authed_client.get("/ai/data_policy").json() == {"send_data_allowed": env == "true"}


def test_route_warns_when_the_profile_fails(
    authed_client: TestClient, table_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    _use_provider(monkeypatch, provider)
    with patch("flowfile_core.catalog.service.trigger_visualize_fields", side_effect=RuntimeError("worker down")):
        resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["degraded"] is False
    assert body["warnings"] == [
        "Column statistics and example values were not available (failed); "
        "the model saw the column names, types and roles only."
    ]
    assert "Statistics and example values are not available" in provider.calls[0]["messages"][1].content


def test_route_drops_an_oversized_current_chart_with_a_warning(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    _use_provider(monkeypatch, provider)
    current = {"mark": "bar", "x": "A(" + " " * 50_000, "y": "sum(revenue)"}
    started = time.perf_counter()
    resp = authed_client.post(
        "/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region", "current_chart": current}
    )
    assert time.perf_counter() - started < 10.0
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["degraded"] is False
    assert body["warnings"] == [
        f"The current chart could not be used for a change (It is larger than {cr.MAX_CURRENT_CHART_CHARS} "
        "characters.); the chart was built from the prompt alone."
    ]
    assert "## Current chart\n\nNone: build a new chart." in provider.calls[0]["messages"][1].content


def test_route_reads_the_database_off_the_event_loop_on_a_closed_session(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: dict[str, Any] = {}
    provider = ScriptedProvider(_answer({"mark": "bar", "x": "region", "y": "sum(revenue)"}))
    readable_table = chart_routes._readable_table

    def _table(db: Any, user: Any, table_id_: int):
        seen["table"] = (threading.get_ident(), db)
        return readable_table(db, user, table_id_)

    def _provider(db: Any, *_a: Any, **kw: Any) -> ScriptedProvider:
        seen["provider"] = (threading.get_ident(), db)
        seen["surface"] = kw.get("surface")
        return provider

    async def _chat(*args: Any, **kwargs: Any):
        seen["loop"] = threading.get_ident()
        seen["in_transaction"] = seen["table"][1].in_transaction()
        return await ScriptedProvider.chat(provider, *args, **kwargs)

    monkeypatch.setattr(chart_routes, "_readable_table", _table)
    monkeypatch.setattr(chart_routes, "_resolve_provider", _provider)
    monkeypatch.setattr(provider, "chat", _chat)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200, resp.text
    assert seen["table"] == seen["provider"]
    assert seen["surface"] == "chart"
    assert seen["table"][0] != seen["loop"]
    assert seen["in_transaction"] is False


def test_every_provider_has_a_chart_preset() -> None:
    from flowfile_core.ai.providers.registry import PROVIDERS

    missing = [name for name, cls in PROVIDERS.items() if "chart" not in cls.surface_models]
    assert missing == []


def test_route_404_on_unknown_provider(authed_client: TestClient, table_id: int) -> None:
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x", "provider": "imaginary"})
    assert resp.status_code == 404
    assert "imaginary" in resp.json()["detail"]


def test_route_404_on_unknown_table(authed_client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = ScriptedProvider()
    _use_provider(monkeypatch, provider)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": 987654321, "prompt": "x"})
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Catalog table not found"
    assert provider.calls == []


def test_route_409_when_no_provider_is_configured(
    authed_client: TestClient, table_id: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise(*_a: Any, **_kw: Any):
        raise ProviderNotConfiguredError("anthropic")

    monkeypatch.setattr(node_codegen_routes, "get_configured_provider", _raise)
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x"})
    assert resp.status_code == 409


def test_route_503_when_ai_is_off(authed_client: TestClient, table_id: int) -> None:
    original = core_settings.FEATURE_FLAG_AI.value
    core_settings.FEATURE_FLAG_AI.set(False)
    try:
        resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x"})
    finally:
        core_settings.FEATURE_FLAG_AI.set(original)
    assert resp.status_code == 503


def test_route_422_on_a_blank_or_long_prompt(authed_client: TestClient, table_id: int) -> None:
    assert authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": ""}).status_code == 422
    long = "x" * 2001
    assert authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": long}).status_code == 422


def test_route_refuses_a_table_the_caller_may_not_read(
    table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    provider = ScriptedProvider(_answer(BAR))
    _use_provider(monkeypatch, provider)
    stranger = PydanticUser(id=987654, username="stranger", is_admin=False)
    main.app.dependency_overrides[get_current_active_user] = lambda: stranger
    try:
        resp = TestClient(main.app).post("/ai/generate_chart", json={"table_id": table_id, "prompt": "x"})
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)
    assert resp.status_code == 403
    assert resp.json()["detail"] == "Not authorized to access this catalog_table"
    assert provider.calls == []


def test_route_degrades_on_a_provider_error(
    authed_client: TestClient, table_id: int, worker: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use_provider(monkeypatch, ScriptedProvider(error=ConnectionError("down")))
    resp = authed_client.post("/ai/generate_chart", json={"table_id": table_id, "prompt": "revenue by region"})
    assert resp.status_code == 200
    assert (resp.json()["degraded"], resp.json()["reason"]) == (True, "provider_error")
    assert (resp.json()["attempts"], resp.json()["repairs"]) == (1, [])
    assert (resp.json()["provider"], resp.json()["model"]) == ("fake", "fake-model")


# The prompt log and the lazy-litellm contract.


def test_prompt_log_records_the_chart_surface(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from flowfile_core.ai import prompt_log
    from flowfile_core.ai.providers import _litellm_base
    from flowfile_core.ai.providers.openai import OpenAIProvider
    from shared.storage_config import storage

    async def acompletion(**_kwargs: Any) -> Any:
        message = SimpleNamespace(content=_answer(BAR), tool_calls=None)
        return SimpleNamespace(choices=[SimpleNamespace(message=message, finish_reason="stop")], usage=None)

    monkeypatch.setattr(_litellm_base, "_lazy_litellm", lambda: SimpleNamespace(acompletion=acompletion))
    monkeypatch.setattr(storage, "_base_dir", tmp_path)
    original = core_settings.FLOWFILE_AI_LOG_PROMPTS.value
    core_settings.FLOWFILE_AI_LOG_PROMPTS.set(True)
    try:
        provider = OpenAIProvider(model="gpt-test", api_key="sk-test")
        resp = asyncio.run(
            generate_chart(
                prompt="revenue by region",
                columns=COLUMNS,
                profile_text="Table",
                provider=provider,
                user_id=42,
                scheduler=_scheduler(),
            )
        )
    finally:
        core_settings.FLOWFILE_AI_LOG_PROMPTS.set(original)
    assert resp.degraded is False
    [entry] = list(prompt_log.grep(r"revenue by region", surface="chart"))
    assert entry.surface == "chart"
    assert entry.user_id == 42


_NEW_MODULES = (
    "flowfile_core.ai.chart_request",
    "flowfile_core.ai.chart_profile",
    "flowfile_core.ai.chart_routes",
)


@pytest.mark.parametrize("module_name", _NEW_MODULES)
def test_lazy_litellm_contract(module_name: str) -> None:
    """Importing the module pulls in no litellm, and its source has no module-level litellm import."""
    module = importlib.import_module(module_name)
    tree = ast.parse(Path(module.__file__).read_text())
    for node in tree.body:
        names = []
        if isinstance(node, ast.Import):
            names = [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            names = [node.module or ""]
        assert not any(n == "litellm" or n.startswith("litellm.") for n in names), module_name

    already = any(name == "litellm" or name.startswith("litellm.") for name in sys.modules)
    saved = {k: v for k, v in sys.modules.items() if k == module_name}
    sys.modules.pop(module_name, None)
    try:
        importlib.import_module(module_name)
        if not already:
            assert "litellm" not in sys.modules, f"{module_name} pulled litellm into sys.modules at import time"
    finally:
        sys.modules.update(saved)


def test_openapi_schema_lists_the_route() -> None:
    schema = main.app.openapi()
    assert "/ai/generate_chart" in schema["paths"]
