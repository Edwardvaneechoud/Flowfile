"""Tests for ``tools/chart_ai_eval``: scoring, the summary, the tables, the cases file and the runner.

The runner is driven through ``httpx.MockTransport`` standing in for a core, so seeding, the
calls, a follow-up's ``current_chart`` and the clean-up on success and on failure run for real.
The stand-in is here for the failures a real core cannot be made to produce on demand (a call
that gets no answer, a delete that fails); the same seeding, call and clean-up run against a
real core and worker in ``flowfile_core/tests/ai/test_chart_ai_eval_tool.py``.
"""

from __future__ import annotations

import json
import signal
from typing import Any

import httpx
import polars as pl
import pytest

from tools.chart_ai_eval import __main__ as cli
from tools.chart_ai_eval import runner, tables
from tools.chart_ai_eval.report import summary_lines
from tools.chart_ai_eval.scoring import (
    EXPECT_KEYS,
    Answer,
    StepResult,
    answer_from_response,
    describe_chart,
    percentile,
    score_step,
    summarize,
    validate_expectation,
)

_REAL_CLIENT = httpx.Client
BAR = {"mark": "bar", "x": {"field": "region"}, "y": {"field": "revenue", "aggregate": "sum"}}


def chart_answer(chart: dict[str, Any], **kw: Any) -> Answer:
    return Answer(status=200, latency_ms=kw.pop("latency_ms", 1000), chart=chart, attempts=1, repairs=(), **kw)


def held(expect: dict[str, Any], chart: dict[str, Any]) -> list[bool]:
    """Whether each check after the outcome held."""
    score = score_step({"outcome": "chart", **expect}, chart_answer(chart))
    assert score.checks[0].held
    return [c.held for c in score.checks[1:]]


# One test per expectation kind.


def test_marks() -> None:
    assert held({"marks": ["bar", "line"]}, BAR) == [True]
    assert held({"marks": ["arc"]}, BAR) == [False]


@pytest.mark.parametrize(
    ("pattern", "ok"),
    [
        ({"field": "region"}, True),
        ({"field": "regio"}, False),
        ({"field": "region", "channel": ["x"]}, True),
        ({"field": "region", "channel": ["color"]}, False),
        ({"field": "region", "aggregate": None}, True),
        ({"field": "revenue", "aggregate": None}, False),
        ({"field": "revenue", "aggregate": ["sum", "mean"]}, True),
        ({"field": "revenue", "aggregate": "mean"}, False),
        ({"field": "revenue", "aggregate": True}, True),
        ({"field": "region", "aggregate": True}, False),
        ({"aggregate": ["sum"]}, True),
        ({"any_of": [{"field": "nope"}, {"field": "region"}]}, True),
        ({"any_of": [{"field": "nope"}, {"field": "other"}]}, False),
    ],
)
def test_field_patterns(pattern: dict, ok: bool) -> None:
    assert held({"fields": [pattern]}, BAR) == [ok]


def test_time_unit_and_bin_patterns() -> None:
    chart = {
        "mark": "bar",
        "x": {"field": "order_date", "timeUnit": "month"},
        "y": {"field": "*", "aggregate": "count"},
    }
    assert held({"fields": [{"field": "order_date", "timeUnit": "month"}]}, chart) == [True]
    assert held({"fields": [{"field": "order_date", "timeUnit": ["month", None]}]}, chart) == [True]
    assert held({"fields": [{"field": "order_date", "timeUnit": None}]}, chart) == [False]
    assert held({"fields": [{"field": "*", "aggregate": "count"}]}, chart) == [True]
    hist = {"mark": "bar", "x": {"field": "price", "bin": 20}, "y": {"field": "*", "aggregate": "count"}}
    assert held({"fields": [{"field": "price", "bin": True}]}, hist) == [True]
    assert held({"fields": [{"field": "price", "bin": 20}]}, hist) == [True]
    assert held({"fields": [{"field": "price", "bin": 10}]}, hist) == [False]
    assert held({"fields": [{"field": "price", "bin": False}]}, hist) == [False]


def test_x_and_y_lists_are_read() -> None:
    table = {
        "mark": "table",
        "y": {"field": "region"},
        "x": [{"field": "a", "aggregate": "sum"}, {"field": "b", "aggregate": "sum"}],
    }
    assert held({"fields": [{"field": "b", "aggregate": "sum", "channel": ["x"]}]}, table) == [True]


def test_absent_and_absent_channels() -> None:
    assert held({"absent": [{"field": "revenue", "aggregate": "mean"}]}, BAR) == [True]
    assert held({"absent": [{"field": "revenue"}]}, BAR) == [False]
    assert held({"absent_channels": ["color"]}, BAR) == [True]
    assert held({"absent_channels": ["x"]}, BAR) == [False]


def test_mentions_reads_channels_and_filters() -> None:
    chart = {**BAR, "filters": [{"field": "is_return", "oneOf": [True]}]}
    assert held({"mentions": ["region", "is_return"]}, chart) == [True, True]
    assert held({"mentions": ["category"]}, chart) == [False]


@pytest.mark.parametrize(
    ("flt", "ok"),
    [
        ({"field": "is_return", "oneOf": [False]}, True),
        ({"field": "is_return", "notIn": [True]}, True),
        ({"field": "is_return", "oneOf": [True]}, False),
        ({"field": "is_return", "notIn": [False]}, False),
    ],
)
def test_filter_include_exclude_on_values(flt: dict, ok: bool) -> None:
    expect = {"filters": [{"field": "is_return", "include": [False], "exclude": [True]}]}
    assert held(expect, {**BAR, "filters": [flt]}) == [ok]


@pytest.mark.parametrize(
    ("bounds", "ok"),
    [
        (["2025-01-01", "2025-12-31"], True),
        (["2025-01-01", None], False),
        (["2025-02-01", "2025-12-31"], False),
    ],
)
def test_filter_include_exclude_on_date_ranges(bounds: list, ok: bool) -> None:
    expect = {"filters": [{"field": "order_date", "include": ["2025-01-01", "2025-12-31"], "exclude": ["2026-01-01"]}]}
    assert held(expect, {**BAR, "filters": [{"field": "order_date", "dateRange": bounds}]}) == [ok]


def test_filter_on_a_range_and_kind_and_existence() -> None:
    chart = {**BAR, "filters": [{"field": "revenue", "range": [100, None]}]}
    assert held({"filters": [{"field": "revenue", "include": [150], "exclude": [50]}]}, chart) == [True]
    assert held({"filters": [{"field": "revenue", "kind": "range"}]}, chart) == [True]
    assert held({"filters": [{"field": "revenue", "kind": ["oneOf"]}]}, chart) == [False]
    assert held({"filters": [{"field": "region"}]}, chart) == [False]


def test_every_filter_on_the_field_must_pass_an_included_value() -> None:
    chart = {**BAR, "filters": [{"field": "region", "oneOf": ["West", "East"]}, {"field": "region", "notIn": ["East"]}]}
    assert held({"filters": [{"field": "region", "include": ["West"], "exclude": ["East"]}]}, chart) == [True]


def test_sort_limit_stack_and_raw_rows() -> None:
    top = {**BAR, "sort": "descending", "limit": 5}
    assert held({"sort": "descending", "limit": 5}, top) == [True, True]
    assert held({"sort": None, "limit": None}, top) == [False, False]
    assert held({"sort": None}, BAR) == [True]
    assert held({"stack": ["stack", None]}, BAR) == [True]
    assert held({"stack": ["normalize"]}, {**BAR, "stack": "stack"}) == [False]
    scatter = {"mark": "point", "x": {"field": "a"}, "y": {"field": "b"}, "aggregate": False}
    assert held({"raw_rows": True}, scatter) == [True]
    assert held({"raw_rows": True}, BAR) == [False]
    assert held({"raw_rows": False}, BAR) == [True]


def test_a_failed_check_says_why() -> None:
    score = score_step({"outcome": "chart", "fields": [{"field": "revenue", "aggregate": "mean"}]}, chart_answer(BAR))
    assert not score.met
    assert score.checks[1].why == "chart: bar · x=region · y=sum(revenue)"


# Outcomes.


def test_outcomes_of_answers() -> None:
    assert chart_answer(BAR).outcome == "chart"
    assert Answer(status=200, degraded=True, reason="no_chart").outcome == "decline"
    assert Answer(status=200, degraded=True, reason="invalid_chart").outcome == "decline"
    assert Answer(status=200, degraded=True, reason="provider_error").outcome == "failed"
    assert Answer(status=409, error="no provider").outcome == "error"
    assert Answer(status=None, error="ConnectError").outcome == "error"


def test_expected_chart_scores_only_the_outcome_when_none_came() -> None:
    expect = {"outcome": "chart", "marks": ["bar"], "fields": [{"field": "region"}]}
    score = score_step(expect, Answer(status=200, degraded=True, reason="provider_error"))
    assert [(c.held, c.why) for c in score.checks] == [(False, "degraded: provider_error")]


def test_expected_decline() -> None:
    declined = Answer(status=200, degraded=True, reason="no_chart", explanation="No weather here.")
    assert score_step({"outcome": "decline"}, declined).met
    score = score_step({"outcome": "decline"}, chart_answer(BAR))
    assert not score.met and score.checks[0].why.startswith("returned a chart: bar")
    assert not score_step({"outcome": "decline"}, Answer(status=200, degraded=True, reason="timeout")).met


def test_either_outcome_scores_the_chart_when_one_came() -> None:
    expect = {"outcome": ["chart", "decline"], "absent": [{"field": "region"}]}
    assert score_step(expect, Answer(status=200, degraded=True, reason="no_chart")).met
    assert not score_step(expect, chart_answer(BAR)).met


def test_http_error_outcome_says_the_status() -> None:
    score = score_step({"outcome": "chart"}, Answer(status=500, error="boom"))
    assert score.checks[0].why == "HTTP 500: boom"


# The route's answer, including a core that does not send attempts.


def test_answer_from_a_core_without_attempts_is_unknown() -> None:
    body = {"chart": BAR, "title": "T", "explanation": "E", "warnings": [], "degraded": False}
    answer = answer_from_response(200, body, 900)
    assert (answer.attempts, answer.repairs) == (None, None)
    newer = answer_from_response(200, {**body, "attempts": 2, "repairs": ["shorthand"]}, 900)
    assert (newer.attempts, newer.repairs) == (2, ("shorthand",))
    assert answer_from_response(404, {"detail": "Catalog table not found"}, 10).error == '"Catalog table not found"'


def _result(answer: Answer, expect: dict | None = None, **kw: Any) -> StepResult:
    expect = expect or {"outcome": "chart"}
    return StepResult(
        case_id=kw.get("case_id", "c"),
        table=kw.get("table", "tidy"),
        kind=kw.get("kind", "single"),
        step=1,
        prompt="p",
        expect=expect,
        answer=answer,
        score=score_step(expect, answer),
        case_steps=kw.get("case_steps", 1),
    )


def test_a_case_stopped_before_its_last_step_is_not_met() -> None:
    stopped = _result(chart_answer(BAR), case_id="chain", case_steps=4)
    s = summarize([stopped, _result(chart_answer(BAR), case_id="whole")])
    assert (s["cases"], s["cases_met"], s["cases_partial"], s["calls_met"]) == (2, 1, 1, 2)
    assert "1 stopped before their last step" in summary_lines(s)[0]
    assert "stopped" not in summary_lines(summarize([_result(chart_answer(BAR))]))[0]


def test_summary_reports_unknown_attempts_as_unknown_not_zero() -> None:
    old = Answer(status=200, latency_ms=1000, chart=BAR)
    s = summarize([_result(old)])
    assert (s["valid_charts"], s["first_call"], s["after_correction"], s["attempts_unknown"]) == (1, 0, 0, 1)
    assert s["repairs_unknown"] == 1


def test_summary_counts() -> None:
    results = [
        _result(chart_answer(BAR, latency_ms=1000), case_id="a"),
        _result(
            Answer(status=200, latency_ms=3000, chart=BAR, attempts=2, repairs=("shorthand", "column_case")),
            case_id="b",
        ),
        _result(
            Answer(status=200, latency_ms=2000, degraded=True, reason="no_chart"), {"outcome": "decline"}, case_id="c"
        ),
        _result(Answer(status=200, latency_ms=4000, degraded=True, reason="no_chart"), case_id="d"),
        _result(chart_answer(BAR, latency_ms=5000), {"outcome": "decline"}, case_id="e"),
        _result(Answer(status=200, latency_ms=500, degraded=True, reason="provider_error"), case_id="f"),
        _result(Answer(status=None, error="ConnectError"), case_id="g"),
    ]
    s = summarize(results)
    assert (s["cases"], s["calls"], s["cases_met"]) == (7, 7, 3)
    assert (s["valid_charts"], s["first_call"], s["after_correction"], s["with_repairs"]) == (3, 2, 1, 1)
    assert s["repairs_by_code"] == {"shorthand": 1, "column_case": 1}
    assert s["degraded_by_reason"] == {"no_chart": 2, "provider_error": 1}
    assert (s["declines"], s["declines_right"], s["declines_wrong"], s["declines_missed"]) == (2, 1, 1, 1)
    assert s["http_errors"] == 1 and s["status_counts"] == {"200": 6, "None": 1}
    assert (s["latency_ms_median"], s["latency_ms_p90"]) == (2500, 5000)


def test_percentile_is_nearest_rank() -> None:
    assert percentile([], 90) is None
    assert percentile(list(range(1, 11)), 90) == 9
    assert percentile([5], 90) == 5


def test_describe_chart() -> None:
    chart = {**BAR, "filters": [{"field": "region", "oneOf": ["West"]}], "sort": "descending", "limit": 5}
    assert (
        describe_chart(chart)
        == 'bar · x=region · y=sum(revenue) · filter region oneOf ["West"] · sort="descending" · limit=5'
    )
    assert describe_chart(None) == "no chart"


# The tables.


def test_tables_have_their_shape() -> None:
    tidy, messy, wide = (tables.build(k) for k in tables.TABLE_KEYS)
    assert tidy.columns == [
        "order_date",
        "region",
        "category",
        "product",
        "quantity",
        "unit_price",
        "revenue",
        "is_return",
    ]
    assert tidy.schema["order_date"] == pl.Date and tidy.schema["is_return"] == pl.Boolean
    assert messy.schema["dt_ord"] == pl.String and set(messy["ret_flg"].unique()) == {"Y", "N"}
    assert "count" in messy.columns and messy.schema["zip_cd"] == pl.Int64
    assert messy["Cust_Name"].n_unique() > 200 and messy["prio_lvl"].n_unique() <= 5
    assert messy["AMT_NET_EUR"].null_count() > 0 and messy["ShipMode"].null_count() > 0
    assert wide.width == 45 and wide.schema["report_month"] == pl.Date
    assert {str(t) for t in wide.schema.values()} >= {"Int64", "Float64", "String", "Boolean", "Date"}


def test_tables_are_deterministic() -> None:
    for key in tables.TABLE_KEYS:
        assert tables.build(key).equals(tables.build(key))
    assert not tables.build_tidy(seed=1).equals(tables.build_tidy(seed=2))


# The cases file.


def test_every_case_is_well_formed_against_its_table() -> None:
    cases = runner.load_cases()
    columns = {k: tables.build(k).columns for k in tables.TABLE_KEYS}
    ids = [c["id"] for c in cases]
    assert len(ids) == len(set(ids))
    for case in cases:
        assert case["table"] in tables.TABLE_KEYS
        assert case["kind"] in ("single", "follow_up", "decline", "vague", "language", "rule_clash")
        assert case["steps"]
        for i, step in enumerate(case["steps"]):
            assert set(step) <= {"prompt", "expect", "fallback_current"}, case["id"]
            assert validate_expectation(step["expect"], columns[case["table"]]) == [], (case["id"], i)
            assert set(step["expect"]) <= set(EXPECT_KEYS)
            assert (i > 0) == ("fallback_current" in step), case["id"]


def test_the_cases_cover_the_requested_spread() -> None:
    cases = runner.load_cases()
    assert 40 <= len(cases) <= 55
    kinds = {c["kind"] for c in cases}
    assert kinds == {"single", "follow_up", "decline", "vague", "language", "rule_clash"}
    assert {c["table"] for c in cases if c["kind"] == "follow_up"} == set(tables.TABLE_KEYS)


def test_select_cases() -> None:
    cases = runner.load_cases()
    assert {c["table"] for c in runner.select_cases(cases, ["messy"], None)} == {"messy"}
    assert len(runner.select_cases(cases, None, 3)) == 3


def test_deletable_takes_only_our_tables() -> None:
    assert runner.deletable("ai_eval_tidy_ab12cd")
    assert not runner.deletable("ai_eval_")
    assert not runner.deletable("sales")
    assert not runner.deletable("my_ai_eval_tidy")
    assert not runner.deletable(None)


# The runner, against a stand-in core.


class FakeCore:
    """A core over ``httpx.MockTransport``: catalog tables, a token, and scripted chart answers."""

    def __init__(
        self, answers: list[dict[str, Any]] | None = None, fail_register: str | None = None, fail_status: int = 500
    ) -> None:
        self.tables: dict[int, str] = {}
        self.answers = list(answers or [])
        self.chart_bodies: list[dict[str, Any]] = []
        self.deleted: list[int] = []
        self.fail_register = fail_register
        self.fail_status = fail_status
        self.auth_headers: set[str] = set()
        self.file_paths: list[str] = []
        self.paths: list[str] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/auth/token":
            return httpx.Response(200, json={"access_token": "tok-123", "token_type": "bearer"})
        self.auth_headers.add(request.headers.get("authorization", ""))
        if path == "/catalog/default-namespace-id":
            return httpx.Response(200, json=4)
        if path == "/catalog/tables" and request.method == "GET":
            return httpx.Response(200, json=[{"id": i, "name": n} for i, n in self.tables.items()])
        if path == "/catalog/tables" and request.method == "POST":
            body = json.loads(request.content)
            self.file_paths.append(body["file_path"])
            if self.fail_register and self.fail_register in body["name"]:
                return httpx.Response(self.fail_status, json={"detail": "worker down"})
            table_id = len(self.tables) + 1 + len(self.deleted)
            self.tables[table_id] = body["name"]
            return httpx.Response(201, json={"id": table_id, "name": body["name"]})
        if path.startswith("/catalog/tables/"):
            table_id = int(path.rsplit("/", 1)[1])
            if request.method == "GET":
                return httpx.Response(200, json={"id": table_id, "name": self.tables[table_id]})
            self.deleted.append(table_id)
            del self.tables[table_id]
            return httpx.Response(204)
        if path == "/ai/generate_chart":
            self.chart_bodies.append(json.loads(request.content))
            reply = self.answers.pop(0) if self.answers else {"degraded": True, "reason": "provider_error"}
            return httpx.Response(200, json={"chart": None, "warnings": [], "degraded": False, **reply})
        return httpx.Response(404)

    def client(self) -> httpx.Client:
        def serve(request: httpx.Request) -> httpx.Response:
            self.paths.append(request.url.path)
            return self.handler(request)

        return _REAL_CLIENT(base_url="http://core.test", transport=httpx.MockTransport(serve))


def run_cli(fake: FakeCore, monkeypatch: pytest.MonkeyPatch, *args: str) -> int:
    monkeypatch.setattr(cli.httpx, "Client", lambda **kw: fake.client())
    return cli.main(["--api", "http://core.test", *args])


def test_a_follow_up_sends_the_previous_chart_or_the_fallback() -> None:
    fake = FakeCore(answers=[{"chart": BAR}, {"degraded": True, "reason": "provider_error"}, {"chart": BAR}])
    case = {
        "id": "chain",
        "table": "tidy",
        "kind": "follow_up",
        "steps": [
            {"prompt": "one", "expect": {"outcome": "chart"}},
            {"prompt": "two", "expect": {"outcome": "chart"}, "fallback_current": {"mark": "line"}},
            {"prompt": "three", "expect": {"outcome": "chart"}, "fallback_current": {"mark": "area"}},
        ],
    }
    with fake.client() as client:
        results = runner.run_case(runner.Core.login(client), case, 9, provider="openai", model="m")
    first, second, third = fake.chart_bodies
    assert first == {"table_id": 9, "prompt": "one", "provider": "openai", "model": "m"}
    assert second["current_chart"] == BAR and not results[1].fallback
    assert third["current_chart"] == {"mark": "area"} and results[2].fallback
    assert [r.score.met for r in results] == [True, False, True]
    assert {r.case_steps for r in results} == {3}


def test_the_run_seeds_calls_cleans_up_and_writes_reports(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeCore()
    monkeypatch.setattr(cli.httpx, "Client", lambda **kw: fake.client())
    out = tmp_path / "out"
    status = cli.main(
        [
            "--api",
            "http://core.test",
            "--provider",
            "openai",
            "--model",
            "m",
            "--out",
            str(out),
            "--tables",
            "tidy,messy",
            "--limit",
            "3",
        ]
    )
    assert status == 0
    assert len(fake.chart_bodies) == 3 and fake.tables == {}
    assert fake.deleted == [1], "only the tables the chosen cases use are seeded"
    report = json.loads((out / "report.json").read_text())
    assert report["summary"]["calls"] == 3
    assert report["summary"]["degraded_by_reason"] == {"provider_error": 3}
    assert set(report["by_table"]) == {"tidy"}
    assert "## Calls that did not meet their expectations (3)" in (out / "report.md").read_text()
    printed = capsys.readouterr().out
    assert "tok-123" not in printed and "tok-123" not in (out / "report.md").read_text()
    assert "tok-123" not in (out / "report.json").read_text()
    assert fake.auth_headers == {"Bearer tok-123"}


def test_a_failed_seed_removes_what_was_seeded(tmp_path, monkeypatch) -> None:
    fake = FakeCore(fail_register="_messy_")
    monkeypatch.setattr(cli.httpx, "Client", lambda **kw: fake.client())
    status = cli.main(["--api", "http://core.test", "--out", str(tmp_path / "o"), "--tables", "tidy,messy"])
    assert status == 2
    assert fake.tables == {} and fake.deleted == [1]
    assert fake.chart_bodies == []


def test_an_interrupt_removes_the_tables(tmp_path, monkeypatch) -> None:
    fake = FakeCore()
    calls = {"n": 0}
    original = fake.handler

    def interrupting(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ai/generate_chart":
            calls["n"] += 1
            if calls["n"] == 2:
                raise KeyboardInterrupt
        return original(request)

    fake.handler = interrupting  # type: ignore[method-assign]
    monkeypatch.setattr(cli.httpx, "Client", lambda **kw: fake.client())
    status = cli.main(["--api", "http://core.test", "--out", str(tmp_path / "o"), "--tables", "tidy"])
    assert status == 130
    assert fake.tables == {}
    report = json.loads((tmp_path / "o" / "report.json").read_text())
    assert report["meta"]["interrupted"] is True and report["summary"]["calls"] == 1


def test_keep_leaves_the_tables(tmp_path, monkeypatch) -> None:
    fake = FakeCore()
    monkeypatch.setattr(cli.httpx, "Client", lambda **kw: fake.client())
    cli.main(["--api", "http://core.test", "--out", str(tmp_path / "o"), "--tables", "wide", "--limit", "1", "--keep"])
    assert list(fake.tables.values())[0].startswith("ai_eval_wide_") and fake.deleted == []


def test_a_table_core_renamed_is_not_deleted() -> None:
    fake = FakeCore()
    with fake.client() as client:
        core = runner.Core.login(client)
        fake.tables[1] = "someone_elses_table"
        left = runner.remove_tables(core, [runner.SeededTable("tidy", 1, "ai_eval_tidy_x")], log=lambda _m: None)
    assert left == ["ai_eval_tidy_x"] and fake.deleted == []


def _register_gets_no_answer(fake: FakeCore, lands_on_listing: int | None) -> None:
    """Interrupt the register call; core makes the table before the N-th listing after it, or never."""
    original = fake.handler
    held: list[httpx.Request] = []
    listings = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/catalog/tables" and request.method == "POST":
            held.append(request)
            raise KeyboardInterrupt
        if request.url.path == "/catalog/tables" and request.method == "GET":
            listings["n"] += 1
            if listings["n"] == lands_on_listing:
                original(held.pop())
        return original(request)

    fake.handler = handler  # type: ignore[method-assign]


@pytest.mark.parametrize("lands_on_listing", [1, 3])
def test_a_table_whose_registration_got_no_answer_is_found_and_removed(
    lands_on_listing: int, tmp_path, monkeypatch, capsys
) -> None:
    fake = FakeCore()
    _register_gets_no_answer(fake, lands_on_listing)
    monkeypatch.setattr(runner, "ADOPT_PAUSE", 0)
    status = run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy")
    assert status == 130
    assert fake.tables == {} and fake.deleted == [1]
    assert "may still be in the catalog" not in capsys.readouterr().err


def test_a_registration_that_never_shows_up_is_named(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeCore()
    _register_gets_no_answer(fake, None)
    monkeypatch.setattr(runner, "ADOPT_WAIT", 0)
    status = run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy")
    assert status == 130
    assert "may still be in the catalog: ai_eval_tidy_" in capsys.readouterr().err


def _first_lookup_times_out(fake: FakeCore) -> None:
    original = fake.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/catalog/tables/1" and request.method == "GET":
            raise httpx.ReadTimeout("core is busy", request=request)
        return original(request)

    fake.handler = handler  # type: ignore[method-assign]


def test_the_clean_up_goes_on_past_a_call_that_fails(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeCore()
    _first_lookup_times_out(fake)
    status = run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"))
    assert status == 1, "the run finished, but a table is left"
    assert fake.deleted == [2, 3] and list(fake.tables) == [1]
    captured = capsys.readouterr()
    assert "could not delete ai_eval_tidy_" in captured.out
    assert "may still be in the catalog: ai_eval_tidy_" in captured.err


def test_an_interrupted_run_stays_interrupted_when_the_clean_up_fails(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeCore()
    _first_lookup_times_out(fake)
    timing_out = fake.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ai/generate_chart" and fake.chart_bodies:
            raise KeyboardInterrupt
        return timing_out(request)

    fake.handler = handler  # type: ignore[method-assign]
    status = run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy")
    assert status == 130
    assert json.loads((tmp_path / "o" / "report.json").read_text())["meta"]["interrupted"] is True
    assert "may still be in the catalog: ai_eval_tidy_" in capsys.readouterr().err


def test_an_unexpected_error_still_removes_the_tables_and_writes_the_report(tmp_path, monkeypatch) -> None:
    fake = FakeCore()
    original = fake.handler

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/ai/generate_chart" and fake.chart_bodies:
            raise RuntimeError("a bug")
        return original(request)

    fake.handler = handler  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="a bug"):
        run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy")
    assert fake.tables == {}
    report = json.loads((tmp_path / "o" / "report.json").read_text())
    assert report["meta"]["interrupted"] is True and report["summary"]["calls"] == 1


@pytest.mark.parametrize(
    "args",
    [["--tables", ""], ["--tables", " , "], ["--limit", "0"], ["--limit", "-2"], ["--seed-dir", "no-such-folder"]],
)
def test_arguments_that_select_nothing_are_refused(args: list[str], tmp_path, monkeypatch) -> None:
    fake = FakeCore()
    with pytest.raises(SystemExit) as stopped:
        run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), *args)
    assert stopped.value.code == 2
    assert fake.paths == [] and not (tmp_path / "o").exists()


def test_an_out_that_cannot_be_written_stops_the_run_before_any_call(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeCore()
    out = tmp_path / "report.md"
    out.write_text("an earlier report")
    assert run_cli(fake, monkeypatch, "--out", str(out), "--tables", "tidy") == 2
    assert fake.paths == []
    assert "cannot write the reports into" in capsys.readouterr().err


def test_seed_dir_holds_the_seed_files_for_the_run_only(tmp_path, monkeypatch) -> None:
    fake = FakeCore()
    seed = tmp_path / "seed"
    seed.mkdir()
    status = run_cli(
        fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy", "--limit", "1", "--seed-dir", str(seed)
    )
    assert status == 0
    (path,) = fake.file_paths
    assert path.startswith(str(seed.resolve())) and path.endswith("tidy.parquet")
    assert list(seed.iterdir()) == []


def test_a_refused_seed_folder_points_at_seed_dir(tmp_path, monkeypatch, capsys) -> None:
    fake = FakeCore(fail_register="_tidy_", fail_status=403)
    assert run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy") == 2
    assert "pass --seed-dir" in capsys.readouterr().err


def test_the_sigterm_handler_is_put_back(tmp_path, monkeypatch) -> None:
    fake = FakeCore()
    during: list[Any] = []
    original = fake.handler

    def handler(request: httpx.Request) -> httpx.Response:
        during.append(signal.getsignal(signal.SIGTERM))
        return original(request)

    fake.handler = handler  # type: ignore[method-assign]

    def earlier(_signum: int, _frame: object) -> None:
        return None

    before = signal.signal(signal.SIGTERM, earlier)
    try:
        run_cli(fake, monkeypatch, "--out", str(tmp_path / "o"), "--tables", "tidy", "--limit", "1")
        assert set(during) == {cli._raise_interrupt}
        assert signal.getsignal(signal.SIGTERM) is earlier
    finally:
        signal.signal(signal.SIGTERM, before)
