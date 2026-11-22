"""The shared chart request cases, judged by core's rules.

``chart_request_cases.json`` beside this file lists chart requests with the verdict core gives them
against a fixed column list. The browser's mirror of these rules (``chartRequest.ts::coreRefusals``,
which keeps a follow-up's ``current_chart`` to what core accepts) reads the same file in
``flowfile_frontend/src/renderer/app/views/CatalogView/chartRequestCases.test.ts``, so a rule that
changes on one side and not the other fails one of the two suites.

Accepted means ``review_chart`` takes the request exactly as written; a request it refuses or
rewrites is rejected, because the browser refuses both.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from flowfile_core.ai.chart_request import ChartRequest, ColumnInfo, review_chart

CASES_FILE = Path(__file__).with_name("chart_request_cases.json")
_DATA = json.loads(CASES_FILE.read_text(encoding="utf-8"))
COLUMNS = [ColumnInfo(name=c["name"], type=c["type"]) for c in _DATA["columns"]]


def core_verdict(raw: dict[str, Any]) -> tuple[str, list[str]]:
    chart, problems = review_chart(raw, COLUMNS)
    if chart is None:
        return "rejected", problems
    if chart.model_dump() != ChartRequest.model_validate(raw).model_dump():
        return "rejected", [f"rewritten to {chart.model_dump()}"]
    return "accepted", []


def test_the_cases_cover_both_verdicts_and_every_mark() -> None:
    verdicts = {case["verdict"] for case in _DATA["cases"]}
    marks = {case["request"]["mark"] for case in _DATA["cases"] if case["verdict"] == "accepted"}
    assert verdicts == {"accepted", "rejected"}
    assert marks == {"bar", "line", "area", "point", "arc", "rect", "boxplot", "table", "map"}
    names = [case["name"] for case in _DATA["cases"]]
    assert len(names) == len(set(names))


@pytest.mark.parametrize("case", _DATA["cases"], ids=lambda case: case["name"])
def test_core_gives_the_recorded_verdict(case: dict[str, Any]) -> None:
    verdict, problems = core_verdict(case["request"])
    assert verdict == case["verdict"], problems
