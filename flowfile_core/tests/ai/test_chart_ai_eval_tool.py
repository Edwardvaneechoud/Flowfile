"""``tools/chart_ai_eval`` against this core and its worker: tables are registered, charted and deleted for real.

Only the model is scripted. The failures a real core cannot be made to produce on demand (a
delete that fails, a registration that never lands) run on a stand-in in
``tools/tests/test_chart_ai_eval.py``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.ai import chart_profile, chart_routes
from flowfile_core.ai.providers.base import ChatResponse
from flowfile_core.auth.jwt import create_access_token
from tools.chart_ai_eval import runner

CASE = {
    "id": "revenue-by-region",
    "table": "tidy",
    "kind": "single",
    "steps": [
        {
            "prompt": "total revenue by region",
            "expect": {
                "outcome": "chart",
                "marks": ["bar"],
                "fields": [{"field": "region", "channel": ["x"]}, {"field": "revenue", "aggregate": "sum"}],
            },
        }
    ],
}


class _Model:
    name = "fake"
    model = "fake-model"

    async def chat(self, messages, tools=None, max_tokens=None, response_format=None, **kwargs):  # type: ignore[no-untyped-def]
        chart = {"mark": "bar", "x": "region", "y": "sum(revenue)"}
        answer = {"chart": chart, "title": "Revenue by region", "explanation": "Total per region."}
        return ChatResponse(model=self.model, content=json.dumps(answer))


@pytest.fixture
def core(monkeypatch: pytest.MonkeyPatch) -> runner.Core:
    monkeypatch.setattr(chart_routes, "_resolve_provider", lambda *_a, **_kw: _Model())
    chart_profile.clear_cache()
    return runner.Core.login(TestClient(main.app), create_access_token(data={"sub": "local_user"}))


def _names(core: runner.Core) -> list[Any]:
    return [t["name"] for t in core.list_tables()]


def test_a_table_is_seeded_charted_and_removed(core: runner.Core) -> None:
    left: list[str] = []
    with runner.seeded_tables(core, ["tidy"], keep=False, log=lambda _m: None, left=left) as seeded:
        table = seeded["tidy"]
        assert core.table_name(table.id) == table.name and table.name in _names(core)
        (result,) = runner.run_case(core, CASE, table.id, provider=None, model=None)
    assert result.answer.status == 200 and result.score.met, result.answer
    assert result.answer.attempts == 1 and result.answer.repairs == ("shorthand",)
    assert core.table_name(table.id) is None
    assert left == [] and table.name not in _names(core)


def test_a_registration_that_got_no_answer_is_found_and_removed(
    core: runner.Core, monkeypatch: pytest.MonkeyPatch
) -> None:
    send = core._call

    def interrupted_once_core_is_done(method: str, path: str, **kwargs: Any):  # type: ignore[no-untyped-def]
        response = send(method, path, **kwargs)
        if (method, path) == ("POST", "/catalog/tables"):
            raise KeyboardInterrupt
        return response

    monkeypatch.setattr(core, "_call", interrupted_once_core_is_done)
    left: list[str] = []
    logs: list[str] = []
    with pytest.raises(KeyboardInterrupt):
        with runner.seeded_tables(core, ["tidy"], keep=False, log=logs.append, left=left):
            pass
    (name,) = core.unanswered
    assert f"deleted {name}" in logs
    assert left == [] and name not in _names(core)
