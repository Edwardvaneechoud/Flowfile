"""Columns kept from AI never reach a prompt: their values, descriptions, labels and synonyms stay out of every
semantic AI surface, the chart profile and the dashboard story.

Each surface runs for real (catalog SQL on the session's worker, the test database's ledger); only the model is a
recording stand-in, and the assertions read everything it was sent. A column is kept by its own level, by its semantic
type's (a namespaced type included) or by its table's. Its name and type may still be sent where the design lists it
(the draft and suggest prompts mark it, the ask menu names it); nothing else about it may.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from flowfile_core.ai import chart_routes, dashboard_routes
from flowfile_core.ai.semantic_draft import PRIVATE_MARK
from flowfile_core.catalog.semantic import confidentiality
from flowfile_core.database.connection import get_db_context
from tests.semantic.conftest import RecordingProvider
from tests.semantic.world import CUSTOMERS, ORDERS, World, say

SECRET_WORDS = ("SECRETDESC", "SECRETSYN", "SECRETLABEL", "SECRETTABLE", "SECRETMETRIC")


@dataclass
class Case:
    """What is kept from AI in one world: the table, its kept and its shared column, and what must never be sent."""

    table: str
    namespace_id: int
    kept: str
    shared: str | None
    whole_table: bool
    values: list[str] = field(default_factory=list)
    controls: list[str] = field(default_factory=list)


def _describe(world: World, table: str, column: str, tag: str) -> None:
    say(world.field(table, column), "description", f"{tag}DESC about {column}")
    say(world.field(table, column), "synonyms", [f"{tag}SYN{column}"])
    say(world.field(table, column), "label", f"{tag}LABEL{column}")
    say(world.field(table, column), "role", "category")


@pytest.fixture(params=["column", "semantic_type", "table"])
def case(request: pytest.FixtureRequest, world: World) -> Case:
    """A column at Restricted by its own fact, one inheriting Restricted from a namespaced semantic type, or a
    whole table at Confidential. The shared column gets the same kinds of facts, as a control."""
    if request.param == "column":
        say(world.field("orders", "card_number"), "confidentiality", "restricted")
        c = Case("orders", world.sales_id, "card_number", "region", False, values=list(ORDERS["card_number"]))
    elif request.param == "semantic_type":
        say("type:crm::email_address", "confidentiality", "restricted")
        say(world.field("customers", "email"), "semantic_type", "crm::email_address")
        c = Case("customers", world.crm_id, "email", "segment", False, values=list(CUSTOMERS["email"]))
    else:
        say(world.dataset("customers"), "confidentiality", "confidential")
        say(world.dataset("customers"), "description", "SECRETTABLE customers description")
        c = Case("customers", world.crm_id, "email", None, True, values=[*CUSTOMERS["email"], *CUSTOMERS["segment"]])
    _describe(world, c.table, c.kept, "SECRET")
    if c.shared:
        _describe(world, c.table, c.shared, "PUBLIC")
    return c


def _assert_nothing_kept_was_sent(sent: str, case: Case) -> None:
    for word in SECRET_WORDS:
        assert word not in sent, f"{word} reached the model"
    for value in case.values:
        assert value not in sent, f"value {value!r} reached the model"


def _metric(client, world: World, case: Case) -> None:
    dims = [case.kept, *([case.shared] if case.shared else [])]
    response = client.post(
        f"/catalog/namespaces/{case.namespace_id}/semantic-model/proposals",
        json={
            "proposals": [
                {
                    "kind": "metric",
                    "table_id": world.table_id(case.table),
                    "name": f"{case.table}_count",
                    "expression": "COUNT(*)",
                    "description": "SECRETMETRIC meaning" if case.whole_table else "How many rows.",
                    "synonyms": ["SECRETMETRIC word"] if case.whole_table else ["row tally"],
                    "dimensions": dims,
                }
            ]
        },
    )
    assert response.status_code == 200, response.text


def _draft_answer(messages) -> dict:
    """Describes every column listed, the kept one included, as a careless model would."""
    columns = [line[2:].split(" (", 1)[0] for line in messages[-1].content.splitlines() if line.startswith("- ")]
    return {"table": "One row is one record.", "columns": {c: f"About {c}." for c in columns}, "metrics": {}}


def test_draft(world: World, case: Case, client, use_provider) -> None:
    provider = use_provider(RecordingProvider(_draft_answer))
    response = client.post("/ai/semantic/draft", json={"namespace_id": case.namespace_id})
    assert response.status_code == 200, response.text
    [outcome] = response.json()["table_outcomes"]
    _assert_nothing_kept_was_sent(provider.sent, case)
    if case.whole_table:
        assert provider.calls == [] and outcome["reason"] == "kept_private"
    else:
        assert f"- {case.kept} (text, {PRIVATE_MARK})" in provider.sent
        assert case.kept in outcome["withheld_columns"]
    model = client.get(f"/catalog/namespaces/{case.namespace_id}/semantic-model").json()
    [table] = [t for t in model["tables"] if t["name"] == case.table]
    [kept] = [f for f in table["fields"] if f["name"] == case.kept]
    stated = [*kept["facts"].values(), *kept["pending"]]
    assert not [f for f in stated if f["source_kind"] == "ai"], "the model's words on a kept column were stored"


def test_ask(world: World, case: Case, client, use_provider) -> None:
    _metric(client, world, case)
    provider = use_provider(RecordingProvider(lambda _m: {"no_metric": True, "why": "Nothing fits."}))
    response = client.post("/ai/semantic/ask", json={"namespace_id": case.namespace_id, "question": "how many?"})
    assert response.status_code == 200, response.text
    sent = provider.sent
    _assert_nothing_kept_was_sent(sent, case)
    assert f"- {case.table}_count:" in sent
    if case.shared:
        assert f"PUBLICSYN{case.shared}" in sent and "row tally" in sent


def test_suggest(world: World, case: Case, client, use_provider) -> None:
    refused = {"kind": "description", "table": case.table, "column": case.kept, "text": "SECRETDESC from the model"}
    provider = use_provider(RecordingProvider(lambda _m: {"suggestions": [refused]}))
    response = client.post("/ai/semantic/suggest", json={"namespace_id": case.namespace_id, "request": "describe it"})
    assert response.status_code == 200, response.text
    body = response.json()
    _assert_nothing_kept_was_sent(provider.calls[0][-1].content, case)
    assert f"- {case.kept} (text, {PRIVATE_MARK})" in provider.sent
    if case.shared:
        assert f"PUBLICDESC about {case.shared}" in provider.sent
    assert body["proposed"] == [] and body["skipped"]
    assert body["withheld_columns"] >= 1


def _chart_answer(column: str) -> str:
    chart = {"mark": "bar", "x": {"field": column}, "y": {"field": "*", "aggregate": "count"}}
    return json.dumps({"chart": chart, "title": "Rows", "explanation": "Rows per value."})


def test_chart_profile(world: World, case: Case, client, monkeypatch) -> None:
    provider = RecordingProvider(lambda _m: _chart_answer(case.shared or case.kept))
    monkeypatch.setattr(chart_routes, "_resolve_provider", lambda *_a, **_k: provider)
    response = client.post(
        "/ai/generate_chart", json={"table_id": world.table_id(case.table), "prompt": "rows by value"}
    )
    assert response.status_code == 200, response.text
    _assert_nothing_kept_was_sent(provider.sent, case)
    if case.shared:
        shared_values = ORDERS["region"] if case.shared == "region" else CUSTOMERS["segment"]
        assert any(v in provider.sent for v in shared_values), "the profile read no values at all"
    assert any("confidential" in w.lower() for w in response.json()["warnings"])


def _aggregate(column: str) -> dict:
    query = {"op": "aggregate", "groupBy": [column], "measures": [{"field": "*", "agg": "count", "asFieldKey": "n"}]}
    return {"workflow": [{"type": "view", "query": [query]}]}


def test_dashboard_story(world: World, case: Case, client, monkeypatch) -> None:
    story = {"title": "Records", "summary": "A short story.", "charts": [{"id": "kept", "finding": "Something."}]}
    provider = RecordingProvider(lambda _m: story)
    monkeypatch.setattr(dashboard_routes, "_resolve_provider", lambda *_a, **_k: provider)
    table_id = world.table_id(case.table)
    bar = {"mark": "bar", "x": {"field": case.kept}, "y": {"field": "*", "aggregate": "count"}}
    charts = [{"id": "kept", "table_id": table_id, "title": "By kept", "chart": bar, "payload": _aggregate(case.kept)}]
    if case.shared:
        shared_bar = {**bar, "x": {"field": case.shared}}
        charts.append(
            {
                "id": "shared",
                "table_id": table_id,
                "title": "By shared",
                "chart": shared_bar,
                "payload": _aggregate(case.shared),
            }
        )
    response = client.post(
        "/ai/dashboard_story",
        json={"table_ids": [table_id], "prompt": "What stands out?", "title": "Records", "charts": charts},
    )
    assert response.status_code == 200, response.text
    evidence = {(e["id"], e["kind"]): e["status"] for e in response.json()["evidence"]}
    assert evidence[("kept", "chart")] == "withheld"
    _assert_nothing_kept_was_sent(provider.sent, case)
    if case.shared:
        assert evidence[("shared", "chart")] == "measured"


def test_an_unreadable_ledger_keeps_every_table(world: World, monkeypatch) -> None:
    import glosslayer.visibility

    def broken(*_a, **_k):
        raise RuntimeError("ledger unreadable")

    monkeypatch.setattr(glosslayer.visibility, "above", broken)
    with get_db_context() as db:
        found = confidentiality.read_withheld(db, [world.orders_id, world.customers_id])
    assert found.unreadable == {world.orders_id, world.customers_id}
    assert found.blocks(world.orders_id, "region") and found.whole_table(world.customers_id)


def test_without_glosslayer_every_table_is_kept(world: World, monkeypatch) -> None:
    for name in ("glosslayer", "glosslayer.model", "glosslayer.store", "glosslayer.visibility"):
        monkeypatch.setitem(__import__("sys").modules, name, None)
    with get_db_context() as db:
        found = confidentiality.read_withheld(db, [world.orders_id])
    assert found.unreadable == {world.orders_id} and found.blocks(world.orders_id, "region")


def test_an_unreadable_ledger_sends_no_values_in_the_chart_profile(world: World, client, monkeypatch) -> None:
    import glosslayer.visibility

    monkeypatch.setattr(glosslayer.visibility, "above", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x")))
    provider = RecordingProvider(lambda _m: _chart_answer("region"))
    monkeypatch.setattr(chart_routes, "_resolve_provider", lambda *_a, **_k: provider)
    response = client.post("/ai/generate_chart", json={"table_id": world.orders_id, "prompt": "rows by region"})
    assert response.status_code == 200, response.text
    for value in [*ORDERS["region"], *ORDERS["card_number"]]:
        assert value not in provider.sent


def test_an_unreadable_ledger_sends_no_table_in_the_draft(world: World, client, use_provider, monkeypatch) -> None:
    import glosslayer.visibility

    monkeypatch.setattr(glosslayer.visibility, "above", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("x")))
    provider = use_provider(RecordingProvider(_draft_answer))
    response = client.post("/ai/semantic/draft", json={"namespace_id": world.sales_id})
    assert response.status_code == 200, response.text
    assert provider.calls == []
    assert response.json()["table_outcomes"][0]["reason"] == "kept_private"
