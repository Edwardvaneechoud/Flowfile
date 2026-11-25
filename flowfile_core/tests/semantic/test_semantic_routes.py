"""The semantic routes' answers: domain errors as 403/404/409/422 (never 401, which the frontend reads as an expired
session), 503 for the AI routes with AI off, and Suggest storing nothing twice.

Runs in electron mode (one user, solo review) on real Delta tables; the 403 paths of team review are in
``tests/sharing/test_sharing_semantic.py``.
"""

from __future__ import annotations

import pytest

from flowfile_core.catalog.semantic import activity, host
from flowfile_core.catalog.semantic.service import SemanticModelService
from flowfile_core.configs.settings import FEATURE_FLAG_AI
from flowfile_core.database.connection import get_db_context
from tests.semantic.conftest import RecordingProvider
from tests.semantic.world import World, say

MISSING = 987654


def _base(ns_id: int) -> str:
    return f"/catalog/namespaces/{ns_id}/semantic-model"


def _metric(world: World, name: str = "order_count", **extra) -> dict:
    return {"kind": "metric", "table_id": world.orders_id, "name": name, "expression": "COUNT(*)", **extra}


def _field(world: World, column: str, attribute: str) -> dict:
    return {"kind": "field", "table_id": world.orders_id, "column": column, "attribute": attribute, "value": "x"}


def _join(world: World, from_id: int, to_id: int, name: str = "orders_customers") -> dict:
    return {
        "kind": "join",
        "name": name,
        "from_table_id": from_id,
        "to_table_id": to_id,
        "from_columns": ["customer_id"],
        "to_columns": ["customer_id"],
    }


def _propose(client, ns_id: int, *proposals: dict):
    return client.post(f"{_base(ns_id)}/proposals", json={"proposals": list(proposals)})


def test_domain_errors_map_to_404_409_and_422_and_never_401(world: World, client) -> None:
    assert _propose(client, world.sales_id, _metric(world)).status_code == 200
    accepted = _propose(client, world.sales_id, _metric(world, "revenue", expression="SUM(amount)")).json()

    cases = {
        "unknown namespace": (client.get(_base(MISSING)), 404),
        "unknown provider": (
            client.post("/ai/semantic/ask", json={"namespace_id": world.sales_id, "question": "x", "provider": "nope"}),
            404,
        ),
        "unknown fact": (client.post(f"{_base(world.sales_id)}/review", json={"action": "accept", "ids": ["x"]}), 404),
        "subject of another model": (
            client.get(f"{_base(world.crm_id)}/history", params={"subject": world.field("orders", "region")}),
            404,
        ),
        "unknown join": (
            client.get(f"{_base(world.sales_id)}/joins/dependents", params={"key": "relationship:x::y"}),
            404,
        ),
        "metric name taken": (_propose(client, world.sales_id, _metric(world)), 409),
        "accept an accepted fact": (
            client.post(f"{_base(world.sales_id)}/review", json={"action": "accept", "ids": [accepted[0]["id"]]}),
            409,
        ),
        "unknown column": (
            _propose(client, world.sales_id, _field(world, "nope", "description")),
            422,
        ),
        "attribute read from the catalog": (
            _propose(client, world.sales_id, _field(world, "amount", "datatype")),
            422,
        ),
        "table of another schema": (
            _propose(client, world.sales_id, _metric(world) | {"table_id": world.customers_id}),
            422,
        ),
        "metric that does not run": (
            _propose(client, world.sales_id, _metric(world, "broken", expression="SUM(nope)")),
            422,
        ),
        "reversed join": (_propose(client, world.catalog_id, _join(world, world.customers_id, world.orders_id)), 422),
        "unknown metric in a query": (client.post(f"{_base(world.sales_id)}/query", json={"metric": "nope"}), 422),
        "review without ids or group": (client.post(f"{_base(world.sales_id)}/review", json={"action": "accept"}), 422),
        "remove a column whole": (
            client.post(
                f"{_base(world.sales_id)}/review",
                json={"action": "retract", "subject": world.field("orders", "region")},
            ),
            422,
        ),
    }
    wrong = {name: (r.status_code, r.text[:200]) for name, (r, want) in cases.items() if r.status_code != want}
    assert wrong == {}
    assert all(r.status_code != 401 for r, _ in cases.values())


def test_the_ai_routes_answer_404_for_an_unknown_namespace(client, use_provider) -> None:
    provider = use_provider(RecordingProvider(lambda _m: {"no_metric": True, "why": "x"}))
    for path, body in (
        ("/ai/semantic/draft", {"namespace_id": MISSING}),
        ("/ai/semantic/ask", {"namespace_id": MISSING, "question": "how many?"}),
        ("/ai/semantic/suggest", {"namespace_id": MISSING, "request": "more"}),
    ):
        assert client.post(path, json=body).status_code == 404, path
    assert provider.calls == []


def test_save_as_flow_refuses_a_query_reading_another_schema(world: World, client) -> None:
    assert _propose(client, world.catalog_id, _join(world, world.orders_id, world.customers_id)).status_code == 200
    assert _propose(client, world.sales_id, _metric(world, dimensions=["customers.segment"])).status_code == 200
    query = {"metric": "order_count", "dimensions": ["customers.segment"]}
    ran = client.post(f"{_base(world.sales_id)}/query", json=query)
    assert ran.status_code == 200 and ran.json()["error"] is None, ran.text
    assert ran.json()["tables"] == [world.key("orders"), world.key("customers")]
    refused = client.post(f"{_base(world.sales_id)}/query/save-as-flow", json={"query": query, "name": "by segment"})
    assert refused.status_code == 422
    assert "in another schema" in refused.json()["detail"]


def test_no_ai_set_up_is_a_409_with_its_code(world: World, client) -> None:
    for path, body in (
        ("/ai/semantic/draft", {"namespace_id": world.sales_id}),
        ("/ai/semantic/ask", {"namespace_id": world.sales_id, "question": "how many?"}),
        ("/ai/semantic/suggest", {"namespace_id": world.sales_id, "request": "more"}),
    ):
        response = client.post(path, json=body)
        assert response.status_code == 409, (path, response.text)
        assert response.json()["error_code"] == "AI_NOT_SET_UP"


def test_a_second_suggest_while_one_runs_is_a_409_with_its_code(world: World, client, use_provider) -> None:
    use_provider(RecordingProvider(lambda _m: {"nothing": True, "why": "x"}))
    with activity.tracking(world.sales_id, "suggest", "Suggesting a model…", user_id=1):
        for path, body in (
            (f"{_base(world.sales_id)}/suggest", None),
            ("/ai/semantic/draft", {"namespace_id": world.sales_id}),
            ("/ai/semantic/suggest", {"namespace_id": world.sales_id, "request": "more"}),
        ):
            response = client.post(path, json=body)
            assert response.status_code == 409, (path, response.text)
            assert response.json()["error_code"] == "SUGGEST_RUNNING"
    assert client.post(f"{_base(world.sales_id)}/suggest").status_code == 200


def test_the_ai_routes_answer_503_with_ai_off(world: World, client, use_provider) -> None:
    provider = use_provider(RecordingProvider(lambda _m: {"no_metric": True, "why": "x"}))
    FEATURE_FLAG_AI.set(False)
    for path, body in (
        ("/ai/semantic/draft", {"namespace_id": world.sales_id}),
        ("/ai/semantic/ask", {"namespace_id": world.sales_id, "question": "how many?"}),
        ("/ai/semantic/suggest", {"namespace_id": world.sales_id, "request": "more"}),
    ):
        assert client.post(path, json=body).status_code == 503, path
    assert provider.calls == []
    assert client.get(_base(world.sales_id)).status_code == 200


def test_ai_words_queue_even_on_a_desktop(world: World, client, use_provider) -> None:
    item = {"kind": "description", "table": "orders", "column": "region", "text": "Where the order ships to."}
    use_provider(RecordingProvider(lambda _m: {"suggestions": [item]}))
    response = client.post("/ai/semantic/suggest", json={"namespace_id": world.sales_id, "request": "describe region"})
    assert response.status_code == 200 and response.json()["proposed"], response.text
    model = client.get(_base(world.sales_id)).json()
    [orders] = model["tables"]
    [region] = [f for f in orders["fields"] if f["name"] == "region"]
    ai = [f for f in region["pending"] if f["source_kind"] == "ai"]
    assert model["review_mode"] == "solo" and [f["status"] for f in ai] == ["proposed"]


def _facts(world: World) -> int:
    from glosslayer.store.base import Query

    return len(host.host().gloss.store.find(Query(namespace=world.catalog)))


def test_suggest_twice_stores_nothing_the_second_time(world: World, client) -> None:
    first = client.post(f"{_base(world.sales_id)}/suggest")
    assert first.status_code == 200, first.text
    added = first.json()["added"]
    assert first.json()["stats_available"] and added["roles"] and added["metrics"]
    stored = _facts(world)

    second = client.post(f"{_base(world.sales_id)}/suggest")
    assert second.json()["added"] == {"roles": 0, "descriptions": 0, "joins": 0, "metrics": 0}
    assert _facts(world) == stored


def test_the_same_suggestions_stored_by_two_runs_add_nothing_the_second_time(world: World) -> None:
    """Two processes do not see each other's Suggest status; the ledger is what keeps the second run's candidates
    out. Both runs compute their candidates before either stores, as two processes would."""

    def run() -> tuple[SemanticModelService, object]:
        db = get_db_context().__enter__()
        service = SemanticModelService(db, type("U", (), {"id": 1})(), None)
        sugg, _tables, _problem = service.suggest_candidates(world.sales_id)
        return service, sugg

    (one, sugg_one), (two, sugg_two) = run(), run()
    first = one.store_suggestions(world.sales_id, sugg_one)
    stored = _facts(world)
    second = two.store_suggestions(world.sales_id, sugg_two)
    assert sum(first.added.model_dump().values()) > 0
    assert second.added.model_dump() == {"roles": 0, "descriptions": 0, "joins": 0, "metrics": 0}
    assert _facts(world) == stored
    one.db.close()
    two.db.close()


@pytest.mark.parametrize("accepted", [True, False])
def test_a_rejected_or_accepted_role_is_not_proposed_again(world: World, client, accepted: bool) -> None:
    if accepted:
        fact = say(world.field("orders", "region"), "role", "category")
    else:
        view = host.host().embedded.view(user_id=1, admin=True, via="ai:test")
        fact = view.propose(world.field("orders", "region"), "role", "category", source="ai:test")
        host.host().embedded.view(user_id=1, admin=True).reject(fact.id)
    before = _facts(world)
    client.post(f"{_base(world.sales_id)}/suggest", json={"kinds": ["role"]})
    from glosslayer.store.base import Query

    found = host.host().gloss.store.find(Query(subject=world.field("orders", "region"), attribute="role"))
    region = list(found)
    assert [a.id for a in region] == [fact.id]
    assert _facts(world) >= before
