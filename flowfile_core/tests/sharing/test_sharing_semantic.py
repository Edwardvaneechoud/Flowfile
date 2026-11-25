"""Team review of a semantic model in docker mode: who may propose, who may accept, who sees which facts.

The ledger is the test database's, built with team review (``sharing_enabled()``); the catalog's own grants decide.
No SQL runs here: proposals of descriptions need none.
"""

from __future__ import annotations

import pytest

from flowfile_core.ai import semantic_routes
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.semantic import host
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from tests.semantic.conftest import RecordingProvider
from tests.semantic.world import World, build_world, drop_world, reset_process_state, say


@pytest.fixture
def world(users):
    reset_process_state()
    w = build_world(owner_id=users["alice"].id)
    yield w
    drop_world(w)


@pytest.fixture
def team(users, group_factory):
    return group_factory("semantic-team", users["admin"].id, {users["bob"].id: "member"})


def _set(model, row_id: int, **values) -> None:
    with get_db_context() as db:
        row = db.get(model, row_id)
        for name, value in values.items():
            setattr(row, name, value)
        db.commit()


def _base(ns_id: int) -> str:
    return f"/catalog/namespaces/{ns_id}/semantic-model"


def _propose(client, world: World, column: str, text: str):
    proposal = {
        "kind": "field",
        "table_id": world.orders_id,
        "column": column,
        "attribute": "description",
        "value": text,
    }
    return client.post(f"{_base(world.sales_id)}/proposals", json={"proposals": [proposal]})


def _decide(client, ns_id: int, action: str, fact_id: str):
    return client.post(f"{_base(ns_id)}/review", json={"action": action, "ids": [fact_id]})


def _field(model: dict, table: str, column: str) -> dict:
    [t] = [t for t in model["tables"] if t["name"] == table]
    [f] = [f for f in t["fields"] if f["name"] == column]
    return f


def test_proposals_queue_until_someone_who_manages_the_schema_accepts(users, client_for, world):
    _set(db_models.CatalogNamespace, world.sales_id, is_public=True)
    bob, alice = client_for("bob"), client_for("alice")

    response = _propose(bob, world, "region", "Where the order ships to.")
    assert response.status_code == 200, response.text
    [fact] = response.json()
    assert fact["status"] == "proposed" and fact["source"] == f"human:{users['bob'].id}"

    owned = _propose(alice, world, "amount", "What the order cost.")
    assert owned.status_code == 200, owned.text
    assert owned.json()[0]["status"] == "proposed", "team review queues the owner's own proposals too"

    seen_by_bob = bob.get(_base(world.sales_id)).json()
    assert (seen_by_bob["review_mode"], seen_by_bob["can_manage"]) == ("team", False)
    assert _decide(bob, world.sales_id, "accept", fact["id"]).status_code == 403

    seen_by_alice = alice.get(_base(world.sales_id)).json()
    assert seen_by_alice["can_manage"] is True
    assert fact["id"] in [f["id"] for f in _field(seen_by_alice, "orders", "region")["pending"]]
    accepted = _decide(alice, world.sales_id, "accept", fact["id"])
    assert accepted.status_code == 200, accepted.text
    assert [f["status"] for f in accepted.json()["changed"]] == ["accepted"]


def test_a_use_grant_reads_the_model_but_may_neither_propose_nor_decide(users, client_for, world, team, grant_factory):
    grant_factory("catalog_namespace", world.sales_id, team, permission="use", granted_by=users["alice"].id)
    alice, bob = client_for("alice"), client_for("bob")
    [fact] = _propose(alice, world, "region", "Where the order ships to.").json()

    model = bob.get(_base(world.sales_id))
    assert model.status_code == 200, model.text
    assert model.json()["can_manage"] is False
    assert fact["id"] in [f["id"] for f in _field(model.json(), "orders", "region")["pending"]]
    assert bob.get(f"{_base(world.sales_id)}/review").status_code == 200

    assert _propose(bob, world, "region", "Something else.").status_code == 403
    for action in ("accept", "reject"):
        response = _decide(bob, world.sales_id, action, fact["id"])
        assert response.status_code == 403, (action, response.text)
    assert _field(alice.get(_base(world.sales_id)).json(), "orders", "region")["pending"][0]["status"] == "proposed"


def test_a_manage_grant_accepts(users, client_for, world, team, grant_factory):
    grant_factory("catalog_namespace", world.sales_id, team, permission="manage", granted_by=users["alice"].id)
    [fact] = _propose(client_for("alice"), world, "region", "Where the order ships to.").json()
    bob = client_for("bob")
    assert bob.get(_base(world.sales_id)).json()["can_manage"] is True
    response = _decide(bob, world.sales_id, "accept", fact["id"])
    assert response.status_code == 200, response.text
    assert response.json()["changed"][0]["status"] == "accepted"


SCHEMA_ROUTES = [
    ("get", "", None),
    ("put", "", {"description": "x"}),
    ("post", "/proposals", {"proposals": []}),
    ("post", "/joins/check", None),
    ("get", "/joins/dependents?key=relationship:x::y", None),
    ("get", "/review", None),
    ("post", "/review", {"action": "accept", "ids": ["x"]}),
    ("get", "/history?subject=field:x.y.z/a", None),
    ("post", "/sync", None),
    ("post", "/suggest", None),
    ("get", "/activity", None),
    ("post", "/query", {"metric": "m"}),
    ("post", "/query/save-as-flow", {"name": "f", "query": {"metric": "m"}}),
    ("get", "/export", None),
]


@pytest.mark.parametrize("ns", ["sales_id", "catalog_id"])
def test_without_a_grant_every_route_is_refused(users, client_for, world, ns, monkeypatch):
    carol = client_for("carol")
    ns_id = getattr(world, ns)
    for method, path, body in SCHEMA_ROUTES:
        response = getattr(carol, method)(f"{_base(ns_id)}{path}", **({"json": body} if body is not None else {}))
        assert response.status_code == 403, (method, path, response.status_code, response.text)

    provider = RecordingProvider(lambda _m: {"no_metric": True, "why": "x"})
    monkeypatch.setattr(semantic_routes, "resolve_semantic_provider", lambda *_a, **_k: provider)
    for path, body in (
        ("/ai/semantic/draft", {"namespace_id": ns_id}),
        ("/ai/semantic/ask", {"namespace_id": ns_id, "question": "how many?"}),
        ("/ai/semantic/suggest", {"namespace_id": ns_id, "request": "more"}),
    ):
        response = carol.post(path, json=body)
        assert response.status_code == 403, (path, response.text)
    assert provider.calls == []


def test_a_reader_of_one_schema_sees_no_fact_of_another(users, client_for, world, team, grant_factory):
    say(world.field("orders", "region"), "description", "SALESONLY region words", user_id=users["alice"].id)
    say(world.dataset("orders"), "description", "SALESONLY table words", user_id=users["alice"].id)
    say(world.field("customers", "segment"), "description", "CRM segment words", user_id=users["alice"].id)
    join = {
        "kind": "join",
        "name": "orders_customers",
        "from_table_id": world.orders_id,
        "to_table_id": world.customers_id,
        "from_columns": ["customer_id"],
        "to_columns": ["customer_id"],
    }
    alice = client_for("alice")
    stored = alice.post(f"{_base(world.catalog_id)}/proposals", json={"proposals": [join]})
    assert stored.status_code == 200, stored.text
    grant_factory("catalog_namespace", world.crm_id, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")

    crm = bob.get(_base(world.crm_id))
    assert crm.status_code == 200, crm.text
    text = crm.text
    assert "CRM segment words" in text
    assert "SALESONLY" not in text and "orders" not in text, "the crm model shows a fact of sales"
    assert bob.get(_base(world.sales_id)).status_code == 403
    assert bob.get(_base(world.catalog_id)).status_code == 403
    queue = bob.get(f"{_base(world.crm_id)}/review")
    assert queue.status_code == 200 and "SALESONLY" not in queue.text and "orders" not in queue.text
    history = bob.get(f"{_base(world.crm_id)}/history", params={"subject": world.field("orders", "region")})
    assert history.status_code == 404


def _paths(user, world: World) -> tuple[list[str], list[str]]:
    with get_db_context() as db:
        attrs = host.view_for(user, AccessResolver(db, user)).context.attrs
    return list(attrs["read"] or []), list(attrs["manage"] or [])


def test_a_catalog_is_read_only_when_every_schema_under_it_is(users, world, team, grant_factory):
    bob, cat = users["bob"], world.catalog
    grant_factory("catalog_namespace", world.sales_id, team, permission="use", granted_by=users["alice"].id)
    read, manage = _paths(bob, world)
    assert f"{cat}.sales" in read and cat not in read and f"{cat}.crm" not in read and manage == []

    _set(db_models.CatalogNamespace, world.catalog_id, is_public=True)
    read, _ = _paths(bob, world)
    assert cat not in read, "a public catalog with a private schema under it was handed over whole"

    grant_factory("catalog_namespace", world.crm_id, team, permission="use", granted_by=users["alice"].id)
    read, _ = _paths(bob, world)
    assert cat in read


def test_a_catalog_is_managed_only_when_every_schema_under_it_is(users, world, team, grant_factory):
    bob, cat = users["bob"], world.catalog
    _set(db_models.CatalogNamespace, world.catalog_id, owner_id=bob.id)
    _set(db_models.CatalogNamespace, world.sales_id, owner_id=bob.id)
    read, manage = _paths(bob, world)
    assert f"{cat}.sales" in manage and cat not in manage and f"{cat}.crm" not in manage

    grant_factory("catalog_namespace", world.crm_id, team, permission="manage", granted_by=users["alice"].id)
    _read, manage = _paths(bob, world)
    assert cat in manage


def test_a_join_to_a_table_the_caller_cannot_read_is_refused_without_naming_its_columns(
    users, client_for, world, team, grant_factory
):
    grant_factory("catalog_namespace", world.crm_id, team, permission="manage", granted_by=users["alice"].id)
    bob = client_for("bob")
    answers = []
    for column in ("no_such_column", "customer_id"):
        join = {
            "kind": "join",
            "name": f"probe_{column}",
            "from_table_id": world.orders_id,
            "to_table_id": world.customers_id,
            "from_columns": [column],
            "to_columns": ["customer_id"],
        }
        response = bob.post(f"{_base(world.crm_id)}/proposals", json={"proposals": [join]})
        answers.append((response.status_code, response.json()["detail"]))
    assert (
        answers[0]
        == answers[1]
        == (422, f"Table id={world.orders_id} is not in this catalog; a join cannot cross catalogs")
    )


def _tree_node(client, ns_id: int) -> dict | None:
    def find(nodes):
        for node in nodes:
            if node["id"] == ns_id:
                return node
            found = find(node["children"])
            if found:
                return found
        return None

    return find(client.get("/catalog/namespaces/tree").json())


def test_a_public_catalog_shows_neither_the_tables_nor_the_counts_of_its_private_schemas(users, client_for, world):
    _set(db_models.CatalogNamespace, world.catalog_id, is_public=True)
    say(world.field("orders", "region"), "description", "PRIVATEWORDS", user_id=users["alice"].id)
    carol = client_for("carol")

    model = carol.get(_base(world.catalog_id))
    assert model.status_code == 200, model.text
    assert model.json()["tables"] == [], "the catalog model listed the tables of schemas the caller cannot open"
    assert "card_number" not in model.text

    node = _tree_node(carol, world.catalog_id)
    assert node is not None and node["children"] == []
    assert node["semantic_model"] == {"accepted": 0, "pending": 0, "stale": 0}
    assert _tree_node(client_for("alice"), world.catalog_id)["semantic_model"]["accepted"] >= 1


def test_a_table_granted_alone_counts_only_its_own_facts(users, client_for, world, team, grant_factory):
    say(world.field("orders", "region"), "description", "orders words", user_id=users["alice"].id)
    say(world.field("customers", "segment"), "description", "customers words", user_id=users["alice"].id)
    grant_factory("catalog_table", world.orders_id, team, permission="use", granted_by=users["alice"].id)
    bob = client_for("bob")
    sales = _tree_node(bob, world.sales_id)
    [orders] = sales["tables"]
    assert sales["semantic_model"] == orders["semantic_model"]
    assert _tree_node(bob, world.crm_id) is None
    assert _tree_node(bob, world.catalog_id)["semantic_model"] == orders["semantic_model"]
