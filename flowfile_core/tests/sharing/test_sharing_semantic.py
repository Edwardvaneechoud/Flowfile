"""Team review of a semantic model in docker mode: who may propose, who may accept, who sees which facts.

The ledger is the test database's, built with team review (``sharing_enabled()``); the catalog's own grants decide.
No SQL runs here: proposals of descriptions need none.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import polars as pl
import pytest

from flowfile_core.ai import semantic_routes
from flowfile_core.catalog.access import AccessResolver
from flowfile_core.catalog.semantic import host
from flowfile_core.catalog.service import CatalogService
from flowfile_core.database import models as db_models
from flowfile_core.database.connection import get_db_context
from tests.semantic.conftest import RecordingProvider
from tests.semantic.world import World, add_table, build_world, drop_world, reset_process_state, say


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
    """Bob owns a table in alice's public schema: he proposes, alice (who owns the schema) decides."""
    _set(db_models.CatalogNamespace, world.sales_id, is_public=True)
    _set(db_models.CatalogTable, world.orders_id, owner_id=users["bob"].id)
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
    ("post", "/data-rules/run", None),
    ("get", "/data-rules/results", None),
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


# A public schema is a container: a reader who neither owns it nor holds a grant on it reads its own facts and the
# tables in it they own or were granted, nothing about its other tables.

BOB_ORDERS = pl.DataFrame({"order_id": [1, 2, 3], "customer": ["a", "b", "c"], "total": [5.0, 7.0, 9.0]})
BOB_LINES = pl.DataFrame({"line_id": [1, 2, 3, 4], "order_id": [1, 1, 2, 3], "qty": [1, 2, 3, 4]})
HIDDEN_WORDS = ("ALICEHIDDEN", "alice_count", "card_number", "region")


@dataclass
class Public:
    world: World
    bob_orders: int
    bob_lines: int
    lines_to_orders: str

    def key(self, name: str) -> str:
        return f"{self.world.catalog}.sales.{name}"


def _join_body(name: str, from_id: int, to_id: int, column: str = "order_id", to_column: str = "order_id") -> dict:
    return {
        "kind": "join",
        "name": name,
        "from_table_id": from_id,
        "to_table_id": to_id,
        "from_columns": [column],
        "to_columns": [to_column],
    }


def _accept_all(client, ns_id: int) -> None:
    for entry in client.get(f"{_base(ns_id)}/review").json():
        body = {"action": "accept", "group": entry["group"]} if entry["group_facts"] else None
        response = client.post(f"{_base(ns_id)}/review", json=body or {"action": "accept", "ids": [entry["id"]]})
        assert response.status_code == 200, response.text


@pytest.fixture
def public(users, world, client_for) -> Public:
    """Alice's catalog and its ``sales`` schema are public; bob owns two tables in ``sales`` and nothing else.

    Alice owns the schema, so she reads it whole: her facts on her ``orders`` table, a metric on it, a join from
    bob's ``bob_lines`` to ``orders`` and one from ``orders`` to ``crm.customers`` are all accepted.
    """
    bob, alice = users["bob"].id, users["alice"].id
    for ns_id in (world.catalog_id, world.sales_id):
        _set(db_models.CatalogNamespace, ns_id, is_public=True)
    orders_b = add_table(world, world.sales_id, "bob_orders", BOB_ORDERS, bob)
    lines_b = add_table(world, world.sales_id, "bob_lines", BOB_LINES, bob)
    say(world.field("orders", "region"), "description", "ALICEHIDDEN region words", user_id=alice)
    say(world.field("orders", "region"), "synonyms", ["ALICEHIDDENSYN"], user_id=alice)
    say(world.field("orders", "region"), "role", "category", user_id=alice)
    say(world.dataset("orders"), "description", "ALICEHIDDEN table words", user_id=alice)
    say(f"model:{world.catalog}.sales", "description", "SCHEMAOWN words", user_id=alice)
    alice_client = client_for("alice")
    metric = {"kind": "metric", "table_id": world.orders_id, "name": "alice_count", "expression": "COUNT(*)"}
    joins = [_join_body("lines_orders", lines_b, world.orders_id)]
    for ns_id, proposals in ((world.sales_id, [metric, *joins]), (world.catalog_id, [_join(world)])):
        response = alice_client.post(f"{_base(ns_id)}/proposals", json={"proposals": proposals})
        assert response.status_code == 200, response.text
        _accept_all(alice_client, ns_id)
    ai = host.host().embedded.view(user_id=alice, admin=True, via="ai:test")
    ai.propose(world.field("orders", "amount"), "description", "ALICEHIDDEN waiting", source="ai:test")
    return Public(world, orders_b, lines_b, f"relationship:{world.catalog}.sales::lines_orders")


def _join(world: World) -> dict:
    return _join_body("orders_customers", world.orders_id, world.customers_id, "customer_id", "customer_id")


def _assert_hidden_out(text: str) -> None:
    for word in HIDDEN_WORDS:
        assert word not in text, f"{word!r} of a table the reader cannot open was answered"


def test_a_public_schema_model_shows_its_own_facts_and_the_readers_tables_only(users, client_for, public):
    world, bob = public.world, client_for("bob")
    model = bob.get(_base(world.sales_id))
    assert model.status_code == 200, model.text
    body = model.json()
    assert sorted(t["name"] for t in body["tables"]) == ["bob_lines", "bob_orders"]
    assert body["description"] == "SCHEMAOWN words" and body["joins"] == []
    _assert_hidden_out(model.text)
    assert body["progress"]["accepted"] == 1, "only the schema's own fact counts"

    catalog = bob.get(_base(world.catalog_id))
    assert catalog.status_code == 200, catalog.text
    assert sorted(t["name"] for t in catalog.json()["tables"]) == ["bob_lines", "bob_orders"]
    assert catalog.json()["joins"] == []
    _assert_hidden_out(catalog.text)

    owner = client_for("alice").get(_base(world.sales_id)).json()
    assert {"orders", "bob_lines", "bob_orders"} == {t["name"] for t in owner["tables"]}
    assert "lines_orders" in [j["name"] for j in owner["joins"]]


def test_a_catalog_grant_reads_its_public_schemas_whole(users, client_for, public, team, grant_factory):
    grant_factory("catalog_namespace", public.world.catalog_id, team, permission="use", granted_by=users["alice"].id)
    model = client_for("bob").get(_base(public.world.sales_id))
    assert model.status_code == 200, model.text
    assert "orders" in {t["name"] for t in model.json()["tables"]} and "ALICEHIDDEN" in model.text


def test_the_queue_history_export_join_routes_and_sync_hold_nothing_of_a_hidden_table(users, client_for, public, world):
    bob, admin = client_for("bob"), client_for("admin")
    describe = {"kind": "field", "table_id": public.bob_orders, "column": "customer", "attribute": "description"}
    described = bob.post(f"{_base(world.sales_id)}/proposals", json={"proposals": [describe | {"value": "BOBWORDS"}]})
    assert described.status_code == 200, described.text
    _accept_all(admin, world.sales_id)

    queue = bob.get(f"{_base(world.sales_id)}/review")
    assert queue.status_code == 200 and queue.json() == [], queue.text

    for subject in (world.field("orders", "region"), world.field("customers", "segment")):
        history = bob.get(f"{_base(world.sales_id)}/history", params={"subject": subject})
        assert (history.status_code, history.json()["detail"]) == (
            404,
            f"{subject!r} is not part of this semantic model",
        ), "a hidden table answers as a table of another model does"

    export = bob.get(f"{_base(world.sales_id)}/export")
    assert export.status_code == 200, export.text
    assert "BOBWORDS" in export.text and "SCHEMAOWN" in export.text
    _assert_hidden_out(export.text)
    assert "lines_orders" not in export.text

    checked = bob.post(f"{_base(world.sales_id)}/joins/check")
    assert checked.status_code == 200 and checked.json() == [], checked.text
    dependents = bob.get(f"{_base(world.sales_id)}/joins/dependents", params={"key": public.lines_to_orders})
    assert dependents.status_code == 404

    hidden_new = add_table(world, world.sales_id, "alice_new", BOB_ORDERS, users["alice"].id)
    own_new = add_table(world, world.sales_id, "bob_new", BOB_ORDERS, users["bob"].id)
    assert hidden_new and own_new
    with get_db_context() as db:
        db.query(db_models.CatalogTable).filter(db_models.CatalogTable.id.in_([hidden_new, own_new])).update(
            {"schema_json": json.dumps([{"name": "order_id", "dtype": "Int64"}])}, synchronize_session=False
        )
        db.commit()
    synced = bob.post(f"{_base(world.sales_id)}/sync")
    assert synced.status_code == 200, synced.text
    named = json.dumps(synced.json())
    assert public.key("bob_new") in named and public.key("alice_new") not in named


def test_proposing_on_or_towards_a_hidden_table_is_refused_as_for_a_table_that_is_not_there(client_for, public):
    world, bob = public.world, client_for("bob")

    def answer(*proposals: dict) -> tuple[int, str]:
        response = bob.post(f"{_base(world.sales_id)}/proposals", json={"proposals": list(proposals)})
        return response.status_code, response.json()["detail"]

    hidden, missing = world.orders_id, 987654
    field = {"kind": "field", "column": "region", "attribute": "description", "value": "x"}
    metric = {"kind": "metric", "name": "probe", "expression": "COUNT(*)"}
    for body in (field, metric):
        assert answer(body | {"table_id": hidden}) == (422, f"Table id={hidden} is not part of this semantic model")
        assert answer(body | {"table_id": missing})[0] == 422
    for right, wrong in (
        (_join_body("p1", public.bob_lines, hidden), _join_body("p2", public.bob_lines, hidden, to_column="nope")),
        (_join_body("p3", hidden, public.bob_orders), _join_body("p4", hidden, public.bob_orders, column="nope")),
    ):
        assert (
            answer(right)
            == answer(wrong)
            == (
                422,
                f"Table id={hidden} is not in this catalog; a join cannot cross catalogs",
            )
        )


@pytest.fixture
def sql_seen(monkeypatch) -> list[str]:
    """Every statement the catalog SQL path runs, recorded on its way to the worker (it still runs)."""
    seen: list[str] = []
    real = CatalogService.execute_sql_query

    def record(self, query, *args, **kwargs):
        seen.append(query)
        return real(self, query, *args, **kwargs)

    monkeypatch.setattr(CatalogService, "execute_sql_query", record)
    return seen


def _provider(monkeypatch, answer) -> RecordingProvider:
    provider = RecordingProvider(answer)
    monkeypatch.setattr(semantic_routes, "resolve_semantic_provider", lambda *_a, **_k: provider)
    return provider


def test_no_ai_surface_and_no_statistics_query_reaches_a_hidden_table(users, client_for, public, monkeypatch, sql_seen):
    world, bob, admin = public.world, client_for("bob"), client_for("admin")
    hidden_table = f'"{world.key("orders")}"'
    metric = {"kind": "metric", "table_id": public.bob_lines, "name": "line_qty", "expression": "SUM(qty)"}
    assert bob.post(f"{_base(world.sales_id)}/proposals", json={"proposals": [metric]}).status_code == 200
    _accept_all(admin, world.sales_id)

    columns = lambda m: {"table": "One row.", "columns": {}, "metrics": {}}  # noqa: E731
    for path, body, answer in (
        ("/ai/semantic/draft", {"namespace_id": world.sales_id}, columns),
        ("/ai/semantic/ask", {"namespace_id": world.sales_id, "question": "total quantity by area"}, None),
        ("/ai/semantic/suggest", {"namespace_id": world.sales_id, "request": "more joins"}, None),
    ):
        provider = _provider(monkeypatch, answer or (lambda m: {"nothing": True, "no_metric": True, "why": "x"}))
        response = bob.post(path, json=body)
        assert response.status_code == 200, (path, response.text)
        assert provider.calls, path
        _assert_hidden_out(provider.sent)
        assert "lines_orders" not in provider.sent, path
        if path != "/ai/semantic/draft":
            assert "line_qty" in provider.sent, path

    rules = bob.post(f"{_base(world.sales_id)}/suggest")
    assert rules.status_code == 200, rules.text
    assert sql_seen and not [q for q in sql_seen if hidden_table in q], "a statistics query read a hidden table"
    queue = bob.get(f"{_base(world.sales_id)}/review").json()
    assert queue and public.lines_to_orders not in {e["subject"] for e in queue}
    _assert_hidden_out(json.dumps(queue))

    reached = bob.post(f"{_base(world.sales_id)}/query", json={"metric": "line_qty", "dimensions": ["orders.region"]})
    assert reached.status_code == 422, "a metric query reached a hidden table through a join"


def _counts(node: dict) -> dict:
    return node["semantic_model"]


def test_a_public_schema_counts_only_what_its_reader_sees(users, client_for, public):
    world = public.world
    say(f"field:{public.key('bob_orders')}/customer", "description", "BOBWORDS", user_id=users["bob"].id)
    bob = client_for("bob")
    sales = _tree_node(bob, world.sales_id)
    assert sorted(t["name"] for t in sales["tables"]) == ["bob_lines", "bob_orders"]
    totals = {k: sum(t["semantic_model"][k] for t in sales["tables"]) for k in ("accepted", "pending", "stale")}
    assert _counts(sales) == totals
    assert _counts(_tree_node(bob, world.catalog_id)) == totals
    assert _counts(_tree_node(client_for("alice"), world.sales_id))["accepted"] > totals["accepted"]


def test_a_readers_own_tables_in_a_public_schema_work_end_to_end(users, client_for, public):
    world, bob, admin = public.world, client_for("bob"), client_for("admin")
    proposals = [
        {
            "kind": "field",
            "table_id": public.bob_orders,
            "column": "customer",
            "attribute": "description",
            "value": "Who placed the order.",
        },
        _join_body("lines_bob_orders", public.bob_lines, public.bob_orders),
        {"kind": "metric", "table_id": public.bob_lines, "name": "quantity", "expression": "SUM(qty)"},
    ]
    stored = bob.post(f"{_base(world.sales_id)}/proposals", json={"proposals": proposals})
    assert stored.status_code == 200, stored.text
    assert {f["status"] for f in stored.json()} == {"proposed"}

    model = bob.get(_base(world.sales_id)).json()
    assert [j["name"] for j in model["joins"]] == ["lines_bob_orders"]
    [lines] = [t for t in model["tables"] if t["name"] == "bob_lines"]
    assert [m["name"] for m in lines["metrics"]] == ["quantity"]
    queue = bob.get(f"{_base(world.sales_id)}/review").json()
    assert len(queue) == 3
    assert model["can_manage"] is False
    assert _decide(bob, world.sales_id, "accept", queue[0]["id"]).status_code == 403

    _accept_all(admin, world.sales_id)
    model = bob.get(_base(world.sales_id)).json()
    assert model["joins"][0]["status"] == "accepted"
    ran = bob.post(f"{_base(world.sales_id)}/query", json={"metric": "quantity", "dimensions": ["bob_orders.customer"]})
    assert ran.status_code == 200 and ran.json()["error"] is None, ran.text


def test_a_join_name_a_hidden_join_holds_is_not_taken_over(users, client_for, public):
    """A join's subject is its schema plus its name, so a reader's join named like a hidden one would land on it."""
    world, bob = public.world, client_for("bob")
    same_name = _join_body("lines_orders", public.bob_lines, public.bob_orders)
    response = bob.post(f"{_base(world.sales_id)}/proposals", json={"proposals": [same_name]})
    refused = (409, "A join called 'lines_orders' already exists in this model")
    assert (response.status_code, response.json()["detail"]) == refused

    # the rules name the join they find between bob's tables like this; alice already holds the name
    alice = client_for("alice")
    taken = _join_body("bob_lines_bob_orders", public.bob_lines, world.orders_id)
    assert alice.post(f"{_base(world.sales_id)}/proposals", json={"proposals": [taken]}).status_code == 200
    assert bob.post(f"{_base(world.sales_id)}/suggest", json={"kinds": ["join"]}).status_code == 200
    from glosslayer.store.base import Query

    for subject in (public.lines_to_orders, f"relationship:{world.catalog}.sales::bob_lines_bob_orders"):
        held = host.host().gloss.store.find(Query(subject=subject))
        assert {a.source for a in held} == {f"human:{users['alice'].id}"}, f"a suggestion was stored on {subject}"


def test_data_rules_run_and_answer_only_for_tables_the_reader_may_open(users, client_for, public, monkeypatch):
    """A queued rule does not run, a reader's run scans none of the tables hidden from them, and the results of a
    hidden table's rules, whoever ran them, are not answered to that reader."""
    world, bob, admin = public.world, client_for("bob"), client_for("admin")
    say(world.field("orders", "amount"), "checks", [{"check": "between", "max": 8}], user_id=users["alice"].id)
    scanned: list[int] = []
    real = CatalogService.stored_table_plan

    def record(self, table_id, user_id, backend=None):
        scanned.append(table_id)
        return real(self, table_id, user_id, backend)

    monkeypatch.setattr(CatalogService, "stored_table_plan", record)
    run, results = f"{_base(world.sales_id)}/data-rules/run", f"{_base(world.sales_id)}/data-rules/results"
    rule = {
        "kind": "field",
        "table_id": public.bob_orders,
        "column": "total",
        "attribute": "checks",
        "value": [{"check": "between", "max": 7}],
    }
    stored = bob.post(f"{_base(world.sales_id)}/proposals", json={"proposals": [rule]})
    assert stored.status_code == 200 and stored.json()[0]["status"] == "proposed", stored.text
    waiting = bob.post(run)
    assert waiting.status_code == 200 and waiting.json() == {"run_id": None, "tables": []}, waiting.text
    assert scanned == []

    _accept_all(admin, world.sales_id)
    ran = bob.post(run)
    assert ran.status_code == 200, ran.text
    [table] = ran.json()["tables"]
    [result] = table["results"]
    assert (table["table_id"], result["failed_rows"], result["total_rows"]) == (public.bob_orders, 1, 3)
    assert scanned == [public.bob_orders]

    everything = admin.post(run)
    assert everything.status_code == 200, everything.text
    assert {t["table_id"] for t in everything.json()["tables"]} == {world.orders_id, public.bob_orders}
    assert {t["table_id"] for t in admin.get(results).json()} == {world.orders_id, public.bob_orders}
    assert [t["table_id"] for t in bob.get(results).json()] == [public.bob_orders]
    hidden = bob.post(run, json={"table_ids": [world.orders_id]})
    assert hidden.status_code == 422, hidden.text
