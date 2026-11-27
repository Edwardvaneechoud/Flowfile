"""The seam between the catalog and the ledger (``catalog/semantic/host.py``): a catalog change never fails for the
ledger's sake, a rename made through the catalog moves the facts, and nothing is written inside a project apply."""

from __future__ import annotations

import logging
import sys

import pytest

from flowfile_core.catalog.semantic import host
from flowfile_core.database.connection import apply_transaction
from tests.semantic.world import World, say

HOOKS = [
    ("on_table_written", lambda w: (w.key("orders"),)),
    ("on_table_renamed", lambda w: (w.key("orders"), f"{w.catalog}.sales.renamed")),
    ("on_namespace_renamed", lambda w: (f"{w.catalog}.sales", f"{w.catalog}.shop", [])),
    ("on_table_deleted", lambda w: (w.key("orders"),)),
]


def _facts(subject: str) -> list:
    from glosslayer.store.base import Query

    return host.host().gloss.store.find(Query(subject=subject))


@pytest.mark.parametrize(("hook", "args"), HOOKS, ids=[h for h, _ in HOOKS])
def test_a_hook_never_raises_when_the_ledger_fails(world: World, monkeypatch, caplog, hook, args) -> None:
    def broken() -> None:
        raise RuntimeError("ledger down")

    monkeypatch.setattr(host, "host", broken)
    with caplog.at_level(logging.WARNING, logger=host.logger.name):
        getattr(host, hook)(*args(world))
    assert any("Semantic ledger: could not" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize(("hook", "args"), HOOKS, ids=[h for h, _ in HOOKS])
def test_a_hook_never_raises_without_the_plugin(world: World, monkeypatch, caplog, hook, args) -> None:
    monkeypatch.setattr(host, "_host", None)
    monkeypatch.setattr(host, "_missing_logged", False)
    monkeypatch.setitem(sys.modules, "glosslayer_flowfile", None)
    with caplog.at_level(logging.WARNING, logger=host.logger.name):
        getattr(host, hook)(*args(world))
        getattr(host, hook)(*args(world))
    said = [r for r in caplog.records if "not installed" in r.getMessage()]
    assert len(said) == 1, "a missing plugin is said once, not on every catalog change"


def test_a_table_renamed_and_moved_through_the_catalog_takes_its_facts(world: World, client) -> None:
    fact = say(world.field("orders", "region"), "description", "Where the order ships to.")
    renamed = client.put(f"/catalog/tables/{world.orders_id}", json={"name": "purchases"})
    assert renamed.status_code == 200, renamed.text
    moved = _facts(f"field:{world.catalog}.sales.purchases/region")
    assert [a.value for a in moved if a.attribute == "description"] == ["Where the order ships to."]
    assert _facts(world.field("orders", "region")) == []

    model = client.get(f"/catalog/namespaces/{world.sales_id}/semantic-model").json()
    [table] = model["tables"]
    [region] = [f for f in table["fields"] if f["name"] == "region"]
    assert table["name"] == "purchases" and region["facts"]["description"]["id"] == fact.id

    back = client.put(f"/catalog/tables/{world.orders_id}", json={"name": "orders"})
    assert back.status_code == 200, back.text
    assert [a.id for a in _facts(world.field("orders", "region")) if a.attribute == "description"] == [fact.id]


def test_a_renamed_schema_takes_the_facts_of_its_tables(world: World, client) -> None:
    say(world.field("orders", "amount"), "description", "What the order cost.")
    response = client.put(f"/catalog/namespaces/{world.sales_id}", json={"name": "shop"})
    assert response.status_code == 200, response.text
    try:
        moved = _facts(f"field:{world.catalog}.shop.orders/amount")
        assert [a.value for a in moved if a.attribute == "description"] == ["What the order cost."]
        assert _facts(world.field("orders", "amount")) == []
    finally:
        assert client.put(f"/catalog/namespaces/{world.sales_id}", json={"name": "sales"}).status_code == 200
    assert [a.attribute for a in _facts(world.field("orders", "amount"))].count("description") == 1


def test_no_hook_writes_inside_a_project_apply(world: World, caplog) -> None:
    fact = say(world.field("orders", "region"), "description", "Where the order ships to.")
    with caplog.at_level(logging.ERROR, logger=host.logger.name), apply_transaction():
        for hook, args in HOOKS:
            getattr(host, hook)(*args(world))
    [kept] = [a for a in _facts(world.field("orders", "region")) if a.id == fact.id]
    assert kept.status.value == "accepted", "a hook inside an apply changed the ledger"
    assert _facts(f"field:{world.catalog}.sales.renamed/region") == []
    lost = [r.getMessage() for r in caplog.records if "inside a project apply" in r.getMessage()]
    assert len(lost) == 2, "a rename that is not recorded is said at error level"
