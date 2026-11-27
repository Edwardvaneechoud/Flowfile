"""Migration 046 is the only writer of the ledger's schema: opening and using the ledger at run time creates, alters
and drops nothing, and a database without the ledger's tables is refused, not given them."""

from __future__ import annotations

import re

import pytest
from sqlalchemy import create_engine, event, inspect

from flowfile_core.catalog.semantic import confidentiality, host
from flowfile_core.database import connection
from tests.semantic.world import World, say

_DDL = re.compile(r"^\s*(CREATE|ALTER|DROP)\b", re.IGNORECASE)


def test_opening_and_using_the_ledger_changes_no_schema(world: World, client) -> None:
    statements: list[str] = []

    def record(_conn, _cursor, statement, *_args) -> None:
        if _DDL.match(statement):
            statements.append(statement)

    event.listen(connection.engine, "before_cursor_execute", record)
    try:
        with host._lock:
            if host._host is not None:
                host._host.embedded.close()
            host._host = None
        host.host()
        assert client.post(f"/catalog/namespaces/{world.sales_id}/semantic-model/sync").status_code == 200
        assert client.get(f"/catalog/namespaces/{world.sales_id}/semantic-model").status_code == 200
        say(world.field("orders", "amount"), "checks", [{"check": "not_null"}])
        ran = client.post(f"/catalog/namespaces/{world.sales_id}/semantic-model/data-rules/run")
        assert ran.status_code == 200 and ran.json()["run_id"], ran.text
        with connection.get_db_context() as db:
            confidentiality.read_withheld(db, [world.orders_id])
    finally:
        event.remove(connection.engine, "before_cursor_execute", record)
    assert statements == []


def test_a_database_without_the_ledger_is_refused_and_left_alone(tmp_path, monkeypatch) -> None:
    from glosslayer.schema import SchemaNotReady

    empty = create_engine(f"sqlite:///{tmp_path / 'empty.db'}")
    monkeypatch.setattr(connection, "engine", empty)
    with pytest.raises(SchemaNotReady):
        host._build(sharing_on=False)
    with connection.get_db_context() as db:
        found = confidentiality.read_withheld(db, [1, 2])
    assert found.unreadable == {1, 2}, "a ledger that is not there must keep every table from AI"
    assert inspect(empty).get_table_names() == []
    empty.dispose()
