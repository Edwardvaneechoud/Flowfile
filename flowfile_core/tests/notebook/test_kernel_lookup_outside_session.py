"""Flowfile in a kernel outside a notebook session (a Catalog-page notebook cell) reads the catalog through core.

A kernel container holds no catalog database. ``lookup.current_lookup`` sees ``FLOWFILE_KERNEL_ID`` and, with no
session hook set, answers every metadata read through ``flowfile_frame._metadata.kernel_lookup``: a lookup with no
flow, which core answers for the kernel's owner with table paths as the kernel sees them.
"""

from __future__ import annotations

import contextvars
import os
import threading

import pytest
from fastapi import HTTPException

from flowfile_core.auth.models import User as PydanticUser
from flowfile_core.notebook import kernel_runner, lookup
from tests.notebook.conftest import IN_KERNEL_OP, NOTEBOOK_OWNER_ID

DEMO_SCHEMA = "Demo.sales_analytics"


@pytest.fixture
def in_kernel(kernel_sim, monkeypatch):
    """This process as a kernel container of ``kernel_sim``; core's answers run without the kernel's variable.

    The sim shares core's engine, so the database guard is recorded here and not installed.
    """
    from flowfile_frame import notebook_kernel

    refused: list[bool] = []

    def as_core(body: dict) -> dict:
        with monkeypatch.context() as core:
            core.delenv("FLOWFILE_KERNEL_ID")
            return kernel_sim.lookup(body)

    monkeypatch.setenv("FLOWFILE_KERNEL_ID", kernel_sim.kernel.id)
    monkeypatch.setattr(notebook_kernel, "lookup_transport", as_core)
    monkeypatch.setattr(notebook_kernel, "_refuse_database", lambda: refused.append(True))
    kernel_sim.refused = refused
    return kernel_sim


def _as_kernel_code(call):
    """Run ``call`` as code in the kernel (``IN_KERNEL_OP``), on a thread of its own, and return its result."""
    done: list = []

    def run():
        IN_KERNEL_OP.set(True)
        try:
            done.append((True, call()))
        except BaseException as exc:
            done.append((False, exc))

    thread = threading.Thread(target=contextvars.copy_context().run, args=(run,))
    thread.start()
    thread.join()
    ok, value = done[0]
    if not ok:
        raise value
    return value


def test_outside_a_kernel_nothing_is_asked(monkeypatch):
    monkeypatch.delenv("FLOWFILE_KERNEL_ID", raising=False)
    assert lookup.current_lookup() is None


def test_a_session_hook_wins_over_the_kernel_lookup(in_kernel):
    from flowfile_frame import _metadata

    assert lookup.current_lookup() is _metadata.kernel_lookup
    with _metadata.installed(7, lambda body: {"result": body["flow_id"]}) as hook:
        assert lookup.current_lookup() is hook
    assert lookup.current_lookup() is _metadata.kernel_lookup


def test_the_catalog_functions_ask_core_and_open_no_database(notebook_corpus, in_kernel, kernel_db_opens):
    """The issue's line: a catalog, a schema and its tables, with no session open on the kernel."""
    import flowfile as ff

    def cell():
        schema = ff.get_catalog("Demo").get_schema(DEMO_SCHEMA.split(".")[1])
        return sorted(table.name for table in schema.list_tables()), sorted(c.name for c in ff.list_catalogs())

    tables, catalogs = _as_kernel_code(cell)
    assert tables == ["regions", "sales"] and "Demo" in catalogs
    assert in_kernel.lookups and all(body["flow_id"] is None for body in in_kernel.lookups)
    assert {body["kind"] for body in in_kernel.lookups} >= {"namespace_by_name", "namespaces", "tables"}
    assert in_kernel.refused, "the kernel's engine refuses connections once flowfile asked core"
    assert not kernel_db_opens, f"kernel code opened the catalog database from {sorted(set(kernel_db_opens))}"


def test_a_catalog_table_is_read_in_the_kernel_from_the_path_core_answers(notebook_corpus, in_kernel, kernel_db_opens):
    import flowfile as ff

    def cell():
        sales = ff.get_catalog("Demo").get_schema(DEMO_SCHEMA.split(".")[1]).read_table("sales")
        return sales.columns, sales.collect().height

    columns, rows = _as_kernel_code(cell)
    assert columns and rows > 0
    assert "catalog_table" in {body["kind"] for body in in_kernel.lookups}
    assert not in_kernel.node_runs, "no session: the kernel reads the table itself"
    assert not kernel_db_opens, f"kernel code opened the catalog database from {sorted(set(kernel_db_opens))}"


def test_a_lookup_without_a_flow_needs_only_the_owner(notebook_corpus, kernel_sim, monkeypatch):
    owner = PydanticUser(username="owner", id=kernel_sim.owner_id, disabled=False)
    other = PydanticUser(username="other", id=kernel_sim.owner_id + 1, disabled=False)
    body = lookup.LookupRequest(kind="default_namespace_id")
    assert body.flow_id is None
    assert isinstance(lookup.answer_request(kernel_sim.kernel.id, owner, body)["result"], int)
    with pytest.raises(HTTPException) as not_owner:
        lookup.answer_request(kernel_sim.kernel.id, other, body)
    assert not_owner.value.status_code == 403
    with pytest.raises(HTTPException) as unknown_kernel:
        lookup.answer_request("another-kernel", owner, body)
    assert unknown_kernel.value.status_code == 403
    monkeypatch.setenv("FLOWFILE_MODE", "docker")
    with pytest.raises(HTTPException) as shared_install:
        lookup.answer_request(kernel_sim.kernel.id, owner, body)
    assert shared_install.value.status_code == 403


def test_table_paths_are_the_kernels_only_outside_a_session(notebook_corpus, kernel_sim, monkeypatch):
    """A session's catalog tables are read by core, so it keeps core's paths; other kernel code reads its mount."""
    from flowfile_frame import _metadata

    owner = PydanticUser(username="owner", id=kernel_sim.owner_id, disabled=False)
    schema_id = _metadata.local_namespace_id_by_full_name(DEMO_SCHEMA, user_id=NOTEBOOK_OWNER_ID)
    monkeypatch.setattr(kernel_sim, "to_kernel_path", lambda path: f"/catalog_tables/{os.path.basename(path)}")
    table = {"node_id": 1, "catalog_table_name": "sales", "catalog_namespace_id": schema_id}

    def ask(kind: str, args: dict, flow_id: int | None) -> dict:
        body = lookup.LookupRequest(flow_id=flow_id, kind=kind, args=args)
        return lookup.answer_request(kernel_sim.kernel.id, owner, body)["result"]

    kernel_runner._sessions[42] = {kernel_sim.kernel.id: None}
    held = ask("catalog_table", table, 42)["file_path"]
    assert held and not held.startswith("/catalog_tables/")
    assert ask("catalog_table", table, None)["file_path"] == f"/catalog_tables/{os.path.basename(held)}"
    held_sql = ask("catalog_sql_tables", {"node_id": 1}, 42)["table_paths"]
    seen = ask("catalog_sql_tables", {"node_id": 1}, None)["table_paths"]
    assert held_sql and set(seen) == set(held_sql)
    assert all(path.startswith("/catalog_tables/") for path in seen.values())
