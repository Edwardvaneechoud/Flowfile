"""A table of a catalog kept elsewhere (Apache Polaris) in a notebook session on a real notebook kernel container.

The kernel holds no catalog database and no secret: a cell's ``ff.read_catalog_table`` takes the table's columns from
core's ``catalog_table`` lookup, and its rows are read by core (``POST /notebook/session/node_run``), as every catalog
reader in a session is. A writer a cell builds is placed, never run there: asking its rows says Push, and once the
session's cells are pushed the canvas run writes the table.

Run it with the kernel image built from this checkout and the catalog rig up::

    make notebook_kernel_dev
    python -m test_utils.iceberg_rest start
    export FLOWFILE_STORAGE_DIR=<scratch>
    SKIP_WORKER_TESTS=0 pytest flowfile_core/tests/notebook/test_kernel_notebook_outside_catalog.py

Every kernel it starts has an id of its own (``iceberg-k-<hex>``), never takes host port 19000, and only its own
container is removed.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
import subprocess
import threading
import time
import uuid

import pytest

from flowfile_core.notebook.render import render
from shared.notebook_display import TABLE_MIME
from test_utils.iceberg_rest import fixtures
from test_utils.iceberg_rest.pytest_fixtures import (
    SecretValues,
    added_catalog,
    clean_aws,
    namespace_on,
    orders_frame as _orders,
    principal,
    secret_findings,
    seed_orders,
    signed,
)
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, cell_provenance

IMAGE = os.environ.get("FLOWFILE_TEST_NOTEBOOK_IMAGE", "flowfile-kernel-notebook:local")
"""The notebook image the kernels run; a published one (``edwardvaneechoud/flowfile-kernel-notebook:<version>``) shows
which reads work without a kernel release."""
LOOPBACK = ("127.0.0.1", 50124)
RESERVED_PORTS = {19000}
"""Host ports a kernel started here never takes: a kernel of the desktop app may come back on them."""


def _image_present() -> bool:
    try:
        return subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode == 0
    except OSError:
        return False


def _missing() -> str | None:
    if not os.environ.get("FLOWFILE_STORAGE_DIR"):
        return "needs a scratch FLOWFILE_STORAGE_DIR, since the kernel's shared folder is under it"
    if not _image_present():
        return f"Docker or the {IMAGE} image is missing; build it with make notebook_kernel_dev"
    if not fixtures.signed_catalog_ready():
        return fixtures.SIGNED_SKIP_REASON
    return None


MISSING = _missing()
pytestmark = [pytest.mark.kernel, pytest.mark.iceberg_rest, pytest.mark.skipif(bool(MISSING), reason=MISSING or "")]


@pytest.fixture(scope="module")
def schema(signed):
    """A namespace of this module's own on Polaris; dropped with its tables after the module."""
    with namespace_on(signed, "kernel") as name:
        yield name


@pytest.fixture(scope="module")
def polaris(principal, schema):
    """Polaris added to Flowfile's catalog with a client id and secret, by user 1: ``(name, catalog id, schema id)``."""
    with added_catalog(principal, schema) as added:
        yield added


@pytest.fixture(scope="module")
def orders(signed, schema):
    """``orders`` in the module's namespace, 1000 rows another engine (PyIceberg) wrote."""
    seed_orders(signed, f"{schema}.orders")
    return "orders"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def core_url(monkeypatch):
    """Core served on a free port, reachable from a kernel container as ``host.docker.internal``; it builds no kernel
    manager of its own and reclaims no container."""
    import uvicorn

    monkeypatch.setenv("FLOWFILE_KERNEL_GC", "0")
    monkeypatch.setenv("FLOWFILE_KERNEL_WARMUP", "0")
    from flowfile_core.main import app

    port = _free_port()
    server = uvicorn.Server(uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 30
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.2)
    monkeypatch.setenv("FLOWFILE_CORE_URL", f"http://host.docker.internal:{port}")
    yield f"http://127.0.0.1:{port}"
    server.should_exit = True
    thread.join(timeout=10)


def _container_exists(name: str) -> bool:
    command = ["docker", "ps", "-a", "--filter", f"name=^/{name}$", "--format", "{{.Names}}"]
    return subprocess.run(command, capture_output=True, text=True).stdout.strip() == name


@pytest.fixture
def start_kernel(core_url, monkeypatch):
    """Start a notebook kernel of ``IMAGE`` with an id of its own for ``owner_id``; its id.

    Only the containers of the kernels made here are stopped and removed; a container that already holds the name is
    a reason to stop, never one to replace.
    """
    import flowfile_core.kernel as kernel_package
    from flowfile_core.kernel import manager as manager_module
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import ImageFlavour, KernelConfig
    from flowfile_core.notebook import kernel_runner
    from shared.storage_config import storage

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    monkeypatch.setenv("FLOWFILE_KERNEL_IMAGE_NOTEBOOK", IMAGE)
    available = manager_module._is_port_available
    monkeypatch.setattr(
        manager_module, "_is_port_available", lambda port: port not in RESERVED_PORTS and available(port)
    )
    manager = KernelManager(shared_volume_path=str(storage.shared_directory))
    # This manager stops only what it starts here, whatever the registry adopted.
    manager._started_here.clear()
    monkeypatch.setattr(kernel_package, "get_kernel_manager", lambda: manager)
    loop = asyncio.new_event_loop()
    made: list[str] = []

    def _start(owner_id: int) -> str:
        kernel_id = f"iceberg-k-{uuid.uuid4().hex[:8]}"
        assert not _container_exists(f"flowfile-kernel-{kernel_id}"), kernel_id
        config = KernelConfig(id=kernel_id, name="Outside catalog", image_flavour=ImageFlavour.NOTEBOOK)
        loop.run_until_complete(manager.create_kernel(config, user_id=owner_id))
        made.append(kernel_id)
        print(f"started kernel {kernel_id}")
        loop.run_until_complete(manager.start_kernel(kernel_id))
        return kernel_id

    _start.manager = manager
    yield _start
    try:
        for kernel_id in made:
            with contextlib.suppress(Exception):
                loop.run_until_complete(manager.delete_kernel(kernel_id))
            subprocess.run(["docker", "rm", "-f", f"flowfile-kernel-{kernel_id}"], capture_output=True)
    finally:
        loop.close()
        kernel_runner._sessions.clear()
        kernel_runner._verified.clear()


@pytest.fixture
def recorded(monkeypatch):
    """Every lookup answer and held-run answer core gives a kernel, as JSON text, and the held runs' node types."""
    from flowfile_core.notebook import held_run, lookup

    seen = {"lookups": [], "runs": [], "run_types": []}
    answer_request, run_held_node = lookup.answer_request, held_run.run_held_node

    def answering(kernel_id, user, body):
        result = answer_request(kernel_id, user, body)
        seen["lookups"].append((body.kind, json.dumps(result, default=str)))
        return result

    def running(kernel_id, user, body):
        seen["run_types"].append((body.node.get("type"), body.schema_only))
        result = run_held_node(kernel_id, user, body)
        seen["runs"].append(json.dumps(result, default=str))
        return result

    monkeypatch.setattr(lookup, "answer_request", answering)
    monkeypatch.setattr(held_run, "run_held_node", running)
    return seen


@pytest.fixture
def registration(tmp_path):
    """A flow registration of this test's own: ``(registration id, path)``."""
    from flowfile_core.catalog import SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import FlowRegistration

    name = f"kernel_outside_{uuid.uuid4().hex[:6]}"
    path = tmp_path / f"{name}.yaml"
    with get_db_context() as db:
        made = FlowRegistration(name=name, flow_path=str(path), owner_id=NOTEBOOK_OWNER_ID)
        db.add(made)
        db.commit()
        registration_id = made.id
    yield registration_id, path
    with get_db_context() as db:
        SQLAlchemyCatalogRepository(db).delete_flow(registration_id)


def _execute(client, key: dict, cell_id: str, code: str) -> dict:
    response = client.post("/notebook/session/execute", json={**key, "cell_id": cell_id, "code": code})
    assert response.status_code == 200, response.text
    return response.json()


def _seen_by_pyiceberg(signed, identifier: str) -> list[int]:
    return sorted(signed.load_table(identifier).scan().to_arrow().column("id").to_pylist())


def test_a_kernel_session_reads_an_outside_table_through_core_and_writes_one_after_a_push(
    open_as, start_kernel, client_as, recorded, polaris, schema, orders, signed, principal, registration
):
    import flowfile as ff
    from shared.iceberg_utils import storage_properties, write_table

    name, _, schema_id = polaris
    namespace = f"{name}.{schema}"
    flow = open_as(ff.from_dict({"id": [1]}).flow_graph)
    kernel_id = start_kernel(NOTEBOOK_OWNER_ID)
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": flow.flow_id, "kernel_id": kernel_id}
    opened = client.post("/notebook/session/open", json=key)
    assert opened.status_code == 200, opened.text
    results: list[dict] = []

    read_cell = f"orders = ff.read_catalog_table({orders!r}, namespace_full_name={namespace!r})"
    built = _execute(client, key, "cell-orders", read_cell + "\nprint('columns', orders.columns)")
    results.append(built)
    assert built["success"], built.get("error")
    assert "columns ['id', 'grp', 'val', 'name']" in built["stdout"], built["stdout"]
    assert any(kind == "catalog_table" for kind, _ in recorded["lookups"]), [k for k, _ in recorded["lookups"]]

    counted = _execute(client, key, "cell-count", "print('rows', orders.collect().height)")
    results.append(counted)
    assert counted["success"], counted.get("error")
    assert "rows 1000" in counted["stdout"], counted["stdout"]
    assert ("catalog_reader", False) in recorded["run_types"], recorded["run_types"]

    write_table(signed.load_table(f"{schema}.{orders}"), _orders(1000, 1000), "append")
    kept = _execute(client, key, "cell-count", "print('rows', orders.collect().height)")
    results.append(kept)
    assert kept["success"] and "rows 1000" in kept["stdout"], "a frame keeps the rows core read for it"
    again = _execute(client, key, "cell-orders", read_cell + "\nprint('rows', orders.collect().height)")
    results.append(again)
    assert again["success"], again.get("error")
    assert "rows 2000" in again["stdout"], again["stdout"]

    write_cell = (
        f"written = ff.write_catalog_table(orders.filter(ff.col('grp') == 3), 'orders_k', "
        f"namespace_full_name={namespace!r}, write_mode='append')"
    )
    placed = _execute(client, key, "cell-write", write_cell + "\nprint('placed', written.columns)")
    results.append(placed)
    assert placed["success"], placed.get("error")
    assert "placed ['id', 'grp', 'val', 'name']" in placed["stdout"], placed["stdout"]
    shown = _execute(client, key, "cell-show", "display(written)")
    results.append(shown)
    assert shown["success"] and TABLE_MIME not in json.dumps(shown["display_outputs"]), "its columns, no rows"
    asked = _execute(client, key, "cell-rows", "written.collect()")
    results.append(asked)
    assert not asked["success"], asked
    assert "writes when the flow runs: Push, then run the flow on the canvas." in asked["error"], asked["error"]
    assert not signed.table_exists(f"{schema}.orders_k"), "a cell never runs a writer"

    rendering = render(flow)
    cells = [(c.cell_id, c.code) for c in rendering.cells]
    pushed = client.post("/editor/notebook/push/", json={
        "flow_id": flow.flow_id,
        "cells": [*cells, ("cell-orders", read_cell), ("cell-write", write_cell)],
        "provenance": cell_provenance(flow, rendering),
        "code_fingerprint": rendering.code_fingerprint,
        "client_max_node_id": max(n.node_id for n in flow.nodes),
        "kernel_id": kernel_id,
        "changed_cell_ids": ["cell-orders", "cell-write"],
    })
    assert pushed.status_code == 200, pushed.text
    types = sorted(n.node_type for n in flow.nodes)
    assert types == ["catalog_reader", "catalog_writer", "filter", "manual_input"], types

    registration_id, path = registration
    flow.flow_settings.source_registration_id = registration_id
    run = flow.run_graph()
    failed = [(step.node_id, step.error) for step in run.node_step_result if not step.success]
    assert run.success, failed
    flow.save_flow(str(path))
    ids = _seen_by_pyiceberg(signed, f"{schema}.orders_k")
    assert ids == [i for i in range(2000) if i % 10 == 3]
    from fastapi.testclient import TestClient

    from flowfile_core import main

    api = TestClient(main.app)
    api.headers = {"Authorization": f"Bearer {api.post('/auth/token').json()['access_token']}"}
    detail = api.get(f"/catalog/flows/{registration_id}")
    assert detail.status_code == 200, detail.text
    read = [(t["name"], t["namespace_id"]) for t in detail.json()["tables_read"]]
    produced = [(t["name"], t["namespace_id"]) for t in detail.json()["tables_produced"]]
    assert (read, produced) == ([(orders, schema_id)], [("orders_k", schema_id)])

    keys = fixtures.key_values(storage_properties(signed.load_table(f"{schema}.{orders}")))
    token, _ = fixtures.fetch_token(principal)
    values = SecretValues([principal.client_secret, principal.credential, token, *keys, fixtures.MINIO_ROOT_PASSWORD])
    log = subprocess.run(["docker", "logs", f"flowfile-kernel-{kernel_id}"], capture_output=True, text=True)
    places = {
        "lookup answers": "\n".join(text for _, text in recorded["lookups"]),
        "held-run answers": "\n".join(recorded["runs"]),
        "session results": json.dumps(results),
        "kernel log": log.stdout + log.stderr,
    }
    assert places["kernel log"].strip(), "the kernel's log was read"
    found = {place: secret_findings(text, values) for place, text in places.items()}
    assert found == {place: [] for place in places}


def test_someone_without_use_on_the_schema_gets_no_columns_and_no_rows_in_a_kernel_session(
    open_as, start_kernel, client_as, polaris, schema, orders, monkeypatch
):
    """Kernel sessions run only in the desktop app, where nobody lacks Use: on a multi-user server the session is
    refused outright. With that gate lifted, the lookup and the held run refuse such a person themselves: no columns,
    and the Catalog Reader's sentence for the rows."""
    import secrets

    import flowfile as ff
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import User
    from flowfile_core.notebook import gate, kernel_runner

    name, _, _ = polaris
    with get_db_context() as db:
        user = User(username=f"kernel_stranger_{uuid.uuid4().hex[:6]}", email=f"{uuid.uuid4().hex[:8]}@example.invalid",
                    hashed_password="not-a-login", disabled=False, is_admin=False)
        db.add(user)
        db.commit()
        stranger_id = user.id
    try:
        flow = open_as(ff.from_dict({"id": [1]}).flow_graph, user_id=stranger_id)
        kernel_id = start_kernel(stranger_id)
        client = client_as(stranger_id, client=LOOPBACK)
        key = {"flow_id": flow.flow_id, "kernel_id": kernel_id}
        monkeypatch.setenv("FLOWFILE_MODE", "docker")
        monkeypatch.setenv("JWT_SECRET_KEY", secrets.token_urlsafe(48))
        refused = client.post("/notebook/session/open", json=key)
        assert refused.status_code == 403 and refused.json()["detail"] == gate.DISABLED_DETAIL, refused.text

        for module in (gate, kernel_runner):
            monkeypatch.setattr(module, "kernel_sessions_allowed", lambda user: True)
        assert client.post("/notebook/session/open", json=key).status_code == 200
        cell = (
            f"orders = ff.read_catalog_table({orders!r}, namespace_full_name={f'{name}.{schema}'!r})\n"
            "print('columns', orders.columns)"
        )
        built = _execute(client, key, "cell-orders", cell)
        assert built["success"], built.get("error")
        assert "columns []" in built["stdout"], built["stdout"]
        rows = _execute(client, key, "cell-rows", "print(orders.collect().height)")
        assert not rows["success"], rows
        assert f"You have no access to {name}.{schema}; ask its owner to share it." in rows["error"], rows["error"]
    finally:
        with get_db_context() as db:
            db.delete(db.get(User, stranger_id))
            db.commit()


@pytest.fixture
def page_orders(signed, polaris):
    """A namespace of this test's own on Polaris holding ``orders`` (1000 rows PyIceberg wrote), brought into the added
    catalog by a sync: ``catalog.schema``. Dropped with its table after the test."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    name, catalog_id, _ = polaris
    with namespace_on(signed, "page") as schema:
        seed_orders(signed, f"{schema}.orders")
        with get_db_context() as db:
            CatalogService(SQLAlchemyCatalogRepository(db)).sync_external_catalog(catalog_id, force=True)
        yield f"{name}.{schema}"


def test_a_catalog_page_cell_reads_an_outside_table_through_a_view_and_sees_another_engines_append(
    start_kernel, polaris, page_orders, signed, principal, running_worker
):
    """Cells sent as the catalog page sends them (a negative flow id, interactive): the kernel's lookup resolves the
    table as a view and core's worker writes its rows to a snapshot the kernel scans; each run of the cell reads the
    table anew. With ``FLOWFILE_TEST_NOTEBOOK_IMAGE`` set to a published image this is the read without a kernel
    release."""
    from flowfile_core.kernel.models import ExecuteRequest
    from shared.iceberg_utils import storage_properties, write_table

    kernel_id = start_kernel(NOTEBOOK_OWNER_ID)
    manager = start_kernel.manager
    results: list[dict] = []
    inspected = subprocess.run(["docker", "inspect", "--format", "{{.Config.Image}}", f"flowfile-kernel-{kernel_id}"],
                               capture_output=True, text=True)
    print(f"kernel {kernel_id} runs {inspected.stdout.strip()}")
    assert inspected.stdout.strip() == IMAGE

    def run(code: str) -> dict:
        result = manager.execute_sync(kernel_id, ExecuteRequest(code=code, flow_id=-123, node_id=1, interactive=True))
        results.append(result.model_dump())
        assert result.success, (result.error, result.stderr)
        return results[-1]

    read_cell = f"print('rows', ff.read_catalog_table('orders', namespace_full_name={page_orders!r}).collect().height)"
    run("import flowfile as ff")
    first = run(read_cell)
    assert "rows 1000" in first["stdout"], first["stdout"]

    schema = page_orders.split(".", 1)[1]
    write_table(signed.load_table(f"{schema}.orders"), _orders(1000, 1000), "append")
    again = run(read_cell)
    assert "rows 2000" in again["stdout"], again["stdout"]
    ctx = run(f"print('ctx', flowfile_ctx.read_catalog_table({page_orders + '.orders'!r}).collect().height)")
    assert "ctx 2000" in ctx["stdout"], ctx["stdout"]

    keys = fixtures.key_values(storage_properties(signed.load_table(f"{schema}.orders")))
    token, _ = fixtures.fetch_token(principal)
    values = SecretValues([principal.client_secret, principal.credential, token, *keys, fixtures.MINIO_ROOT_PASSWORD])
    log = subprocess.run(["docker", "logs", f"flowfile-kernel-{kernel_id}"], capture_output=True, text=True)
    places = {"cell results": json.dumps(results), "kernel log": log.stdout + log.stderr}
    found = {place: secret_findings(text, values) for place, text in places.items()}
    assert found == {place: [] for place in places}


def test_a_catalog_page_cell_and_a_flow_session_cell_list_an_outside_schemas_tables(
    open_as, start_kernel, client_as, polaris, page_orders
):
    """``ff.get_catalog(...).get_schema(...).list_tables()`` in a catalog page's cell (the runtime's lookup over
    ``GET /catalog/tables``) and in a flow's session (core's ``tables`` lookup), on one kernel: both list the table
    the other catalog has, which Flowfile's catalog keeps no listed row of."""
    import flowfile as ff
    from flowfile_core.kernel.models import ExecuteRequest

    catalog, schema = page_orders.split(".", 1)
    kernel_id = start_kernel(NOTEBOOK_OWNER_ID)
    listing = (
        f"print('tables', [t.qualified_name for t in ff.get_catalog({catalog!r}).get_schema({schema!r}).list_tables()])"
    )
    expected = f"tables ['{page_orders}.orders']"

    page = start_kernel.manager.execute_sync(
        kernel_id,
        ExecuteRequest(code=f"import flowfile as ff\n{listing}", flow_id=-123, node_id=1, interactive=True),
    )
    assert page.success, (page.error, page.stderr)
    assert expected in page.stdout, page.stdout

    flow = open_as(ff.from_dict({"id": [1]}).flow_graph)
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": flow.flow_id, "kernel_id": kernel_id}
    opened = client.post("/notebook/session/open", json=key)
    assert opened.status_code == 200, opened.text
    in_session = _execute(client, key, "cell-list", listing)
    assert in_session["success"], in_session.get("error")
    assert expected in in_session["stdout"], in_session["stdout"]


WRITE_SENTENCE = (
    "A catalog table is not written from this notebook with ff.write_catalog_table: a table of Flowfile's own is "
    "written with flowfile_ctx.write_catalog_table(frame.collect(), ...), and a table kept by another catalog is "
    "written by a flow, with a Write to Catalog node."
)


def test_the_panels_two_lines_pasted_into_a_catalog_page_cell_read_the_table(
    start_kernel, polaris, page_orders, running_worker
):
    """The table panel's snippet (``externalCatalog.ts::readSnippet``) pasted into a fresh catalog-page kernel: its
    import line and read line run, and the frame holds the table's rows."""
    from flowfile_core.kernel.models import ExecuteRequest

    kernel_id = start_kernel(NOTEBOOK_OWNER_ID)
    where = json.dumps(page_orders)
    snippet = f"import flowfile as ff\nframe = ff.read_catalog_table(\"orders\", namespace_full_name={where})"

    pasted = start_kernel.manager.execute_sync(
        kernel_id,
        ExecuteRequest(code=f"{snippet}\nprint('rows', frame.collect().height)", flow_id=-123, node_id=1,
                       interactive=True),
    )

    assert pasted.success, (pasted.error, pasted.stderr)
    assert "rows 1000" in pasted.stdout, pasted.stdout


def test_a_catalog_page_cell_says_in_a_sentence_what_it_does_not_do_and_the_call_it_names_writes_a_delta_table(
    start_kernel, polaris, page_orders, signed, running_worker
):
    """In a fresh catalog-page kernel, ``ff.write_catalog_table`` typed in a cell answers the sentence and writes
    nothing to Polaris, a mistyped table answers core's sentence, and the
    ``flowfile_ctx.write_catalog_table(frame.collect(), ...)`` the sentence names writes one of Flowfile's own tables."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable
    from flowfile_core.kernel.models import ExecuteRequest

    kernel_id = start_kernel(NOTEBOOK_OWNER_ID)
    manager = start_kernel.manager
    schema = page_orders.split(".", 1)[1]
    own = f"own_{uuid.uuid4().hex[:6]}"

    def run(code: str):
        return manager.execute_sync(kernel_id, ExecuteRequest(code=code, flow_id=-123, node_id=1, interactive=True))

    where = json.dumps(page_orders)
    try:
        read = run(f"import flowfile as ff\nframe = ff.read_catalog_table(\"orders\", namespace_full_name={where})")
        assert read.success, (read.error, read.stderr)

        written = run(f"ff.write_catalog_table(frame, \"orders_copy\", namespace_full_name={where})")
        assert not written.success
        assert WRITE_SENTENCE in written.error, written.error
        assert not signed.table_exists(f"{schema}.orders_copy")

        mistyped = run(f"ff.read_catalog_table(\"ordrs\", namespace_full_name={where})")
        assert not mistyped.success
        said = f"The catalog at {fixtures.SIGNED_CATALOG_URI} has no table {schema}.ordrs."
        assert said in mistyped.error and "422" not in mistyped.error, mistyped.error

        named = run(f"flowfile_ctx.write_catalog_table(frame.collect(), {own!r})")
        assert named.success, (named.error, named.stderr)
        read_back = run(f"print('own', ff.read_catalog_table({own!r}, namespace_full_name='General.default')"
                        ".collect().height)")
        assert read_back.success, (read_back.error, read_back.stderr)
        assert "own 1000" in read_back.stdout, read_back.stdout
    finally:
        with get_db_context() as db:
            for row in db.query(CatalogTable).filter_by(name=own).all():
                CatalogService(SQLAlchemyCatalogRepository(db)).delete_table(row.id)


@pytest.fixture
def own_delta_table():
    """A Delta table of Flowfile's own in ``General.default``, written in this process; its name. Deleted after."""
    import flowfile as ff
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context
    from flowfile_core.database.models import CatalogTable

    name = f"own_{uuid.uuid4().hex[:6]}"
    ff.write_catalog_table(ff.from_dict({"id": [1, 2, 3]}), name, namespace_full_name="General.default")
    yield name
    with get_db_context() as db:
        for row in db.query(CatalogTable).filter_by(name=name).all():
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_table(row.id)


def test_a_kernel_session_reads_a_delta_table_of_flowfiles_own(
    open_as, start_kernel, client_as, own_delta_table, running_worker
):
    """Core's ``catalog_table`` answer leaves out ``kept_elsewhere`` when it is unset, so a kernel whose flowfile
    predates that field (the published image) builds its ``CatalogTableInfo`` and reads the table."""
    import flowfile as ff

    flow = open_as(ff.from_dict({"id": [1]}).flow_graph)
    kernel_id = start_kernel(NOTEBOOK_OWNER_ID)
    inspected = subprocess.run(["docker", "inspect", "--format", "{{.Config.Image}}", f"flowfile-kernel-{kernel_id}"],
                               capture_output=True, text=True)
    print(f"kernel {kernel_id} runs {inspected.stdout.strip()}")
    assert inspected.stdout.strip() == IMAGE
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": flow.flow_id, "kernel_id": kernel_id}
    opened = client.post("/notebook/session/open", json=key)
    assert opened.status_code == 200, opened.text

    cell = f"own = ff.read_catalog_table({own_delta_table!r}, namespace_full_name='General.default')"
    built = _execute(client, key, "cell-own", cell + "\nprint('columns', own.columns)")
    assert built["success"], built.get("error")
    assert "columns ['id']" in built["stdout"], built["stdout"]
    counted = _execute(client, key, "cell-count", "print('rows', own.collect().height)")
    assert counted["success"], counted.get("error")
    assert "rows 3" in counted["stdout"], counted["stdout"]
