"""The canvas notebook on a real notebook kernel container (``make notebook_kernel_dev``).

Run it on its own with a scratch storage folder, since the kernel mounts the Flowfile folders::

    FLOWFILE_STORAGE_DIR=$(mktemp -d) poetry run pytest flowfile_core/tests/notebook/test_kernel_notebook_docker.py -m kernel

Skipped without Docker, without the ``flowfile-kernel-notebook:dev`` image or without ``FLOWFILE_STORAGE_DIR``;
with ``FLOWFILE_REQUIRE_NOTEBOOK_KERNEL`` set (the CI job that builds the image) those are failures instead.
"""

from __future__ import annotations

import ast
import asyncio
import json
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import pytest

from flowfile_core.notebook.render import render
from shared.notebook_display import TABLE_MIME
from tests.notebook.conftest import NOTEBOOK_OWNER_ID, cell_provenance

IMAGE = "flowfile-kernel-notebook:dev"
KERNEL_ID = "nb-smoke"
LOOPBACK = ("127.0.0.1", 50123)
REQUIRED = bool(os.environ.get("FLOWFILE_REQUIRE_NOTEBOOK_KERNEL"))


def _image_present() -> bool:
    try:
        return subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode == 0
    except OSError:
        return False


def _missing() -> str | None:
    if not os.environ.get("FLOWFILE_STORAGE_DIR"):
        return "needs a scratch FLOWFILE_STORAGE_DIR"
    if not _image_present():
        return f"{IMAGE} not built (make notebook_kernel_dev)"
    return None


MISSING = _missing()
pytestmark = [pytest.mark.kernel, pytest.mark.skipif(bool(MISSING) and not REQUIRED, reason=MISSING or "")]


@pytest.fixture(autouse=True)
def _required():
    """Where the image and the folder are required (CI), their absence fails instead of skipping."""
    if REQUIRED and MISSING:
        pytest.fail(MISSING)


def _in_kernel(path: str) -> bool:
    """Whether ``path`` exists inside the kernel container."""
    command = ["docker", "exec", f"flowfile-kernel-{KERNEL_ID}", "test", "-e", path]
    return subprocess.run(command, capture_output=True).returncode == 0


def _kernel_storage() -> str:
    """The kernel's own storage folder (``~/.flowfile`` in the container): no host folder is mounted into it."""
    code = "from shared.storage_config import storage; print(storage.base_directory)"
    command = ["docker", "exec", f"flowfile-kernel-{KERNEL_ID}", "python", "-c", code]
    return subprocess.run(command, capture_output=True, text=True, check=True).stdout.strip()


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def core_url(monkeypatch):
    """Core served on a free port, reachable from a kernel container as ``host.docker.internal``."""
    import uvicorn

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


@pytest.fixture
def notebook_kernel(core_url, monkeypatch):
    import flowfile_core.kernel as kernel_package
    from flowfile_core.kernel.manager import KernelManager
    from flowfile_core.kernel.models import ImageFlavour, KernelConfig
    from flowfile_core.notebook import kernel_runner

    monkeypatch.setenv("FLOWFILE_MODE", "electron")
    shared = str(Path(tempfile.mkdtemp(prefix="nb_kernel_shared_")).resolve())
    manager = KernelManager(shared_volume_path=shared)
    monkeypatch.setattr(kernel_package, "get_kernel_manager", lambda: manager)
    loop = asyncio.new_event_loop()
    subprocess.run(["docker", "rm", "-f", f"flowfile-kernel-{KERNEL_ID}"], capture_output=True)
    if manager.get_kernel_sync(KERNEL_ID) is not None:
        loop.run_until_complete(manager.delete_kernel(KERNEL_ID))
    config = KernelConfig(id=KERNEL_ID, name="Notebook smoke", image_flavour=ImageFlavour.CUSTOM, custom_image=IMAGE)
    loop.run_until_complete(manager.create_kernel(config, user_id=NOTEBOOK_OWNER_ID))
    try:
        loop.run_until_complete(manager.start_kernel(KERNEL_ID))
        yield manager
    finally:
        try:
            loop.run_until_complete(manager.delete_kernel(KERNEL_ID))
        finally:
            loop.close()
            subprocess.run(["docker", "rm", "-f", f"flowfile-kernel-{KERNEL_ID}"], capture_output=True)
            shutil.rmtree(shared, ignore_errors=True)
            kernel_runner._sessions.clear()
            kernel_runner._verified.clear()


@pytest.fixture
def smoke_flow(open_as):
    import flowfile as ff

    orders = ff.from_dict({"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]})
    return open_as(
        orders.filter(ff.col("amount") > 10).polars_code("input_df.with_columns(pl.col('amount') * 10)").flow_graph
    )


def _node_id(flow, node_type: str) -> int:
    return next(node.node_id for node in flow.nodes if node.node_type == node_type)


def _bind(name: str, node_id: int) -> str:
    frames = "(v for v in list(globals().values()) if type(v).__name__ == 'FlowFrame')"
    return f"{name} = next(v for v in {frames} if v.node_id == {node_id})\n"


def _table_rows(result: dict) -> list:
    tables = [json.loads(out["data"]) for out in result["display_outputs"] if out["mime_type"] == TABLE_MIME]
    assert tables, result
    return tables[0]["data"]


def test_a_notebook_session_on_a_real_kernel(smoke_flow, notebook_kernel, client_as):
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": smoke_flow.flow_id, "kernel_id": KERNEL_ID}
    opened = client.post("/notebook/session/open", json=key)
    assert opened.status_code == 200, opened.text

    filter_id, coded_id = _node_id(smoke_flow, "filter"), _node_id(smoke_flow, "polars_code")
    cell = _bind("filtered", filter_id) + (
        "import math\n"
        "frame = filtered\n"
        "for power in range(2):\n"
        "    frame = frame.with_columns((ff.col('amount') * math.pow(10, power)).alias(f'scaled_{power}'))\n"
        "print('columns', frame.columns)\n"
        "display(frame)\n"
    )
    executed = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-loop", "code": cell})
    assert executed.status_code == 200, executed.text
    result = executed.json()
    assert result["success"], result
    assert "columns" in result["stdout"] and "scaled_1" in result["stdout"]
    assert len(_table_rows(result)) == 3

    canvas = client.post(
        "/notebook/session/execute",
        json={**key, "cell_id": "cell-canvas", "code": _bind("coded", coded_id) + "display(coded)"},
    )
    assert canvas.status_code == 200, canvas.text
    assert canvas.json()["success"], canvas.json()
    assert len(_table_rows(canvas.json())) == 3

    rendering = render(smoke_flow)
    cells = [(c.cell_id, c.code) for c in rendering.cells]
    body = {
        "flow_id": smoke_flow.flow_id,
        "cells": cells,
        "provenance": cell_provenance(smoke_flow, rendering),
        "code_fingerprint": rendering.code_fingerprint,
        "client_max_node_id": max(n.node_id for n in smoke_flow.nodes),
        "kernel_id": KERNEL_ID,
    }
    unedited = client.post("/notebook/plan", json=body)
    assert unedited.status_code == 200, unedited.text
    assert unedited.json()["operations"] == []

    last = next(c for c in rendering.cells if coded_id in c.node_ids)
    name = next(node.targets[0].id for node in ast.parse(last.code).body if isinstance(node, ast.Assign))
    added = f"plus_one = {name}.with_columns((ff.col('amount') + 1).alias('plus_one'))"
    before = {n.node_id for n in smoke_flow.nodes}
    pushed = client.post(
        "/editor/notebook/push/",
        json={**body, "cells": [*cells, ("cell-new", added)], "changed_cell_ids": ["cell-new"]},
    )
    assert pushed.status_code == 200, pushed.text
    new_nodes = [n for n in smoke_flow.nodes if n.node_id not in before]
    assert [n.node_type for n in new_nodes] == ["formula"], [n.node_type for n in new_nodes]

    database = f"{_kernel_storage()}/database/flowfile_catalog.db"
    assert _in_kernel(os.path.dirname(database)), "the kernel imported flowfile, whose storage folders exist there"
    assert not _in_kernel(database), "nothing above opens a catalog database in the kernel"
    code = (
        "print('kernels', sorted(ff.kernels))\n"
        "print('catalogs', sorted(c.name for c in ff.list_catalogs()))\n"
        "print('default', ff.default_schema().name)\n"
    )
    catalog = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-catalog", "code": code})
    assert catalog.status_code == 200 and catalog.json()["success"], catalog.text
    stdout = catalog.json()["stdout"]
    assert KERNEL_ID in stdout and "General" in stdout and "default" in stdout, stdout
    assert not _in_kernel(database), "the saved kernels and the catalog come from core, never from a database here"


def test_a_cell_that_opens_the_database_itself_is_refused_and_reads_the_catalog_live(
    smoke_flow, notebook_kernel, client_as
):
    """The kernel holds no catalog database: a cell opening one through flowfile_core stops before pysqlite could
    create a file, and what core writes to the catalog shows in the next cell through the lookups."""
    from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
    from flowfile_core.database.connection import get_db_context

    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": smoke_flow.flow_id, "kernel_id": KERNEL_ID}
    assert client.post("/notebook/session/open", json=key).status_code == 200
    database = f"{_kernel_storage()}/database/flowfile_catalog.db"
    probe = (
        "from sqlalchemy import text\n"
        "from flowfile_core.database.connection import get_db_context\n"
        "with get_db_context() as db:\n"
        "    print('probe', db.execute(text('SELECT 1')).scalar())\n"
    )
    read = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-probe", "code": probe})
    assert read.status_code == 200, read.text
    result = read.json()
    assert not result["success"] and "probe" not in result["stdout"], result
    assert "no catalog database to open (a cell)" in result["error"], result["error"]
    assert "ff.list_catalogs" in result["error"], result["error"]
    assert not _in_kernel(database), "the refusal ran before pysqlite could create the file"

    name = "nb_live_probe"
    with get_db_context() as db:
        namespace_id = CatalogService(SQLAlchemyCatalogRepository(db)).create_namespace(name, NOTEBOOK_OWNER_ID).id
    try:
        code = "print(sorted(c.name for c in ff.list_catalogs()))"
        listed = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-catalogs", "code": code})
        assert listed.status_code == 200 and listed.json()["success"], listed.text
        assert name in listed.json()["stdout"], listed.json()
    finally:
        with get_db_context() as db:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_namespace(namespace_id)
    assert not _in_kernel(database)


SUMMING_SCRIPT = (
    "import polars as pl\n"
    "df = flowfile_ctx.read_input()\n"
    "flowfile_ctx.publish_output(df.select(pl.col('amount').sum().alias('column_0')))\n"
)


def test_a_script_on_the_sessions_kernel_has_its_columns_once_the_canvas_ran_it(
    open_as, notebook_kernel, client_as, monkeypatch
):
    """A script's columns are known only once it ran: the canvas runs it while the kernel is free (Run and preview
    on canvas), and the session's variable then knows them."""
    import flowfile as ff
    from flowfile_core.flowfile import flow_graph as flow_graph_module

    monkeypatch.setattr(flow_graph_module, "get_kernel_manager", lambda: notebook_kernel)
    orders = ff.from_dict({"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]})
    flow = open_as(ff.PythonScript(orders, code=SUMMING_SCRIPT, kernel=KERNEL_ID).output.flow_graph)
    script_id = _node_id(flow, "python_script")
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": flow.flow_id, "kernel_id": KERNEL_ID}
    assert client.post("/notebook/session/open", json=key).status_code == 200
    nodes = "(v for v in list(globals().values()) if type(v).__name__ == 'SeededNode')"
    cell = f"script = next(v for v in {nodes} if v.node_id == {script_id})\nprint(script.columns)"
    seeded = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-a", "code": cell})
    assert seeded.status_code == 200 and seeded.json()["success"], seeded.text
    assert "column_0" not in seeded.json()["stdout"], seeded.json()

    ran = client.post("/editor/notebook/run_lineage/", json={"flow_id": flow.flow_id, "node_id": script_id})
    assert ran.status_code == 200, ran.text
    assert flow.get_run_info().success, flow.get_run_info()

    known = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-b", "code": "print(script.columns)"})
    assert known.status_code == 200 and known.json()["success"], known.text
    assert known.json()["stdout"].strip() == "['column_0']", known.json()


def test_a_cells_artifacts_and_display_order_outlive_the_calls_after_it(smoke_flow, notebook_kernel, client_as):
    """A cell runs as its own node, so the schemas refresh and the next cell leave what it published; its text and
    frame displays come back in the order it made them."""
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": smoke_flow.flow_id, "kernel_id": KERNEL_ID}
    assert client.post("/notebook/session/open", json=key).status_code == 200

    cell = _bind("filtered", _node_id(smoke_flow, "filter")) + (
        "flowfile_ctx.publish_artifact('kept', {'a': 1})\ndisplay('first')\ndisplay(filtered)\ndisplay('last')\n"
    )
    first = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-a", "code": cell, "node_id": 1234})
    assert first.status_code == 200 and first.json()["success"], first.text
    shown = [(out["mime_type"], out["data"]) for out in first.json()["display_outputs"]]
    assert [mime for mime, _ in shown] == ["text/plain", TABLE_MIME, "text/plain"], shown
    assert (shown[0][1], shown[2][1]) == ("first", "last"), shown

    assert client.post("/notebook/session/schemas", json=key).status_code == 200
    code = "print([a.name for a in flowfile_ctx.list_artifacts()])"
    listed = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-b", "code": code, "node_id": 5678})
    assert listed.status_code == 200 and listed.json()["success"], listed.text
    assert "kept" in listed.json()["stdout"], listed.json()


def test_kernel_completions_see_the_session(smoke_flow, notebook_kernel, client_as):
    """The editor's Jedi completions, sent under the session's kernel flow id, read the session's variables."""
    from flowfile_core.notebook import kernel_runner

    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": smoke_flow.flow_id, "kernel_id": KERNEL_ID}
    assert client.post("/notebook/session/open", json=key).status_code == 200
    cell = _bind("orders", _node_id(smoke_flow, "filter")) + "priced = orders.with_columns(ff.col('amount') * 2)\n"
    executed = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-priced", "code": cell})
    assert executed.status_code == 200 and executed.json()["success"], executed.text

    def labels(code: str) -> set[str]:
        flow_id = kernel_runner.kernel_flow_id(smoke_flow.flow_id)
        body = {"code": code, "line": 1, "column": len(code), "flow_id": flow_id}
        response = client.post(f"/kernels/{KERNEL_ID}/lsp/complete", json=body)
        assert response.status_code == 200, response.text
        return {item["label"] for item in response.json()["items"]}

    assert "filter" in labels("priced.fil")
    assert "col" in labels("ff.co")
    assert client.post("/notebook/session/reset", json=key).status_code == 200
    assert "priced" not in labels("pri")


def test_a_file_a_cell_names_is_read_by_core(smoke_flow, notebook_kernel, client_as, tmp_path):
    """A cell reads a file on this machine: the kernel mounts no host folder, so core predicts its columns and reads
    it from the cell's settings (``POST /notebook/session/node_run``), without a push; plain Polars in the cell has
    no such file."""
    path = tmp_path / "outside.csv"
    path.write_text("a,b\n1,x\n2,y\n")
    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": smoke_flow.flow_id, "kernel_id": KERNEL_ID}
    assert client.post("/notebook/session/open", json=key).status_code == 200
    cell = f"new = ff.read_csv({str(path)!r})\nprint(new.columns)\ndisplay(new)"
    shown = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-outside", "code": cell})
    assert shown.status_code == 200 and shown.json()["success"], shown.text
    assert shown.json()["stdout"].strip() == "['a', 'b']", shown.json()
    assert _table_rows(shown.json()) == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}], shown.json()

    plain = f"import polars as pl\npl.read_csv({str(path)!r})"
    missing = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-plain", "code": plain})
    assert missing.status_code == 200 and not missing.json()["success"], missing.text
    assert "No such file" in json.dumps(missing.json()), missing.json()
    assert not _in_kernel(str(path))


def test_an_installed_custom_node_is_mirrored_into_the_kernel(smoke_flow, notebook_kernel, client_as):
    """Core's installed custom node files reach the kernel through the ``custom_node_sources`` lookup: a cell places
    ``mood_emoji`` and the kernel runs it on its own copy of the file."""
    from test_utils.notebook_demo import installed_mood_emoji

    client = client_as(NOTEBOOK_OWNER_ID, client=LOOPBACK)
    key = {"flow_id": smoke_flow.flow_id, "kernel_id": KERNEL_ID}
    cell = _bind("filtered", _node_id(smoke_flow, "filter")) + (
        "mood = ff.custom_nodes.mood_emoji(filtered, source_column='amount', threshold_value=25, "
        "emoji_column_name='mood', add_random_sparkle=False)\nprint(mood.columns)\ndisplay(mood)"
    )
    with installed_mood_emoji():
        assert client.post("/notebook/session/open", json=key).status_code == 200
        shown = client.post("/notebook/session/execute", json={**key, "cell_id": "cell-node", "code": cell})
    assert shown.status_code == 200 and shown.json()["success"], shown.text
    assert "'mood'" in shown.json()["stdout"], shown.json()
    assert len(_table_rows(shown.json())) == 3, shown.json()
    assert _in_kernel(f"{_kernel_storage()}/user_defined_nodes/mood_emoji.py")
