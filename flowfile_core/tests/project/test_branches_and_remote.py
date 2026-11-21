"""Branches, a remote, push and pull in a project, and a pull whose flow both sides changed.

The remote is a bare repository in ``tmp_path``; "a commit made elsewhere" is a plain git clone of it, edited and
pushed with git alone, as a colleague's checkout would. The database side is real: a branch switch or a pull is an
import, read back from the flow's registered file.
"""

import base64
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from git import Repo

from flowfile_core import main
from flowfile_core.catalog import CatalogService, SQLAlchemyCatalogRepository
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration, Secret
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.manage.io_flowfile import open_flow
from flowfile_core.project import git_ops, importer, kinds, merging, project_sync
from flowfile_core.schemas import input_schema, schemas

OWNER = 1


def _client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _client()


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=Ben", "-c", "user.email=ben@example.invalid", *args],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    ).stdout


@pytest.fixture
def project(tmp_path):
    """A project holding one flow, ``manual input (1) -> sample of 10 (2)``, saved as a first version."""
    project_sync.close_project(OWNER)
    handler = FlowfileHandler()
    flow_id = 828282
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="branchy", path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="sample"))
    graph.add_sample(input_schema.NodeSample(flow_id=flow_id, node_id=2, depending_on_id=1, sample_size=10))
    flow_path = str(tmp_path / "branchy.yaml")
    graph.save_flow(flow_path)
    graph.close_flow()
    auto_register_flow(flow_path, "branchy", OWNER)
    with get_db_context() as db:
        flow_uuid = db.query(FlowRegistration).filter_by(flow_path=flow_path).one().flow_uuid
    root = tmp_path / "proj"
    assert client.post("/project/init", json={"folder_path": str(root), "name": "Branchy"}).status_code == 201
    _git(root, "branch", "-M", "main")
    yield root, flow_uuid
    project_sync.close_project(OWNER)
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
        if reg is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg.id, delete_file=True)
        db.query(Secret).filter_by(name="project_remote_token", user_id=OWNER).delete()
        db.commit()


def _flow_path(flow_uuid: str) -> str:
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).one().flow_path


def _step(flow_uuid: str, node_id: int) -> dict:
    data = yaml.safe_load(Path(_flow_path(flow_uuid)).read_text())
    return next(n for n in data["nodes"] if n["id"] == node_id)["setting_input"]


def _set_sample(flow_uuid: str, size: int) -> None:
    """Change step 2 in the database's flow, as the editor would save it."""
    path = _flow_path(flow_uuid)
    graph = open_flow(Path(path))
    try:
        node = graph.get_node(2).setting_input.model_copy(deep=True)
        node.sample_size = size
        graph.add_sample(node)
        graph.save_flow(path)
    finally:
        graph.close_flow()


def _save(message: str) -> str:
    response = client.post("/project/versions", json={"message": message})
    assert response.status_code == 200, response.text
    return response.json()["sha"]


@pytest.fixture
def remote(tmp_path):
    bare = tmp_path / "remote.git"
    _git(tmp_path, "init", "--bare", "-b", "main", str(bare))
    return bare


def _elsewhere(tmp_path: Path, remote: Path, edit) -> None:
    """Clone the remote, ``edit(flow document)`` in its only flow file, commit and push with git alone."""
    clone = tmp_path / "ben"
    if not clone.exists():
        _git(tmp_path, "clone", "-q", str(remote), str(clone))
    else:
        _git(clone, "pull", "-q", "--ff-only")
    flow_file = next((clone / "flows").glob("*.flow.yaml"))
    data = yaml.safe_load(flow_file.read_text())
    edit(data)
    flow_file.write_text(yaml.safe_dump(data, sort_keys=False))
    _git(clone, "commit", "-qam", "Changed elsewhere")
    _git(clone, "push", "-q", "origin", "main")


def _node(data: dict, node_id: int) -> dict:
    return next(n for n in data["nodes"] if n["id"] == node_id)["setting_input"]


def _set_remote(url: str, **extra) -> None:
    response = client.put("/project/remote", json={"url": url, **extra})
    assert response.status_code == 200, response.text


def test_switch_branch_rebuilds_the_database(project):
    root, flow_uuid = project
    assert client.post("/project/branches", json={"name": "feature"}).status_code == 201
    _set_sample(flow_uuid, 25)
    _save("bigger sample")

    response = client.post("/project/branches/switch", json={"name": "main"})
    assert response.status_code == 200, response.text
    assert _step(flow_uuid, 2)["sample_size"] == 10
    assert git_ops.current_branch(root) == "main"

    assert client.post("/project/branches/switch", json={"name": "feature"}).status_code == 200
    assert _step(flow_uuid, 2)["sample_size"] == 25
    state = client.get("/project/branches").json()
    assert state["current"] == "feature"
    assert {b["name"] for b in state["branches"]} == {"main", "feature"}


def _register_flow(folder: Path, name: str, flow_id: int) -> str:
    """A second flow, ``manual input (1)``, registered for the owner; its uuid."""
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name=name, path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"b": 2}])
        )
    )
    path = str(folder / f"{name}.yaml")
    graph.save_flow(path)
    graph.close_flow()
    auto_register_flow(path, name, OWNER)
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_path=path).one().flow_uuid


def _drop_flow(flow_uuid: str) -> None:
    with get_db_context() as db:
        reg = db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first()
        if reg is not None:
            CatalogService(SQLAlchemyCatalogRepository(db)).delete_flow(reg.id, delete_file=True)


def test_a_switch_builds_and_writes_only_the_flows_that_changed(project, tmp_path, monkeypatch):
    root, flow_uuid = project
    other = _register_flow(tmp_path, "untouched", 828283)
    try:
        _save("a second flow")
        client.post("/project/branches", json={"name": "feature"})
        _set_sample(flow_uuid, 25)
        _save("bigger sample")
        other_file = Path(_flow_path(other))
        written_at = other_file.stat().st_mtime_ns

        built, imported = [], []
        real_build, real_import = importer.ephemeral_flow_graph, importer._import_flow
        monkeypatch.setattr(
            importer, "ephemeral_flow_graph", lambda path, owner: built.append(Path(path).name) or real_build(path, owner)
        )
        monkeypatch.setattr(
            importer, "_import_flow", lambda data, owner: imported.append(data["flow_uuid"]) or real_import(data, owner)
        )

        response = client.post("/project/branches/switch", json={"name": "main"})
        assert response.status_code == 200, response.text
        assert imported == [flow_uuid]
        assert len(built) == 1
        assert _step(flow_uuid, 2)["sample_size"] == 10
        assert other_file.stat().st_mtime_ns == written_at
        assert client.get("/project/active").json()["dirty"] is False
    finally:
        _drop_flow(other)


def _flow_file(root: Path, name: str, node_type: str) -> str:
    rel = f"flows/{name}.flow.yaml"
    (root / "flows").mkdir(exist_ok=True)
    (root / rel).write_text(yaml.safe_dump({"flow_uuid": name, "nodes": [{"id": 1, "type": node_type}]}))
    return rel


def test_a_plan_that_moves_tables_also_applies_the_flows_that_read_the_catalog(tmp_path):
    reader = _flow_file(tmp_path, "reads", "catalog_reader")
    plain = _flow_file(tmp_path, "plain", "manual_input")
    edited = _flow_file(tmp_path, "edited", "manual_input")
    flow_change = kinds.Change("flow", "edited", "modified", "edited", edited)

    assert importer.flows_to_apply(tmp_path, None) is None
    assert importer.flows_to_apply(tmp_path, [flow_change]) == {edited}
    table_changed = kinds.Change("tables", ("a", "b", "t"), "modified", "t", "tables.yaml")
    assert importer.flows_to_apply(tmp_path, [flow_change, table_changed]) == {edited}
    for change in ("added", "removed"):
        moved = kinds.Change("tables", ("a", "b", "t"), change, "t", "tables.yaml")
        assert importer.flows_to_apply(tmp_path, [flow_change, moved]) == {edited, reader}
    schema_gone = kinds.Change("namespaces", ("a", "b"), "removed", "b", "namespaces.yaml")
    assert plain not in importer.flows_to_apply(tmp_path, [schema_gone])


def test_switch_refused_with_unsaved_changes(project):
    root, flow_uuid = project
    client.post("/project/branches", json={"name": "feature"})
    _set_sample(flow_uuid, 33)

    refused = client.post("/project/branches/switch", json={"name": "main"})
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "UNSAVED_CHANGES"
    assert git_ops.current_branch(root) == "feature"
    assert _step(flow_uuid, 2)["sample_size"] == 33

    forced = client.post("/project/branches/switch", json={"name": "main", "force": True})
    assert forced.status_code == 200, forced.text
    recovery = forced.json()["recovery_sha"]
    assert recovery
    kept = git_ops.read_tree_at(root, recovery)
    flow_text = next(text for path, text in kept.items() if path.startswith("flows/"))
    assert _node(yaml.safe_load(flow_text), 2)["sample_size"] == 33
    assert _step(flow_uuid, 2)["sample_size"] == 10


def test_push_writes_the_branch_to_the_remote(project, remote):
    root, _ = project
    _set_remote(str(remote))
    response = client.post("/project/push")
    assert response.status_code == 200, response.text
    assert response.json()["unsaved"] is False
    assert Repo(str(remote)).commit("main").hexsha == git_ops.head_sha(root)
    row = next(b for b in client.get("/project/branches").json()["branches"] if b["name"] == "main")
    assert (row["ahead"], row["behind"]) == (0, 0)


def test_pull_applies_a_commit_made_elsewhere(project, remote, tmp_path):
    root, flow_uuid = project
    _set_remote(str(remote))
    client.post("/project/push")
    _elsewhere(tmp_path, remote, lambda d: _node(d, 2).update(sample_size=40))

    response = client.post("/project/pull")
    assert response.status_code == 200, response.text
    assert response.json()["state"] == "fast_forward"
    assert _step(flow_uuid, 2)["sample_size"] == 40
    assert git_ops.head_sha(root) == Repo(str(remote)).commit("main").hexsha
    assert client.post("/project/pull").json()["state"] == "up_to_date"


def test_a_remote_address_with_a_password_is_refused(project):
    response = client.put("/project/remote", json={"url": "https://ben:hunter2@git.example.invalid/x.git"})
    assert response.status_code == 409
    assert "token" in response.json()["detail"]["message"]
    assert client.put("/project/remote", json={"url": "ext::sh -c touch% /tmp/x"}).status_code == 409


class _AuthRecorder(BaseHTTPRequestHandler):
    seen: list[str] = []

    def do_GET(self):  # noqa: N802
        auth = self.headers.get("Authorization")
        if auth is None:
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="git"')
            self.end_headers()
            return
        type(self).seen.append(auth)
        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        pass


def test_the_token_is_in_no_file_of_the_folder(project, tmp_path):
    root, _ = project
    server = HTTPServer(("127.0.0.1", 0), _AuthRecorder)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    token = "tok-NEVER-ON-DISK-8231"
    try:
        _set_remote(f"http://127.0.0.1:{server.server_port}/team/project.git", token=token, username="ben")
        state = client.get("/project/branches").json()
        assert state["has_token"] is True
        assert token not in str(state)
        refused = client.post("/project/push")
    finally:
        server.shutdown()
    assert refused.status_code == 409
    assert token not in refused.text
    assert _AuthRecorder.seen, "git never sent the credentials"
    assert base64.b64decode(_AuthRecorder.seen[-1].split()[1]).decode() == f"ben:{token}"
    for path in root.rglob("*"):
        if path.is_file():
            assert token.encode() not in path.read_bytes(), path
    assert token not in git_ops.remote_url(root)


def _diverge(project, remote, tmp_path, elsewhere_size: int, edit_here=None):
    root, flow_uuid = project
    _set_remote(str(remote))
    client.post("/project/push")
    _elsewhere(tmp_path, remote, lambda d: _node(d, 2).update(sample_size=elsewhere_size))
    (edit_here or (lambda: _set_sample(flow_uuid, 25)))()
    _save("here")
    return root, flow_uuid


def test_pull_merges_two_changes_to_one_flow(project, remote, tmp_path):
    root, flow_uuid = project
    _set_remote(str(remote))
    client.post("/project/push")
    _elsewhere(tmp_path, remote, lambda d: _node(d, 2).update(sample_size=40))

    path = _flow_path(flow_uuid)
    graph = open_flow(Path(path))
    try:
        node = graph.get_node(1).setting_input.model_copy(deep=True)
        node.raw_data_format = input_schema.RawData.from_pylist([{"a": 7}])
        graph.add_manual_input(node)
        graph.save_flow(path)
    finally:
        graph.close_flow()
    ours = _save("new input rows")

    response = client.post("/project/pull")
    assert response.status_code == 200, response.text
    assert response.json()["state"] == "merged"
    assert _step(flow_uuid, 2)["sample_size"] == 40
    assert _step(flow_uuid, 1)["raw_data_format"]["data"] == [[7]]
    parents = [p.hexsha for p in Repo(str(root)).head.commit.parents]
    assert parents == [ours, Repo(str(remote)).commit("main").hexsha]
    assert merging.load_state(root) is None


def test_pull_with_a_conflict_applies_nothing_until_settled(project, remote, tmp_path):
    root, flow_uuid = _diverge(project, remote, tmp_path, 40)
    head = git_ops.head_sha(root)

    response = client.post("/project/pull")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["state"] == "conflicted"
    assert [(c["key"], c["kind"]) for c in body["conflicts"]] == [("step:2", "both_changed")]
    assert git_ops.head_sha(root) == head
    assert _step(flow_uuid, 2)["sample_size"] == 25

    waiting = client.get("/project/merge").json()["merge"]
    assert waiting["conflicts"][0]["ours"] == ["settings.sample_size: 10 → 25"]
    assert waiting["conflicts"][0]["theirs"] == ["settings.sample_size: 10 → 40"]
    refused = client.post("/project/versions", json={"message": "meanwhile"})
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "MERGE_IN_PROGRESS"

    assert client.post("/project/merge/abort").json() == {"ok": True}
    assert client.get("/project/merge").json()["merge"] is None
    assert git_ops.head_sha(root) == head


def test_settling_the_last_conflict_concludes_the_merge(project, remote, tmp_path):
    root, flow_uuid = _diverge(project, remote, tmp_path, 40)
    conflict = client.post("/project/pull").json()["conflicts"][0]

    response = client.post(
        "/project/merge/settle", json={"path": conflict["path"], "key": conflict["key"], "take": "theirs"}
    )
    assert response.status_code == 200, response.text
    assert response.json()["pull"]["state"] == "merged"
    assert _step(flow_uuid, 2)["sample_size"] == 40
    assert len(Repo(str(root)).head.commit.parents) == 2
    assert client.get("/project/merge").json()["merge"] is None
    assert client.post("/project/push").status_code == 200


def test_a_conflicted_flow_is_settled_in_the_editor(project, remote, tmp_path):
    root, flow_uuid = _diverge(project, remote, tmp_path, 40)
    conflict = client.post("/project/pull").json()["conflicts"][0]

    opened = client.post("/project/merge/open", json={"path": conflict["path"]})
    assert opened.status_code == 200, opened.text
    flow_id = opened.json()["flow_id"]
    rows = client.get("/editor/merge/conflicts", params={"flow_id": flow_id}).json()["conflicts"]
    assert [r["key"] for r in rows] == ["step:2"]
    assert client.post("/flow/run/", params={"flow_id": flow_id}).status_code == 409

    settled = client.post("/editor/merge/resolve/", json={"flow_id": flow_id, "key": "step:2", "take": "ours"})
    assert settled.status_code == 200, settled.text
    assert settled.json()["conflicts"] == []
    assert client.get("/project/merge").json()["merge"] is None
    assert _step(flow_uuid, 2)["sample_size"] == 25
    assert len(Repo(str(root)).head.commit.parents) == 2
