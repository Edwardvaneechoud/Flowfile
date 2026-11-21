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

from flowfile_core import flow_file_handler, main
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
            importer,
            "ephemeral_flow_graph",
            lambda path, owner: built.append(Path(path).name) or real_build(path, owner),
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


def _open_in_editor(conflict: dict) -> int:
    opened = client.post("/project/merge/open", json={"path": conflict["path"]})
    assert opened.status_code == 200, opened.text
    return opened.json()["flow_id"]


def _editor_conflicts(flow_id: int) -> list[str]:
    rows = client.get("/editor/merge/conflicts", params={"flow_id": flow_id}).json()["conflicts"]
    return [r["key"] for r in rows]


def test_keeping_the_side_the_editor_shows_leaves_an_edit_made_since(project, remote, tmp_path):
    """The canvas of a flow opened from a pull shows the remote's step; taking that side only stops holding the
    conflict, so what was typed into the step since the pull is what the merge commits."""
    root, flow_uuid = _diverge(project, remote, tmp_path, 40)
    conflict = client.post("/project/pull").json()["conflicts"][0]
    flow_id = _open_in_editor(conflict)
    flow = flow_file_handler.get_flow(flow_id)
    assert flow.merge_conflicts["shown"] == "theirs"
    assert flow.get_node(2).setting_input.sample_size == 40

    edited = flow.get_node(2).setting_input.model_copy(deep=True)
    edited.sample_size = 55
    flow.add_sample(edited)
    steps_before = client.get("/editor/history_status/", params={"flow_id": flow_id}).json()["undo_count"]

    settled = client.post("/editor/merge/resolve/", json={"flow_id": flow_id, "key": "step:2", "take": "theirs"})
    assert settled.status_code == 200, settled.text
    assert settled.json()["history"]["undo_count"] == steps_before
    assert client.get("/project/merge").json()["merge"] is None
    assert _step(flow_uuid, 2)["sample_size"] == 55
    assert len(Repo(str(root)).head.commit.parents) == 2


@pytest.mark.parametrize("ending", ["abort", "settle"])
def test_a_merge_that_ends_on_the_project_page_frees_the_flow_open_in_the_editor(project, remote, tmp_path, ending):
    _diverge(project, remote, tmp_path, 40)
    conflict = client.post("/project/pull").json()["conflicts"][0]
    flow_id = _open_in_editor(conflict)
    assert _editor_conflicts(flow_id) == ["step:2"]

    if ending == "abort":
        assert client.post("/project/merge/abort").json() == {"ok": True}
    else:
        body = {"path": conflict["path"], "key": conflict["key"], "take": "theirs"}
        assert client.post("/project/merge/settle", json=body).json()["pull"]["state"] == "merged"

    assert _editor_conflicts(flow_id) == []
    flow = flow_file_handler.get_flow(flow_id)
    assert flow.merge_conflicts is None
    assert getattr(flow, "project_merge", None) is None


def test_a_saved_version_never_holds_a_flows_merge_conflicts(project):
    """A flow file saved with open conflicts (a hub merge) is exported without them."""
    _, flow_uuid = project
    path = _flow_path(flow_uuid)
    graph = open_flow(Path(path))
    try:
        node = graph.get_node(2).setting_input.model_copy(deep=True)
        node.sample_size = 30
        graph.add_sample(node)
        graph.merge_conflicts = {"conflicts": [{"key": "step:2", "kind": "both_changed"}], "shown": "theirs"}
        graph.save_flow(path)
    finally:
        graph.close_flow()
    assert "merge_conflicts" in yaml.safe_load(Path(path).read_text())["flowfile_settings"]

    sha = _save("with conflicts held")
    exported = [text for name, text in git_ops.read_tree_at(project[0], sha).items() if name.endswith(".flow.yaml")]
    assert exported
    assert all("merge_conflicts" not in text for text in exported)


def _ben(tmp_path: Path, remote: Path, change, message: str = "Changed elsewhere") -> None:
    """Clone the remote (or bring the clone up to date), ``change(clone folder)``, commit everything, push."""
    clone = tmp_path / "ben"
    if not clone.exists():
        _git(tmp_path, "clone", "-q", str(remote), str(clone))
    else:
        _git(clone, "pull", "-q", "--ff-only")
    change(clone)
    _git(clone, "add", "-A")
    _git(clone, "commit", "-qm", message)
    _git(clone, "push", "-q", "origin", "main")


def _ben_adds_a_flow_named(name: str, flow_uuid: str):
    """A flow file Ben made on his own install: another flow (``flow_uuid``) that took the name ``name``."""

    def change(clone: Path) -> None:
        source = next((clone / "flows").glob("branchy*.flow.yaml"))
        data = yaml.safe_load(source.read_text())
        data.update(flow_uuid=flow_uuid, catalog_name=name)
        (clone / "flows" / f"{name}.flow.yaml").write_text(yaml.safe_dump(data, sort_keys=False))

    return change


def _registered(flow_uuid: str) -> bool:
    with get_db_context() as db:
        return db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).first() is not None


@pytest.mark.parametrize("take", ["ours", "theirs"])
def test_two_flows_that_took_one_name_are_never_fused(project, remote, tmp_path, take):
    root, _ = project
    ben_uuid = "b0b0b0b0-0000-4000-8000-000000000001"
    _set_remote(str(remote))
    client.post("/project/push")
    _ben(tmp_path, remote, _ben_adds_a_flow_named("daily_load", ben_uuid))
    ana_uuid = _register_flow(tmp_path, "daily_load", 828290)
    try:
        ours = _save("ana's daily_load")
        path = "flows/daily_load.flow.yaml"
        ana_text = git_ops.read_tree_at(root, ours)[path]
        assert yaml.safe_load(ana_text)["flow_uuid"] == ana_uuid

        body = client.post("/project/pull").json()
        assert body["state"] == "conflicted", body
        ben_text = git_ops.read_tree_at(root, git_ops.branch_sha(root, "refs/remotes/origin/main"))[path]
        [row] = body["conflicts"]
        assert (row["path"], row["key"], row["kind"]) == (path, None, "file")
        assert "different flow" in row["message"]
        assert row["ours"] == [f"holds daily_load ({ana_uuid[:8]})"]
        assert row["theirs"] == [f"holds daily_load ({ben_uuid[:8]})"]
        assert _registered(ana_uuid) and not _registered(ben_uuid)

        settled = client.post("/project/merge/settle", json={"path": path, "take": take})
        assert settled.status_code == 200, settled.text
        assert settled.json()["pull"]["state"] == "merged"
        merged = git_ops.read_tree_at(root, "HEAD")[path]
        assert merged == (ana_text if take == "ours" else ben_text)
        assert _registered(ana_uuid) is (take == "ours")
        assert _registered(ben_uuid) is (take == "theirs")
    finally:
        _drop_flow(ana_uuid)
        _drop_flow(ben_uuid)


def test_one_flow_added_on_both_sides_still_merges_by_step():
    document = {"flow_uuid": "u-1", "catalog_name": "f", "nodes": [{"id": 1, "type": "manual_input"}]}
    ours, theirs = dict(document), {**document, "flowfile_name": "f"}
    merged = merging.merge_trees(
        {}, {"flows/f.flow.yaml": yaml.safe_dump(ours)}, {"flows/f.flow.yaml": yaml.safe_dump(theirs)}, {}
    )
    assert not merged.files
    other = merging.merge_trees(
        {},
        {"flows/f.flow.yaml": yaml.safe_dump(ours)},
        {"flows/f.flow.yaml": yaml.safe_dump({**theirs, "flow_uuid": "u-2"})},
        {},
    )
    assert list(other.files) == ["flows/f.flow.yaml"] and not other.changes


def test_a_settle_retried_with_force_after_unsaved_changes_concludes(project, remote, tmp_path):
    root, flow_uuid = _diverge(project, remote, tmp_path, 40)
    conflict = client.post("/project/pull").json()["conflicts"][0]
    unknown = client.post("/project/merge/settle", json={"path": conflict["path"], "key": "step:999", "take": "ours"})
    assert unknown.status_code == 404
    _set_sample(flow_uuid, 33)
    settle = {"path": conflict["path"], "key": conflict["key"], "take": "theirs"}

    refused = client.post("/project/merge/settle", json=settle)
    assert refused.status_code == 409
    assert refused.json()["detail"]["code"] == "UNSAVED_CHANGES"
    assert client.get("/project/merge").json()["merge"]["conflicts"] == []

    forced = client.post("/project/merge/settle", json={**settle, "force": True})
    assert forced.status_code == 200, forced.text
    pull = forced.json()["pull"]
    assert pull["state"] == "merged"
    assert pull["result"]["recovery_sha"]
    kept = git_ops.read_tree_at(root, pull["result"]["recovery_sha"])
    assert _node(yaml.safe_load(kept[conflict["path"]]), 2)["sample_size"] == 33
    assert _step(flow_uuid, 2)["sample_size"] == 40
    assert client.get("/project/merge").json()["merge"] is None


def _here(root: Path, change, message: str) -> str:
    """A commit made in the project's own folder with plain git, as someone would outside Flowfile."""
    change(root)
    _git(root, "add", "-A")
    _git(root, "commit", "-qm", message)
    return git_ops.head_sha(root)


def test_a_merging_pull_keeps_what_the_remote_changed_outside_the_managed_files(project, remote, tmp_path):
    root, flow_uuid = project
    _here(root, lambda r: (r / "OLD.md").write_text("old\n"), "an old note")
    _set_remote(str(remote))
    client.post("/project/push")

    def teammate(clone: Path) -> None:
        (clone / "README.md").write_text("# Team project\n")
        (clone / "ci").mkdir()
        (clone / "ci" / "run.sh").write_text("#!/bin/sh\necho ok\n")
        (clone / "ci" / "run.sh").chmod(0o755)
        (clone / "logo.bin").write_bytes(bytes(range(256)))
        (clone / ".gitignore").write_text((clone / ".gitignore").read_text() + "build/\n")
        (clone / "OLD.md").unlink()
        flow_file = next((clone / "flows").glob("*.flow.yaml"))
        data = yaml.safe_load(flow_file.read_text())
        _node(data, 2).update(sample_size=40)
        flow_file.write_text(yaml.safe_dump(data, sort_keys=False))

    _ben(tmp_path, remote, teammate)
    _here(root, lambda r: (r / "NOTES.md").write_text("mine\n"), "my notes")
    _set_sample(flow_uuid, 25)
    _save("here")

    conflict = client.post("/project/pull").json()["conflicts"][0]
    settled = client.post(
        "/project/merge/settle", json={"path": conflict["path"], "key": conflict["key"], "take": "theirs"}
    )
    assert settled.status_code == 200, settled.text
    assert settled.json()["pull"]["state"] == "merged"

    tree = Repo(str(root)).head.commit.tree
    assert (tree / "README.md").data_stream.read() == b"# Team project\n"
    assert (tree / "ci" / "run.sh").mode == 0o100755
    assert (tree / "logo.bin").data_stream.read() == bytes(range(256))
    assert (tree / ".gitignore").data_stream.read().decode().endswith("build/\n")
    assert (tree / "NOTES.md").data_stream.read() == b"mine\n"
    assert "OLD.md" not in {b.path for b in tree.traverse()}
    assert (root / "README.md").read_text() == "# Team project\n"
    assert _step(flow_uuid, 2)["sample_size"] == 40

    assert client.post("/project/push").status_code == 200
    pushed = Repo(str(remote)).commit("main").tree
    assert (pushed / "README.md").data_stream.read() == b"# Team project\n"


def test_a_clean_merging_pull_takes_the_remotes_other_files_too(project, remote, tmp_path):
    root, flow_uuid = project
    _set_remote(str(remote))
    client.post("/project/push")
    _ben(tmp_path, remote, lambda c: (c / "README.md").write_text("from ben\n"))
    _set_sample(flow_uuid, 25)
    _save("here")

    body = client.post("/project/pull").json()
    assert body["state"] == "merged", body
    assert (Repo(str(root)).head.commit.tree / "README.md").data_stream.read() == b"from ben\n"
    assert _step(flow_uuid, 2)["sample_size"] == 25


def test_a_pull_is_refused_when_both_changed_an_unmanaged_file_differently(project, remote, tmp_path):
    root, flow_uuid = project
    _here(root, lambda r: (r / "README.md").write_text("base\n"), "readme")
    _set_remote(str(remote))
    client.post("/project/push")
    _ben(tmp_path, remote, lambda c: (c / "README.md").write_text("ben's\n"))
    ours = _here(root, lambda r: (r / "README.md").write_text("ana's\n"), "my readme")
    _set_sample(flow_uuid, 25)
    ours = _save("here") or ours

    refused = client.post("/project/pull")
    assert refused.status_code == 409, refused.text
    detail = refused.json()["detail"]
    assert detail["code"] == "GIT_REFUSED"
    assert detail["paths"] == ["README.md"]
    assert "README.md" in detail["message"]
    assert git_ops.head_sha(root) == ours
    assert client.get("/project/merge").json()["merge"] is None
    assert (root / "README.md").read_text() == "ana's\n"
    assert _step(flow_uuid, 2)["sample_size"] == 25
    assert client.get("/project/active").json()["dirty"] is False


_LOCAL_REMOTES = [
    r"C:\Users\me\remote.git",
    "D:/shared/proj.git",
    r"\\fileserver\share\proj.git",
    "/srv/git/proj.git",
    "file:///srv/git/proj.git",
    "FILE:///C:/repos/proj.git",
]


@pytest.mark.parametrize("url", _LOCAL_REMOTES)
def test_a_local_folder_remote_in_any_os_form_is_judged_as_a_folder(url):
    assert git_ops._check_remote_url(url, local_allowed=True) == url
    with pytest.raises(git_ops.GitRemoteError, match="folder on the server"):
        git_ops._check_remote_url(url, local_allowed=False)


@pytest.mark.parametrize(
    "url", ["https://git.example.invalid/team/x.git", "ssh://git@git.example.invalid/x.git", "git@host.invalid:t/x.git"]
)
def test_network_remotes_are_still_accepted_without_local_folders(url):
    assert git_ops._check_remote_url(url, local_allowed=False) == url


@pytest.mark.parametrize("url", ["c:relative.git", "relative/x.git", "ftp://host.invalid/x.git"])
def test_other_addresses_are_still_refused(url):
    with pytest.raises(git_ops.GitRemoteError):
        git_ops._check_remote_url(url, local_allowed=True)


def _schedules(cron: str) -> str:
    entry = {"id": "s-1", "name": "nightly", "schedule_type": "cron", "cron_expression": cron, "enabled": True}
    return yaml.safe_dump({"flow_uuid": "u-1", "schedules": [entry]})


def test_a_schedule_conflict_tells_what_each_side_changed():
    path = "schedules/f.yaml"
    merged = merging.merge_trees(
        {path: _schedules("0 1 * * *")}, {path: _schedules("0 3 * * *")}, {path: _schedules("0 5 * * *")}, {}
    )
    [row] = merging.open_items(merging.new_state("main", "a" * 40, "b" * 40, merged, {}))
    assert row["key"] == "schedule:s-1"
    assert row["ours"] == ["cron_expression: '0 1 * * *' → '0 3 * * *'"]
    assert row["theirs"] == ["cron_expression: '0 1 * * *' → '0 5 * * *'"]


def test_a_parameter_conflict_tells_what_each_side_changed():
    def flow(value: str) -> str:
        return yaml.safe_dump(
            {
                "flow_uuid": "u-1",
                "flowfile_settings": {"parameters": [{"name": "region", "default_value": value}]},
                "nodes": [{"id": 1, "type": "manual_input"}],
            }
        )

    path = "flows/f.flow.yaml"
    merged = merging.merge_trees({path: flow("North")}, {path: flow("South")}, {path: flow("East")}, {})
    rows = merging.open_items(merging.new_state("main", "a" * 40, "b" * 40, merged, {}))
    row = next(r for r in rows if r["key"] == "parameter:region")
    assert row["ours"] == ["default_value: 'North' → 'South'"]
    assert row["theirs"] == ["default_value: 'North' → 'East'"]
    assert row["step_id"] is None


def test_an_entry_added_differently_on_both_sides_names_its_values():
    lines = merging._side(
        "schedule", None, {"id": "s", "cron_expression": "0 3 * * *"}, {"id": "s", "cron_expression": "x"}
    )
    assert lines == ["added the schedule", "cron_expression = '0 3 * * *'"]
    assert merging._side("parameter", {"name": "r"}, None, {"name": "r"}) == ["removed the parameter"]
