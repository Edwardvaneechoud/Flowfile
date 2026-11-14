"""Save version: "unsaved" means what a save would commit, and a save reads each flow file a bounded
number of times.

Files Flowfile did not write (``.DS_Store``, a README) are not unsaved changes and never reach a
commit or the object store, so a save with nothing managed to record answers ``sha: null`` and the
status reads clean.
"""

import pathlib
from collections import Counter
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from git import Repo

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.project import git_ops, project_sync
from flowfile_core.project.service import write_export

OWNER = 1


def _make_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _make_client()


@pytest.fixture(autouse=True)
def _require_git():
    if not git_ops.git_available():
        pytest.skip("git not available")


def _unmanaged(root: Path) -> list[Path]:
    (root / "connections").mkdir(exist_ok=True)
    files = [root / ".DS_Store", root / "connections" / ".DS_Store", root / "README.md"]
    for f in files:
        f.write_text("not written by Flowfile\n", encoding="utf-8")
    return files


def _in_store(repo: Repo, blob: str) -> bool:
    """Whether the object store holds ``blob``, reachable or not (``rev-list`` sees reachable ones only)."""
    status, _out, _err = repo.git.cat_file("-e", blob, with_extended_output=True, with_exceptions=False)
    return status == 0


def test_unmanaged_files_are_not_unsaved_and_never_committed(tmp_path):
    root = tmp_path / "proj"
    secret = "SAVE_VERSION_SECRET"
    project_sync.close_project(OWNER)
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Save"}).status_code == 201
        _unmanaged(root)

        assert client.get("/project/active").json()["dirty"] is False
        assert client.get("/project/uncommitted").json()["changes"] == []
        saved = client.post("/project/versions", json={"message": "nothing"})
        assert saved.status_code == 200
        assert saved.json()["sha"] is None

        client.post("/project/secrets", json={"secrets": [{"name": secret, "value": "v"}]})
        assert client.get("/project/active").json()["dirty"] is True
        sha = client.post("/project/versions", json={"message": "secret"}).json()["sha"]
        assert sha
        assert client.get("/project/active").json()["dirty"] is False
        assert client.get("/project/uncommitted").json()["changes"] == []

        repo = Repo(str(root))
        committed = {b.path for b in repo.commit(sha).tree.traverse() if b.type == "blob"}
        assert "secrets.yaml" in committed
        unmanaged = {".DS_Store", "connections/.DS_Store", "README.md"}
        assert not committed & unmanaged
        assert not {path for path, _stage in repo.index.entries} & unmanaged
        for p in unmanaged:
            blob = repo.git.hash_object(str(root / p))
            assert not _in_store(repo, blob), f"{p} was hashed into the object store"
    finally:
        project_sync.close_project(OWNER)
        client.delete(f"/secrets/secrets/{secret}")


def test_the_export_reads_each_flow_file_at_most_twice(tmp_path, monkeypatch):
    src = tmp_path / "src"
    src.mkdir()
    root = tmp_path / "proj"
    root.mkdir()
    created: list[int] = []
    with get_db_context() as db:
        for i in range(40):
            if i % 2:
                path = tmp_path / "missing" / f"gone_{i}.yaml"
            else:
                path = src / f"flow_{i}.yaml"
                path.write_text(yaml.safe_dump({"flowfile_name": f"flow_{i}", "nodes": []}), encoding="utf-8")
            reg = FlowRegistration(name=f"save_version_{i}", flow_path=str(path), owner_id=OWNER)
            db.add(reg)
            db.flush()
            created.append(reg.id)
        db.commit()

    reads: Counter = Counter()
    real_open = pathlib.Path.open

    def counting_open(self, *args, **kwargs):
        mode = args[0] if args else kwargs.get("mode", "r")
        if "r" in mode and (self.name.endswith(".flow.yaml") or self.parent == src):
            reads[self] += 1
        return real_open(self, *args, **kwargs)

    try:
        monkeypatch.setattr(pathlib.Path, "open", counting_open)
        for label in ("fresh", "populated"):
            reads.clear()
            write_export(root, OWNER)
            ours = {p: n for p, n in reads.items() if p.parent == src or p.parent == root / "flows"}
            if label == "populated":
                assert not ours, f"populated: the flow text cache read {sorted(map(str, ours))}"
                continue
            assert ours, f"{label}: no flow file was read"
            worst = max(ours.values())
            print(f"{label}: {len(ours)} flow files read, at most {worst} times each")
            assert worst <= 2, f"{label}: {[(str(p), n) for p, n in ours.items() if n > 2]}"
        assert len(list((root / "flows").glob("flow_*.flow.yaml"))) == 20
    finally:
        monkeypatch.undo()
        with get_db_context() as db:
            db.query(FlowRegistration).filter(FlowRegistration.id.in_(created)).delete(synchronize_session=False)
            db.commit()


def test_flows_whose_names_differ_in_case_keep_a_file_each(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    root = tmp_path / "proj"
    root.mkdir()
    uuids: list[str] = []
    created: list[int] = []
    with get_db_context() as db:
        for name in ("Sales", "sales"):
            path = src / f"{name}_{len(created)}.yaml"
            path.write_text(yaml.safe_dump({"flowfile_name": name, "nodes": []}), encoding="utf-8")
            reg = FlowRegistration(name=name, flow_path=str(path), owner_id=OWNER)
            db.add(reg)
            db.flush()
            created.append(reg.id)
            uuids.append(reg.flow_uuid)
        db.commit()
    try:
        for label in ("first", "second"):
            write_export(root, OWNER)
            held = {
                yaml.safe_load(p.read_text(encoding="utf-8"))["flow_uuid"] for p in (root / "flows").glob("*.flow.yaml")
            }
            assert set(uuids) <= held, f"{label} pass: {sorted(p.name for p in (root / 'flows').iterdir())}"
    finally:
        with get_db_context() as db:
            db.query(FlowRegistration).filter(FlowRegistration.id.in_(created)).delete(synchronize_session=False)
            db.commit()
