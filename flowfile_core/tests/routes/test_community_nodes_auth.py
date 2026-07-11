"""Auth + fixture-mode route tests for /community_nodes.

Every route rejects unauthenticated requests with 401; install/uninstall carry an
extra admin gate; and the full install happy path runs end-to-end against an
on-disk fixture registry (no HTTP), proving the router wiring, consent gate, and
receipt lifecycle over the real app.
"""
import hashlib
import json
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.auth.jwt import get_current_active_user
from flowfile_core.auth.models import User
from shared import storage

BENIGN_NODE = '''
import polars as pl
from shared.node_designer import CustomNodeBase, NodeSettings, Section, TextInput


class {class_name}(CustomNodeBase):
    node_name: str = "{node_name}"
    node_category: str = "Community Test"

    settings_schema: NodeSettings = NodeSettings(
        main=Section(title="Main", value=TextInput(label="Value", default="x")),
    )

    def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
        return inputs[0]
'''

_FAKE_PNG = b"\x89PNG\r\n\x1a\n" + b"0" * 48


def get_authed_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


unauthed_client = TestClient(main.app)
authed_client = get_authed_client()


UNAUTHENTICATED_REQUESTS = [
    ("GET", "/community_nodes/index", {}),
    ("GET", "/community_nodes/node/some_node", {}),
    ("GET", "/community_nodes/media/some_node/icon.png", {}),
    ("POST", "/community_nodes/install", {"json": {"node_id": "x", "version": "1.0.0"}}),
    ("DELETE", "/community_nodes/uninstall/some_node", {}),
    ("GET", "/community_nodes/installed", {}),
    ("GET", "/community_nodes/updates", {}),
    ("POST", "/community_nodes/screenshots/some_stem", {"files": {"file": ("s.png", b"x", "image/png")}}),
    ("GET", "/community_nodes/screenshots/some_stem", {}),
    ("GET", "/community_nodes/screenshots/some_stem/shot.png", {}),
    ("DELETE", "/community_nodes/screenshots/some_stem/shot.png", {}),
    ("POST", "/community_nodes/publish-bundle", {"json": {"file_name": "x.py", "license": "MIT"}}),
]


@pytest.mark.parametrize(
    "method,url,kwargs", UNAUTHENTICATED_REQUESTS, ids=[f"{m} {u}" for m, u, _ in UNAUTHENTICATED_REQUESTS]
)
def test_every_route_requires_auth(method, url, kwargs):
    response = unauthed_client.request(method, url, **kwargs)
    assert response.status_code == 401, f"{method} {url} returned {response.status_code}, expected 401"


def _pin(path_rel: str, data: bytes) -> dict:
    return {"path": path_rel, "sha256": hashlib.sha256(data).hexdigest(), "size": len(data)}


def _build_fixture(root: Path, node_id: str, node_name: str, class_name: str) -> Path:
    node_dir = root / "nodes" / node_id
    node_dir.mkdir(parents=True, exist_ok=True)
    node_bytes = BENIGN_NODE.format(class_name=class_name, node_name=node_name).encode("utf-8")
    (node_dir / "node.py").write_bytes(node_bytes)
    manifest_bytes = json.dumps({"id": node_id, "node_name": node_name, "version": "1.0.0"}).encode("utf-8")
    (node_dir / "manifest.json").write_bytes(manifest_bytes)
    (node_dir / "icon.png").write_bytes(_FAKE_PNG)
    index = {
        "schema_version": 1,
        "registry": {"repo": "test/community", "commit": "commit0"},
        "categories": ["Community Test"],
        "nodes": [
            {
                "id": node_id,
                "node_name": node_name,
                "version": "1.0.0",
                "description": "A community test node.",
                "author": {"github": "octocat"},
                "category": "Community Test",
                "min_flowfile_version": "0.0.1",
                "artifacts": {
                    "node": _pin(f"nodes/{node_id}/node.py", node_bytes),
                    "manifest": _pin(f"nodes/{node_id}/manifest.json", manifest_bytes),
                    "icon": _pin(f"nodes/{node_id}/icon.png", _FAKE_PNG),
                },
            }
        ],
    }
    index_path = root / "index.json"
    index_path.write_text(json.dumps(index), encoding="utf-8")
    return index_path


@pytest.fixture
def fixture_registry(tmp_path, monkeypatch):
    suffix = uuid.uuid4().hex[:8]
    node_id = f"authnode_{suffix}"
    node_name = f"Authnode {suffix}"
    class_name = f"AuthNode{suffix}"
    index_path = _build_fixture(tmp_path, node_id, node_name, class_name)
    monkeypatch.setenv("FLOWFILE_COMMUNITY_INDEX_URL", str(index_path))
    yield node_id
    # Best-effort cleanup of anything the install wrote into real storage.
    authed_client.delete(f"/community_nodes/uninstall/{node_id}")
    icon = storage.user_defined_nodes_icons / f"{node_id}__icon.png"
    if icon.exists():
        icon.unlink()


def test_install_admin_gate(fixture_registry):
    node_id = fixture_registry
    main.app.dependency_overrides[get_current_active_user] = lambda: User(
        username="member", id=2, disabled=False, is_admin=False, must_change_password=False
    )
    try:
        install = authed_client.post(
            "/community_nodes/install",
            json={"node_id": node_id, "version": "1.0.0", "acknowledged_risks": True},
        )
        assert install.status_code == 403, install.text
        uninstall = authed_client.delete(f"/community_nodes/uninstall/{node_id}")
        assert uninstall.status_code == 403
    finally:
        main.app.dependency_overrides.pop(get_current_active_user, None)


def test_install_consent_required_400(fixture_registry):
    node_id = fixture_registry
    response = authed_client.post(
        "/community_nodes/install", json={"node_id": node_id, "version": "1.0.0"}
    )
    assert response.status_code == 400, response.text
    assert response.json()["detail"]["error_code"] == "CONSENT_REQUIRED"
    assert not (storage.user_defined_nodes_directory / f"{node_id}.py").exists()


def test_install_happy_path_then_uninstall(fixture_registry):
    node_id = fixture_registry
    response = authed_client.post(
        "/community_nodes/install",
        json={"node_id": node_id, "version": "1.0.0", "acknowledged_risks": True},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["success"] is True
    assert body["load_error"] is None
    assert body["receipt"]["node_id"] == node_id
    assert (storage.user_defined_nodes_directory / f"{node_id}.py").exists()

    installed = authed_client.get("/community_nodes/installed")
    assert installed.status_code == 200
    assert any(n["receipt"]["node_id"] == node_id for n in installed.json())

    uninstall = authed_client.delete(f"/community_nodes/uninstall/{node_id}")
    assert uninstall.status_code == 200
    assert not (storage.user_defined_nodes_directory / f"{node_id}.py").exists()


def test_index_lists_fixture_node(fixture_registry):
    node_id = fixture_registry
    response = authed_client.get("/community_nodes/index")
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["source"] == "fixture"
    assert any(n["id"] == node_id and n["install_state"] == "not_installed" for n in body["nodes"])


def test_uninstall_unknown_node_404(fixture_registry):
    response = authed_client.delete("/community_nodes/uninstall/never_installed_xyz")
    assert response.status_code == 404
    assert response.json()["detail"]["error_code"] == "NOT_INSTALLED"


def test_media_serves_pinned_icon(fixture_registry):
    node_id = fixture_registry
    response = authed_client.get(f"/community_nodes/media/{node_id}/icon.png")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"] == "image/png"
    assert response.headers["x-content-type-options"] == "nosniff"


def test_media_rejects_unlisted_file(fixture_registry):
    node_id = fixture_registry
    response = authed_client.get(f"/community_nodes/media/{node_id}/not_a_real_file.png")
    assert response.status_code == 404


def test_screenshots_upload_list_delete_cycle():
    stem = f"pubprep_{uuid.uuid4().hex[:8]}"
    try:
        upload = authed_client.post(
            f"/community_nodes/screenshots/{stem}",
            files={"file": ("shot.png", _FAKE_PNG, "image/png")},
        )
        assert upload.status_code == 200, upload.text
        file_name = upload.json()["file_name"]

        listed = authed_client.get(f"/community_nodes/screenshots/{stem}")
        assert listed.status_code == 200
        assert file_name in {s["file_name"] for s in listed.json()}

        served = authed_client.get(f"/community_nodes/screenshots/{stem}/{file_name}")
        assert served.status_code == 200
        assert served.headers["content-type"] == "image/png"

        deleted = authed_client.delete(f"/community_nodes/screenshots/{stem}/{file_name}")
        assert deleted.status_code == 200
        assert authed_client.get(f"/community_nodes/screenshots/{stem}").json() == []
    finally:
        stem_dir = storage.user_defined_nodes_screenshots / stem
        if stem_dir.exists():
            for p in stem_dir.iterdir():
                p.unlink()
            stem_dir.rmdir()


def test_screenshots_rejects_bad_extension():
    stem = f"pubprep_{uuid.uuid4().hex[:8]}"
    response = authed_client.post(
        f"/community_nodes/screenshots/{stem}",
        files={"file": ("evil.svg", b"<svg/>", "image/svg+xml")},
    )
    assert response.status_code == 400
