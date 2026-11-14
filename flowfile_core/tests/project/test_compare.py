"""``kinds.compare`` diffs two trees per kind and key, and ``POST /project/plan`` answers it for a restore or reload.

The pure cases run on the every-kind tree ``test_kinds.py`` recorded; the route cases on owner 1, real git and
the test database.
"""

import copy
import json
from uuid import uuid4

import pytest
import yaml
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import DatabaseConnection
from flowfile_core.project import git_ops, kinds, project_sync
from tests.project.test_kinds import FIXTURE
from tests.project.test_restore_safety import _delete_db_connection, _make_db_connection

OWNER = 1
TREE = json.loads(FIXTURE.read_text(encoding="utf-8"))

# kind: (file, list key or None for a file per object, key field, field an edit changes)
SPECS = {
    "secrets": ("secrets.yaml", "required_secrets", None, None),
    "database_connection": ("connections/database/kinds_db.yaml", None, "connection_name", "host"),
    "cloud_connection": ("connections/cloud/kinds_cloud.yaml", None, "connection_name", "aws_region"),
    "namespaces": ("namespaces.yaml", "namespaces", "catalog", "description"),
    "flow": ("flows/flow_b.flow.yaml", None, "flow_uuid", "catalog_name"),
    "schedule": ("schedules/flow_a.yaml", "schedules", "id", "name"),
    "tables": ("tables.yaml", "tables", "name", "storage_format"),
    "models": ("models.yaml", "models", "version", "serialization_format"),
    "kernels": ("kernels.yaml", "kernels", "id", "name"),
    "visualizations": ("visualizations.yaml", "visualizations", "viz_uuid", "chart_type"),
    "dashboards": ("dashboards.yaml", "dashboards", "dashboard_uuid", "name"),
    "notebook": ("notebooks/kinds_notebook.notebook.yaml", None, "notebook_uuid", "description"),
}


def _changed(kind: str, change: str) -> dict[str, str]:
    path, list_key, key, field = SPECS[kind]
    tree = dict(TREE)
    data = yaml.safe_load(tree[path])
    if list_key is None:
        if change == "added":
            data[key] = "new"
            folder, _, name = path.rpartition("/")
            tree[f"{folder}/new_{name}"] = yaml.safe_dump(data, sort_keys=False)
        elif change == "modified":
            data[field] = "changed"
            tree[path] = yaml.safe_dump(data, sort_keys=False)
        else:
            del tree[path]
        return tree
    items = data[list_key]
    if kind == "secrets":
        items.append("new") if change == "added" else items.pop()
    elif change == "added":
        item = copy.deepcopy(items[0])
        item.pop("schemas", None)
        item[key] = "new"
        items.append(item)
    elif change == "modified":
        items[0][field] = "changed"
    elif kind == "namespaces":
        items[0]["schemas"].pop()
    else:
        items.pop(0)
    tree[path] = yaml.safe_dump(data, sort_keys=False)
    return tree


def test_a_tree_compared_with_itself_is_empty():
    assert kinds.compare(TREE, TREE) == []


CASES = [(k, c) for k in SPECS for c in ("added", "modified", "removed") if (k, c) != ("secrets", "modified")]


@pytest.mark.parametrize("kind, change", CASES)
def test_one_change_of_each_kind_is_one_row(kind, change):
    rows = [(c.kind, c.change) for c in kinds.compare(TREE, _changed(kind, change))]
    expected = [] if kind == "secrets" and change == "removed" else [(kind, change)]
    assert rows == expected


def test_a_comment_in_a_root_file_is_no_change():
    tree = {**TREE, "tables.yaml": "# by hand\n" + TREE["tables.yaml"]}
    assert kinds.compare(TREE, tree) == []


def test_schedules_without_an_id_differing_only_in_backend_are_two_entries():
    data = yaml.safe_load(TREE["schedules/flow_a.yaml"])
    data["schedules"][0].pop("id")
    before = {**TREE, "schedules/flow_a.yaml": yaml.safe_dump(data, sort_keys=False)}
    data["schedules"].append({**data["schedules"][0], "backend": "worker:other"})
    after = {**TREE, "schedules/flow_a.yaml": yaml.safe_dump(data, sort_keys=False)}
    assert [(c.kind, c.change) for c in kinds.compare(before, after)] == [("schedule", "added")]


def test_kinds_restricts_the_comparison():
    tree = _changed("flow", "modified")
    tree.update({k: v for k, v in _changed("kernels", "removed").items() if k == "kernels.yaml"})
    assert [c.kind for c in kinds.compare(TREE, tree, kinds={"kernels"})] == ["kernels"]


def _make_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _make_client()


@pytest.fixture
def project(tmp_path):
    if not git_ops.git_available():
        pytest.skip("git not available")
    name = f"plan_{uuid4().hex[:6]}"
    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    assert client.post("/project/init", json={"folder_path": str(root), "name": "Plan"}).status_code == 201
    before = git_ops.head_sha(root)
    _make_db_connection(name)
    project_sync.save_version(OWNER, "with the connection")
    try:
        yield root, before, name
    finally:
        project_sync.close_project(OWNER)
        _delete_db_connection(name)


def _rows_for(response, name: str) -> list[dict]:
    assert response.status_code == 200, response.text
    return [c for c in response.json()["changes"] if c["label"] == name]


def test_plan_of_a_version_names_a_removed_connection_and_its_consequence(project):
    root, before, name = project
    rows = _rows_for(client.post("/project/plan", json={"sha": before}), name)
    assert rows == [
        {
            "change": "removed",
            "kind": "database connection",
            "label": name,
            "path": f"connections/database/{name}.yaml",
            "consequence": "its stored password is not in any version",
        }
    ]


def test_plan_of_the_folder_names_a_hand_deleted_file(project):
    root, _before, name = project
    assert _rows_for(client.post("/project/plan"), name) == []
    (root / "connections" / "database" / f"{name}.yaml").unlink()
    assert [r["change"] for r in _rows_for(client.post("/project/plan"), name)] == ["removed"]


def test_plan_of_a_malformed_folder_is_a_422_that_changes_nothing(project):
    root, _before, name = project
    (root / "secrets.yaml").write_text("required_secrets: [\n", encoding="utf-8")
    head = git_ops.head_sha(root)
    assert client.post("/project/plan").status_code == 422
    assert git_ops.head_sha(root) == head
    assert (root / "secrets.yaml").read_text(encoding="utf-8") == "required_secrets: [\n"
    with get_db_context() as db:
        assert db.query(DatabaseConnection).filter_by(connection_name=name, user_id=OWNER).count() == 1


def test_plan_with_a_malformed_sha_is_a_422(project):
    assert client.post("/project/plan", json={"sha": "not-a-sha"}).status_code == 422
