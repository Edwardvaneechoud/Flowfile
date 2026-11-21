"""A version's change list names the steps that changed inside each changed flow (``shared.flow_merge``)."""

import pytest
from fastapi.testclient import TestClient

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.database.models import FlowRegistration
from flowfile_core.flowfile.catalog_helpers import auto_register_flow
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.project import git_ops, project_sync
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


def _flow(flow_id: int, flow_path: str):
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name="stepwise", path="."))
    graph = handler.get_flow(flow_id)
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=1, node_type="manual_input"))
    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=flow_id, node_id=1, raw_data_format=input_schema.RawData.from_pylist([{"a": 1}])
        )
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=flow_id, node_id=2, node_type="sample"))
    graph.add_sample(input_schema.NodeSample(flow_id=flow_id, node_id=2, depending_on_id=1, sample_size=10))
    graph.save_flow(flow_path)
    return graph


def test_a_changed_flow_lists_its_changed_steps(tmp_path):
    root = tmp_path / "proj"
    flow_path = str(tmp_path / "stepwise.flow.yaml")
    project_sync.close_project(OWNER)
    flow_uuid = None
    try:
        graph = _flow(737373, flow_path)
        auto_register_flow(flow_path, "stepwise", OWNER)
        with get_db_context() as db:
            flow_uuid = db.query(FlowRegistration).filter_by(flow_path=flow_path).first().flow_uuid
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Steps"}).status_code == 201

        graph.add_sample(input_schema.NodeSample(flow_id=737373, node_id=2, depending_on_id=1, sample_size=25))
        graph.save_flow(flow_path)
        assert client.post("/project/versions", json={"message": "bigger sample"}).json()["sha"]
        sha = client.get("/project/versions").json()["versions"][0]["sha"]

        changes = client.get(f"/project/versions/{sha}/diff").json()["changes"]
        flow_row = next(c for c in changes if c["kind"] == "flow")
        assert flow_row["change"] == "modified"
        assert flow_row["steps"] == [
            {"kind": "changed", "label": flow_row["steps"][0]["label"], "details": ["settings.sample_size: 10 → 25"]}
        ]
        assert flow_row["steps"][0]["label"].startswith("step 2 sample")
    finally:
        project_sync.close_project(OWNER)
        with get_db_context() as db:
            for reg in db.query(FlowRegistration).filter_by(flow_uuid=flow_uuid).all():
                db.delete(reg)
            db.commit()


def test_init_writes_gitattributes_and_commits_it(tmp_path):
    """Git merges and diffs a project's flow files through ``flowfile-merge`` once the clone is configured."""
    root = tmp_path / "proj"
    project_sync.close_project(OWNER)
    try:
        assert client.post("/project/init", json={"folder_path": str(root), "name": "Attrs"}).status_code == 201
        text = (root / ".gitattributes").read_text(encoding="utf-8")
        assert "flows/*.flow.yaml merge=flowfile diff=flowfile" in text
        assert "git config merge.flowfile.driver" in text
        from git import Repo

        assert ".gitattributes" in Repo(str(root)).git.ls_files().split("\n")
    finally:
        project_sync.close_project(OWNER)
