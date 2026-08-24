"""Route tests for POST /converters/alteryx.

Covers the happy path (converted flow lands on disk and opens in the handler),
the 400 rejections (extension, invalid XML, zero tools, oversize), the 502 when
opening the converted flow fails, and the 401 for unauthenticated callers.
"""

import io
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from fastapi.testclient import TestClient

from flowfile_core import flow_file_handler, main
from flowfile_core.routes import converters as converters_module

FIXTURE_DIR = Path(__file__).parent.parent / "flowfile" / "converters" / "fixtures"
ENDPOINT = "/converters/alteryx"


def _get_authed_client() -> TestClient:
    with TestClient(main.app) as c:
        token = c.post("/auth/token").json()["access_token"]
    client = TestClient(main.app)
    client.headers = {"Authorization": f"Bearer {token}"}
    return client


client = _get_authed_client()
unauthed_client = TestClient(main.app)


@pytest.fixture(autouse=True)
def flows_dir(tmp_path):
    """Redirect the flows directory so imports never touch real user data."""
    target = tmp_path / "flows"
    target.mkdir()
    with patch.object(
        type(converters_module.storage),
        "flows_directory",
        new=property(lambda self: target),
    ):
        yield target


@pytest.fixture(autouse=True)
def _drop_imported_flows():
    """Remove every flow the test imported from the shared in-memory handler."""
    before = set(flow_file_handler._flows)
    yield
    for flow_id in set(flow_file_handler._flows) - before:
        flow_file_handler.delete_flow(flow_id)


def _post(name: str, content: bytes | None = None):
    payload = content if content is not None else (FIXTURE_DIR / name).read_bytes()
    return client.post(ENDPOINT, files={"file": (name, io.BytesIO(payload), "application/octet-stream")})


class TestHappyPath:
    def test_import_returns_flow_id_path_and_report(self, flows_dir):
        response = _post("formulas.yxmd")
        assert response.status_code == 200, response.text
        body = response.json()

        assert body["flow_id"] > 0
        assert flow_file_handler.get_flow(body["flow_id"]) is not None

        flow_path = Path(body["flow_path"])
        assert flow_path.parent == flows_dir
        assert flow_path.name == "formulas.yaml"
        assert flow_path.exists()

        report = body["report"]
        assert report["workflow_name"] == "formulas"
        assert report["total_tools"] == 2
        assert report["converted"] == 1
        assert report["commented"] == 1
        assert len(report["rows"]) == 2

    def test_written_yaml_is_the_converted_flow(self, flows_dir):
        response = _post("all_supported.yxmd")
        assert response.status_code == 200, response.text

        data = yaml.safe_load(Path(response.json()["flow_path"]).read_text(encoding="utf-8"))
        assert data["flowfile_name"] == "All Supported Tools"
        assert len(data["nodes"]) == len(flow_file_handler.get_flow(response.json()["flow_id"]).nodes)

    def test_xml_extension_is_accepted(self):
        content = (FIXTURE_DIR / "formulas.yxmd").read_bytes()
        assert _post("workflow.xml", content).status_code == 200

    def test_second_import_of_the_same_name_gets_a_unique_file(self, flows_dir):
        first = _post("formulas.yxmd")
        second = _post("formulas.yxmd")
        assert second.status_code == 200, second.text
        assert Path(first.json()["flow_path"]).name == "formulas.yaml"
        assert Path(second.json()["flow_path"]).name == "formulas (1).yaml"
        assert first.json()["flow_id"] != second.json()["flow_id"]


class TestRejections:
    def test_bad_extension(self):
        response = _post("workflow.csv", b"a,b\n1,2\n")
        assert response.status_code == 400
        assert ".csv" in response.json()["detail"]

    def test_invalid_xml(self):
        response = _post("invalid.xml")
        assert response.status_code == 400
        assert "not valid XML" in response.json()["detail"]

    def test_not_an_alteryx_document(self):
        response = _post("workflow.xml", b"<?xml version='1.0'?><SomethingElse />")
        assert response.status_code == 400
        assert "AlteryxDocument" in response.json()["detail"]

    def test_zero_tools(self):
        response = _post("zero_tools.yxmd")
        assert response.status_code == 400
        assert "no Alteryx tools" in response.json()["detail"]

    def test_oversized_upload(self, monkeypatch, flows_dir):
        monkeypatch.setattr(converters_module, "MAX_YXMD_SIZE", 32)
        response = _post("formulas.yxmd")
        assert response.status_code == 400
        assert "too large" in response.json()["detail"].lower()
        assert list(flows_dir.iterdir()) == []


class TestImportFailure:
    def test_failure_to_open_the_flow_returns_502_and_removes_the_yaml(self, monkeypatch, flows_dir):
        def _boom(*args, **kwargs):
            raise RuntimeError("node type not supported")

        monkeypatch.setattr(converters_module.flow_file_handler, "import_flow", _boom)
        response = _post("formulas.yxmd")
        assert response.status_code == 502
        assert "node type not supported" in response.json()["detail"]
        assert list(flows_dir.iterdir()) == []


class TestAuth:
    def test_unauthenticated_request_is_rejected(self):
        response = unauthed_client.post(
            ENDPOINT,
            files={"file": ("formulas.yxmd", io.BytesIO(b"<AlteryxDocument />"), "application/octet-stream")},
        )
        assert response.status_code == 401
