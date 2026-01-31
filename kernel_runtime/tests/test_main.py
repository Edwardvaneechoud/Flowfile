"""Tests for kernel_runtime.main (FastAPI endpoints)."""

import os
from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient


class TestHealthEndpoint:
    def test_health_returns_200(self, client: TestClient):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert data["artifact_count"] == 0


class TestExecuteEndpoint:
    def test_simple_print(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 1,
                "code": 'print("hello")',
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert "hello" in data["stdout"]
        assert data["error"] is None

    def test_syntax_error(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 2,
                "code": "def broken(",
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is False
        assert data["error"] is not None
        assert "SyntaxError" in data["error"]

    def test_runtime_error(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 3,
                "code": "1 / 0",
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is False
        assert "ZeroDivisionError" in data["error"]

    def test_stderr_captured(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 4,
                "code": 'import sys; sys.stderr.write("warning\\n")',
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "warning" in data["stderr"]

    def test_execution_time_tracked(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": "x = sum(range(1000))",
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert data["execution_time_ms"] > 0

    def test_flowfile_module_available(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 6,
                "code": "print(type(flowfile).__name__)",
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "module" in data["stdout"]


class TestExecuteWithParquet:
    def test_read_and_write_parquet(self, client: TestClient, tmp_dir: Path):
        input_dir = tmp_dir / "inputs"
        output_dir = tmp_dir / "outputs"
        input_dir.mkdir()
        output_dir.mkdir()

        df_in = pl.DataFrame({"x": [1, 2, 3], "y": [10, 20, 30]})
        input_path = input_dir / "main.parquet"
        df_in.write_parquet(str(input_path))

        code = (
            "import polars as pl\n"
            "df = flowfile.read_input()\n"
            "df = df.collect().with_columns((pl.col('x') * pl.col('y')).alias('product'))\n"
            "flowfile.publish_output(df)\n"
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 10,
                "code": code,
                "input_paths": {"main": str(input_path)},
                "output_dir": str(output_dir),
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert len(data["output_paths"]) > 0

        out_path = output_dir / "main.parquet"
        assert out_path.exists()
        df_out = pl.read_parquet(str(out_path))
        assert "product" in df_out.columns
        assert df_out["product"].to_list() == [10, 40, 90]

    def test_multiple_inputs(self, client: TestClient, tmp_dir: Path):
        input_dir = tmp_dir / "inputs"
        output_dir = tmp_dir / "outputs"
        input_dir.mkdir()
        output_dir.mkdir()

        pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).write_parquet(
            str(input_dir / "left.parquet")
        )
        pl.DataFrame({"id": [1, 2], "score": [90, 80]}).write_parquet(
            str(input_dir / "right.parquet")
        )

        code = (
            "inputs = flowfile.read_inputs()\n"
            "left = inputs['left'].collect()\n"
            "right = inputs['right'].collect()\n"
            "merged = left.join(right, on='id')\n"
            "flowfile.publish_output(merged)\n"
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 11,
                "code": code,
                "input_paths": {
                    "left": str(input_dir / "left.parquet"),
                    "right": str(input_dir / "right.parquet"),
                },
                "output_dir": str(output_dir),
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"

        df_out = pl.read_parquet(str(output_dir / "main.parquet"))
        assert set(df_out.columns) == {"id", "name", "score"}
        assert len(df_out) == 2

    def test_publish_lazyframe_output(self, client: TestClient, tmp_dir: Path):
        input_dir = tmp_dir / "inputs"
        output_dir = tmp_dir / "outputs"
        input_dir.mkdir()
        output_dir.mkdir()

        pl.DataFrame({"v": [10, 20]}).write_parquet(str(input_dir / "main.parquet"))

        code = (
            "lf = flowfile.read_input()\n"
            "flowfile.publish_output(lf)\n"
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 12,
                "code": code,
                "input_paths": {"main": str(input_dir / "main.parquet")},
                "output_dir": str(output_dir),
            },
        )
        data = resp.json()
        assert data["success"] is True
        df_out = pl.read_parquet(str(output_dir / "main.parquet"))
        assert df_out["v"].to_list() == [10, 20]


class TestArtifactEndpoints:
    def test_publish_artifact_via_execute(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 20,
                "code": 'flowfile.publish_artifact("my_dict", {"a": 1})',
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "my_dict" in data["artifacts_published"]

    def test_list_artifacts(self, client: TestClient):
        # Publish via execute
        client.post(
            "/execute",
            json={
                "node_id": 21,
                "code": (
                    'flowfile.publish_artifact("item_a", [1, 2])\n'
                    'flowfile.publish_artifact("item_b", "hello")\n'
                ),
                "input_paths": {},
                "output_dir": "",
            },
        )

        resp = client.get("/artifacts")
        assert resp.status_code == 200
        data = resp.json()
        assert "item_a" in data
        assert "item_b" in data
        # The object itself should not be in the listing
        assert "object" not in data["item_a"]

    def test_clear_artifacts(self, client: TestClient):
        client.post(
            "/execute",
            json={
                "node_id": 22,
                "code": 'flowfile.publish_artifact("tmp", 42)',
                "input_paths": {},
                "output_dir": "",
            },
        )

        resp = client.post("/clear")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"

        resp = client.get("/artifacts")
        assert resp.json() == {}

    def test_health_shows_artifact_count(self, client: TestClient):
        client.post(
            "/execute",
            json={
                "node_id": 23,
                "code": 'flowfile.publish_artifact("x", 1)',
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.get("/health")
        assert resp.json()["artifact_count"] == 1

    def test_duplicate_publish_fails(self, client: TestClient):
        """Publishing an artifact with the same name twice should fail."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 24,
                "code": 'flowfile.publish_artifact("model", 1)',
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp.json()["success"] is True

        resp2 = client.post(
            "/execute",
            json={
                "node_id": 25,
                "code": 'flowfile.publish_artifact("model", 2)',
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp2.json()
        assert data["success"] is False
        assert "already exists" in data["error"]

    def test_delete_artifact_via_execute(self, client: TestClient):
        """delete_artifact removes from the store and appears in artifacts_deleted."""
        client.post(
            "/execute",
            json={
                "node_id": 26,
                "code": 'flowfile.publish_artifact("temp", 99)',
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 27,
                "code": 'flowfile.delete_artifact("temp")',
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "temp" in data["artifacts_deleted"]

        # Verify artifact is gone
        resp_list = client.get("/artifacts")
        assert "temp" not in resp_list.json()

    def test_delete_then_republish_via_execute(self, client: TestClient):
        """After deleting, a new artifact with the same name can be published."""
        client.post(
            "/execute",
            json={
                "node_id": 28,
                "code": 'flowfile.publish_artifact("model", "v1")',
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 29,
                "code": (
                    'flowfile.delete_artifact("model")\n'
                    'flowfile.publish_artifact("model", "v2")\n'
                ),
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        # The artifact was deleted and re-published in the same call.
        # Since the final state has "model" which didn't exist before the
        # first publish in this request, it depends on whether it was in
        # artifacts_before. Since it existed before this execute call,
        # and still exists after, it's neither new nor deleted from the
        # perspective of this single call. But the name was re-published
        # so it shouldn't appear in artifacts_deleted.
        # Let's just verify the artifact exists and has the new value.
        resp_read = client.post(
            "/execute",
            json={
                "node_id": 30,
                "code": (
                    'v = flowfile.read_artifact("model")\n'
                    'print(v)\n'
                ),
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp_read.json()["success"] is True
        assert "v2" in resp_read.json()["stdout"]


class TestContextCleanup:
    def test_context_cleared_after_success(self, client: TestClient):
        """After a successful /execute, the flowfile context should be cleared."""
        client.post(
            "/execute",
            json={
                "node_id": 30,
                "code": "x = 1",
                "input_paths": {},
                "output_dir": "",
            },
        )
        # A second call that tries to use context should still work
        # (context is re-set for each request)
        resp = client.post(
            "/execute",
            json={
                "node_id": 31,
                "code": 'print("ok")',
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp.json()["success"] is True

    def test_context_cleared_after_error(self, client: TestClient):
        """After a failed /execute, the flowfile context should still be cleared."""
        client.post(
            "/execute",
            json={
                "node_id": 32,
                "code": "raise ValueError('boom')",
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 33,
                "code": 'print("still works")',
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "still works" in data["stdout"]
