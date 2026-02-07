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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
                "input_paths": {"main": [str(input_path)]},
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
            "left = inputs['left'][0].collect()\n"
            "right = inputs['right'][0].collect()\n"
            "merged = left.join(right, on='id')\n"
            "flowfile.publish_output(merged)\n"
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 11,
                "code": code,
                "flow_id": 1,
                "input_paths": {
                    "left": [str(input_dir / "left.parquet")],
                    "right": [str(input_dir / "right.parquet")],
                },
                "output_dir": str(output_dir),
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"

        df_out = pl.read_parquet(str(output_dir / "main.parquet"))
        assert set(df_out.columns) == {"id", "name", "score"}
        assert len(df_out) == 2

    def test_multi_main_inputs_union(self, client: TestClient, tmp_dir: Path):
        """Multiple paths under 'main' are concatenated (union) by read_input."""
        input_dir = tmp_dir / "inputs"
        output_dir = tmp_dir / "outputs"
        input_dir.mkdir()
        output_dir.mkdir()

        pl.DataFrame({"v": [1, 2]}).write_parquet(str(input_dir / "main_0.parquet"))
        pl.DataFrame({"v": [3, 4]}).write_parquet(str(input_dir / "main_1.parquet"))

        code = (
            "df = flowfile.read_input().collect()\n"
            "flowfile.publish_output(df)\n"
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 13,
                "code": code,
                "flow_id": 1,
                "input_paths": {
                    "main": [
                        str(input_dir / "main_0.parquet"),
                        str(input_dir / "main_1.parquet"),
                    ],
                },
                "output_dir": str(output_dir),
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"

        df_out = pl.read_parquet(str(output_dir / "main.parquet"))
        assert sorted(df_out["v"].to_list()) == [1, 2, 3, 4]

    def test_read_first_via_execute(self, client: TestClient, tmp_dir: Path):
        """read_first returns only the first input file."""
        input_dir = tmp_dir / "inputs"
        output_dir = tmp_dir / "outputs"
        input_dir.mkdir()
        output_dir.mkdir()

        pl.DataFrame({"v": [10, 20]}).write_parquet(str(input_dir / "a.parquet"))
        pl.DataFrame({"v": [30, 40]}).write_parquet(str(input_dir / "b.parquet"))

        code = (
            "df = flowfile.read_first().collect()\n"
            "flowfile.publish_output(df)\n"
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 14,
                "code": code,
                "flow_id": 1,
                "input_paths": {
                    "main": [
                        str(input_dir / "a.parquet"),
                        str(input_dir / "b.parquet"),
                    ],
                },
                "output_dir": str(output_dir),
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"

        df_out = pl.read_parquet(str(output_dir / "main.parquet"))
        assert df_out["v"].to_list() == [10, 20]

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
                "flow_id": 1,
                "input_paths": {"main": [str(input_dir / "main.parquet")]},
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 27,
                "code": 'flowfile.delete_artifact("temp")',
                "flow_id": 1,
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

    def test_same_node_reexecution_clears_own_artifacts(self, client: TestClient):
        """Re-executing the same node auto-clears its previous artifacts."""
        resp1 = client.post(
            "/execute",
            json={
                "node_id": 24,
                "code": 'flowfile.publish_artifact("model", "v1")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp1.json()["success"] is True
        assert "model" in resp1.json()["artifacts_published"]

        # Same node re-executes — should NOT fail with "already exists"
        resp2 = client.post(
            "/execute",
            json={
                "node_id": 24,
                "code": 'flowfile.publish_artifact("model", "v2")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp2.json()["success"] is True
        assert "model" in resp2.json()["artifacts_published"]

        # Verify we get v2
        resp3 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile.read_artifact("model"); print(v)',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp3.json()["success"] is True
        assert "v2" in resp3.json()["stdout"]

    def test_delete_then_republish_via_execute(self, client: TestClient):
        """After deleting, a new artifact with the same name can be published."""
        client.post(
            "/execute",
            json={
                "node_id": 28,
                "code": 'flowfile.publish_artifact("model", "v1")',
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp_read.json()["success"] is True
        assert "v2" in resp_read.json()["stdout"]


class TestClearNodeArtifactsEndpoint:
    def test_clear_node_artifacts_selective(self, client: TestClient):
        """Only artifacts from specified node IDs should be removed."""
        # Publish artifacts from two different nodes
        client.post(
            "/execute",
            json={
                "node_id": 40,
                "code": 'flowfile.publish_artifact("model", {"v": 1})',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 41,
                "code": 'flowfile.publish_artifact("scaler", {"v": 2})',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )

        # Clear only node 40's artifacts
        resp = client.post("/clear_node_artifacts", json={"node_ids": [40]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cleared"
        assert "model" in data["removed"]

        # "scaler" from node 41 should still exist
        artifacts = client.get("/artifacts").json()
        assert "model" not in artifacts
        assert "scaler" in artifacts

    def test_clear_node_artifacts_empty_list(self, client: TestClient):
        """Passing empty list should not remove anything."""
        client.post(
            "/execute",
            json={
                "node_id": 42,
                "code": 'flowfile.publish_artifact("keep_me", 42)',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post("/clear_node_artifacts", json={"node_ids": []})
        assert resp.status_code == 200
        assert resp.json()["removed"] == []
        assert "keep_me" in client.get("/artifacts").json()

    def test_clear_node_artifacts_allows_republish(self, client: TestClient):
        """After clearing, the same artifact name can be re-published."""
        client.post(
            "/execute",
            json={
                "node_id": 43,
                "code": 'flowfile.publish_artifact("reuse", "v1")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        client.post("/clear_node_artifacts", json={"node_ids": [43]})
        resp = client.post(
            "/execute",
            json={
                "node_id": 43,
                "code": 'flowfile.publish_artifact("reuse", "v2")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp.json()["success"] is True


class TestNodeArtifactsEndpoint:
    def test_list_node_artifacts(self, client: TestClient):
        """Should return only artifacts for the specified node."""
        client.post(
            "/execute",
            json={
                "node_id": 50,
                "code": (
                    'flowfile.publish_artifact("a", 1)\n'
                    'flowfile.publish_artifact("b", 2)\n'
                ),
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 51,
                "code": 'flowfile.publish_artifact("c", 3)',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )

        resp = client.get("/artifacts/node/50")
        assert resp.status_code == 200
        data = resp.json()
        assert set(data.keys()) == {"a", "b"}

        resp2 = client.get("/artifacts/node/51")
        assert set(resp2.json().keys()) == {"c"}

    def test_list_node_artifacts_empty(self, client: TestClient):
        resp = client.get("/artifacts/node/999")
        assert resp.status_code == 200
        assert resp.json() == {}


class TestDisplayOutputs:
    def test_display_outputs_empty_by_default(self, client: TestClient):
        """Execute code without displays should return empty display_outputs."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 60,
                "code": 'print("hello")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert data["display_outputs"] == []

    def test_display_output_explicit(self, client: TestClient):
        """Execute flowfile.display() should return a display output."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 61,
                "code": 'flowfile.display("hello")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["mime_type"] == "text/plain"
        assert data["display_outputs"][0]["data"] == "hello"

    def test_display_output_html(self, client: TestClient):
        """Execute flowfile.display() with HTML should return HTML mime type."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 62,
                "code": 'flowfile.display("<b>bold</b>")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["mime_type"] == "text/html"
        assert data["display_outputs"][0]["data"] == "<b>bold</b>"

    def test_display_output_with_title(self, client: TestClient):
        """Display with title should preserve the title."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 63,
                "code": 'flowfile.display("data", title="My Chart")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["title"] == "My Chart"

    def test_multiple_display_outputs(self, client: TestClient):
        """Multiple display calls should return multiple outputs."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 64,
                "code": (
                    'flowfile.display("first")\n'
                    'flowfile.display("second")\n'
                    'flowfile.display("third")\n'
                ),
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert len(data["display_outputs"]) == 3
        assert data["display_outputs"][0]["data"] == "first"
        assert data["display_outputs"][1]["data"] == "second"
        assert data["display_outputs"][2]["data"] == "third"

    def test_display_outputs_cleared_between_executions(self, client: TestClient):
        """Display outputs should not persist between execution calls."""
        # First execution
        client.post(
            "/execute",
            json={
                "node_id": 65,
                "code": 'flowfile.display("from first call")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )

        # Second execution should not include first call's displays
        resp = client.post(
            "/execute",
            json={
                "node_id": 66,
                "code": 'flowfile.display("from second call")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["data"] == "from second call"

    def test_display_output_on_error_still_collected(self, client: TestClient):
        """Display outputs generated before an error should still be returned."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 67,
                "code": (
                    'flowfile.display("before error")\n'
                    'raise ValueError("oops")\n'
                ),
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is False
        assert "ValueError" in data["error"]
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["data"] == "before error"

    def test_interactive_mode_auto_display_last_expression(self, client: TestClient):
        """Interactive mode should auto-display the last expression."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 68,
                "code": "1 + 2 + 3",
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": True,
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["data"] == "6"

    def test_non_interactive_mode_no_auto_display(self, client: TestClient):
        """Non-interactive mode should not auto-display the last expression."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 69,
                "code": "1 + 2 + 3",
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": False,
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert data["display_outputs"] == []

    def test_interactive_mode_with_print_no_double_display(self, client: TestClient):
        """Print statements should not trigger auto-display."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 70,
                "code": 'print("hello")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": True,
            },
        )
        data = resp.json()
        assert data["success"] is True
        # print doesn't return a value worth displaying
        assert data["display_outputs"] == []


class TestContextCleanup:
    def test_context_cleared_after_success(self, client: TestClient):
        """After a successful /execute, the flowfile context should be cleared."""
        client.post(
            "/execute",
            json={
                "node_id": 30,
                "code": "x = 1",
                "flow_id": 1,
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
                "flow_id": 1,
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
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 33,
                "code": 'print("still works")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "still works" in data["stdout"]


class TestFlowIsolation:
    """Artifacts published by different flows don't interfere with each other."""

    def test_same_artifact_name_different_flows(self, client: TestClient):
        """Two flows can each publish an artifact called 'model' independently."""
        resp1 = client.post(
            "/execute",
            json={
                "node_id": 1,
                "code": 'flowfile.publish_artifact("model", "flow1_model")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        assert resp1.json()["success"] is True

        resp2 = client.post(
            "/execute",
            json={
                "node_id": 1,
                "code": 'flowfile.publish_artifact("model", "flow2_model")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )
        assert resp2.json()["success"] is True

        # Each flow reads its own artifact
        resp_read1 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile.read_artifact("model"); print(v)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        assert resp_read1.json()["success"] is True
        assert "flow1_model" in resp_read1.json()["stdout"]

        resp_read2 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile.read_artifact("model"); print(v)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )
        assert resp_read2.json()["success"] is True
        assert "flow2_model" in resp_read2.json()["stdout"]

    def test_flow_cannot_read_other_flows_artifact(self, client: TestClient):
        """Flow 1 publishes 'secret'; flow 2 should not see it."""
        client.post(
            "/execute",
            json={
                "node_id": 1,
                "code": 'flowfile.publish_artifact("secret", "hidden")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 2,
                "code": 'flowfile.read_artifact("secret")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )
        data = resp.json()
        assert data["success"] is False
        assert "not found" in data["error"]

    def test_reexecution_only_clears_own_flow(self, client: TestClient):
        """Re-executing a node in flow 1 doesn't clear flow 2's artifacts."""
        # Flow 1, node 5 publishes "model"
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile.publish_artifact("model", "f1v1")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        # Flow 2, node 5 publishes "model"
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile.publish_artifact("model", "f2v1")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )

        # Re-execute node 5 in flow 1 — auto-clear only affects flow 1
        resp = client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile.publish_artifact("model", "f1v2")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        assert resp.json()["success"] is True

        # Flow 2's artifact should be untouched
        resp_f2 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile.read_artifact("model"); print(v)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )
        assert resp_f2.json()["success"] is True
        assert "f2v1" in resp_f2.json()["stdout"]

    def test_list_artifacts_filtered_by_flow(self, client: TestClient):
        """GET /artifacts?flow_id=X returns only that flow's artifacts."""
        client.post(
            "/execute",
            json={
                "node_id": 1,
                "code": 'flowfile.publish_artifact("a", 1)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 10,
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 2,
                "code": 'flowfile.publish_artifact("b", 2)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 20,
            },
        )

        resp10 = client.get("/artifacts", params={"flow_id": 10})
        assert set(resp10.json().keys()) == {"a"}

        resp20 = client.get("/artifacts", params={"flow_id": 20})
        assert set(resp20.json().keys()) == {"b"}

        # No filter returns both
        resp_all = client.get("/artifacts")
        assert set(resp_all.json().keys()) == {"a", "b"}

    def test_clear_node_artifacts_scoped_to_flow(self, client: TestClient):
        """POST /clear_node_artifacts with flow_id only clears that flow."""
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile.publish_artifact("model", "f1")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile.publish_artifact("model", "f2")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )

        resp = client.post(
            "/clear_node_artifacts",
            json={"node_ids": [5], "flow_id": 1},
        )
        assert resp.json()["status"] == "cleared"
        assert "model" in resp.json()["removed"]

        # Flow 2's artifact survives
        artifacts_f2 = client.get("/artifacts", params={"flow_id": 2}).json()
        assert "model" in artifacts_f2
