"""Tests for kernel_runtime.main (FastAPI endpoints)."""

import os
import signal
import threading
import time
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

    def test_broken_matplotlib_does_not_abort_cell(self, client: TestClient, monkeypatch):
        """A present-but-broken matplotlib must not fail the user's code.

        Seen in the wild as ``AttributeError: module 'matplotlib' has no attribute
        '_docstring'`` while pip was mid-install in the kernel: the plt.show() hook
        raised something other than ImportError and took the whole cell with it.
        """
        import sys
        import types

        broken = types.ModuleType("matplotlib")

        def _use(*args, **kwargs):
            raise AttributeError("module 'matplotlib' has no attribute '_docstring'")

        broken.use = _use
        monkeypatch.setitem(sys.modules, "matplotlib", broken)

        resp = client.post(
            "/execute",
            json={
                "node_id": 4,
                "code": 'print("ran anyway")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "ran anyway" in data["stdout"]
        assert data["error"] is None

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

    def test_flowfile_ctx_module_available(self, client: TestClient):
        """``flowfile_ctx`` is injected as the canonical kernel-context module."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 6,
                "code": "print(type(flowfile_ctx).__name__)",
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
            "df = flowfile_ctx.read_input()\n"
            "df = df.collect().with_columns((pl.col('x') * pl.col('y')).alias('product'))\n"
            "flowfile_ctx.publish_output(df)\n"
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

        pl.DataFrame({"id": [1, 2], "name": ["a", "b"]}).write_parquet(str(input_dir / "left.parquet"))
        pl.DataFrame({"id": [1, 2], "score": [90, 80]}).write_parquet(str(input_dir / "right.parquet"))

        code = (
            "inputs = flowfile_ctx.read_inputs()\n"
            "left = inputs['left'][0].collect()\n"
            "right = inputs['right'][0].collect()\n"
            "merged = left.join(right, on='id')\n"
            "flowfile_ctx.publish_output(merged)\n"
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

        code = "df = flowfile_ctx.read_input().collect()\n" "flowfile_ctx.publish_output(df)\n"

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

        code = "df = flowfile_ctx.read_first().collect()\n" "flowfile_ctx.publish_output(df)\n"

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

        code = "lf = flowfile_ctx.read_input()\n" "flowfile_ctx.publish_output(lf)\n"

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

    def test_legacy_flowfile_alias_still_works_with_warning(self, client: TestClient, tmp_dir: Path):
        """Legacy ``flowfile.foo()`` continues to forward to ``flowfile_ctx`` but
        emits a ``DeprecationWarning`` so users migrate. Pins both halves of the
        backward-compat contract: forwarding works AND warning fires.

        (We assert via ``pytest.warns`` because pytest's warning subsystem
        intercepts the warning before it reaches the kernel's ``stderr_buf``
        redirect. In production there is no pytest interception, so the message
        does reach the user-visible stderr panel.)"""
        input_dir = tmp_dir / "inputs"
        output_dir = tmp_dir / "outputs"
        input_dir.mkdir()
        output_dir.mkdir()

        pl.DataFrame({"v": [1, 2, 3]}).write_parquet(str(input_dir / "main.parquet"))

        code = "df = flowfile.read_input().collect()\nflowfile.publish_output(df)\n"

        with pytest.warns(DeprecationWarning, match=r"flowfile_ctx"):
            resp = client.post(
                "/execute",
                json={
                    "node_id": 99,
                    "code": code,
                    "flow_id": 42,
                    "input_paths": {"main": [str(input_dir / "main.parquet")]},
                    "output_dir": str(output_dir),
                },
            )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        df_out = pl.read_parquet(str(output_dir / "main.parquet"))
        assert df_out["v"].to_list() == [1, 2, 3]


class TestArtifactEndpoints:
    def test_publish_artifact_via_execute(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 20,
                "code": 'flowfile_ctx.publish_artifact("my_dict", {"a": 1})',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "my_dict" in [a["name"] for a in data["artifacts_published"]]

    def test_list_artifacts(self, client: TestClient):
        client.post(
            "/execute",
            json={
                "node_id": 21,
                "code": (
                    'flowfile_ctx.publish_artifact("item_a", [1, 2])\n' 'flowfile_ctx.publish_artifact("item_b", "hello")\n'
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
        assert "object" not in data["item_a"]

    def test_clear_artifacts(self, client: TestClient):
        client.post(
            "/execute",
            json={
                "node_id": 22,
                "code": 'flowfile_ctx.publish_artifact("tmp", 42)',
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
                "code": 'flowfile_ctx.publish_artifact("x", 1)',
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
                "code": 'flowfile_ctx.publish_artifact("model", 1)',
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
                "code": 'flowfile_ctx.publish_artifact("model", 2)',
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
                "code": 'flowfile_ctx.publish_artifact("temp", 99)',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 27,
                "code": 'flowfile_ctx.delete_artifact("temp")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "temp" in data["artifacts_deleted"]

        resp_list = client.get("/artifacts")
        assert "temp" not in resp_list.json()

    def test_same_node_reexecution_clears_own_artifacts(self, client: TestClient):
        """Re-executing the same node auto-clears its previous artifacts."""
        resp1 = client.post(
            "/execute",
            json={
                "node_id": 24,
                "code": 'flowfile_ctx.publish_artifact("model", "v1")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp1.json()["success"] is True
        assert "model" in [a["name"] for a in resp1.json()["artifacts_published"]]

        resp2 = client.post(
            "/execute",
            json={
                "node_id": 24,
                "code": 'flowfile_ctx.publish_artifact("model", "v2")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp2.json()["success"] is True
        assert "model" in [a["name"] for a in resp2.json()["artifacts_published"]]

        resp3 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile_ctx.read_artifact("model"); print(v)',
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
                "code": 'flowfile_ctx.publish_artifact("model", "v1")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        resp = client.post(
            "/execute",
            json={
                "node_id": 29,
                "code": ('flowfile_ctx.delete_artifact("model")\n' 'flowfile_ctx.publish_artifact("model", "v2")\n'),
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        resp_read = client.post(
            "/execute",
            json={
                "node_id": 30,
                "code": ('v = flowfile_ctx.read_artifact("model")\n' "print(v)\n"),
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
        client.post(
            "/execute",
            json={
                "node_id": 40,
                "code": 'flowfile_ctx.publish_artifact("model", {"v": 1})',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 41,
                "code": 'flowfile_ctx.publish_artifact("scaler", {"v": 2})',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )

        resp = client.post("/clear_node_artifacts", json={"node_ids": [40]})
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "cleared"
        assert "model" in data["removed"]

        artifacts = client.get("/artifacts").json()
        assert "model" not in artifacts
        assert "scaler" in artifacts

    def test_clear_node_artifacts_empty_list(self, client: TestClient):
        """Passing empty list should not remove anything."""
        client.post(
            "/execute",
            json={
                "node_id": 42,
                "code": 'flowfile_ctx.publish_artifact("keep_me", 42)',
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
                "code": 'flowfile_ctx.publish_artifact("reuse", "v1")',
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
                "code": 'flowfile_ctx.publish_artifact("reuse", "v2")',
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
                "code": ('flowfile_ctx.publish_artifact("a", 1)\n' 'flowfile_ctx.publish_artifact("b", 2)\n'),
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 51,
                "code": 'flowfile_ctx.publish_artifact("c", 3)',
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
        """Execute flowfile_ctx.display() should return a display output."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 61,
                "code": 'flowfile_ctx.display("hello")',
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
        """Execute flowfile_ctx.display() with HTML should return HTML mime type."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 62,
                "code": 'flowfile_ctx.display("<b>bold</b>")',
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
                "code": 'flowfile_ctx.display("data", title="My Chart")',
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
                "code": ('flowfile_ctx.display("first")\n' 'flowfile_ctx.display("second")\n' 'flowfile_ctx.display("third")\n'),
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
        client.post(
            "/execute",
            json={
                "node_id": 65,
                "code": 'flowfile_ctx.display("from first call")',
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 66,
                "code": 'flowfile_ctx.display("from second call")',
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
                "code": ('flowfile_ctx.display("before error")\n' 'raise ValueError("oops")\n'),
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

    def test_interactive_bare_dataframe_is_text_not_table(self, client: TestClient):
        """A bare DataFrame as the last expression shows its repr, not the table."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 70,
                "code": "import polars as pl\npl.DataFrame({'a': [1, 2]})",
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": True,
            },
        )
        data = resp.json()
        assert data["success"] is True
        outs = data["display_outputs"]
        assert len(outs) == 1
        assert outs[0]["mime_type"] == "text/plain"
        assert "shape:" in outs[0]["data"]

    def test_interactive_explicit_display_dataframe_is_table(self, client: TestClient):
        """An explicit display(df) renders the interactive table (and isn't double-wrapped)."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 71,
                "code": "import polars as pl\nflowfile_ctx.display(pl.DataFrame({'a': [1]}))",
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": True,
            },
        )
        data = resp.json()
        assert data["success"] is True
        outs = data["display_outputs"]
        assert len(outs) == 1
        assert outs[0]["mime_type"] == "application/vnd.flowfile.table+json"

    def test_explore_dataframe_emits_gwalker_mime(self, client: TestClient):
        """flowfile_ctx.explore(df) renders via the full Graphic Walker explorer."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 72,
                "code": "import polars as pl\nflowfile_ctx.explore(pl.DataFrame({'a': [1, 2, 3]}))",
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert len(data["display_outputs"]) == 1
        assert data["display_outputs"][0]["mime_type"] == "application/vnd.flowfile.gwalker+json"

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
        assert data["display_outputs"] == []


class TestPublishGlobalInteractiveExecution:
    """publish_global should be silently skipped when running interactively."""

    def test_publish_global_skipped_in_interactive_mode(self, client: TestClient):
        """Calling publish_global without a source_registration_id should
        succeed but degrade gracefully (warn + return -1)."""
        code = 'result = flowfile_ctx.publish_global("my_model", {"key": "value"})\nprint(result)'
        resp = client.post(
            "/execute",
            json={
                "node_id": 80,
                "code": code,
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": True,
                "source_registration_id": None,
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "without a source_registration_id" in data["stdout"]
        assert "-1" in data["stdout"]

    def test_publish_global_warns_in_flow_mode_without_registration(self, client: TestClient):
        """Flow mode without a source_registration_id mirrors interactive mode:
        publish_global warns + returns -1 instead of raising. Core normally
        provisions a scratch FlowRegistration so this fallback only fires
        when talking to an older Core.
        """
        code = 'result = flowfile_ctx.publish_global("my_model", {"key": "value"})\nprint(result)'
        resp = client.post(
            "/execute",
            json={
                "node_id": 81,
                "code": code,
                "flow_id": 1,
                "input_paths": {},
                "output_dir": "",
                "interactive": False,
                "source_registration_id": None,
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "without a source_registration_id" in data["stdout"]
        assert "-1" in data["stdout"]


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
                "code": 'flowfile_ctx.publish_artifact("model", "flow1_model")',
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
                "code": 'flowfile_ctx.publish_artifact("model", "flow2_model")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )
        assert resp2.json()["success"] is True

        resp_read1 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile_ctx.read_artifact("model"); print(v)',
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
                "code": 'v = flowfile_ctx.read_artifact("model"); print(v)',
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
                "code": 'flowfile_ctx.publish_artifact("secret", "hidden")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 2,
                "code": 'flowfile_ctx.read_artifact("secret")',
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
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile_ctx.publish_artifact("model", "f1v1")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile_ctx.publish_artifact("model", "f2v1")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 2,
            },
        )

        resp = client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile_ctx.publish_artifact("model", "f1v2")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        assert resp.json()["success"] is True

        resp_f2 = client.post(
            "/execute",
            json={
                "node_id": 99,
                "code": 'v = flowfile_ctx.read_artifact("model"); print(v)',
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
                "code": 'flowfile_ctx.publish_artifact("a", 1)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 10,
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 2,
                "code": 'flowfile_ctx.publish_artifact("b", 2)',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 20,
            },
        )

        resp10 = client.get("/artifacts", params={"flow_id": 10})
        assert set(resp10.json().keys()) == {"a"}

        resp20 = client.get("/artifacts", params={"flow_id": 20})
        assert set(resp20.json().keys()) == {"b"}

        resp_all = client.get("/artifacts")
        assert set(resp_all.json().keys()) == {"a", "b"}

    def test_clear_node_artifacts_scoped_to_flow(self, client: TestClient):
        """POST /clear_node_artifacts with flow_id only clears that flow."""
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile_ctx.publish_artifact("model", "f1")',
                "input_paths": {},
                "output_dir": "",
                "flow_id": 1,
            },
        )
        client.post(
            "/execute",
            json={
                "node_id": 5,
                "code": 'flowfile_ctx.publish_artifact("model", "f2")',
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

        artifacts_f2 = client.get("/artifacts", params={"flow_id": 2}).json()
        assert "model" in artifacts_f2


class TestExecutionCancellation:
    """Tests for execution cancellation via /interrupt and SIGUSR1."""

    @pytest.mark.skipif(
        os.getenv("GITHUB_ACTIONS") == "true" or os.getenv("CI") == "true",
        reason="Signal-based thread interrupt is unreliable in CI runners.",
    )
    def test_request_interrupt_hits_running_cell(self):
        """_request_interrupt raises KeyboardInterrupt in the registered cell's thread."""
        import kernel_runtime.main as main_module

        caught: list[bool] = [False]
        ready = threading.Event()

        def _target():
            ready.set()
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    pass  # bytecode loop so the async KeyboardInterrupt fires promptly
            except KeyboardInterrupt:
                caught[0] = True

        # Register the handler ourselves (the lifespan normally does): sending SIGUSR1
        # with the default disposition would kill the test process.
        prev = signal.getsignal(signal.SIGUSR1)
        signal.signal(signal.SIGUSR1, main_module._cancel_signal_handler)
        t = threading.Thread(target=_target, daemon=True)
        t.start()
        ready.wait()

        with main_module._exec_lock:
            main_module._exec_generation += 1
            gen = main_module._exec_generation
            main_module._running_execs[gen] = t.ident
        try:
            assert main_module._request_interrupt() is True
            t.join(timeout=5)
            assert caught[0], "KeyboardInterrupt was not raised in the target thread"
        finally:
            with main_module._exec_lock:
                main_module._running_execs.pop(gen, None)
                main_module._interrupt_generation = None
            signal.signal(signal.SIGUSR1, prev)

    def test_request_interrupt_noop_when_not_executing(self):
        """With no running cell the interrupt request is a no-op (no crash)."""
        import kernel_runtime.main as main_module

        with main_module._exec_lock:
            main_module._running_execs.clear()
            main_module._interrupt_generation = None
        assert main_module._request_interrupt() is False
        main_module._cancel_signal_handler(None, None)  # should not raise

    def test_running_execs_cleared_after_success(self, client: TestClient):
        """No execution stays registered after a successful cell."""
        import kernel_runtime.main as main_module

        resp = client.post(
            "/execute",
            json={"node_id": 200, "code": "x = 1", "flow_id": 1, "input_paths": {}, "output_dir": ""},
        )
        assert resp.json()["success"] is True
        assert main_module._running_execs == {}

    def test_running_execs_cleared_after_error(self, client: TestClient):
        """No execution stays registered even when user code raises."""
        import kernel_runtime.main as main_module

        resp = client.post(
            "/execute",
            json={"node_id": 201, "code": "1/0", "flow_id": 1, "input_paths": {}, "output_dir": ""},
        )
        assert resp.json()["success"] is False
        assert main_module._running_execs == {}

    def test_stale_interrupt_does_not_target_later_cell(self, client: TestClient):
        """An interrupt bound to an earlier (still-registered) cell must never land on a new cell."""
        import kernel_runtime.main as main_module

        # A real, harmless "cell A" thread (pthread_kill needs a valid tid); interrupting it is safe.
        stop = threading.Event()
        a_ready = threading.Event()

        def _cell_a():
            a_ready.set()
            try:
                while not stop.wait(0.02):
                    pass
            except KeyboardInterrupt:
                pass

        prev = signal.getsignal(signal.SIGUSR1)
        signal.signal(signal.SIGUSR1, main_module._cancel_signal_handler)
        a = threading.Thread(target=_cell_a, daemon=True)
        a.start()
        a_ready.wait()
        with main_module._exec_lock:
            main_module._exec_generation += 1
            gen_a = main_module._exec_generation
            main_module._running_execs[gen_a] = a.ident
        try:
            assert main_module._request_interrupt() is True  # binds _interrupt_generation to A
            # A fresh cell B must run cleanly: the stale interrupt is bound to A, never B.
            resp = client.post(
                "/execute",
                json={"node_id": 100, "code": "x = 1 + 1", "flow_id": 999, "input_paths": {}, "output_dir": ""},
            )
            assert resp.status_code == 200
            assert resp.json()["success"] is True
        finally:
            stop.set()
            with main_module._exec_lock:
                main_module._running_execs.pop(gen_a, None)
                main_module._interrupt_generation = None
            signal.signal(signal.SIGUSR1, prev)
            a.join(timeout=5)

    def test_signal_handler_does_not_resend_sigusr1(self, monkeypatch):
        """The SIGUSR1 handler re-asserts the interrupt but never re-sends a signal (no storm)."""
        import kernel_runtime.main as main_module

        sent: list = []
        monkeypatch.setattr(main_module.signal, "pthread_kill", lambda *a, **k: sent.append(a))
        with main_module._exec_lock:
            main_module._exec_generation += 1
            gen = main_module._exec_generation
            main_module._running_execs[gen] = 123456789
            main_module._interrupt_generation = gen
        try:
            main_module._cancel_signal_handler(None, None)
            assert sent == [], "signal handler must not re-send SIGUSR1"
        finally:
            with main_module._exec_lock:
                main_module._running_execs.pop(gen, None)
                main_module._interrupt_generation = None

    def test_stale_interrupt_after_target_finished_spares_later_cell(self, monkeypatch):
        """A SIGUSR1 handler firing after its bound cell finished must not hit a later cell."""
        import kernel_runtime.main as main_module

        injected: list = []
        monkeypatch.setattr(main_module, "_raise_in_thread", lambda tid: injected.append(tid))
        with main_module._exec_lock:
            main_module._exec_generation += 1
            gen_a = main_module._exec_generation  # bound interrupt target, already finished
            main_module._interrupt_generation = gen_a
            main_module._exec_generation += 1
            gen_b = main_module._exec_generation  # a different cell running now
            main_module._running_execs[gen_b] = 999999
        try:
            main_module._cancel_signal_handler(None, None)
            assert injected == [], "handler injected into a later cell after its target finished"
        finally:
            with main_module._exec_lock:
                main_module._running_execs.pop(gen_b, None)
                main_module._interrupt_generation = None

    def test_completing_bound_cell_clears_interrupt_generation(self, client: TestClient):
        """When the interrupt's bound cell finishes, _interrupt_generation is reset to None."""
        import kernel_runtime.main as main_module

        with main_module._exec_lock:
            main_module._interrupt_generation = main_module._exec_generation + 1  # the next cell's generation
        resp = client.post(
            "/execute",
            json={"node_id": 1, "code": "x = 1", "flow_id": 1, "input_paths": {}, "output_dir": ""},
        )
        assert resp.json()["success"] is True
        assert main_module._interrupt_generation is None

    def test_interrupt_endpoint_no_execution(self, client: TestClient):
        """POST /interrupt returns 'no_execution_running' when idle."""
        resp = client.post("/interrupt")
        assert resp.status_code == 200
        assert resp.json()["status"] == "no_execution_running"


class TestDisplayOutputStore:
    """Tests for the GET /display_outputs endpoint that persists display
    outputs across executions so the frontend can retrieve them after a
    flow run."""

    def _execute(self, client: TestClient, code: str, flow_id: int = 1, node_id: int = 1):
        return client.post(
            "/execute",
            json={
                "node_id": node_id,
                "code": code,
                "flow_id": flow_id,
                "input_paths": {},
                "output_dir": "",
            },
        )

    def test_empty_before_any_execution(self, client: TestClient):
        """GET /display_outputs returns [] when node has never been executed."""
        resp = client.get("/display_outputs", params={"flow_id": 1, "node_id": 1})
        assert resp.status_code == 200
        assert resp.json() == []

    def test_display_outputs_persisted_after_execution(self, client: TestClient):
        """Display outputs should be retrievable via GET after execution."""
        self._execute(client, 'flowfile_ctx.display("hello")', flow_id=1, node_id=10)

        resp = client.get("/display_outputs", params={"flow_id": 1, "node_id": 10})
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["mime_type"] == "text/plain"
        assert data[0]["data"] == "hello"

    def test_multiple_displays_persisted(self, client: TestClient):
        """Multiple flowfile_ctx.display() calls should all be retrievable."""
        code = (
            'flowfile_ctx.display("first")\n'
            'flowfile_ctx.display("<b>second</b>")\n'
            'flowfile_ctx.display("third", title="Chart")\n'
        )
        self._execute(client, code, flow_id=2, node_id=20)

        resp = client.get("/display_outputs", params={"flow_id": 2, "node_id": 20})
        data = resp.json()
        assert len(data) == 3
        assert data[0]["mime_type"] == "text/plain"
        assert data[0]["data"] == "first"
        assert data[1]["mime_type"] == "text/html"
        assert data[1]["data"] == "<b>second</b>"
        assert data[2]["title"] == "Chart"

    def test_re_execution_overwrites_previous(self, client: TestClient):
        """Re-executing the same node should replace its stored outputs."""
        self._execute(client, 'flowfile_ctx.display("old")', flow_id=1, node_id=30)
        self._execute(client, 'flowfile_ctx.display("new")', flow_id=1, node_id=30)

        resp = client.get("/display_outputs", params={"flow_id": 1, "node_id": 30})
        data = resp.json()
        assert len(data) == 1
        assert data[0]["data"] == "new"

    def test_scoped_by_flow_and_node(self, client: TestClient):
        """Display outputs are keyed by (flow_id, node_id) independently."""
        self._execute(client, 'flowfile_ctx.display("flow1-node1")', flow_id=1, node_id=1)
        self._execute(client, 'flowfile_ctx.display("flow1-node2")', flow_id=1, node_id=2)
        self._execute(client, 'flowfile_ctx.display("flow2-node1")', flow_id=2, node_id=1)

        r1 = client.get("/display_outputs", params={"flow_id": 1, "node_id": 1}).json()
        r2 = client.get("/display_outputs", params={"flow_id": 1, "node_id": 2}).json()
        r3 = client.get("/display_outputs", params={"flow_id": 2, "node_id": 1}).json()

        assert len(r1) == 1 and r1[0]["data"] == "flow1-node1"
        assert len(r2) == 1 and r2[0]["data"] == "flow1-node2"
        assert len(r3) == 1 and r3[0]["data"] == "flow2-node1"

    def test_display_outputs_persisted_on_error(self, client: TestClient):
        """Outputs generated before an error should still be stored."""
        code = 'flowfile_ctx.display("before crash")\nraise RuntimeError("boom")\n'
        resp = self._execute(client, code, flow_id=3, node_id=40)
        assert resp.json()["success"] is False

        stored = client.get("/display_outputs", params={"flow_id": 3, "node_id": 40}).json()
        assert len(stored) == 1
        assert stored[0]["data"] == "before crash"

    def test_no_displays_stores_empty_list(self, client: TestClient):
        """Execution with no display() calls should store an empty list."""
        self._execute(client, 'x = 42', flow_id=4, node_id=50)

        resp = client.get("/display_outputs", params={"flow_id": 4, "node_id": 50})
        assert resp.json() == []


class TestArtifactLineage:
    """Lineage enforcement: reading an artifact outside a node's input lineage
    warns (deprecation) but still returns the value; in-lineage / no-lineage-context
    reads stay silent. The warning falls back to stdout when no log_callback_url."""

    def _publish_model(self, client: TestClient, node_id: int, flow_id: int):
        resp = client.post(
            "/execute",
            json={
                "node_id": node_id,
                "code": 'flowfile_ctx.publish_artifact("model", {"weight": 5})',
                "flow_id": flow_id,
                "input_paths": {},
                "output_dir": "",
            },
        )
        assert resp.json()["success"] is True

    def test_out_of_lineage_read_warns_but_returns_value(self, client: TestClient):
        self._publish_model(client, node_id=300, flow_id=500)

        resp = client.post(
            "/execute",
            json={
                "node_id": 301,
                "code": 'm = flowfile_ctx.read_artifact("model")\nprint("derived:", m["weight"] * 2)',
                "flow_id": 500,
                "input_paths": {},
                "output_dir": "",
                "available_artifacts": {},
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "not an upstream input" in data["stdout"]
        assert "node 300" in data["stdout"]
        # The read actually returned the object (derived value proves it).
        assert "derived: 10" in data["stdout"]

    def test_log_falls_back_to_stdout_when_callback_unreachable(self, client: TestClient):
        self._publish_model(client, node_id=300, flow_id=507)

        resp = client.post(
            "/execute",
            json={
                "node_id": 301,
                "code": 'flowfile_ctx.read_artifact("model")',
                "flow_id": 507,
                "input_paths": {},
                "output_dir": "",
                "available_artifacts": {},
                "log_callback_url": "http://127.0.0.1:1/raw_logs",
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        # POST to the dead callback fails -> the warning must surface via stdout.
        assert "not an upstream input" in data["stdout"]

    def test_in_lineage_read_is_silent(self, client: TestClient):
        self._publish_model(client, node_id=300, flow_id=501)

        resp = client.post(
            "/execute",
            json={
                "node_id": 301,
                "code": 'm = flowfile_ctx.read_artifact("model")\nprint("derived:", m["weight"] * 2)',
                "flow_id": 501,
                "input_paths": {},
                "output_dir": "",
                "available_artifacts": {"model": 300},
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "not an upstream input" not in data["stdout"]
        assert "derived: 10" in data["stdout"]

    def test_no_lineage_context_is_silent(self, client: TestClient):
        """available_artifacts omitted (None) => legacy/interactive path, no enforcement."""
        self._publish_model(client, node_id=300, flow_id=502)

        resp = client.post(
            "/execute",
            json={
                "node_id": 301,
                "code": 'm = flowfile_ctx.read_artifact("model")\nprint("derived:", m["weight"] * 2)',
                "flow_id": 502,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "not an upstream input" not in data["stdout"]
        assert "derived: 10" in data["stdout"]

    def test_published_artifact_rich_metadata(self, client: TestClient):
        resp = client.post(
            "/execute",
            json={
                "node_id": 310,
                "code": 'flowfile_ctx.publish_artifact("cfg", {"a": 1})',
                "flow_id": 503,
                "input_paths": {},
                "output_dir": "",
            },
        )
        data = resp.json()
        assert data["success"] is True
        entries = {a["name"]: a for a in data["artifacts_published"]}
        assert "cfg" in entries
        entry = entries["cfg"]
        assert entry["type_name"] == "dict"
        assert entry["module"] == "builtins"
        assert entry["size_bytes"] > 0

    def test_out_of_lineage_read_warns_once(self, client: TestClient):
        self._publish_model(client, node_id=300, flow_id=504)

        resp = client.post(
            "/execute",
            json={
                "node_id": 301,
                "code": (
                    'flowfile_ctx.read_artifact("model")\n'
                    'flowfile_ctx.read_artifact("model")\n'
                    'print("done")'
                ),
                "flow_id": 504,
                "input_paths": {},
                "output_dir": "",
                "available_artifacts": {},
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "done" in data["stdout"]
        assert data["stdout"].count("not an upstream input") == 1

    def test_same_node_publish_then_read_is_silent(self, client: TestClient):
        """A node reading back its own just-published artifact must not warn."""
        resp = client.post(
            "/execute",
            json={
                "node_id": 320,
                "code": (
                    'flowfile_ctx.publish_artifact("m", {"weight": 7})\n'
                    'back = flowfile_ctx.read_artifact("m")\n'
                    'print("read back:", back["weight"])'
                ),
                "flow_id": 505,
                "input_paths": {},
                "output_dir": "",
                "available_artifacts": {},
            },
        )
        data = resp.json()
        assert data["success"] is True, f"Execution failed: {data['error']}"
        assert "read back: 7" in data["stdout"]
        assert "not an upstream input" not in data["stdout"]
