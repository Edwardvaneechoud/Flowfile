"""Node-scoped attribution of published / deleted artifacts.

Artifacts published on a shared kernel are attributed to the owning node via the
``node_id`` stamped in the store metadata, never via a store-wide name diff. This
guards against cross-node misattribution when independent nodes run concurrently
on the same kernel — the artifact store is shared across every node in a flow.
"""

import threading

from fastapi.testclient import TestClient

from kernel_runtime import main


def _execute(client: TestClient, code: str, node_id: int = 1, flow_id: int = 1):
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


def _published_names(resp) -> list[str]:
    return sorted(a["name"] for a in resp.json()["artifacts_published"])


def _deleted_names(resp) -> list[str]:
    return sorted(resp.json()["artifacts_deleted"])


class TestPublishedAttribution:
    def test_excludes_artifact_from_another_node(self, client: TestClient):
        # A foreign node's artifact landing in the shared store mid-execution
        # (exactly what a concurrent node does) must not be attributed here.
        code = (
            "import kernel_runtime.main as _m\n"
            "flowfile_ctx.publish_artifact('mine', {'x': 1})\n"
            "_m.artifact_store.publish('foreign', {'y': 2}, node_id=999, flow_id=1)\n"
        )
        resp = _execute(client, code, node_id=1, flow_id=1)
        assert resp.json()["success"] is True
        assert _published_names(resp) == ["mine"]

    def test_two_nodes_report_only_their_own(self, client: TestClient):
        a = _execute(client, "flowfile_ctx.publish_artifact('a', {'v': 1})\n", node_id=1)
        b = _execute(client, "flowfile_ctx.publish_artifact('b', {'v': 2})\n", node_id=2)
        assert _published_names(a) == ["a"]
        assert _published_names(b) == ["b"]

    def test_concurrent_execution_no_cross_attribution(self, client: TestClient):
        results: dict[int, list[str]] = {}

        def worker(node_id: int, name: str, barrier: threading.Barrier) -> None:
            barrier.wait()
            code = f"flowfile_ctx.publish_artifact('{name}', {{'v': {node_id}}})\n"
            results[node_id] = _published_names(_execute(client, code, node_id=node_id, flow_id=1))

        for _ in range(8):
            main.artifact_store.clear(flow_id=1)
            results.clear()
            barrier = threading.Barrier(2)
            t1 = threading.Thread(target=worker, args=(1, "a", barrier))
            t2 = threading.Thread(target=worker, args=(2, "b", barrier))
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)
            assert results.get(1) == ["a"]
            assert results.get(2) == ["b"]


class TestDeletedAttribution:
    def test_reports_own_delete(self, client: TestClient):
        code = (
            "flowfile_ctx.publish_artifact('keep', {'x': 1})\n"
            "flowfile_ctx.publish_artifact('gone', {'y': 2})\n"
            "flowfile_ctx.delete_artifact('gone')\n"
        )
        resp = _execute(client, code, node_id=1, flow_id=1)
        assert resp.json()["success"] is True
        assert _published_names(resp) == ["keep"]
        assert _deleted_names(resp) == ["gone"]

    def test_excludes_foreign_delete(self, client: TestClient):
        # Pre-seed a foreign artifact, then have this node's execution remove it
        # directly (simulating a concurrent foreign delete). It must not be
        # attributed to this node's deletions.
        main.artifact_store.publish("foreign", {"y": 2}, node_id=999, flow_id=1)
        code = (
            "import kernel_runtime.main as _m\n"
            "flowfile_ctx.publish_artifact('mine', {'x': 1})\n"
            "_m.artifact_store.delete('foreign', flow_id=1)\n"
        )
        resp = _execute(client, code, node_id=1, flow_id=1)
        assert resp.json()["success"] is True
        assert _published_names(resp) == ["mine"]
        assert _deleted_names(resp) == []
