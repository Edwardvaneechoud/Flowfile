"""Port contracts through the real core HTTP API, using the existing service harness."""

import os
from pathlib import Path

import pytest

from flowfile_core.schemas.input_schema import NodeConnection
from test_utils.services import flow_document, running_services


@pytest.mark.skipif(os.name == "nt", reason="the service harness manages POSIX process groups")
def test_editor_enforces_serialized_ports_without_mutating_rejected_edges(tmp_path: Path):
    with running_services(tmp_path / "services", "electron", {}) as services:
        palette = {node["item"]: node for node in services.get("/node_list")}
        assert palette["manual_input"]["inputs"] == []
        assert palette["manual_input"]["outputs"] == [{"id": "output-0", "name": None}]
        assert palette["select"]["inputs"] == [
            {"id": "input-0", "name": None, "kind": "data", "required": True, "multiple": False, "hidden": False}
        ]
        flow_id = services.import_flow(
            flow_document(
                718,
                "Port HTTP contract",
                [
                    {"id": node_id, "type": node_type, "outputs": [], "setting_input": {}}
                    for node_id, node_type in [(1, "manual_input"), (2, "select"), (3, "manual_input")]
                ],
            )
        )

        def graph():
            return services.get("/flow_data/v2", flow_id=flow_id)

        def connect(source, target, output="output-0", input_handle="input-0"):
            return services.http.post(
                f"{services.base}/editor/connect_node/",
                params={"flow_id": flow_id},
                json=NodeConnection.create_from_simple_input(
                    source, target, input_type=input_handle, output_handle=output
                ).model_dump(mode="json"),
                timeout=10,
            )

        initial = graph()
        nodes = {node["id"]: node for node in initial["node_inputs"]}
        assert nodes[1]["inputs"] == palette["manual_input"]["inputs"]
        assert nodes[2]["inputs"] == palette["select"]["inputs"]
        assert initial["node_edges"] == []
        for source, target, output, input_handle in [
            (1, 2, "output-1", "input-0"),
            (1, 2, "output-0", "input-1"),
            (1, 3, "output-0", "input-0"),
        ]:
            response = connect(source, target, output, input_handle)
            assert response.status_code == 422, response.text
            assert graph() == initial

        response = connect(1, 2)
        assert response.status_code == 200, response.text
        connected = graph()
        assert len(connected["node_edges"]) == 1
        edge = connected["node_edges"][0]
        assert (edge["source"], edge["sourceHandle"], edge["target"], edge["targetHandle"]) == (
            "1", "output-0", "2", "input-0"
        )
        # Replaying an edge is idempotent, but a different source cannot replace it.
        assert connect(1, 2).status_code == 200
        assert graph() == connected
        response = connect(3, 2)
        assert response.status_code == 422, response.text
        assert graph() == connected
