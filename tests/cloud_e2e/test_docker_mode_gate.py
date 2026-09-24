"""Docker mode must never run a "No connection" cloud node on the server's own AWS credentials.

The docker stack's environment can reach MinIO, so only the docker-mode gate can fail the node.
"""

import pytest

from .helpers import list_keys

pytestmark = pytest.mark.cloud_e2e

READER_ID, WRITER_ID = 1, 3
GATE_MESSAGE = "Select a cloud storage connection"


@pytest.mark.parametrize("role", ["writer", "reader"])
def test_no_connection_node_refuses_server_credentials(
    docker_stack, docker_minio_connection, user_flow, new_target, role
):
    target = new_target("table")
    if role == "writer":
        flow = user_flow("remote", docker_minio_connection, writer={"resource_path": target})
    else:
        no_connection = {"auth_mode": "aws-cli", "connection_name": None}
        flow = user_flow("remote", docker_minio_connection, reader=no_connection, writer={"resource_path": target})
    run = docker_stack.run_flow(flow)
    node = run["nodes"][WRITER_ID if role == "writer" else READER_ID]
    assert node["success"] is False
    assert GATE_MESSAGE in node["error"], node["error"]
    assert not list_keys(target)
