"""Kafka helper functions for FlowFrame operations.

This module provides functions for reading from Kafka topics using
stored Flowfile connections.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from flowfile_frame.flow_frame import FlowFrame


def get_current_user_id() -> int:
    """Get the current user ID for Kafka operations.

    Returns:
        int: The current user ID (defaults to 1 for single-user mode).
    """
    return 1


def add_kafka_source(
    flow_graph,
    *,
    connection_name: str,
    topic_name: str,
    max_messages: int = 100_000,
    start_offset: str = "latest",
    poll_timeout_seconds: float = 30.0,
    value_format: str = "json",
) -> int:
    """Add a Kafka source node to the flow graph.

    Args:
        flow_graph: The flow graph to add the node to.
        connection_name: Name of the stored Kafka connection to use.
        topic_name: Kafka topic to consume from.
        max_messages: Maximum number of messages to consume.
        start_offset: Where to start consuming ('earliest' or 'latest').
        poll_timeout_seconds: How long to poll for messages.
        value_format: Message value format ('json').

    Returns:
        int: The node ID of the created Kafka source node.
    """
    from flowfile_core.schemas.input_schema import KafkaSourceSettings, NodeKafkaSource
    from flowfile_frame.utils import generate_node_id

    node_id = generate_node_id()
    flow_id = flow_graph.flow_id

    settings = NodeKafkaSource(
        flow_id=flow_id,
        node_id=node_id,
        user_id=get_current_user_id(),
        kafka_settings=KafkaSourceSettings(
            kafka_connection_name=connection_name,
            topic_name=topic_name,
            max_messages=max_messages,
            start_offset=start_offset,
            poll_timeout_seconds=poll_timeout_seconds,
            value_format=value_format,
        ),
    )

    flow_graph.add_kafka_source(settings)
    return node_id


def read_kafka(
    connection_name: str,
    *,
    topic_name: str,
    max_messages: int = 100_000,
    start_offset: str = "latest",
    poll_timeout_seconds: float = 30.0,
    value_format: str = "json",
    flow_graph=None,
) -> FlowFrame:
    """Read messages from a Kafka topic using a named Flowfile connection.

    Resolves the Kafka connection by name, creates a Kafka source node
    in the flow graph, and returns a FlowFrame.

    Args:
        connection_name: Name of the stored Kafka connection to use.
        topic_name: Kafka topic to consume from.
        max_messages: Maximum number of messages to consume.
        start_offset: Where to start consuming ('earliest' or 'latest').
        poll_timeout_seconds: How long to poll for messages.
        value_format: Message value format ('json').
        flow_graph: Optional existing FlowGraph to add the node to.

    Returns:
        FlowFrame: A FlowFrame backed by a Kafka source node.

    Raises:
        ValueError: If the connection is not found.
    """
    from flowfile_frame.flow_frame import FlowFrame
    from flowfile_frame.utils import create_flow_graph

    if flow_graph is None:
        flow_graph = create_flow_graph()

    node_id = add_kafka_source(
        flow_graph,
        connection_name=connection_name,
        topic_name=topic_name,
        max_messages=max_messages,
        start_offset=start_offset,
        poll_timeout_seconds=poll_timeout_seconds,
        value_format=value_format,
    )

    return FlowFrame(
        data=flow_graph.get_node(node_id).get_resulting_data().data_frame,
        flow_graph=flow_graph,
        node_id=node_id,
    )
