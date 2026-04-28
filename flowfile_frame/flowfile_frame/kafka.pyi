# This file was auto-generated to provide type information for flowfile_frame.kafka
# DO NOT MODIFY THIS FILE MANUALLY
# Run `python flowfile_frame/submodule_stub_generator.py` to regenerate
from __future__ import annotations

from typing import Any, Callable, Iterable, Optional, Union

from typing import TYPE_CHECKING
from flowfile_frame.flow_frame import FlowFrame
from flowfile_core.schemas.input_schema import KafkaSourceSettings, NodeKafkaSource
from flowfile_frame.utils import generate_node_id
from flowfile_frame.utils import create_flow_graph

def add_kafka_source(flow_graph, connection_name: str, topic_name: str, max_messages: int=100000, start_offset: str='latest', poll_timeout_seconds: float=30.0, value_format: str='json') -> int: ...

def get_current_user_id() -> int: ...

def read_kafka(connection_name: str, topic_name: str, max_messages: int=100000, start_offset: str='latest', poll_timeout_seconds: float=30.0, value_format: str='json', flow_graph=None) -> FlowFrame: ...

