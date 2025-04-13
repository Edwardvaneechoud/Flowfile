
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.schemas import transform_schema
import pytest
from typing import List, Dict
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection, RunInformation


try:
    from tests.flowfile_core_test_utils import (is_docker_available, ensure_password_is_available)
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    # noinspection PyUnresolvedReferences


@pytest.fixture
def flow_logger() -> FlowLogger:
    return FlowLogger(1)


@pytest.fixture
def raw_data() -> List[Dict]:
    return [{'name': 'John', 'city': 'New York'},
            {'name': 'Jane', 'city': 'Los Angeles'},
            {'name': 'Edward', 'city': 'Chicago'},
            {'name': 'Courtney', 'city': 'Chicago'}]


def handle_run_info(run_info: RunInformation):
    if not run_info.success:
        errors = 'errors:'
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f'\n node_id:{node_step.node_id}, error: {node_step.error}'
        raise ValueError(f'Graph should run successfully:\n{errors}')


def create_flowfile_handler():
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def create_graph():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    graph = handler.get_flow(1)
    return graph


def add_manual_input(graph: EtlGraph, data, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id, raw_data=data)
    graph.add_manual_input(input_file)
    return graph


def get_graph():
    graph = create_graph()
    add_manual_input(graph, data=[{'name': 'John', 'city': 'New York'}], node_id=1)
    return graph


def add_node_promise_on_type(graph: EtlGraph, node_type: str, node_id: int, flow_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(node_promise)


def get_dependency_example():
    graph = create_graph()
    graph = add_manual_input(graph, data=[{'name': 'John', 'city': 'New York'},
            {'name': 'Jane', 'city': 'Los Angeles'},
            {'name': 'Edward', 'city': 'Chicago'},
            {'name': 'Courtney', 'city': 'Chicago'}]
)
    node_promise = input_schema.NodePromise(flow_id=1, node_id=2, node_type='unique')
    graph.add_node_promise(node_promise)

    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    input_file = input_schema.NodeUnique(flow_id=1, node_id=2,
                                         unique_input=transform_schema.UniqueInput(columns=['city'])
                                         )
    graph.add_unique(input_file)
    return graph


def test_node_step_needs_run():
    graph = get_graph()
    node = graph.get_node(1)
    node_logger = graph.flow_logger.get_node_logger(node.node_id)
    assert node.needs_run(False), 'Node should run'
    node.execute_node(run_location='auto', performance_mode=False,
                      node_logger=node_logger)
    assert not node.needs_run(False), 'Node should not need to run'


def test_example_data_from_main_output():
    graph = get_graph()
    graph.run_graph()
    node = graph.get_node(1)
    node_info = node.get_node_data(1, True)
    data = node_info.main_output.data
    assert data == [{'name': 'John', 'city': 'New York'}], 'Data should be the same'


def test_hash_change_data_with_old_data():
    graph = get_dependency_example()
    graph.run_graph()
    data = graph.get_node_data(2, True).main_output.data
    node = graph.get_node(2)
    assert len(data) > 0, 'Data should be present'
    graph = add_manual_input(
        graph,
        data=[
            {"name": "John", "city": "New York"},
            {"name": "Jane", "city": "Los Angeles"},
            {"name": "Edward", "city": "Chicago"},
            {"name": "Courtney", "city": "Chicago"},
            {"name": "John", "city": "New York"},
            {"name": "Jane", "city": "Los Angeles"},
            {"name": "Edward", "city": "Chicago"},
            {"name": "Courtney", "city": "Chicago"},
        ],
    )
    # this should not have impacted the
    data = graph.get_node_data(2, True).main_output.data
    assert len(data) > 0, 'Data should be present, cause we did not run yet'