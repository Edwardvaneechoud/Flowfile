from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.FlowfileFlow import RunInformation
from flowfile_core.schemas import schemas

from time import sleep
from flowfile_core.configs.flow_logger import FlowLogger
import pytest
from pathlib import Path
from typing import List, Dict


@pytest.fixture
def flow_logger() -> FlowLogger:
    return FlowLogger(1)


@pytest.fixture
def raw_data()-> List[Dict]:
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


def test_initiate_handler():
    create_flowfile_handler()


def test_register_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    assert handler.flowfile_flows[0].flow_id == 1, "Flow should be registered"


def test_warning_register_double_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    with pytest.raises(Exception):
        handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))


def test_register_second_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    sleep(0.5)
    handler.register_flow(schemas.FlowSettings(flow_id=2, name='new_flow', path='.'))
    assert handler.flowfile_flows[1].flow_id == 2, "Second flow should be registered"


def test_import_flow():
    handler = create_flowfile_handler()
    flow_path = "flowfile_core/tests/support_files/flows/read_csv.flowfile"
    flow_id = handler.import_flow(Path(flow_path))
    assert handler.flowfile_flows[0].flow_id == flow_id, "Flow should be imported"
    assert handler.get_flow(flow_id).flow_settings.name == 'read_csv', "First flow should be named read_csv"
    assert handler.get_flow(flow_id).flow_settings.path == flow_path, "Path should be correct"


def test_import_second_flow():
    handler = create_flowfile_handler()
    first_graph_id = handler.import_flow(Path("flowfile_core/tests/support_files/flows/read_csv.flowfile"))
    sleep(0.1)
    second_graph_id = handler.import_flow(Path("flowfile_core/tests/support_files/flows/text_to_rows.flowfile"))
    assert handler.flowfile_flows[0].flow_id == first_graph_id, "First flow should be imported"
    assert handler.flowfile_flows[1].flow_id == second_graph_id, "Second flow should be imported"
    assert handler.flowfile_flows[0].flow_id != handler.flowfile_flows[1].flow_id, "Flows should have different ids"
    assert handler.get_flow(first_graph_id).flow_settings.name == 'read_csv', "First flow should be named read_csv"
    assert (handler.get_flow(first_graph_id).flow_settings.path == "flowfile_core/tests/support_files/flows/read_csv.flowfile"), "Path should be correct"
    assert handler.get_flow(second_graph_id).flow_settings.name == 'text_to_rows', "Second flow should be named text_to_rows"
    assert (handler.get_flow(second_graph_id).flow_settings.path == "flowfile_core/tests/support_files/flows/text_to_rows.flowfile"), "Path should be correct"


def test_get_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    assert handler.get_flow(1).flow_id == 1, "Flow should be retrieved"


def test_add_flow():
    handler = create_flowfile_handler()
    first_id = handler.add_flow('new_flow', 'flowfile_core/tests/support_files/flows/read_csv.flowfile')
    sleep(1)
    second_id = handler.add_flow('second_flow', 'flowfile_core/tests/support_files/flows/text_to_rows.flowfile')
    assert len(handler.flowfile_flows) == 2, "Two flows should be added"
    assert handler.flowfile_flows[0].flow_settings.name == 'new_flow', "First flow should be named new_flow"
    assert handler.flowfile_flows[1].flow_settings.name == 'second_flow', "Second flow should be named second_flow"
    assert first_id != second_id, "Flow ids should be different"


def test_delete_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    handler.delete_flow(1)
    assert handler.flowfile_flows == [], "Flow should be deleted"
