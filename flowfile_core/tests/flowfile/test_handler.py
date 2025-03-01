from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection, RunInformation
from flowfile_core.schemas import input_schema, transform_schema, schemas
from flowfile_core.schemas.schemas import FlowSettings
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.flowfile.analytics.main import AnalyticsProcessor
from flowfile_core.configs.flow_logger import FlowLogger
import pytest
from pathlib import Path
import subprocess
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


def is_docker_available():
    """Check if Docker is running."""
    try:
        subprocess.run(["docker", "info"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


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


def test_error_register_double_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    with pytest.raises(Exception):
        handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))


def test_register_second_flow():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    handler.register_flow(schemas.FlowSettings(flow_id=2, name='new_flow', path='.'))
    assert handler.flowfile_flows[1].flow_id == 2, "Second flow should be registered"


def test_import_flow():
    handler = create_flowfile_handler()
    flow_path = "flowfile_core/tests/support_files/flows/read_csv.flowfile"
    flow_id = handler.import_flow(Path(flow_path))
    assert handler.flowfile_flows[0].flow_id == flow_id, "Flow should be imported"


def test_import_second_flow():
    handler = create_flowfile_handler()
    first_graph_id = handler.import_flow(Path("flowfile_core/tests/support_files/flows/read_csv.flowfile"))
    second_graph_id = handler.import_flow(Path("flowfile_core/tests/support_files/flows/text_to_rows.flowfile"))
    assert handler.flowfile_flows[0].flow_id == first_graph_id, "First flow should be imported"
    assert handler.flowfile_flows[1].flow_id == second_graph_id, "Second flow should be imported"
    assert handler.flowfile_flows[0].flow_id != handler.flowfile_flows[1].flow_id, "Flows should have different ids"