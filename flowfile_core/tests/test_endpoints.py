import datetime
import os
import threading
from pathlib import Path
from time import sleep

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from flowfile_core import main
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.database_connection_manager.db_connections import (
    delete_database_connection,
    get_all_database_connections_interface,
    get_database_connection,
    get_local_cloud_connection,
)
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection
from flowfile_core.routes.routes import (
    add_node,
    connect_node,
    flow_file_handler,
    input_schema,
    output_model,
)
from flowfile_core.schemas import cloud_storage_schemas as cloud_ss
from flowfile_core.schemas.transform_schema import SelectInput
from flowfile_core.secret_manager.secret_manager import get_encrypted_secret
from shared.storage_config import storage

try:
    from tests.flowfile.test_flowfile import find_parent_directory
    from tests.flowfile_core_test_utils import (
        ensure_db_connection_is_available,
        ensure_password_is_available,
        is_docker_available,
    )
    from tests.utils import (
        ensure_cloud_storage_connection_is_available_and_get_connection,
        ensure_no_cloud_storage_connection_is_available,
        get_cloud_connection,
    )
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile/test_flowfile.py")))
    from flowfile_core_test_utils import (
        ensure_db_connection_is_available,
        ensure_password_is_available,
        is_docker_available,
    )

    from tests.flowfile.test_flowfile import find_parent_directory
    from tests.utils import (
        ensure_cloud_storage_connection_is_available_and_get_connection,
        ensure_no_cloud_storage_connection_is_available,
        get_cloud_connection,
    )

FlowId = int


def get_auth_token():
    """Get authentication token for testing"""
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]


def ensure_no_database_connections():
    with get_db_context() as db:
        all_connections = get_all_database_connections_interface(db, 1)
        for connection in all_connections:
            delete_database_connection(db, connection.connection_name, 1)


# Create an authenticated test client
def get_test_client():
    """Get an authenticated test client"""
    token = get_auth_token()
    _client = TestClient(main.app)
    _client.headers = {
        "Authorization": f"Bearer {token}"
    }
    return _client


client = get_test_client()


def get_flow_settings() -> dict:
    return {'flow_id': 1, 'description': None, 'save_location': None, 'auto_save': False, 'name': '',
            'modified_on': None, 'path': 'flowfile_core/tests/support_files/flows/tmp/test_flow.yaml',
            'execution_mode': 'Development', 'is_running': False, 'is_canceled': False}


def get_join_data(flow_id: int, how: str = 'inner'):
    return {'flow_id': flow_id, 'node_id': 3, 'cache_results': False, 'pos_x': 788.8727272727273, 'pos_y': 186.4,
            'is_setup': True, 'description': '', 'depending_on_ids': [], 'auto_generate_selection': True,
            'verify_integrity': True, 'join_input': {'join_mapping': [{'left_col': 'name', 'right_col': 'name'}],
                                                     'left_select': {'renames': [
                                                         {'old_name': 'name', 'new_name': 'name', 'data_type': None,
                                                          'data_type_change': False, 'join_key': False,
                                                          'is_altered': False, 'position': None, 'is_available': True,
                                                          'keep': True}]}, 'right_select': {'renames': [
                {'old_name': 'name', 'new_name': 'right_name', 'data_type': None, 'data_type_change': False,
                 'join_key': False, 'is_altered': False, 'position': None, 'is_available': True, 'keep': True}]},
                                                     'how': how}, 'auto_keep_all': True, 'auto_keep_right': True,
            'auto_keep_left': True}


def add_manual_input(graph: FlowGraph, data, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id,
                                              raw_data_format=input_schema.RawData.from_pylist(data))
    graph.add_manual_input(input_file)
    return graph


def remove_flow(file_path: str):
    if os.path.exists(file_path):
        os.remove(file_path)


def add_node_placeholder(node_type: str, flow_id: FlowId = 1, node_id: int = 1):
    client.post("/editor/add_node", params={'flow_id': flow_id, 'node_id': node_id, 'node_type': node_type,
                                            'pos_x': 0, 'pos_y': 0})


def ensure_no_flow_registered():
    for flow in flow_file_handler.flowfile_flows:
        flow_file_handler.delete_flow(flow.flow_id)


def ensure_clean_flow() -> FlowId:
    flow_path: str = str(find_parent_directory("Flowfile") / 'flowfile_core/tests/support_files/flows/tmp/sample_flow_path.yaml')
    remove_flow(flow_path)  # Remove the flow if it exists
    sleep(.1)
    r = client.post("editor/create_flow", params={'flow_path': flow_path})
    if r.status_code != 200:
        raise Exception('Flow not created')
    return r.json()


def create_join_graph() -> FlowId:
    sleep(0.5)
    flow_id = ensure_clean_flow()
    graph = flow_file_handler.get_flow(flow_id)
    left_data = [{"name": "eduward"},
                 {"name": "edward"},
                 {"name": "courtney"}]

    add_manual_input(graph, data=left_data)
    right_data = left_data[:1]
    add_manual_input(graph, data=right_data, node_id=2)
    add_node(flow_id, 3, node_type='join', pos_x=0, pos_y=0)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3)
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    return flow_id


def create_flow_with_manual_input_and_select() -> FlowId:
    flow_id = create_flow_with_manual_input()
    add_node(flow_id, 2, node_type='select', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    client.post("/editor/connect_node/", data=connection.model_dump_json(), params={"flow_id": flow_id})
    flow = flow_file_handler.get_flow(flow_id)
    select_settings = input_schema.NodeSelect(flow_id=flow_id, node_id=2, select_input=[SelectInput(old_name='name')],
                                              keep_missing=False)
    flow.add_select(select_settings)
    return flow_id


def add_select_node(node_id: int, flow_id: int, node_input_id: int):
    add_node(flow_id, node_id, node_type='select', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(node_input_id, node_id)
    client.post("/editor/connect_node/", data=connection.json(), params={"flow_id": flow_id})
    flow = flow_file_handler.get_flow(flow_id)
    select_settings = input_schema.NodeSelect(flow_id=1, node_id=node_id, select_input=[SelectInput(old_name='name')],
                                              keep_missing=False)
    flow.add_select(select_settings)


def create_flow_with_manual_input() -> FlowId:
    flow_id = ensure_clean_flow()
    add_node_placeholder('manual_input', node_id=1, flow_id=flow_id)
    input_file = input_schema.NodeManualInput(flow_id=flow_id, node_id=1,
                                              raw_data_format=input_schema.RawData.from_pylist([
                                                  {'name': 'John', 'city': 'New York'},
                                                  {'name': 'Jane', 'city': 'Los Angeles'},
                                                  {'name': 'Edward', 'city': 'Chicago'},
                                                  {'name': 'Courtney', 'city': 'Chicago'}]
                                              ))
    r = client.post("/update_settings/", json=input_file.model_dump(), params={"node_type": "manual_input"})
    return flow_id


def test_register_flow():
    ensure_no_flow_registered()
    flow_path: str = 'flowfile_core/tests/support_files/flows/tmp/test_flow.yaml'
    response = client.post("editor/create_flow", params={'flow_path': flow_path})
    assert response.status_code == 200, 'Flow not registered'
    flow = flow_file_handler.get_flow(response.json())
    if flow is None:
        raise Exception('Flow could not be opened')
    flow_response = client.get("editor/flow", params={'flow_id': response.json()})
    if flow_response.status_code != 200:
        raise Exception('Flow not retrieved')
    assert flow_response.json()['flow_id'] == response.json(), 'Flow not retrieved'


def test_get_flow():
    flow_id = ensure_clean_flow()
    response = client.get("editor/flow", params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow not retrieved'
    assert response.json()['flow_id'] == flow_id, 'Flow not retrieved'


def test_add_node():
    flow_id = ensure_clean_flow()
    response = client.post("/editor/add_node",
                           params={'flow_id': flow_id, 'node_id': 1, 'node_type': 'manual_input', 'pos_x': 0,
                                   'pos_y': 0})
    assert response.status_code == 200, 'Node not added'
    assert flow_file_handler.get_node(flow_id, 1).node_type == 'manual_input', 'Node type not set'


def test_add_generic_settings():
    flow_id = ensure_clean_flow()
    add_node_placeholder('manual_input', flow_id=flow_id)
    input_file = input_schema.NodeManualInput(flow_id=flow_id, node_id=1,
                                              raw_data_format=
                                              input_schema.RawData.from_pylist([
                                                  {'name': 'John', 'city': 'New York'},
                                                  {'name': 'Jane', 'city': 'Los Angeles'},
                                                  {'name': 'Edward', 'city': 'Chicago'},
                                                  {'name': 'Courtney', 'city': 'Chicago'}]
                                              )
                                              )
    r = client.post("/update_settings/", json=input_file.model_dump(), params={"node_type": "manual_input"})
    assert r.status_code == 200, 'Settings not added'
    assert flow_file_handler.get_node(flow_id, 1).setting_input.raw_data_format == input_file.raw_data_format, 'Settings not set'


def test_connect_node():
    flow_id = ensure_clean_flow()
    add_node(flow_id, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id, 2, node_type='select', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    r = client.post("/editor/connect_node/", data=connection.json(), params={"flow_id": flow_id})
    assert r.status_code == 200, 'Node not connected'
    assert flow_file_handler.get_node(flow_id, 1).leads_to_nodes[0].node_id == 2, 'Node from to not connected'
    assert flow_file_handler.get_node(flow_id, 2).all_inputs[0].node_id == 1, 'Node to from not connected'


def test_create_flow_with_join():
    flow_id = create_join_graph()
    data = get_join_data(flow_id, how='inner')
    flow = flow_file_handler.get_flow(flow_id)
    r = client.post("/update_settings/", json=data, params={"node_type": "join"})
    assert r.status_code == 200, 'Settings not added'
    assert flow.get_node(3).setting_input.join_input.how == 'inner', 'Settings not set'
    assert flow.get_node(3).node_inputs.main_inputs[0].node_id == 1, 'Node not connected'
    assert flow.get_node(3).node_inputs.right_input.node_id == 2, 'Node not connected'


def test_delete_main_connection():
    flow_id = create_join_graph()
    data = get_join_data(flow_id, how='inner')
    flow = flow_file_handler.get_flow(flow_id)
    client.post("/update_settings/", json=data, params={"node_type": "join"})
    node_connection: input_schema.NodeConnection = input_schema.NodeConnection.create_from_simple_input(1, 3)
    response = client.post("/editor/delete_connection", data=node_connection.model_dump_json(),
                           params={"flow_id": flow_id})
    assert response.status_code == 200, 'Connection not deleted'
    assert flow.get_node(1).leads_to_nodes == [], 'Connection not deleted'
    assert flow.get_node(3).node_inputs.main_inputs == [], 'Connection not deleted'


def test_delete_right_connection():
    flow_id = create_join_graph()
    data = get_join_data(flow_id, how='inner')
    flow = flow_file_handler.get_flow(flow_id)
    client.post("/update_settings/", json=data, params={"node_type": "join"})
    right_connection: input_schema.NodeConnection = input_schema.NodeConnection.create_from_simple_input(2, 3)
    right_connection.input_connection.connection_class = 'input-1'
    response = client.post("/editor/delete_connection", data=right_connection.model_dump_json(),
                           params={"flow_id": flow_id})
    assert response.status_code == 200, 'Connection not deleted'
    assert flow.get_node(2).leads_to_nodes == [], 'Connection not deleted'
    assert flow.get_node(3).node_inputs.right_input is None, 'Connection not deleted'


def test_delete_connection_with_wrong_input():
    flow_id = create_join_graph()
    data = get_join_data(flow_id, how='inner')
    flow = flow_file_handler.get_flow(flow_id)
    client.post("/update_settings/", json=data, params={"node_type": "join"})
    right_connection: input_schema.NodeConnection = input_schema.NodeConnection.create_from_simple_input(2, 3)
    response = client.post("/editor/delete_connection", data=right_connection.model_dump_json(),
                           params={"flow_id": flow_id})
    assert response.status_code == 422, 'Connection should not be able to delete'
    assert flow.get_node(2).leads_to_nodes != [], 'Connection should not be deleted'
    assert flow.get_node(3).node_inputs.main_inputs != [], 'Connection not should not be deleted'


def test_run_error_flow_with_join():
    flow_id = create_join_graph()
    data = get_join_data(flow_id, how='inner')
    flow = flow_file_handler.get_flow(flow_id)
    client.post("/update_settings/", json=data, params={"node_type": "join"})
    right_connection: input_schema.NodeConnection = input_schema.NodeConnection.create_from_simple_input(2, 3)
    right_connection.input_connection.connection_class = 'input-1'
    response = client.post("/editor/delete_connection", data=right_connection.model_dump_json(),
                           params={"flow_id": flow_id})
    assert response.status_code == 200, 'Connection not deleted, breaking off test'
    response = client.post("/flow/run/", params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow should just start as normal'
    assert len(flow.latest_run_info.node_step_result) == 2, 'Flow should have only executed 2 nodes'


def test_import_flow():
    if flow_file_handler.get_flow(1):
        flow_file_handler.delete_flow(1)
    flow_path = str(find_parent_directory("Flowfile")/'flowfile_core/tests/support_files/flows/tmp/test_flow.yaml')
    response = client.get("/import_flow", params={'flow_path': flow_path})
    assert response.status_code == 200, 'Flow not imported'
    flow_id = response.json()
    assert flow_file_handler.get_flow(flow_id).flow_id == flow_id, 'Flow not set'
    assert flow_file_handler.get_flow(flow_id).flow_settings.path == flow_path, 'Flow path not set'


def test_delete_connection():
    flow_id = create_flow_with_manual_input_and_select()
    if not flow_file_handler.get_node(flow_id, 1).leads_to_nodes:
        raise Exception('Node not connected, breaking off test')
    node_connection: input_schema.NodeConnection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    response = client.post("/editor/delete_connection", data=node_connection.model_dump_json(),
                           params={"flow_id": flow_id})
    assert response.status_code == 200, 'Connection not deleted'
    assert 2 not in flow_file_handler.get_node(flow_id, 1).leads_to_nodes, 'Connection not deleted'
    assert 1 not in flow_file_handler.get_node(flow_id, 2).all_inputs, 'Connection not deleted'


def test_run_invalid_flow():
    flow_id = create_flow_with_manual_input_and_select()
    add_select_node(node_id=3, flow_id=flow_id, node_input_id=2)
    add_select_node(node_id=4, flow_id=flow_id, node_input_id=1)
    flow = flow_file_handler.get_flow(flow_id)
    if not flow_file_handler.get_node(flow_id, 1).leads_to_nodes:
        raise Exception('Node not connected, breaking off test')
    node_connection: input_schema.NodeConnection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    response = client.post("/editor/delete_connection", data=node_connection.model_dump_json(),
                           params={"flow_id": flow_id})
    assert response.status_code == 200, 'Connection not deleted, breaking off test'
    response = client.post("/flow/run/", params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow should just start as normal'


def test_save_flow():
    flow_id = create_flow_with_manual_input_and_select()
    imported_flow = flow_file_handler.get_flow(flow_id)
    assert imported_flow.__name__ != "sample_save"
    assert imported_flow.flow_settings.name != "sample_save"
    file_path = str(find_parent_directory("Flowfile") / 'flowfile_core/tests/support_files/flows/sample_save.yaml')
    remove_flow(file_path)
    start_time = datetime.datetime.now().timestamp()
    # def save_flow(flow_id: int, flow_path: str = None)
    response = client.get("/save_flow", params={'flow_id': flow_id, 'flow_path': file_path})
    assert response.status_code == 200, 'Flow not saved'
    assert os.path.exists(file_path), 'Flow not saved, file not found'
    imported_flow_id = flow_file_handler.import_flow(file_path)
    assert imported_flow_id == flow_id, 'Flow not stored or imported correctly correctly'
    assert imported_flow.__name__ == "sample_save"
    assert imported_flow.flow_settings.name == "sample_save"
    assert imported_flow.flow_settings.modified_on > start_time
    remove_flow(file_path)


def test_save_imported_flow():
    path = str(storage.flows_directory / "random_value.yaml")
    response = client.post("/editor/create_flow/", params={'flow_path': path})
    assert response.status_code == 200, 'Flow not created'
    created_flow = flow_file_handler.get_flow(response.json())
    assert created_flow.__name__ == "random_value"
    new_path = str(storage.flows_directory / "readable_flow.yaml")

    response = client.get("/save_flow", params={'flow_id': created_flow.flow_id, 'flow_path': new_path})
    assert response.status_code == 200, 'Flow not saved'
    assert created_flow.__name__ == "readable_flow"


def test_delete_node():
    flow_id = create_flow_with_manual_input_and_select()
    response = client.post("/editor/delete_node", params={"flow_id": flow_id, "node_id": 2})
    assert response.status_code == 200, 'Node not deleted'
    assert flow_file_handler.get_flow(flow_id).get_node(2) is None, 'Node not deleted'


def test_get_flow_data_v2():
    flow_id = ensure_clean_flow()
    add_node(flow_id, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id, 2, node_type='select', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    client.post("/editor/connect_node/", data=connection.json(), params={"flow_id": flow_id})
    response = client.get('/flow_data/v2', params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow data not retrieved'


def create_flow_with_graphic_walker_input() -> FlowId:
    flow_id = create_flow_with_manual_input()
    add_node(flow_id=flow_id, node_id=2, node_type='explore_data')
    assert flow_file_handler.get_node(flow_id, 2) is not None, 'Node not added stopping test'
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)
    return flow_id


def test_get_graphic_walker_input():
    flow_id = create_flow_with_graphic_walker_input()
    flow_file_handler.get_flow(flow_id).run_graph()

    response = client.get('/analysis_data/graphic_walker_input', params={'flow_id': flow_id, 'node_id': 2})
    assert response.status_code == 200, 'Graphic walker input not retrieved'
    try:
        input_schema.NodeExploreData(**response.json())
    except Exception as e:
        raise Exception('Invalid response: ' + str(e))


def test_error_graphic_walker_input():
    flow_id = create_flow_with_graphic_walker_input()
    response = client.get('/analysis_data/graphic_walker_input', params={'flow_id': flow_id, 'node_id': 2})
    assert response.status_code == 422, 'Expected 422 Unprocessable Entity when data is not available'


def test_update_flow_with_settings_polars_code():
    flow_id = create_flow_with_manual_input()
    add_node(flow_id, 2, node_type='polars_code', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)
    settings = {'flow_id': flow_id, 'node_id': 2, 'pos_x': 668, 'pos_y': 450,
                'polars_code_input': {'polars_code': '# Add your polars code here\ninput_df.select(pl.col("name"))'},
                'cache_results': False, 'is_setup': True}
    response = client.post('/update_settings/', json=settings, params={'node_type': 'polars_code'})
    assert response.status_code == 200, 'Settings not updated'
    assert (flow_file_handler.get_node(flow_id, 2).setting_input.polars_code_input.polars_code ==
            settings['polars_code_input']['polars_code']), 'Settings not set'


def test_instant_function_result():
    # Setup nodes
    flow_id = create_flow_with_manual_input()
    add_node(flow_id=flow_id, node_id=2, node_type='formula', pos_x=0, pos_y=0)

    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)
    # Await the result
    response = client.get("/custom_functions/instant_result",
                          params={'flow_id': flow_id, 'node_id': 2, 'func_string': '[name]'})
    assert response.status_code == 200, 'Instant function result failed'
    assert response.json()['success'] is not False, 'Instant function result failed'


def test_instant_function_result_fail():
    flow_id = create_flow_with_manual_input()
    add_node(flow_id=flow_id, node_id=2, node_type='formula', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)

    # Await the result
    response = client.get("/custom_functions/instant_result", params={'flow_id': flow_id, 'node_id': 2,
                                                                      'func_string': 'name'})
    assert response.status_code == 200, 'Instant function result failed'
    assert not response.json().get('success'), 'Instant function result did not fail'


def test_flow_run():
    flow_id = create_flow_with_manual_input()
    response = client.post("/flow/run/", params={'flow_id': flow_id})
    assert response.status_code in (200, 202), 'Flow not Started'
    assert flow_file_handler.get_flow(flow_id).get_run_info().start_time is not None, 'Flow did not run'


def test_instant_function_result_after_run():
    flow_id = create_flow_with_manual_input()
    add_node(flow_id=flow_id, node_id=2, node_type='formula', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)
    flow = flow_file_handler.get_flow(flow_id)
    _ = flow._flow_starts
    flow.flow_settings.execution_mode = "Development"
    flow.run_graph()
    response = client.get("/custom_functions/instant_result",
                          params={'flow_id': flow_id, 'node_id': 2, 'func_string': '[name]'})
    assert response.status_code == 200, 'Instant function result failed'
    assert response.json().get('success'), 'Instant function result did not fail'


def test_get_node():
    flow_id = create_flow_with_manual_input()
    response = client.get("/node", params={'flow_id': flow_id, 'node_id': 1})
    assert response.status_code == 200, 'Node not retrieved'
    assert response.json()['node_id'] == 1, 'Node not retrieved'
    try:
        output_model.NodeData(**response.json())
    except Exception as e:
        raise Exception('Invalid response: ' + str(e))


def test_get_node_data_not_run():
    flow_id = create_flow_with_manual_input()
    response = client.get("/node", params={'flow_id': flow_id, 'node_id': 1, 'get_data': True})
    assert response.status_code == 200, 'Node not retrieved'
    assert response.json()['node_id'] == 1, 'Node not retrieved'
    try:
        output_model.NodeData(**response.json())
    except Exception as e:
        raise Exception('Invalid response: ' + str(e))
    node_data_parsed = output_model.NodeData(**response.json())
    assert node_data_parsed.main_output.columns == ['name', 'city'], 'Node data not correct'
    assert node_data_parsed.main_output.data == [], "Node data should be empty"


def test_get_node_data_after_run():
    flow_id = create_flow_with_manual_input()
    flow = flow_file_handler.get_flow(flow_id)
    flow.flow_settings.execution_mode = "Development"
    flow.run_graph()
    response = client.get("/node", params={'flow_id': flow_id, 'node_id': 1, 'get_data': True})
    assert response.status_code == 200, 'Node not retrieved'
    assert response.json()['node_id'] == 1, 'Node not retrieved'
    try:
        output_model.NodeData(**response.json())
    except Exception as e:
        raise Exception('Invalid response: ' + str(e))
    node_data_parsed = output_model.NodeData(**response.json())
    assert node_data_parsed.main_output.columns == ['name', 'city'], 'Node data not correct'
    assert node_data_parsed.main_output.data == [{'name': 'John', 'city': 'New York'},
                                                 {'name': 'Jane', 'city': 'Los Angeles'},
                                                 {'name': 'Edward', 'city': 'Chicago'},
                                                 {'name': 'Courtney', 'city': 'Chicago'}], "Node data should be filled"


def create_slow_flow() -> FlowId:
    flow_id = create_flow_with_manual_input()
    add_node(flow_id, 2, node_type='polars_code', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)
    settings = {'flow_id': flow_id, 'node_id': 2, 'pos_x': 668, 'pos_y': 450,
                'polars_code_input': {
                    'polars_code': '# Add your polars code here\ntime.sleep(5)\noutput_df = input_df.select(pl.col("name"))'},
                'cache_results': False, 'is_setup': True}
    response = client.post('/update_settings/', json=settings, params={'node_type': 'polars_code'})
    assert response.status_code == 200, 'Settings not updated'
    return flow_id


def test_flow_cancel():
    flow_id = create_slow_flow()
    flow = flow_file_handler.get_flow(flow_id)
    thread = threading.Thread(target=flow.run_graph)
    thread.start()
    sleep(0.5)
    while flow.latest_run_info is not None and 2 not in [n.node_id for n in flow.latest_run_info.node_step_result]:
        sleep(0.5)
    sleep(2)  # give it some time to start up
    # actual start of the test
    response = client.post("/flow/cancel/", params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow not canceled'
    assert flow.flow_settings.is_canceled, 'Flow not requested to cancel'
    assert flow.flow_settings.is_running, 'Flow stopped running to early, without waiting for all processes to cancel'
    thread.join()  # Wait for the thread to finish
    assert flow.get_run_info().node_step_result[1].success is None, 'Flow not canceled'
    assert flow.get_run_info().node_step_result[0].success, 'Cancel should not reset nodes that ran before cancel'
    assert flow.flow_settings.is_running is False, 'Indicator not set to false'


def test_flow_cancel_when_not_running():
    flow_id = create_slow_flow()
    response = client.post("/flow/cancel/", params={'flow_id': flow_id})
    assert response.status_code == 422, 'Flow should not be able to cancel'


def test_error_run_flow_while_running():
    flow_id = create_slow_flow()
    flow = flow_file_handler.get_flow(flow_id)
    thread = threading.Thread(target=flow.run_graph)
    thread.start()
    responses = [client.post("/flow/run/", params={'flow_id': flow_id}) for i in range(10)]
    thread.join()
    for response in responses:
        assert response.status_code == 422, 'Flow should not be able to run while running'


@pytest.mark.skipif(not is_docker_available(),
                    reason="Docker is not available or not running so database reader cannot be tested")
def test_add_database_input():
    ensure_password_is_available()
    flow_id = ensure_clean_flow()
    response = client.post("/editor/add_node",
                           params={'flow_id': flow_id, 'node_id': 1, 'node_type': 'database_reader', 'pos_x': 0,
                                   'pos_y': 0})
    assert response.status_code == 200, 'Node not added'
    assert flow_file_handler.get_node(flow_id, 1).node_type == 'database_reader', 'Node type not set'
    database_connection = input_schema.DatabaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_database_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')

    database_settings = input_schema.DatabaseSettings(database_connection=database_connection,
                                                      schema_name='public', table_name='movies')
    node_database_reader = input_schema.NodeDatabaseReader(database_settings=database_settings, node_id=1,
                                                           flow_id=flow_id, schema_name='public', table_name='movies',
                                                           user_id=1)
    r = client.post("/update_settings/", json=node_database_reader.model_dump(),
                    params={"node_type": "database_reader"})
    assert r.status_code == 200, 'Settings not added'
    flow_file_handler.get_flow(flow_id).flow_settings.execution_mode = 'Development'
    flow_file_handler.get_flow(flow_id).run_graph()
    assert not flow_file_handler.get_flow(flow_id).get_node(1).needs_run(False), 'Node should not need to run'


def test_add_database_input_with_query():
    ensure_password_is_available()
    flow_id = ensure_clean_flow()
    response = client.post("/editor/add_node",
                           params={'flow_id': flow_id, 'node_id': 1, 'node_type': 'database_reader', 'pos_x': 0,
                                   'pos_y': 0})
    assert response.status_code == 200, 'Node not added'

    input_data = {
        'flow_id': flow_id, 'node_id': 2, 'cache_results': False, 'pos_x': 389.87716758034446,
        'pos_y': 281.12954906116835, 'is_setup': True, 'description': '', 'user_id': 1,
        'database_settings': {
            'database_connection': {'database_type': 'postgresql', 'username': 'testuser',
                                    'password_ref': 'test_database_pw', 'host': 'localhost', 'port': 5433,
                                    'database': 'testdb', 'url': None}, 'schema_name': None, 'table_name': None,
            'query': 'select * from movies', 'query_mode': 'query'},
        'fields': [{'name': 'number', 'data_type': 'Int32'}]}
    r = client.post("/update_settings/", json=input_data,
                    params={"node_type": "database_reader"})
    node_database_reader = input_schema.NodeDatabaseReader.model_validate(input_data)


@pytest.mark.skipif(not is_docker_available(),
                    reason="Docker is not available or not running so database reader cannot be tested")
def test_validate_db_settings():
    ensure_password_is_available()
    database_connection = input_schema.DatabaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_database_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')

    database_settings = input_schema.DatabaseSettings(database_connection=database_connection,
                                                      schema_name='public', table_name='movies')

    r = client.post("/validate_db_settings", json=database_settings.model_dump())
    assert r.status_code == 200, 'Settings be validated'


@pytest.mark.skipif(not is_docker_available(),
                    reason="Docker is not available or not running so database reader cannot be tested")
def test_validate_db_settings_connection_reference():
    ensure_db_connection_is_available()
    settings = {
        "query_mode": "query",
        "connection_mode": "reference",
        "query": "select 1",
        "database_connection_name": "test_connection_endpoint"
    }
    r = client.post("/validate_db_settings", json=settings)
    assert r.status_code == 200, 'Settings should be valid'


@pytest.mark.skipif(not is_docker_available(),
                    reason="Docker is not available or not running so database reader cannot be tested")
def test_validate_db_settings_non_existing_password():
    ensure_password_is_available()
    database_connection = input_schema.DatabaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_databasse_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')

    database_settings = input_schema.DatabaseSettings(database_connection=database_connection,
                                                      schema_name='public', table_name='movies')

    r = client.post("/validate_db_settings", json=database_settings.model_dump())
    assert r.status_code == 422, 'Settings should not be validated'


@pytest.mark.skipif(not is_docker_available(),
                    reason="Docker is not available or not running so database reader cannot be tested")
def test_validate_db_settings_non_existing_table():
    ensure_password_is_available()
    database_connection = input_schema.DatabaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_database_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')

    database_settings = input_schema.DatabaseSettings(database_connection=database_connection,
                                                      schema_name='public', table_name='MOV')

    r = client.post("/validate_db_settings", json=database_settings.model_dump())
    assert r.status_code == 422, 'Settings should not be validated'


@pytest.mark.skipif(not is_docker_available(),
                    reason="Docker is not available or not running so database reader cannot be tested")
def test_validate_db_settings_wrong_query():
    ensure_password_is_available()
    database_connection = input_schema.DatabaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_database_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')

    database_settings = input_schema.DatabaseSettings(database_connection=database_connection,
                                                      query='SELECT *  public.movies')

    r = client.post("/validate_db_settings", json=database_settings.model_dump())
    assert r.status_code == 422, 'Settings should not be validated'


def test_create_secret():
    if get_encrypted_secret(current_user_id=1, secret_name='test_secret'):
        response = client.delete("/secrets/secrets/test_secret", )
    response = client.post("/secrets/secrets",
                           json={'name': 'test_secret', 'value': 'test_value'}, )
    assert response.status_code == 200, 'Secret not created'
    created_secret = get_encrypted_secret(current_user_id=1, secret_name='test_secret')
    assert created_secret is not None, 'Secret not created'


def test_remove_secret():
    if get_encrypted_secret(current_user_id=1, secret_name='test_secret'):
        response = client.post("/secrets/secrets",
                               json={'name': 'test_secret', 'value': 'test_value'}, )
        created_secret = get_encrypted_secret(current_user_id=1, secret_name='test_secret')
        assert created_secret is not None, 'Secret not created'
    response = client.delete("/secrets/secrets/test_secret", )
    assert response.status_code == 204, 'Secret not deleted'
    created_secret = get_encrypted_secret(current_user_id=1, secret_name='test_secret')
    assert created_secret is None, 'Secret not deleted'


def test_editor_code_to_polars():
    flow_id = create_flow_with_manual_input_and_select()
    v = client.get('/editor/code_to_polars', params={"flow_id": flow_id})
    assert v.status_code == 200, "Request should be successful"
    exec_globals = {}
    exec(v.json(), exec_globals)
    result = exec_globals['run_etl_pipeline']()
    assert len(result.collect()) == 4, "Length of result should be 4"
    assert result.columns == ['name'], "Colum name should name"


def test_copy_node_not_run():
    flow_id = create_flow_with_manual_input_and_select()
    flow = flow_file_handler.get_flow(flow_id)
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type='select')
    r = client.post('/editor/copy_node', params={'node_id_to_copy_from': 2,
                                                 'flow_id_to_copy_from': flow_id}, json=node_promise.__dict__)
    assert r.status_code == 200, 'Node not copied'
    copied_node = flow.get_node(3)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 3)
    client.post("/editor/connect_node/", data=connection.model_dump_json(), params={"flow_id": flow_id})

    assert copied_node is not None, 'Node not copied'
    assert copied_node.node_type == 'select', 'Node type not copied'
    assert copied_node.needs_run(False), 'Node should need to run'


def test_from_other_flow():
    flow_id_to_copy_from = create_flow_with_manual_input_and_select()
    flow_id_to_copy_to = create_flow_with_manual_input()
    node_promise = input_schema.NodePromise(flow_id=flow_id_to_copy_to, node_id=34, node_type='select')
    r = client.post('/editor/copy_node', params={'node_id_to_copy_from': 2,
                                                 'flow_id_to_copy_from': flow_id_to_copy_from}, json=node_promise.__dict__)
    assert r.status_code == 200, 'Node not copied'
    assert flow_file_handler.get_node(flow_id_to_copy_to, 34) is not None, 'Node not copied'
    connection = input_schema.NodeConnection.create_from_simple_input(1, 34)
    client.post("/editor/connect_node/", data=connection.model_dump_json(), params={"flow_id": flow_id_to_copy_to})
    r = flow_file_handler.get_flow(flow_id_to_copy_to).run_graph()
    assert r.success, 'Flow not run'


def test_copy_node_run():
    flow_id = create_flow_with_manual_input_and_select()
    flow = flow_file_handler.get_flow(flow_id)
    flow.run_graph()
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=3, node_type='select')
    r = client.post('/editor/copy_node', params={'node_id_to_copy_from': 2,
                                                 'flow_id_to_copy_from': flow_id}, json=node_promise.__dict__)
    assert r.status_code == 200, "Node not copied"
    copied_node = flow.get_node(3)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 3)
    client.post(
        "/editor/connect_node/",
        data=connection.model_dump_json(),
        params={"flow_id": flow_id},
    )

    assert copied_node is not None, "Node not copied"
    assert copied_node.node_type == "select", "Node type not copied"
    assert copied_node.needs_run(False), "Node should need to run"
    resulting_data = copied_node.get_resulting_data()
    try:
        resulting_data.assert_equal(flow.get_node(2).get_resulting_data())
    except Exception as e:
        raise Exception("Node results should be equal: " + str(e))


def test_copy_placeholder_node():
    flow_id = ensure_clean_flow()
    add_node_placeholder('manual_input', node_id=1, flow_id=flow_id)
    new_node = {'flow_id': flow_id, 'node_id': 2, 'node_type': 'manual_input',  'pos_x': 0, 'pos_y': 0}
    r = client.post('/editor/copy_node', params={'node_id_to_copy_from': 1, 'flow_id_to_copy_from': flow_id},
                    json=new_node)
    assert r.status_code == 200, 'Node not copied'


def test_create_db_connection():
    ensure_password_is_available()
    with get_db_context() as db:
        for con_name in ['test_connection', 'test_connection_2']:
            db_connection = get_database_connection(db, con_name, 1)
            if db_connection is not None:
                delete_database_connection(db, con_name, 1)
    database_connection = input_schema.FullDatabaseConnection(database_type='postgresql',
                                                              username='testuser',
                                                              password='test_database_pw',
                                                              host='localhost',
                                                              port=5433,
                                                              database='testdb',
                                                              connection_name='test_connection')
    db_data = database_connection.model_dump()
    db_data['password'] = 'test_database_pw'
    response = client.post("/db_connection_lib", json=db_data)
    assert response.status_code == 200, 'Connection not created'

    response = client.post("/db_connection_lib", json=db_data)
    assert response.status_code == 422, 'Connection should not be created, since already exists'
    db_data['connection_name'] = 'test_connection_2'
    response = client.post("/db_connection_lib", json=db_data)
    assert response.status_code == 200, 'Connection_2 not created'

    with get_db_context() as db:
        db_connections = [get_database_connection(db, c, 1) for c in ('test_connection', 'test_connection_2')]
        assert all(dbc is not None for dbc in db_connections), 'Connections not created'
        for db_connection in db_connections:
            delete_database_connection(db, db_connection.connection_name, 1)


def test_delete_db_connection():
    ensure_password_is_available()
    with get_db_context() as db:
        for con_name in ['test_connection', 'test_connection_2']:
            db_connection = get_database_connection(db, con_name, 1)
            if db_connection is None:
                database_connection = input_schema.FullDatabaseConnection(database_type='postgresql',
                                                                          username='testuser',
                                                                          password='test_database_pw',
                                                                          host='localhost',
                                                                          port=5433,
                                                                          database='testdb',
                                                                          connection_name=con_name)

                db_data = database_connection.model_dump()
                db_data['password'] = 'test_database_pw'
                response = client.post("/db_connection_lib", json=db_data)
                assert response.status_code == 200, 'Connection not created'
    for con_name in ['test_connection', 'test_connection_2']:
        response = client.delete("/db_connection_lib", params={'connection_name': con_name})
        assert response.status_code == 200, 'Connection not deleted'
    with get_db_context() as db:
        db_connections = [get_database_connection(db, c, 1) for c in ('test_connection', 'test_connection_2')]
        assert all(dbc is None for dbc in db_connections), 'Connections not deleted'


def test_get_db_connection_libs():
    ensure_password_is_available()
    ensure_no_database_connections()
    with get_db_context() as db:
        for con_name in ['test_connection', 'test_connection_2']:
            db_connection = get_database_connection(db, con_name, 1)
            if db_connection is None:
                database_connection = input_schema.FullDatabaseConnection(database_type='postgresql',
                                                                          username='testuser',
                                                                          password='test_database_pw',
                                                                          host='localhost',
                                                                          port=5433,
                                                                          database='testdb',
                                                                          connection_name=con_name)

                db_data = database_connection.model_dump()
                db_data['password'] = 'test_database_pw'
                response = client.post("/db_connection_lib", json=db_data)
                assert response.status_code == 200, 'Connection not created'
    all_connections = client.get('/db_connection_lib')
    assert all_connections.status_code == 200, 'Connections not retrieved'
    connections = all_connections.json()
    assert len(connections) == 2, f'Not all connections not retrieved, number of connections found: {len(connections)}:\n {connections}'
    parsed_connections = [input_schema.FullDatabaseConnectionInterface.model_validate(c) for c in connections]
    with get_db_context() as db:
        for con_name in ['test_connection', 'test_connection_2']:
            delete_database_connection(db, con_name, 1)


def test_create_cloud_storage_connection():
    ensure_password_is_available()
    user_id = 1
    ensure_no_cloud_storage_connection_is_available(user_id=user_id)
    new_cloud_connection = get_cloud_connection()
    cloud_connection_dict = new_cloud_connection.model_dump()
    for field_name, field_value in cloud_connection_dict.items():
        if isinstance(field_value, SecretStr):
            cloud_connection_dict[field_name] = field_value.get_secret_value()
    response = client.post("/cloud_connections/cloud_connection", json=cloud_connection_dict)
    assert response.status_code == 200, "Connection not created"
    created_cloud_connection = get_local_cloud_connection(new_cloud_connection.connection_name, user_id=user_id)
    assert created_cloud_connection is not None, "Connection should be available in database"
    assert created_cloud_connection == new_cloud_connection


def test_create_cloud_storage_connection_allow_unsafe_html():
    ensure_password_is_available()
    user_id = 1
    ensure_no_cloud_storage_connection_is_available(user_id=user_id)
    new_cloud_connection = get_cloud_connection()
    new_cloud_connection.aws_allow_unsafe_html = True
    cloud_connection_dict = new_cloud_connection.model_dump()
    for field_name, field_value in cloud_connection_dict.items():
        if isinstance(field_value, SecretStr):
            cloud_connection_dict[field_name] = field_value.get_secret_value()
    response = client.post("/cloud_connections/cloud_connection", json=cloud_connection_dict)
    assert response.status_code == 200, "Connection not created"
    created_cloud_connection = get_local_cloud_connection(new_cloud_connection.connection_name, user_id=user_id)
    assert created_cloud_connection is not None, "Connection should be available in database"
    assert created_cloud_connection == new_cloud_connection
    cloud_connections_response = client.get("cloud_connections/cloud_connections")
    assert cloud_connections_response.status_code == 200, "Connections not retrieved"
    cloud_connections = cloud_connections_response.json()
    assert len(cloud_connections) == 1, "Should be one connection available"
    assert cloud_connections[0]["aws_allow_unsafe_html"], "Connection should have allow_unsafe_html set to True"


def test_create_cloud_storage_connection_already_exists():
    ensure_password_is_available()
    user_id = 1
    ensure_no_cloud_storage_connection_is_available(user_id=user_id)
    new_cloud_connection = get_cloud_connection()
    cloud_connection_dict = new_cloud_connection.model_dump()
    for field_name, field_value in cloud_connection_dict.items():
        if isinstance(field_value, SecretStr):
            cloud_connection_dict[field_name] = field_value.get_secret_value()
    response = client.post("/cloud_connections/cloud_connection", json=cloud_connection_dict)
    assert response.status_code == 200, "Connection not created"

    response = client.post("/cloud_connections/cloud_connection", json=cloud_connection_dict)
    assert response.status_code == 422, "Connection should not be created again"


def test_delete_cloud_storage_connection():
    ensure_password_is_available()
    user_id = 1
    connection = ensure_cloud_storage_connection_is_available_and_get_connection(user_id)
    response = client.delete("/cloud_connections/cloud_connection",
                             params={'connection_name': connection.connection_name})
    assert response.status_code == 200
    available_cloud_account = get_local_cloud_connection(connection.connection_name, user_id=user_id)
    assert available_cloud_account is None


def test_storage_connections_not_available_when_not_logged_in():
    with TestClient(main.app) as non_logged_in_client:
        assert non_logged_in_client.delete("/cloud_connections/cloud_connection").status_code == 401
        assert non_logged_in_client.post("/cloud_connections/cloud_connection").status_code == 401
        assert non_logged_in_client.get("/cloud_connections/cloud_connections").status_code == 401


@pytest.mark.skipif(not is_docker_available(),
                    reason='Docker is not available or not running so cloud storage connection cannot be tested')
def test_create_cloud_storage_reader():
    flow_id = ensure_clean_flow()
    add_node_placeholder("cloud_storage_reader", flow_id=flow_id)
    conn = ensure_cloud_storage_connection_is_available_and_get_connection(user_id=1)
    read_settings = cloud_ss.CloudStorageReadSettings(
        resource_path="s3://test-bucket/single-file-parquet/data.parquet",
        file_format="parquet",
        scan_mode="single_file",
        connection_name=conn.connection_name,
        auth_mode="access_key"
    )
    node_settings = input_schema.NodeCloudStorageReader(flow_id=flow_id, node_id=1, user_id=1,
                                                        cloud_storage_settings=read_settings)
    r = client.post("/update_settings/", json=node_settings.model_dump(), params={"node_type": "cloud_storage_reader"})
    assert r.status_code == 200, 'Settings not updated'
    node = flow_file_handler.get_node(flow_id, 1)
    assert node._hash is not None, 'Node hash should be set after settings update'
    _ = node.schema

    assert node.node_schema.predicted_schema is not None, 'Node schema should be set'
    r = client.post("/update_settings/", json=node_settings.model_dump(), params={"node_type": "cloud_storage_reader"})
    assert r.status_code == 200, 'Settings not updated'
    assert node.node_schema.result_schema is not None, "Node schema should be set after run"


def test_editor_create_flow_only_name():
    response = client.post("/editor/create_flow/", params={'name': 'test_flow_1'})
    assert response.status_code == 200, 'Flow not created'
    flow_info = flow_file_handler.get_flow_info(response.json())
    assert ".flowfile/temp/flows/test_flow_1.yaml" in flow_info.path
    assert Path(flow_info.path).exists()


def test_editor_create_flow_no_params():
    response = client.post("/editor/create_flow/")
    assert response.status_code == 200, 'Flow not created'
    flow_info = flow_file_handler.get_flow_info(response.json())
    assert ".flowfile/temp/flows/" in flow_info.path
    assert Path(flow_info.path).exists()


def test_editor_create_flow_with_only_path():
    path = str(storage.flows_directory / "test_flow_1.yaml")
    response = client.post("/editor/create_flow/", params={'flow_path': path})
    assert response.status_code == 200, 'Flow not created'
    flow_info = flow_file_handler.get_flow_info(response.json())
    assert path == flow_info.path
    assert Path(flow_info.path).exists()


def test_editor_create_flow_with_both_name_and_path():
    path = str(storage.flows_directory)
    response = client.post("/editor/create_flow/", params={'flow_path': path, "name": "test_flow_1.yaml"})
    assert response.status_code == 200, 'Flow not created'
    flow_info = flow_file_handler.get_flow_info(response.json())
    assert storage.flows_directory / "test_flow_1.yaml" == Path(flow_info.path)
    assert Path(flow_info.path).exists()


def test_editor_create_flow_with_both_name_and_non_existing_path():
    path = str(storage.flows_directory/"WRONG_SUBDIR")
    response = client.post("/editor/create_flow/", params={'flow_path': path, "name": "test_flow_1.yaml"})
    assert response.status_code == 422, "Flow should not be created"
    assert response.json()["detail"] == "The directory does not exist"


def test_editor_create_flow_with_both_name_no_overlap():
    path = str(storage.flows_directory/"test_flow_2.yaml")
    response = client.post("/editor/create_flow/", params={'flow_path': path, "name": "test_flow_1.yaml"})
    assert response.status_code == 422, "Flow should not be created"
    assert response.json()["detail"] == 'The name must be part of the flow path when a full path is provided'


def test_get_table_example():
    flow_id = create_flow_with_manual_input_and_select()
    response = client.get("/node/data", params={'flow_id': flow_id, 'node_id': 2})
    assert response.status_code == 200, 'Node data not retrieved'
    assert response.json()["data"] == [], "Node data should be empty"


def test_fetch_node_data():
    flow_id = create_flow_with_manual_input_and_select()
    response = client.post("/node/trigger_fetch_data", params={'flow_id': flow_id, 'node_id': 2})
    assert response.status_code == 200, 'Node data not retrieved'
    response = client.get("/node/data", params={'flow_id': flow_id, 'node_id': 2})
    assert response.status_code == 200, 'Node data not retrieved'
    assert len(response.json()["data"]) > 0 , "Data should not be empty"
    assert response.json()["has_run_with_current_setup"], "Node should have run"


def test_flow_run_status():
    flow_id = create_flow_with_manual_input_and_select()
    response = client.get("/flow/run_status", params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow run status not retrieved'
    assert response.json()['start_time'] is None, 'Flow should not be running'
    flow = flow_file_handler.get_flow(flow_id)
    flow.flow_settings.execution_mode = "Development"
    flow.run_graph()
    response = client.get("/flow/run_status", params={'flow_id': flow_id})
    assert response.status_code == 200, 'Flow run status not retrieved'
    assert response.json()['end_time'] is not None, 'Flow should have ended'


# =============================================================================
# Path Traversal Security Tests
#
# Note: These tests verify sandboxing behavior which is enforced in Docker/package mode.
# In Electron mode, users have access to their local filesystem (which is expected for
# a desktop application). Tests that check absolute path blocking use monkeypatch to
# simulate Docker mode.
# =============================================================================

def test_get_local_files_path_traversal_blocked():
    """Test that get_local_files blocks access to directories outside sandbox."""
    # Attempt to access /etc directory (should be blocked)
    # Note: This endpoint uses SecureFileExplorer which sandboxes to user_data_directory
    response = client.get("/files/files_in_local_directory/", params={'directory': '/etc'})
    assert response.status_code == 403, 'Path traversal to /etc should be blocked'
    assert 'Access denied' in response.json()['detail'], 'Should return access denied message'


def test_get_local_files_path_traversal_with_dots():
    """Test that get_local_files blocks path traversal using .. patterns."""
    response = client.get("/files/files_in_local_directory/",
                          params={'directory': '../../../etc'})
    assert response.status_code == 403, 'Path traversal with .. should be blocked'


def test_upload_file_sanitizes_filename():
    """Test that upload_file sanitizes filenames to prevent path traversal."""
    import io
    # Create a file with a malicious filename containing path traversal
    malicious_filename = "../../../etc/cron.d/evil"
    file_content = b"malicious content"

    files = {'file': (malicious_filename, io.BytesIO(file_content), 'application/octet-stream')}
    response = client.post("/upload/", files=files)

    assert response.status_code == 200, 'Upload should succeed with sanitized filename'
    result = response.json()
    # The filename should be sanitized to just 'evil' (basename without ..)
    assert result['filename'] == 'evil', f"Filename should be sanitized to 'evil', got: {result['filename']}"
    assert '../' not in result['filepath'], 'Filepath should not contain path traversal sequences'
    # Normalize path separators for cross-platform comparison
    normalized_filepath = result['filepath'].replace('\\', '/')
    assert normalized_filepath == 'uploads/evil', f"Filepath should be 'uploads/evil', got: {result['filepath']}"

    # Clean up uploaded file
    if os.path.exists(result['filepath']):
        os.remove(result['filepath'])


def test_upload_file_sanitizes_filename_with_multiple_traversals():
    """Test that upload_file handles multiple path traversal attempts."""
    import io
    malicious_filename = "..%2F..%2F..%2Fetc/passwd"
    file_content = b"test content"

    files = {'file': (malicious_filename, io.BytesIO(file_content), 'application/octet-stream')}
    response = client.post("/upload/", files=files)

    assert response.status_code == 200, 'Upload should succeed with sanitized filename'
    result = response.json()
    assert '../' not in result['filename'], 'Filename should not contain path traversal'
    assert '/' not in result['filename'], 'Filename should not contain directory separators'

    # Clean up
    if os.path.exists(result['filepath']):
        os.remove(result['filepath'])


def test_import_flow_path_traversal_blocked(monkeypatch):
    """Test that import_flow blocks access to files outside sandbox in Docker mode."""
    # Patch is_electron_mode to return False (simulating Docker/package mode)
    # This is needed because FLOWFILE_MODE is cached at module load time
    from flowfile_core.configs import settings
    monkeypatch.setattr(settings, 'is_electron_mode', lambda: False)
    # Attempt to import /etc/passwd (should be blocked in Docker mode)
    response = client.get("/import_flow/", params={'flow_path': '/etc/passwd'})
    assert response.status_code == 403, 'Path traversal to /etc/passwd should be blocked in Docker mode'
    assert 'Access denied' in response.json()['detail'], 'Should return access denied message'


def test_import_flow_path_traversal_with_dots():
    """Test that import_flow blocks path traversal using .. patterns."""
    # .. patterns are blocked in all modes
    response = client.get("/import_flow/", params={'flow_path': '../../../etc/passwd'})
    assert response.status_code == 403, 'Path traversal with .. should be blocked'


def test_save_flow_path_traversal_blocked(monkeypatch):
    """Test that save_flow blocks saving to paths outside sandbox in Docker mode."""
    # Patch is_electron_mode to return False (simulating Docker/package mode)
    from flowfile_core.configs import settings
    monkeypatch.setattr(settings, 'is_electron_mode', lambda: False)
    flow_id = create_flow_with_manual_input()
    response = client.get("/save_flow", params={'flow_id': flow_id, 'flow_path': '/etc/malicious.yaml'})
    assert response.status_code == 403, 'Path traversal to /etc should be blocked in Docker mode'
    assert 'Access denied' in response.json()['detail'], 'Should return access denied message'


def test_save_flow_path_traversal_with_dots():
    """Test that save_flow blocks path traversal using .. patterns."""
    # .. patterns are blocked in all modes
    flow_id = create_flow_with_manual_input()

    response = client.get("/save_flow",
                          params={'flow_id': flow_id, 'flow_path': '../../../etc/malicious.yaml'})
    assert response.status_code == 403, 'Path traversal with .. should be blocked'


def test_get_excel_sheet_names_path_traversal_blocked(monkeypatch):
    """Test that get_excel_sheet_names blocks access to files outside sandbox in Docker mode."""
    # Patch is_electron_mode to return False (simulating Docker/package mode)
    from flowfile_core.configs import settings
    monkeypatch.setattr(settings, 'is_electron_mode', lambda: False)
    # Attempt to read /etc/passwd (should be blocked in Docker mode)
    response = client.get("/api/get_xlsx_sheet_names", params={'path': '/etc/passwd'})
    assert response.status_code == 403, 'Path traversal to /etc/passwd should be blocked in Docker mode'
    assert 'Access denied' in response.json()['detail'], 'Should return access denied message'


def test_get_excel_sheet_names_path_traversal_with_dots():
    """Test that get_excel_sheet_names blocks path traversal using .. patterns."""
    # .. patterns are blocked in all modes
    response = client.get("/api/get_xlsx_sheet_names", params={'path': '../../../etc/passwd'})
    assert response.status_code == 403, 'Path traversal with .. should be blocked'


def test_get_available_flow_files_path_traversal_blocked():
    """Test that available_flow_files blocks access to directories outside sandbox."""
    # Attempt to scan /etc directory (should return empty or be blocked)
    response = client.get("/files/available_flow_files", params={'path': '/etc'})
    # Should return empty list for paths outside sandbox (graceful handling)
    assert response.status_code == 200, 'Should return 200 with empty list'
    assert response.json() == [], 'Should return empty list for paths outside sandbox'


def test_get_available_flow_files_path_traversal_with_dots():
    """Test that available_flow_files blocks path traversal using .. patterns."""
    response = client.get("/files/available_flow_files", params={'path': '../../../etc'})
    assert response.status_code == 200, 'Should return 200 with empty list'
    assert response.json() == [], 'Should return empty list for path traversal attempts'


# ==================== Node Reference Tests ====================


def test_get_node_reference():
    """Test retrieving node reference from a node."""
    flow_id = create_flow_with_manual_input()
    response = client.get("/node/reference", params={'flow_id': flow_id, 'node_id': 1})
    assert response.status_code == 200, 'Node reference not retrieved'
    # Default should be empty string
    assert response.json() == "", 'Default node reference should be empty string'


def test_set_node_reference():
    """Test setting a node reference."""
    flow_id = create_flow_with_manual_input()

    # Set a valid reference
    response = client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="my_custom_ref"
    )
    assert response.status_code == 200, 'Node reference not set'
    assert response.json() is True, 'Should return True on success'

    # Verify the reference was set
    response = client.get("/node/reference", params={'flow_id': flow_id, 'node_id': 1})
    assert response.status_code == 200
    assert response.json() == "my_custom_ref", 'Node reference should be updated'


def test_set_node_reference_rejects_uppercase():
    """Test that uppercase characters are rejected in node reference."""
    flow_id = create_flow_with_manual_input()

    response = client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="MyRef"
    )
    assert response.status_code == 422, 'Uppercase should be rejected'
    assert 'lowercase' in response.json()['detail'].lower(), 'Error should mention lowercase'


def test_set_node_reference_rejects_spaces():
    """Test that spaces are rejected in node reference."""
    flow_id = create_flow_with_manual_input()

    response = client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="my ref"
    )
    assert response.status_code == 422, 'Spaces should be rejected'
    assert 'spaces' in response.json()['detail'].lower(), 'Error should mention spaces'


def test_set_node_reference_allows_underscores():
    """Test that underscores are allowed in node reference."""
    flow_id = create_flow_with_manual_input()

    response = client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="my_custom_ref_123"
    )
    assert response.status_code == 200, 'Underscores should be allowed'
    assert response.json() is True


def test_set_node_reference_empty_clears():
    """Test that empty string clears the node reference."""
    flow_id = create_flow_with_manual_input()

    # First set a reference
    client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="my_ref"
    )

    # Then clear it with empty string
    response = client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json=""
    )
    assert response.status_code == 200, 'Empty reference should be allowed'

    # Verify it was cleared
    response = client.get("/node/reference", params={'flow_id': flow_id, 'node_id': 1})
    assert response.json() == "", 'Node reference should be empty after clearing'


def test_validate_node_reference_valid():
    """Test validation of a valid node reference."""
    flow_id = create_flow_with_manual_input()

    response = client.get(
        "/node/validate_reference",
        params={'flow_id': flow_id, 'node_id': 1, 'reference': 'valid_ref'}
    )
    assert response.status_code == 200
    result = response.json()
    assert result['valid'] is True, 'Valid reference should be valid'
    assert result['error'] is None, 'Valid reference should have no error'


def test_validate_node_reference_empty_is_valid():
    """Test that empty reference is always valid (uses default)."""
    flow_id = create_flow_with_manual_input()

    response = client.get(
        "/node/validate_reference",
        params={'flow_id': flow_id, 'node_id': 1, 'reference': ''}
    )
    assert response.status_code == 200
    result = response.json()
    assert result['valid'] is True, 'Empty reference should be valid'


def test_validate_node_reference_rejects_uppercase():
    """Test that validation rejects uppercase characters."""
    flow_id = create_flow_with_manual_input()

    response = client.get(
        "/node/validate_reference",
        params={'flow_id': flow_id, 'node_id': 1, 'reference': 'MyRef'}
    )
    assert response.status_code == 200
    result = response.json()
    assert result['valid'] is False, 'Uppercase reference should be invalid'
    assert 'lowercase' in result['error'].lower(), 'Error should mention lowercase'


def test_validate_node_reference_rejects_spaces():
    """Test that validation rejects spaces."""
    flow_id = create_flow_with_manual_input()

    response = client.get(
        "/node/validate_reference",
        params={'flow_id': flow_id, 'node_id': 1, 'reference': 'my ref'}
    )
    assert response.status_code == 200
    result = response.json()
    assert result['valid'] is False, 'Reference with spaces should be invalid'
    assert 'spaces' in result['error'].lower(), 'Error should mention spaces'


def test_validate_node_reference_unique():
    """Test that validation checks uniqueness across nodes."""
    flow_id = ensure_clean_flow()

    # Add two nodes
    add_node_placeholder('manual_input', flow_id=flow_id, node_id=1)
    add_node_placeholder('manual_input', flow_id=flow_id, node_id=2)

    # Set reference on first node
    client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="my_unique_ref"
    )

    # Try to validate same reference for second node
    response = client.get(
        "/node/validate_reference",
        params={'flow_id': flow_id, 'node_id': 2, 'reference': 'my_unique_ref'}
    )
    assert response.status_code == 200
    result = response.json()
    assert result['valid'] is False, 'Duplicate reference should be invalid'
    assert 'already used' in result['error'].lower(), 'Error should mention duplicate'


def test_set_node_reference_rejects_duplicate():
    """Test that setting a duplicate reference is rejected."""
    flow_id = ensure_clean_flow()

    # Add two nodes
    add_node_placeholder('manual_input', flow_id=flow_id, node_id=1)
    add_node_placeholder('manual_input', flow_id=flow_id, node_id=2)

    # Set reference on first node
    client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 1},
        json="my_ref"
    )

    # Try to set same reference on second node
    response = client.post(
        "/node/reference/",
        params={'flow_id': flow_id, 'node_id': 2},
        json="my_ref"
    )
    assert response.status_code == 422, 'Duplicate reference should be rejected'
    assert 'already used' in response.json()['detail'].lower(), 'Error should mention duplicate'


def test_node_reference_not_found():
    """Test getting reference for non-existent node returns 404."""
    flow_id = ensure_clean_flow()

    response = client.get("/node/reference", params={'flow_id': flow_id, 'node_id': 9999})
    assert response.status_code == 404, 'Non-existent node should return 404'
