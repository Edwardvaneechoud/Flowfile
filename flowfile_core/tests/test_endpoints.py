from flowfile_core.routes.routes import (add_node,
                                         flow_file_handler,
                                         input_schema,
                                         connect_node,
                                         output_model, )
from flowfile_core.schemas.transform_schema import SelectInput
from time import sleep
import os
import threading
from typing import Dict
from fastapi.testclient import TestClient
import pickle
from flowfile_core import main
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection

FlowId = int

def get_auth_token():
    """Get authentication token for testing"""
    with TestClient(main.app) as client:
        response = client.post("/auth/token")
        return response.json()["access_token"]

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

def get_flow_settings() -> Dict:
    return {'flow_id': 1, 'description': None, 'save_location': None, 'auto_save': False, 'name': '',
            'modified_on': None, 'path': 'flowfile_core/tests/support_files/flows/test_flow.flowfile',
            'execution_mode': 'Development', 'is_running': False, 'is_canceled': False}


def get_join_data(flow_id: int, how: str = 'inner'):
    return {'flow_id': flow_id, 'node_id': 3, 'cache_results': False, 'pos_x': 788.8727272727273, 'pos_y': 186.4,
            'is_setup': True, 'description': '', 'depending_on_ids': [-1], 'auto_generate_selection': True,
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


def add_manual_input(graph: EtlGraph, data, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id, raw_data=data)
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
    flow_path: str = 'flowfile_core/tests/support_files/flows/sample_flow_path.flowfile'
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
                                              raw_data=[{'name': 'John', 'city': 'New York'},
                                                        {'name': 'Jane', 'city': 'Los Angeles'},
                                                        {'name': 'Edward', 'city': 'Chicago'},
                                                        {'name': 'Courtney', 'city': 'Chicago'}]).__dict__
    client.post("/update_settings/", json=input_file, params={"node_type": "manual_input"})
    return flow_id


def test_register_flow():
    ensure_no_flow_registered()
    flow_path: str = 'flowfile_core/tests/support_files/flows/test_flow.flowfile'
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
                                              raw_data=[{'name': 'John', 'city': 'New York'},
                                                        {'name': 'Jane', 'city': 'Los Angeles'},
                                                        {'name': 'Edward', 'city': 'Chicago'},
                                                        {'name': 'Courtney', 'city': 'Chicago'}]).__dict__
    r = client.post("/update_settings/", json=input_file, params={"node_type": "manual_input"})
    assert r.status_code == 200, 'Settings not added'
    assert flow_file_handler.get_node(flow_id, 1).setting_input.raw_data == input_file['raw_data'], 'Settings not set'


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
    assert len(flow.node_results) == 2, 'Flow should have only executed 2 nodes'


def test_import_flow():
    if flow_file_handler.get_flow(1):
        flow_file_handler.delete_flow(1)

    flow_path = 'flowfile_core/tests/support_files/flows/test_flow.flowfile'
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
    file_path = 'flowfile_core/tests/support_files/flows/sample_save.flowfile'
    remove_flow(file_path)
    # def save_flow(flow_id: int, flow_path: str = None)
    response = client.get("/save_flow", params={'flow_id': flow_id, 'flow_path': file_path})
    assert response.status_code == 200, 'Flow not saved'
    assert os.path.exists(file_path), 'Flow not saved, file not found'
    with open(file_path, 'rb') as f:
        pickle_obj = pickle.load(f)
        assert pickle_obj.flow_id == flow_id, 'Flow not stored correctly'
    imported_flow_id = flow_file_handler.import_flow(file_path)
    assert imported_flow_id == flow_id, 'Flow not stored or imported correctly correctly'
    remove_flow(file_path)


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
    expected_data = {'node_edges': [
        {'id': '1-2-0', 'source': '1', 'target': '2', 'targetHandle': 'input-0', 'sourceHandle': 'output-0'}],
                     'node_inputs': [{'name': 'Manual input', 'item': 'manual_input', 'input': 0, 'output': 1,
                                      'image': 'manual_input.png', 'multi': False, 'node_group': 'input',
                                      'prod_ready': True, 'id': 1, 'pos_x': 0.0, 'pos_y': 0.0},
                                     {'name': 'Select data', 'item': 'select', 'input': 1, 'output': 1,
                                      'image': 'select.png', 'multi': False, 'node_group': 'transform',
                                      'prod_ready': True, 'id': 2, 'pos_x': 0.0, 'pos_y': 0.0}]}
    assert response.json() == expected_data, 'Flow data not correct'


def create_flow_with_graphic_walker_input() -> FlowId:
    flow_id = create_flow_with_manual_input()
    add_node(flow_id=flow_id, node_id=2, node_type='explore_data')
    assert flow_file_handler.get_node(flow_id, 2) is not None, 'Node not added stopping test'
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(flow_id, connection)
    return flow_id


def test_get_graphic_walker_input():
    flow_id = create_flow_with_graphic_walker_input()
    response = client.get('/analysis_data/graphic_walker_input', params={'flow_id': flow_id, 'node_id': 2})
    assert response.status_code == 200, 'Graphic walker input not retrieved'
    try:
        input_schema.NodeExploreData(**response.json())
    except Exception as e:
        raise Exception('Invalid response: ' + str(e))


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
    assert response.json()['success'], 'Instant function result failed'


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
    flow_file_handler.get_flow(flow_id).run_graph()
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
    while 2 not in [n.node_id for n in flow.node_results]:
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


def test_add_database_input():
    flow_id = ensure_clean_flow()
    response = client.post("/editor/add_node",
                           params={'flow_id': flow_id, 'node_id': 1, 'node_type': 'database_reader', 'pos_x': 0,
                                   'pos_y': 0})
    assert response.status_code == 200, 'Node not added'
    assert flow_file_handler.get_node(flow_id, 1).node_type == 'database_reader', 'Node type not set'
    database_connection = input_schema.DataBaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_database_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')
    node_database_reader = input_schema.NodeDatabaseReader(database_connection=database_connection, node_id=1,
                                                           flow_id=flow_id, schema_name='public', table_name='movies',
                                                           user_id=1)
    r = client.post("/update_settings/", json=node_database_reader.model_dump(), params={"node_type": "database_reader"})
    assert r.status_code == 200, 'Settings not added'
    flow_file_handler.get_flow(flow_id).run_graph()
    assert not flow_file_handler.get_flow(flow_id).get_node(1).needs_run(False), 'Node should not need to run'
