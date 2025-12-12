
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_graph import (FlowGraph, add_connection, RunInformation)
from flowfile_core.schemas import input_schema, transform_schema, schemas, cloud_storage_schemas as cloud_ss
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor
from flowfile_core.configs.flow_logger import FlowLogger
from flowfile_core.flowfile.database_connection_manager.db_connections import (get_local_database_connection,
                                                                               store_database_connection,
                                                                               store_cloud_connection,
                                                                               delete_cloud_connection,
                                                                               get_all_cloud_connections_interface)
from flowfile_core.database.connection import get_db_context
from flowfile_core.flowfile.flow_data_engine.flow_file_column.main import FlowfileColumn
from flowfile_core.flowfile.schema_callbacks import pre_calculate_pivot_schema

import pytest
from pathlib import Path
from typing import List, Dict, Literal
from copy import deepcopy
from time import sleep




def find_parent_directory(target_dir_name,):
    """Navigate up directories until finding the target directory"""
    current_path = Path(__file__)

    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")

try:
    from tests.flowfile_core_test_utils import (is_docker_available, ensure_password_is_available)
    from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection
except ModuleNotFoundError:
    import os
    import sys
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/flowfile_core_test_utils.py")))
    sys.path.append(os.path.dirname(os.path.abspath("flowfile_core/tests/utils.py")))
    # noinspection PyUnresolvedReferences
    from flowfile_core_test_utils import (is_docker_available, ensure_password_is_available)
    from tests.utils import ensure_cloud_storage_connection_is_available_and_get_connection


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


def create_graph(flow_id: int = 1, execution_mode: Literal['Development', 'Performance'] = 'Development') -> FlowGraph:
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name='new_flow', path='.', execution_mode=execution_mode))
    graph = handler.get_flow(flow_id)
    return graph


def add_manual_input(graph: FlowGraph, data, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id, raw_data_format=input_schema.RawData.from_pylist(data))
    graph.add_manual_input(input_file)
    return graph


def add_node_promise_for_manual_input(graph: FlowGraph, node_type: str = 'manual_input', node_id: int = 1,
                                      flow_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(node_promise)
    return graph


def add_node_promise_on_type(graph: FlowGraph, node_type: str, node_id: int, flow_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(node_promise)


def get_group_by_flow():
    graph = create_graph()
    # breakpoint()
    input_data = (FlowDataEngine.create_random(100).apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'sales_data']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'group_by', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    group_by_input = transform_schema.GroupByInput([transform_schema.AggColl('groups', 'groupby'),
                                                    transform_schema.AggColl('sales_data', 'sum', 'sales_data_output')])
    # breakpoint()
    node_group_by = input_schema.NodeGroupBy(flow_id=1, node_id=2, groupby_input=group_by_input)
    graph.add_group_by(node_group_by)
    return graph

@pytest.fixture
def complex_graph():
    group_by_flow = get_group_by_flow()


def test_save_flow(complex_graph):
    ...


def test_create_flowfile_handler():
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'


def test_import_flow():
    handler = create_flowfile_handler()
    flow_path = find_parent_directory("Flowfile") / "flowfile_core/tests/support_files/flows/read_csv.flowfile"
    flow_id = handler.import_flow(flow_path)
    flow = handler.get_flow(flow_id)
    run_results = flow.run_graph()
    handle_run_info(run_results)


def test_create_graph():
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    graph = handler.get_flow(1)
    assert graph.flow_id == 1, 'Flow ID should be 1'
    assert graph.__name__ == 'new_flow', 'Flow name should be new_flow'


def test_add_node_promise_for_manual_input():
    graph = create_graph()
    node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type='manual_input')
    graph.add_node_promise(node_promise)
    assert len(graph.nodes) == 1, 'There should be 1 node in the graph'


def test_add_two_input_nodes():
    graph = create_graph()
    add_manual_input(graph, data=[{'name': 'John', 'city': 'New York'}], node_id=1)
    add_manual_input(graph, data=[{'name': 'Jane', 'city': 'Los Angeles'}], node_id=2)
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_add_manual_input(raw_data):
    graph = create_graph()
    graph = add_node_promise_for_manual_input(graph)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=1,
                                              raw_data_format=input_schema.RawData.from_pylist(raw_data))
    graph.add_manual_input(input_file)
    assert len(graph.nodes) == 1, 'There should be 1 node in the graph'
    assert not graph.get_node(1).has_input, 'Node should not have input'


def test_update_manual_input(raw_data):
    graph = create_graph()
    graph = add_node_promise_for_manual_input(graph)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=1,
                                              raw_data_format=input_schema.RawData.from_pylist(raw_data))
    graph.add_manual_input(input_file)
    assert graph.get_node(1).get_resulting_data().columns == ["name", "city"]
    # Add a fixed column to table data and extract it in the raw data
    new_data = (graph.get_node(1).get_resulting_data().apply_flowfile_formula(func='100', col_name='new').to_raw_data())
    existing_setting_inputs = graph.get_node(1).setting_input
    new_settings = deepcopy(existing_setting_inputs)
    new_settings.raw_data_format = new_data
    graph.add_manual_input(new_settings)
    assert graph.get_node(1).get_resulting_data().columns == ["name", "city", "new"]


def test_get_schema(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, data=raw_data)
    schema = graph.get_node(1).get_predicted_schema()
    node = graph.get_node(1)
    columns = [s.column_name for s in node.schema]
    assert len(schema) == 2, 'There should be 2 columns in the schema'
    assert ['name', 'city'] == columns, 'Columns should be name and city'


def test_run_graph(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, data=raw_data)
    graph.run_graph()
    node = graph.get_node(1)
    assert node.node_stats.has_run_with_current_setup, 'Node should have run'
    assert node.results.resulting_data.collect().to_dicts() == node.setting_input.raw_data_format.to_pylist(), 'Data should be the same'


def test_execute_manual_node_externally(flow_logger: FlowLogger, raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, data=raw_data)
    node = graph.get_node(1)
    node.execute_remote(node_logger=flow_logger.get_node_logger(1))
    assert node.get_resulting_data().collect().to_dicts() == node.setting_input.raw_data_format.to_pylist(), 'Data should be the same'


def test_add_unique(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, data=raw_data)
    node_promise = input_schema.NodePromise(flow_id=1, node_id=2, node_type='unique')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeUnique(flow_id=1, node_id=2,
                                         unique_input=transform_schema.UniqueInput(columns=['city'])
                                         )
    graph.add_unique(input_file)
    assert len(graph.nodes) == 2, 'There should be 2 nodes in the graph'


def test_connect_node(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, data=raw_data)
    node_promise = input_schema.NodePromise(flow_id=1, node_id=2, node_type='unique')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeUnique(flow_id=1, node_id=2,
                                         unique_input=transform_schema.UniqueInput(columns=['city'])
                                         )
    graph.add_unique(input_file)
    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    assert graph.node_connections == [(1, 2)], 'Node connections should be [(1, 2)]'
    assert graph.get_node(1).leads_to_nodes[0] == graph.get_node(2), 'Node 1 should lead to node 2'
    assert graph.get_node(2).node_inputs.main_inputs[0] == graph.get_node(1), 'Node 2 should have node 1 as input'


def test_running_unique(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, data=raw_data)
    node_promise = input_schema.NodePromise(flow_id=1, node_id=2, node_type='unique')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeUnique(flow_id=1, node_id=2,
                                         unique_input=transform_schema.UniqueInput(columns=['city'])
                                         )
    graph.add_unique(input_file)
    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    graph.run_graph()
    node = graph.get_node(2)
    assert node.node_stats.has_run_with_current_setup, 'Node should have run'
    df = node.results.resulting_data.collect()
    assert len(df) == 3, 'There should be 3 rows in the data'
    assert (set(df.select('city').to_series(0).to_list()) ==
            {'New York', 'Los Angeles', 'Chicago'}), 'Cities should be unique'


def test_opening_parquet_file(flow_logger: FlowLogger):
    graph = create_graph()
    add_node_promise_on_type(graph, 'read', 1, 1)
    path = str(find_parent_directory("Flowfile") / 'flowfile_core/tests/support_files/data/table.parquet')
    received_table = input_schema.ReceivedTable(file_type='parquet', name='table.parquet',
                                                path=path)
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received_table)
    graph.add_read(node_read)
    self = graph.get_node(1)
    self.execute_remote(node_logger=flow_logger.get_node_logger(1))


def test_running_performance_mode():
    graph = create_graph()
    from flowfile_core.configs.settings import OFFLOAD_TO_WORKER
    add_node_promise_on_type(graph, 'read', 1, 1)
    from flowfile_core.configs.flow_logger import main_logger
    received_table = input_schema.ReceivedTable(
        file_type='parquet', name='table.parquet',
        path=str(find_parent_directory("Flowfile")/'flowfile_core/tests/support_files/data/table.parquet'))
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received_table)
    graph.add_read(node_read)
    main_logger.warning(str(graph))
    main_logger.warning(OFFLOAD_TO_WORKER)
    add_node_promise_on_type(graph, 'record_count', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node_number_of_records = input_schema.NodeRecordCount(flow_id=1, node_id=2)
    graph.add_record_count(node_number_of_records)
    graph.flow_settings.execution_mode = 'Performance'
    fast = graph.run_graph()
    graph.reset()
    graph.flow_settings.execution_mode = 'Development'
    slow = graph.run_graph()

    assert slow.node_step_result[1].run_time > fast.node_step_result[1].run_time, 'Performance mode should be faster'


def test_adding_graph_solver():
    graph = create_graph()
    input_data = [{'from': 'a', 'to': 'b'}, {'from': 'b', 'to': 'c'}, {'from': 'g', 'to': 'd'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'graph_solver', 2)
    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    graph_solver_input = transform_schema.GraphSolverInput(col_from='from', col_to='to', output_column_name='g')
    graph.add_graph_solver(input_schema.NodeGraphSolver(flow_id=1, node_id=2, graph_solver_input=graph_solver_input))
    graph.run_graph()
    output_data = graph.get_node(2).get_resulting_data()
    input_data = graph.get_node(1).get_resulting_data()
    expected_data = input_data.add_new_values([1, 1, 2], 'g')
    output_data.assert_equal(expected_data)


def test_add_fuzzy_match():
    graph = create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'fuzzy_match', 2)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    data = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'join_input':
        {'join_mapping': [{'left_col': 'name', 'right_col': 'name', 'threshold_score': 75, 'fuzzy_type': 'levenshtein',
                           'valid': True}],
         'left_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'right_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'how': 'inner'}, 'auto_keep_all': True, 'auto_keep_right': True, 'auto_keep_left': True}
    graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(**data))
    run_info = graph.run_graph()
    handle_run_info(run_info)
    output_data = graph.get_node(2).get_resulting_data()
    output_data.to_dict()
    expected_data = FlowDataEngine({
        'name': ['edward', 'eduward', 'courtney', 'edward', 'eduward'],
        'name_right': ['edward', 'edward', 'courtney', 'eduward', 'eduward'],
        'name_vs_name_right_levenshtein': [1.0, 0.8571428571428572, 1.0, 0.8571428571428572, 1.0]}
    )
    output_data.assert_equal(expected_data)


def test_add_fuzzy_match_lcoal():
    graph = create_graph()
    graph.flow_settings.execution_location = "local"
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'fuzzy_match', 2)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    data = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'join_input':
        {'join_mapping': [{'left_col': 'name', 'right_col': 'name', 'threshold_score': 75, 'fuzzy_type': 'levenshtein',
                           'valid': True}],
         'left_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'right_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'how': 'inner'}, 'auto_keep_all': True, 'auto_keep_right': True, 'auto_keep_left': True}
    graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(**data))
    run_info = graph.run_graph()
    handle_run_info(run_info)
    output_data = graph.get_node(2).get_resulting_data()
    expected_data = FlowDataEngine({
        'name': ['edward', 'eduward', 'courtney', 'edward', 'eduward'],
        'name_right': ['edward', 'edward', 'courtney', 'eduward', 'eduward'],
        'name_vs_name_right_levenshtein': [1.0, 0.8571428571428572, 1.0, 0.8571428571428572, 1.0]}
    )
    output_data.assert_equal(expected_data)


def test_add_record_count():
    graph = create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'record_count', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node_number_of_records = input_schema.NodeRecordCount(flow_id=1, node_id=2)
    graph.add_record_count(node_number_of_records)
    run_info = graph.run_graph()
    handle_run_info(run_info)
    expected_data = FlowDataEngine(raw_data=[3], schema=['number_of_records'])
    d = graph.get_node(2).get_resulting_data()
    d.assert_equal(expected_data)


def test_add_read_excel():
    settings = {
        'flow_id': 1,
        'node_id': 1,
        'cache_results': True,
        'pos_x': 234.37272727272727,
        'pos_y': 271.5272727272727,
        'is_setup': True,
        'description': '',
        'received_file': {
            'id': None,
            'name': 'fake_data.xlsx',
            'path': 'flowfile_core/tests/support_files/data/fake_data.xlsx',
            'directory': None,
            'analysis_file_available': False,
            'status': None,
            'file_type': 'excel',
            'fields': [],
            'table_settings': {
                'file_type': 'excel',
                'sheet_name': 'Sheet1',
                'start_row': 0,
                'start_column': 0,
                'end_row': 0,
                'end_column': 0,
                'has_headers': True,
                'type_inference': False
            }
        }
    }
    graph = create_graph()
    add_node_promise_on_type(graph, node_type='read', node_id=1)
    graph.add_read(input_file=input_schema.NodeRead(**settings))

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


def ensure_excel_is_read_from_arrow_object():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': True, 'pos_x': 234.37272727272727,
                'pos_y': 271.5272727272727, 'is_setup': True, 'description': '',
                'received_file': {'id': None, 'name': 'fake_data.xlsx',
                                  'path': 'flowfile_core/tests/support_files/data/fake_data.xlsx',
                                  'directory': None, 'analysis_file_available': False, 'status': None,
                                  'file_type': 'excel', 'fields': [], 'reference': '', 'starting_from_line': 0,
                                  'delimiter': ',', 'has_headers': True, 'encoding': 'utf-8', 'parquet_ref': None,
                                  'row_delimiter': '\n', 'quote_char': '"', 'infer_schema_length': 1000,
                                  'truncate_ragged_lines': False, 'ignore_errors': False, 'sheet_name': 'Sheet1',
                                  'start_row': 0, 'start_column': 0, 'end_row': 0, 'end_column': 0,
                                  'type_inference': False}}
    graph = create_graph()
    add_node_promise_on_type(graph, node_type='read', node_id=1)
    graph.add_read(input_file=input_schema.NodeRead(**settings))
    graph.get_node(1).get_resulting_data()


def test_add_record_id():
    graph = create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'record_id', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)

    node_record_id = input_schema.NodeRecordId(flow_id=1, node_id=2, record_id_input=transform_schema.RecordIdInput())
    graph.add_record_id(node_record_id)
    run_info = graph.run_graph()
    handle_run_info(run_info)
    output_data = graph.get_node(2).get_resulting_data()
    expected_data = FlowDataEngine([{'record_id': 1, 'name': 'eduward'},
                                   {'record_id': 2, 'name': 'edward'},
                                   {'record_id': 3, 'name': 'courtney'}]
                                  )
    output_data.assert_equal(expected_data)


def test_copy_add_record_id():
    graph = create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'record_id', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)

    node_record_id = input_schema.NodeRecordId(flow_id=1, node_id=2, record_id_input=transform_schema.RecordIdInput())
    graph.add_record_id(node_record_id)
    copied_info = input_schema.NodePromise(node_type= 'record_id', node_id=3, flow_id=1)
    node_to_copy = graph.get_node(2)
    graph.copy_node(new_node_settings=copied_info, existing_setting_input=node_to_copy.setting_input, node_type=node_to_copy.node_type)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 3)
    add_connection(graph, connection)
    graph.run_graph()
    output_data = graph.get_node(3).get_resulting_data()
    expected_data = FlowDataEngine([{'record_id': 1, 'name': 'eduward'},
                                   {'record_id': 2, 'name': 'edward'},
                                   {'record_id': 3, 'name': 'courtney'}]
                                  )
    output_data.assert_equal(expected_data)


def test_copy_from_other_flow():
    FLOW_ID_TO_COPY = 1
    NODE_ID_TO_COPY = 1
    NEW_FLOW_ID = 2
    NEW_NODE_ID = 44


    flow_1 = create_graph(FLOW_ID_TO_COPY)

    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(flow_1, data=input_data, node_id=NODE_ID_TO_COPY)
    flow_2 = create_graph(flow_id=NEW_FLOW_ID)
    copied_info = input_schema.NodePromise(node_type= 'manual_input', node_id=NEW_NODE_ID, flow_id=2)
    node_to_copy = flow_1.get_node(NODE_ID_TO_COPY)
    flow_2.copy_node(new_node_settings=copied_info,
                     existing_setting_input=node_to_copy.setting_input,
                     node_type=node_to_copy.node_type)
    copied_node = flow_2.get_node(NEW_NODE_ID)
    assert copied_node is not None, 'Node should be copied'
    assert copied_node.node_id == NEW_NODE_ID, f"Node ID should be {NEW_NODE_ID}"
    assert copied_node.setting_input.flow_id == NEW_FLOW_ID, f"Node flow ID should be {flow_2.flow_id}"
    # Assert that they are not coupled.
    assert flow_2.get_node(NEW_NODE_ID).needs_run(False), 'Node should need to run'
    flow_1.run_graph()
    assert flow_2.get_node(NEW_NODE_ID).needs_run(False), 'Node should still need to run'
    # Assert that the data is the same.
    flow_2.get_node(NEW_NODE_ID).get_resulting_data().assert_equal(node_to_copy.get_resulting_data())


def test_add_and_run_group_by():
    graph = get_group_by_flow()
    predicted_df = graph.get_node(2).get_predicted_resulting_data()
    assert set(predicted_df.columns) == {'groups',
                                         'sales_data_output'}, 'Columns should be groups, Country, sales_data_sum'
    assert {'numeric', 'numeric'} == set(
        p.generic_datatype() for p in predicted_df.schema), 'Data types should be the same'
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_add_and_run_group_by_string():
    graph = create_graph()
    input_data = (FlowDataEngine.create_random(100)
                  .apply_flowfile_formula('to_string(random_int(0, 40))', 'groups')
                  .apply_flowfile_formula('to_string(random_int(0, 10))', 'vals')
                  .select_columns(['groups', 'vals']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'group_by', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    group_by_input = transform_schema.GroupByInput([transform_schema.AggColl('groups', 'groupby'),
                                                    transform_schema.AggColl('vals', 'concat', 'vals_output')])
    node_group_by = input_schema.NodeGroupBy(flow_id=1, node_id=2, groupby_input=group_by_input)
    graph.add_group_by(node_group_by)
    predicted_df = graph.get_node(2).get_predicted_resulting_data()
    assert set(predicted_df.columns) == {'groups', 'vals_output'}, 'Columns should be groups, Country, sales_data_sum'
    assert {'str'} == set(p.generic_datatype() for p in predicted_df.schema), 'Data types should be the same'
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_add_pivot():
    graph = create_graph()
    input_data = (FlowDataEngine.create_random(10000).apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'sales_data']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'pivot', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    pivot_input = transform_schema.PivotInput(pivot_column='groups', value_col='sales_data', index_columns=['Country'],
                                              aggregations=['sum'])
    pivot_settings = input_schema.NodePivot(flow_id=1, node_id=2, pivot_input=pivot_input)
    graph.add_pivot(pivot_settings)
    predicted_df = graph.get_node(2).get_predicted_resulting_data()
    assert set(predicted_df.columns) == {'Country', '0', '3', '2', '1'}, 'Columns should be Country, 0, 3, 2, 1'
    assert {'str', 'numeric', 'numeric', 'numeric', 'numeric'} == set(
        p.generic_datatype() for p in predicted_df.schema), 'Data types should be the same'
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_pivot_schema_callback():
    graph = create_graph()
    input_data = (FlowDataEngine.create_random(10000).apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'sales_data']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'pivot', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    pivot_input = transform_schema.PivotInput(pivot_column='groups', value_col='sales_data', index_columns=['Country'],
                                              aggregations=['sum'])
    pivot_settings = input_schema.NodePivot(flow_id=1, node_id=2, pivot_input=pivot_input)
    graph.add_pivot(pivot_settings)


def test_schema_callback_in_graph():
    pivot_input = transform_schema.PivotInput(index_columns=['Country'], pivot_column='groups',
                                              value_col='sales_data', aggregations=['sum'])

    data = (FlowDataEngine.create_random(10000)
            .apply_flowfile_formula('random_int(0, 4)', 'groups')
            .select_columns(['groups', 'Country', 'Work', 'sales_data']))
    node_input_schema = data.schema
    input_lf = data.data_frame
    result_schema = pre_calculate_pivot_schema(node_input_schema=node_input_schema,
                                               pivot_input=pivot_input,
                                               input_lf=input_lf,)
    result_data = FlowDataEngine.create_from_schema(result_schema)
    expected_schema = [input_schema.MinimalFieldInfo(name="Country", data_type="String"),
                       input_schema.MinimalFieldInfo(name='0', data_type='Float64'),
                       input_schema.MinimalFieldInfo(name='1', data_type='Float64'),
                       input_schema.MinimalFieldInfo(name='2', data_type='Float64'),
                       input_schema.MinimalFieldInfo(name='3', data_type='Float64')]
    expected_data = FlowDataEngine.create_from_schema([FlowfileColumn.create_from_minimal_field_info(mfi)
                                                       for mfi in expected_schema])
    result_data.assert_equal(expected_data)


def test_add_pivot_string_count():
    graph = create_graph()
    input_data = (FlowDataEngine.create_random(10000)
                  .apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'Work']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'pivot', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    pivot_input = transform_schema.PivotInput(pivot_column='groups', value_col='Work', index_columns=['Country'],
                                              aggregations=['count'])
    pivot_settings = input_schema.NodePivot(flow_id=1, node_id=2, pivot_input=pivot_input)
    graph.add_pivot(pivot_settings)
    predicted_df = graph.get_node(2).get_predicted_resulting_data()
    assert set(predicted_df.columns) == {'Country', '0', '3', '2',
                                         '1'}, 'Columns should be Country, 0, 3, 2, 1'
    assert {'str', 'numeric', 'numeric', 'numeric', 'numeric'} == set(
        p.generic_datatype() for p in predicted_df.schema), 'Data types should be the same'
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_add_pivot_string_concat():
    graph = create_graph()
    input_data = (FlowDataEngine.create_random(10000)
                  .apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'Work']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'pivot', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    pivot_input = transform_schema.PivotInput(pivot_column='groups', value_col='Work', index_columns=['Country'],
                                              aggregations=['concat'])
    pivot_settings = input_schema.NodePivot(flow_id=1, node_id=2, pivot_input=pivot_input)
    graph.add_pivot(pivot_settings)
    predicted_df = graph.get_node(2).get_predicted_resulting_data()
    assert set(predicted_df.columns) == {'Country', '0', '3', '2',
                                         '1'}, 'Columns should be Country, 0, 3, 2, 1'
    assert {'str'} == set(p.generic_datatype() for p in predicted_df.schema), 'Data types should be the same'
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_try_add_to_big_pivot():
    graph = create_graph()
    graph.execution_mode = 'Performance'
    input_data = (FlowDataEngine.create_random(10000)
                  .add_record_id(record_id_settings=transform_schema.RecordIdInput(output_column_name='groups'))
                  .select_columns(['groups', 'Country', 'sales_data']))
    add_manual_input(graph, data=input_data.to_pylist())
    add_node_promise_on_type(graph, 'pivot', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    pivot_input = transform_schema.PivotInput(pivot_column='groups', value_col='sales_data', index_columns=['Country'],
                                              aggregations=['sum'])
    pivot_settings = input_schema.NodePivot(flow_id=1, node_id=2, pivot_input=pivot_input)
    graph.add_pivot(pivot_settings)
    predicted_df = graph.get_node(2).get_predicted_resulting_data()
    expected_columns = ['Country'] + [f'{i + 1}' for i in range(200)]
    assert set(predicted_df.columns) == set(expected_columns), 'Should not have calculated the columns'
    run_info = graph.run_graph()
    handle_run_info(run_info)
    error_line = None
    with open(graph.flow_logger.log_file_path, 'r') as file:
        for line in file:
            if "WARNING" in line and "Pivot column has too many unique values" in line and 'Node ID: 2' in line:
                error_line = line
    if error_line is None:
        raise ValueError('There should be a warning')


def test_add_cross_join():
    graph = create_graph()
    input_data = [{'name': 'eduward'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'cross_join', 2)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    try:
        _ = graph.get_node_data(2)
    except:
        raise ValueError('Node data should be available')
    data = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 632.8727272727273, 'pos_y': 298.4,
            'is_setup': True, 'description': '', 'depending_on_ids': [], 'auto_generate_selection': True,
            'verify_integrity': True, 'cross_join_input': {'left_select': {'renames': [
            {'old_name': 'name', 'new_name': 'name', 'keep': True, 'data_type': None, 'data_type_change': False,
             'join_key': False, 'is_altered': False, 'position': None, 'is_available': True}]}, 'right_select': {
            'renames': [{'old_name': 'name', 'new_name': 'right_name', 'keep': True, 'data_type': None,
                         'data_type_change': False, 'join_key': False, 'is_altered': False, 'position': None,
                         'is_available': True}]}}, 'auto_keep_all': True, 'auto_keep_right': True,
            'auto_keep_left': True}
    graph.add_cross_join(input_schema.NodeCrossJoin(**data))
    graph.run_graph()
    output_data = graph.get_node(2).get_resulting_data()
    expected_data = FlowDataEngine([{'name': 'eduward', 'right_name': 'eduward'}]
                                  )
    output_data.assert_equal(expected_data)


def test_read_excel():
    settings = {
        'flow_id': 1,
        'node_id': 1,
        'cache_results': True,
        'pos_x': 234.37272727272727,
        'pos_y': 271.5272727272727,
        'is_setup': True,
        'description': '',
        'received_file': {
            'id': None,
            'name': 'fake_data.xlsx',
            'path': 'flowfile_core/tests/support_files/data/fake_data.xlsx',
            'directory': None,
            'analysis_file_available': False,
            'status': None,
            'file_type': 'excel',
            'fields': [],
            'table_settings': {
                'file_type': 'excel',
                'sheet_name': 'Sheet1',
                'start_row': 0,
                'start_column': 0,
                'end_row': 0,
                'end_column': 0,
                'has_headers': True,
                'type_inference': False
            }
        }
    }
    graph = create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    run_info = graph.run_graph()
    handle_run_info(run_info)
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_read_csv():
    settings = {
        'flow_id': 1,
        'node_id': 1,
        'cache_results': True,
        'pos_x': 304.8727272727273,
        'pos_y': 549.5272727272727,
        'is_setup': True,
        'description': 'Test csv',
        'received_file': {
            'id': None,
            'name': 'fake_data.csv',
            'path': 'flowfile_core/tests/support_files/data/fake_data.csv',
            'directory': None,
            'analysis_file_available': False,
            'status': None,
            'file_type': 'csv',
            'fields': [],
            'table_settings': {
                'file_type': 'csv',
                'reference': '',
                'starting_from_line': 0,
                'delimiter': ',',
                'has_headers': True,
                'encoding': 'utf-8',
                'parquet_ref': None,
                'row_delimiter': '',
                'quote_char': '',
                'infer_schema_length': 20000,
                'truncate_ragged_lines': False,
                'ignore_errors': False
            }
        }
    }
    graph = create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    run_info = graph.run_graph()
    handle_run_info(run_info)
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_read_parquet():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 421.8727272727273,
                'pos_y': 224.52727272727273, 'is_setup': True, 'description': '', 'node_type': 'read',
                'received_file': {'name': 'fake_data.parquet',
                                  'path': 'flowfile_core/tests/support_files/data/fake_data.parquet',
                                  'file_type': 'parquet'}}
    graph = create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    run_info = graph.run_graph()
    handle_run_info(run_info)
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_write_csv():
    settings = {
        'flow_id': 1,
        'node_id': 2,
        'cache_results': False,
        'pos_x': 596.8727272727273,
        'pos_y': 518.3272727272728,
        'is_setup': True,
        'description': '',
        'output_settings': {
            'name': 'output_data.csv',
            'directory': 'flowfile_core/tests/support_files/data',
            'file_type': 'csv',
            'fields': [],
            'write_mode': 'overwrite',
            'table_settings': {
                'file_type': 'csv',
                'delimiter': ',',
                'encoding': 'utf-8',
            },
        },
    }
    graph = create_graph()
    add_manual_input(graph, data=[{'name': 'eduward'}, {'name': 'edward'}, {'name': 'courtney'}])
    add_node_promise_on_type(graph, 'output', 2)
    output_file = input_schema.NodeOutput(**settings)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.add_output(output_file)
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_filter():
    settings = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 864.533596836909,
                'pos_y': 592.7749104575811, 'is_setup': True, 'description': '', 'depending_on_id': -1,
                'filter_input': {'advanced_filter': "[ID] = '50000'",
                                 'basic_filter': {'field': '', 'filter_type': '', 'filter_value': ''},
                                 'filter_type': 'advanced'}}
    graph = create_graph()
    add_manual_input(graph, data=[{'ID': 'eduward'}, {'ID': 'edward'}, {'ID': 'courtney'}])
    add_node_promise_on_type(graph, 'filter', 2)
    filter_settings = input_schema.NodeFilter(**settings)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.add_filter(filter_settings)
    graph.run_graph()


def test_analytics_processor(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, raw_data)
    add_node_promise_on_type(graph, 'explore_data', 2, 1)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node = graph.get_node(2)
    try:
        AnalyticsProcessor.create_graphic_walker_input(node)
    except Exception as e:
        raise ValueError(f'Error in get_graphic_walker_input: {str(e)}')


def test_analytics_processor_after_run(raw_data):
    graph = create_graph()
    graph = add_manual_input(graph, raw_data)
    add_node_promise_on_type(graph, 'explore_data', 2, 1)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node = graph.get_node(2)
    graph.run_graph()
    try:
        AnalyticsProcessor.create_graphic_walker_input(node)
    except Exception as e:
        raise ValueError(f'Error in get_graphic_walker_input: {str(e)}')


def test_text_to_rows():
    handler = create_flowfile_handler()
    graph_id = handler.import_flow(Path("flowfile_core/tests/support_files/flows/text_to_rows.flowfile"))
    graph = handler.get_flow(graph_id)
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_polars_code():
    graph = create_graph()
    file_path = str(find_parent_directory("Flowfile") / "flowfile_core/tests/support_files/data/fake_data.parquet")
    # Add read node with test data
    read_node = input_schema.NodeRead(
        flow_id=graph.flow_id,
        node_id=1,
        received_file=input_schema.ReceivedTable.create_from_path(
            file_path
            ,
            file_type='parquet'
        )
    )
    graph.add_read(read_node)

    # Add polars code node
    polars_node = input_schema.NodePolarsCode(
        flow_id=graph.flow_id,
        node_id=2,
        polars_code_input=transform_schema.PolarsCodeInput(
            polars_code='output_df = input_df.with_columns(pl.col("Email").str.to_uppercase())'
        ),
        depending_on_ids=[1]
    )
    graph.add_polars_code(polars_node)

    # Connect nodes
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    # Run and verify
    run_info = graph.run_graph()
    handle_run_info(run_info)

    # Verify the transformation worked
    result = graph.get_node(2).get_resulting_data()
    assert result is not None
    # Check that Email column is uppercase
    emails = result.to_dict()["Email"]
    assert all(email == email.upper() for email in emails if email)


def get_join_data(how: str = 'inner'):
    return {'flow_id': 1, 'node_id': 3, 'cache_results': False, 'pos_x': 788.8727272727273, 'pos_y': 186.4,
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


def test_add_join():
    graph = create_graph()
    # graph.flow_settings.execution_mode = 'Performance'
    left_data = [{"name": "eduward"},
                 {"name": "edward"},
                 {"name": "courtney"}]
    add_manual_input(graph, data=left_data)
    right_data = left_data[:1]
    add_manual_input(graph, data=right_data, node_id=2)
    add_node_promise_on_type(graph, 'join', 3)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 3)
    right_connection = input_schema.NodeConnection.create_from_simple_input(2, 3)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    data = get_join_data(how='inner')
    graph.add_join(input_schema.NodeJoin(**data))
    run_info = graph.run_graph()
    handle_run_info(run_info)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database reader cannot be tested")
def test_add_database_reader():

    ensure_password_is_available()
    graph = create_graph()
    add_node_promise_on_type(graph, 'database_reader', 1)
    database_connection = input_schema.DatabaseConnection(database_type='postgresql',
                                                          username='testuser',
                                                          password_ref='test_database_pw',
                                                          host='localhost',
                                                          port=5433,
                                                          database='testdb')
    database_settings = input_schema.DatabaseSettings(database_connection=database_connection,
                                                      schema_name='public', table_name='movies')
    node_database_reader = input_schema.NodeDatabaseReader(database_settings=database_settings, node_id=1,
                                                           flow_id=1,
                                                           user_id=1)
    graph.add_database_reader(node_database_reader)
    node = graph.get_node(1)
    assert node.name == 'database_reader', 'Node name should be database_reader'
    predicted_schema = node.get_predicted_schema()
    assert len(predicted_schema) == 20, 'Expected 20 columns in the schema'
    predicted_lf = node.get_predicted_resulting_data()
    assert len(predicted_lf.collect()) == 0, 'Should be able to predict data frame without actually getting any data'
    run_info = graph.run_graph()
    assert run_info.success, 'Run should be successful'
    lf = node.get_resulting_data()
    assert lf.count() > 0, 'Should be able to get data frame after running'



@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database reader cannot be tested")
def test_add_database_reader_from_stored_database():
    ensure_password_is_available()
    graph = create_graph()
    add_node_promise_on_type(graph, 'database_reader', 1)
    # Ensure the database connection is stored
    database_connection = input_schema.FullDatabaseConnection(database_type='postgresql',
                                                              username='testuser',
                                                              password_ref='test_database_pw',
                                                              host='localhost',
                                                              port=5433,
                                                              database='testdb',
                                                              password='testpass',
                                                              connection_name="database_test_connection")

    database_settings = input_schema.DatabaseSettings(database_connection_name='database_test_connection',
                                                      schema_name='public', table_name='movies',
                                                      connection_mode='reference')
    db_connection = get_local_database_connection('database_test_connection', 1)
    if db_connection is None:
        with get_db_context() as db:
            store_database_connection(db, connection=database_connection, user_id=1)
    # End of ensuring the database connection is stored

    node_database_reader = input_schema.NodeDatabaseReader(database_settings=database_settings, node_id=1,
                                                           flow_id=1,
                                                           user_id=1)
    graph.add_database_reader(node_database_reader)
    node = graph.get_node(1)
    assert node.name == 'database_reader', 'Node name should be database_reader'
    predicted_schema = node.get_predicted_schema()
    assert len(predicted_schema) == 20, 'Expected 20 columns in the schema'
    predicted_lf = node.get_predicted_resulting_data()
    assert len(predicted_lf.collect()) == 0, 'Should be able to predict data frame without actually getting any data'
    run_info = graph.run_graph()
    assert run_info.success, 'Run should be successful'
    lf = node.get_resulting_data()
    assert lf.count() > 0, 'Should be able to get data frame after running'


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database reader cannot be tested")
def test_add_database_writer():
    ensure_password_is_available()
    graph = create_graph()
    add_manual_input(graph, data=[{'name': 'eduward'}, {'name': 'edward'}, {'name': 'courtney'}])
    add_node_promise_on_type(graph, 'database_writer', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    # Ensure the database connection is stored
    database_connection = input_schema.FullDatabaseConnection(database_type='postgresql',
                                                              username='testuser',
                                                              password_ref='test_database_pw',
                                                              host='localhost',
                                                              port=5433,
                                                              database='testdb',
                                                              password='testpass',
                                                              connection_name="database_test_connection")
    db_connection = get_local_database_connection('database_test_connection', 1)
    if db_connection is None:
        with get_db_context() as db:
            store_database_connection(db, connection=database_connection, user_id=1)

    database_write_settings = input_schema.DatabaseWriteSettings(database_connection_name='database_test_connection',
                                                           schema_name='public', table_name='test_table',
                                                           connection_mode='reference',
                                                           if_exists='replace'
                                                           )

    node_database_writer = input_schema.NodeDatabaseWriter(database_write_settings=database_write_settings, node_id=2,
                                                           flow_id=1,
                                                           user_id=1)
    graph.add_database_writer(node_database_writer)
    node = graph.get_node(2)
    assert node.name == 'database_writer', 'Node name should be database_reader'
    run_info = graph.run_graph()
    assert run_info.success, 'Run should be successful'
    lf = node.get_resulting_data()
    assert lf.count() > 0, 'Should be able to get data frame after running'


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database reader cannot be tested")
def test_add_database_no_schema_writer():
    ensure_password_is_available()
    graph = create_graph()
    add_manual_input(graph, data=[{'name': 'eduward'}, {'name': 'edward'}, {'name': 'courtney'}])
    add_node_promise_on_type(graph, 'database_writer', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    # Ensure the database connection is stored
    database_connection = input_schema.FullDatabaseConnection(database_type='postgresql',
                                                              username='testuser',
                                                              password_ref='test_database_pw',
                                                              host='localhost',
                                                              port=5433,
                                                              database='testdb',
                                                              password='testpass',
                                                              connection_name="database_test_connection")
    db_connection = get_local_database_connection('database_test_connection', 1)
    if db_connection is None:
        with get_db_context() as db:
            store_database_connection(db, connection=database_connection, user_id=1)

    database_write_settings = input_schema.DatabaseWriteSettings(database_connection_name='database_test_connection',
                                                                 table_name='test_table',
                                                                 connection_mode='reference', if_exists='replace'
                                                                 )

    node_database_writer = input_schema.NodeDatabaseWriter(database_write_settings=database_write_settings, node_id=2,
                                                           flow_id=1,
                                                           user_id=1)
    graph.add_database_writer(node_database_writer)
    node = graph.get_node(2)
    assert node.name == 'database_writer', 'Node name should be database_reader'
    _ = node.schema
    assert node.schema == graph.get_node(1).schema, 'Schema should be the same as the input'
    run_info = graph.run_graph()
    assert run_info.success, 'Run should be successful'
    lf = node.get_resulting_data()
    assert lf.count() > 0, 'Should be able to get data frame after running'


def test_empty_manual_input_should_run():
    graph = get_group_by_flow() # create a random graph with working stuff in it
    r = graph.run_graph()
    if not r.success:
        raise 'Cannot start the test'
    add_node_promise_for_manual_input(graph, node_id=44)
    try:
        r = graph.run_graph()
        assert r.success, 'Should be able to run even with empty manual input'
    except:
        raise ValueError('Should be able to run empty manual input')


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so cloud reader cannot be tested")
def test_cloud_reader(flow_logger):
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()
    read_settings = cloud_ss.CloudStorageReadSettings(
        resource_path="s3://test-bucket/single-file-parquet/data.parquet",
        file_format="parquet",
        scan_mode="single_file",
        connection_name=conn.connection_name
    )
    graph = create_graph()
    node_settings = input_schema.NodeCloudStorageReader(flow_id=graph.flow_id, node_id=1, user_id=1,
                                                        cloud_storage_settings=read_settings)
    graph.add_cloud_storage_reader(node_settings)
    assert graph.get_node(1) is not None, 'Node should be added to the graph'
    node = graph.get_node(1)
    try:
        node.execute_remote(node_logger=flow_logger.get_node_logger(1))
    except Exception as e:
        flow_logger.error(f"Error executing cloud storage read node: {str(e)}")
        raise ValueError(f"Error executing cloud storage read node: {str(e)}")
    assert not node.needs_run(False), 'Node should not need to run after execution'
    assert node.get_resulting_data().number_of_records == 100_000, 'Should have read 100000 records from the cloud storage'
    assert len(node.schema) == 4, 'Should have 4 columns in the schema'


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so cloud writer cannot be tested")
def test_schema_callback_cloud_read(flow_logger):
    # Validate that when the node is added, the schema is being calculated in a separate thread, so that the user has
    # the least amount of waiting time.

    conn = ensure_cloud_storage_connection_is_available_and_get_connection()  # Just store it so you can
    read_settings = cloud_ss.CloudStorageReadSettings(
        resource_path="s3://test-bucket/single-file-parquet/data.parquet",
        file_format="parquet",
        scan_mode="single_file",
        connection_name=conn.connection_name
    )
    graph = create_graph()
    node_settings = input_schema.NodeCloudStorageReader(flow_id=graph.flow_id, node_id=1, user_id=1,
                                                        cloud_storage_settings=read_settings)
    graph.add_cloud_storage_reader(node_settings)
    node = graph.get_node(1)
    assert node.schema_callback._future is not None, 'Schema callback future should be set'
    assert len(node.schema_callback()) == 4, 'Schema should have 4 columns'
    original_schema_callback = id(node.schema_callback)
    graph.add_cloud_storage_reader(node_settings)
    new_schema_callback = id(node.schema_callback)
    assert new_schema_callback == original_schema_callback, 'Schema callback future should not be set again'
    node.get_table_example(True)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so cloud writer cannot be tested")
def test_add_cloud_writer(flow_logger):
    conn = ensure_cloud_storage_connection_is_available_and_get_connection()  # Just store it so you can
    read_settings = cloud_ss.CloudStorageWriteSettings(
        resource_path="s3://flowfile-test/flow_graph_data.parquet",
        file_format="parquet",
        connection_name=conn.connection_name
    )
    graph = create_graph()
    add_manual_input(graph, data=[{'name': 'eduward', 'city': "a"},
                                  {'name': 'edward', 'city': "a"},
                                  {'name': 'courtney', 'city': "a"}])
    node_settings = input_schema.NodeCloudStorageWriter(flow_id=graph.flow_id, node_id=2, user_id=1,
                                                        cloud_storage_settings=read_settings,)
    graph.add_cloud_storage_writer(node_settings)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node = graph.get_node(2)
    original_method = node._predicted_data_getter

    call_count = {'count': 0}
    def tracking_method(*args, **kwargs):
        call_count['count'] += 1
        return original_method(*args, **kwargs)

    node._predicted_data_getter = tracking_method

    predicted_schema = node.schema
    assert len(predicted_schema) == 2, 'Should have 2 columns in the schema'
    assert call_count['count'] == 0, 'Predicted data getter should not be called when getting schema'

    result = graph.run_graph()
    handle_run_info(result)


@pytest.mark.skipif(not is_docker_available(), reason="Docker is not available or not running so database reader cannot be tested")
def test_complex_cloud_write_scenario():
    ensure_cloud_storage_connection_is_available_and_get_connection()
    handler = FlowfileHandler()
    flow_id = handler.import_flow(find_parent_directory("Flowfile") / "flowfile_core/tests/support_files/flows/test_cloud_local.flowfile")
    graph = handler.get_flow(flow_id)
    node = graph.get_node(3)
    example_data = node.get_table_example(True)
    assert example_data.number_of_columns == 4
    run_info = graph.run_graph()
    handle_run_info(run_info)


def test_no_re_calculate_example_data_after_change_no_run():
    graph = get_dependency_example()
    graph.flow_settings.execution_location = "local"
    graph.run_graph()
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="titlecase([city])"),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=3))
    graph.run_graph()

    first_data = [row["titleCity"] for row in graph.get_node_data(3, True).main_output.data]
    assert len(first_data) > 0, 'Data should be present'
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="lowercase([city])"),
        )
    )
    after_change_data_before_run = [row["titleCity"] for row in graph.get_node_data(3, True).main_output.data]

    assert after_change_data_before_run == first_data, 'Data should be the same after change without run'
    assert not graph.get_node(3).node_stats.has_run_with_current_setup
    assert graph.get_node(3).node_stats.has_completed_last_run
    graph.run_graph()
    assert graph.get_node(3).node_stats.has_run_with_current_setup
    after_change_data_after_run = [row["titleCity"] for row in graph.get_node_data(3, True).main_output.data]

    assert after_change_data_after_run != first_data, 'Data should be different after run'


def test_add_fuzzy_match_only_local():
    graph = create_graph()
    graph.flow_settings.execution_location = "local"
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'fuzzy_match', 2)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    data = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'join_input':
        {'join_mapping': [{'left_col': 'name', 'right_col': 'name', 'threshold_score': 75, 'fuzzy_type': 'levenshtein',
                           'valid': True}],
         'left_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'right_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'how': 'inner'}, 'auto_keep_all': True, 'auto_keep_right': True, 'auto_keep_left': True}
    graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(**data))
    run_info = graph.run_graph()
    handle_run_info(run_info)
    output_data = graph.get_node(2).get_resulting_data()
    expected_data = FlowDataEngine(
        {'name': ['courtney', 'eduward', 'edward', 'eduward', 'edward'],
         'name_right': ['courtney', 'edward', 'edward', 'eduward', 'eduward'],
         'name_vs_name_right_levenshtein': [1.0, 0.8571428571428572, 1.0, 1.0, 0.8571428571428572]}
    )
    output_data.assert_equal(expected_data)


def test_changes_execution_mode(flow_logger):
    settings = {
        'flow_id': 1,
        'node_id': 1,
        'cache_results': True,
        'pos_x': 304.8727272727273,
        'pos_y': 549.5272727272727,
        'is_setup': True,
        'description': 'Test csv',
        'received_file': {
            'id': None,
            'name': 'fake_data.csv',
            'path': 'flowfile_core/tests/support_files/data/fake_data.csv',
            'directory': None,
            'analysis_file_available': False,
            'status': None,
            'file_type': 'csv',
            'fields': [],
            'table_settings': {
                'file_type': 'csv',
                'reference': '',
                'starting_from_line': 0,
                'delimiter': ',',
                'has_headers': True,
                'encoding': 'utf-8',
                'parquet_ref': None,
                'row_delimiter': '',
                'quote_char': '',
                'infer_schema_length': 20000,
                'truncate_ragged_lines': False,
                'ignore_errors': False
            }
        }
    }
    graph = create_graph()
    flow_logger.warning(str(graph))
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    run_info = graph.run_graph()
    handle_run_info(run_info)
    graph.add_select(select_settings=input_schema.NodeSelect(flow_id=1, node_id=2,
                                                             select_input=[transform_schema.SelectInput("City")],
                                                             keep_missing=True))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    explain_node_2 = graph.get_node(2).get_resulting_data().data_frame.explain()
    assert "flowfile_core/tests/support_files/data/fake_data.csv" not in explain_node_2
    graph.execution_location = "local"

    explain_node_2 = graph.get_node(2).get_resulting_data().data_frame.explain()
    # now it should read from the actual source, since we do not cache the data with the external worker

    assert "flowfile_core/tests/support_files/data/fake_data.csv" in explain_node_2


def test_fuzzy_match_schema_predict(flow_logger):
    graph = create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'fuzzy_match', 2)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    data = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'join_input':
        {'join_mapping': [{'left_col': 'name', 'right_col': 'name', 'threshold_score': 75, 'fuzzy_type': 'levenshtein',
                           'valid': True}],
         'left_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'right_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, }]},
         'how': 'inner'}, 'auto_keep_all': True, 'auto_keep_right': True, 'auto_keep_left': True}
    graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(**data))
    node = graph.get_node(2)
    org_func = node._function

    def test_func(*args, **kwargs):
        raise ValueError('This is a test error')
    node._function = test_func
    # enforce to calculate the data based on the schema
    predicted_data = node.get_predicted_resulting_data()
    assert predicted_data.columns == ['name', 'name_right', 'name_vs_name_right_levenshtein']
    input_data = [{'name': 'eduward', 'other_field': 'test'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    sleep(0.1)
    predicted_data = node.get_predicted_resulting_data()  # Gives none because the schema predict is programmed to run only once.
    flow_logger.info("This is the test")
    flow_logger.info(str(len(predicted_data.columns)))
    flow_logger.warning(str(predicted_data.collect()))
    assert len(predicted_data.columns) == 5
    node._function = org_func  # Restore the original function
    result = node.get_resulting_data()
    assert result.columns == predicted_data.columns


def test_fuzzy_match_schema_predict_no_selection(flow_logger):
    graph = create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'fuzzy_match', 2)
    left_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    right_connection.input_connection.connection_class = 'input-1'
    add_connection(graph, left_connection)
    add_connection(graph, right_connection)
    data = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'join_input':
        {'join_mapping': [{'left_col': 'name', 'right_col': 'name', 'threshold_score': 75, 'fuzzy_type': 'levenshtein',
                           'valid': True}],
         'left_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, 'keep': False }]},
         'right_select': {'renames': [{'old_name': 'name', 'new_name': 'name', 'join_key': True, 'keep': False }]},
         'how': 'inner'}, 'auto_keep_all': True, 'auto_keep_right': True, 'auto_keep_left': True}
    graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(**data))
    node = graph.get_node(2)

    predicted_data = node.get_predicted_resulting_data()

    result = node.get_resulting_data()
    assert result.columns == predicted_data.columns


def test_no_data_available_performance_with_cache():

    graph = get_dependency_example()
    graph.flow_settings.execution_location = "remote"
    graph.flow_settings.execution_mode = "Performance"
    graph.run_graph()
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="titlecase([city])"),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=3))

    #  Initial graph to test with

    node = graph.get_node(3)
    first_table_example = node.get_table_example(True)
    assert len(first_table_example.data) == 0, 'Since running in performance mode there is no data expected'
    assert not first_table_example.has_example_data, \
        'Since performance mode does not trigger explicit run of the example data, it should not have example data'
    assert not first_table_example.has_run_with_current_setup, "There should be no run with current setup"

    # Trigger a fetch operation for our data
    data=graph.trigger_fetch_node(3)
    graph.get_run_info()
    after_fetch_table_example = node.get_table_example(True)
    assert len(after_fetch_table_example.data) > 0, "There should be data after fetch operation"
    assert after_fetch_table_example.has_example_data, "There should be example data after fetch operation"
    assert after_fetch_table_example.has_run_with_current_setup, "There should be a run with current setup after fetch operation"
    after_fetch_data = [row["titleCity"] for row in after_fetch_table_example.data]

    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="lowercase([city])"),
        )
    )

    after_change_before_run_table_example = node.get_table_example(True)
    assert len(after_change_before_run_table_example.data) > 0, "There should be data after fetch operation"
    assert after_change_before_run_table_example.has_example_data, "There should be example data after fetch operation"
    assert not after_change_before_run_table_example.has_run_with_current_setup, "After change there should be no run with current setup"
    after_fetch_after_change_data = [row["titleCity"] for row in after_change_before_run_table_example.data]
    assert after_fetch_data == after_fetch_after_change_data, 'Data should be the same after change without run'

    # Validate that the impact of running the graph again
    graph.run_graph()

    after_change_after_run_table_example = node.get_table_example(True)
    assert len(after_change_after_run_table_example.data) == 0, 'Since running in performance mode there is no data expected'
    assert not after_change_after_run_table_example.has_example_data, \
        'Since performance mode does not trigger explicit run of the example data, it should not have example data'
    assert not after_change_after_run_table_example.has_run_with_current_setup, "There should be no run with current setup"

    # Fetch again

    graph.trigger_fetch_node(3)
    after_change_and_fetch_table_example = node.get_table_example(True)
    assert len(after_change_and_fetch_table_example.data) > 0, "There should be data after fetch operation"
    assert after_change_and_fetch_table_example.has_example_data, "There should be example data after fetch operation"
    assert after_change_and_fetch_table_example.has_run_with_current_setup, \
        "There should be a run with current setup after fetch operation"
    after_second_fetch_data = [row["titleCity"] for row in after_change_and_fetch_table_example.data]

    assert after_second_fetch_data != after_fetch_data, 'Data should be different after run'

    # Run again
    graph.run_graph()

    # Fetch again
    graph.trigger_fetch_node(3)


def test_no_data_available_performance_with_cache():

    graph = get_dependency_example()
    graph.flow_settings.execution_location = "local"
    graph.flow_settings.execution_mode = "Performance"
    graph.run_graph()
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="titlecase([city])"),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=3))

    #  Initial graph to test with

    node = graph.get_node(3)
    first_table_example = node.get_table_example(True)
    assert len(first_table_example.data) == 0, 'Since running in performance mode there is no data expected'
    assert not first_table_example.has_example_data, \
        'Since performance mode does not trigger explicit run of the example data, it should not have example data'
    assert not first_table_example.has_run_with_current_setup, "There should be no run with current setup"

    # Trigger a fetch operation for our data
    data=graph.trigger_fetch_node(3)
    graph.get_run_info()
    after_fetch_table_example = node.get_table_example(True)
    assert len(after_fetch_table_example.data) > 0, "There should be data after fetch operation"
    assert after_fetch_table_example.has_example_data, "There should be example data after fetch operation"
    assert after_fetch_table_example.has_run_with_current_setup, "There should be a run with current setup after fetch operation"
    after_fetch_data = [row["titleCity"] for row in after_fetch_table_example.data]

    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="lowercase([city])"),
        )
    )

    after_change_before_run_table_example = node.get_table_example(True)
    assert len(after_change_before_run_table_example.data) > 0, "There should be data after fetch operation"
    assert after_change_before_run_table_example.has_example_data, "There should be example data after fetch operation"
    assert not after_change_before_run_table_example.has_run_with_current_setup, "After change there should be no run with current setup"
    after_fetch_after_change_data = [row["titleCity"] for row in after_change_before_run_table_example.data]
    assert after_fetch_data == after_fetch_after_change_data, 'Data should be the same after change without run'
    # Validate that the impact of running the graph again
    graph.run_graph()
    after_change_after_run_table_example = node.get_table_example(True)
    assert len(after_change_after_run_table_example.data) == 0, 'Since running in performance mode there is no data expected'
    assert not after_change_after_run_table_example.has_example_data, \
        'Since performance mode does not trigger explicit run of the example data, it should not have example data'
    assert not after_change_after_run_table_example.has_run_with_current_setup, "There should be no run with current setup"

    # Fetch again

    graph.trigger_fetch_node(3)
    after_change_and_fetch_table_example = node.get_table_example(True)
    assert len(after_change_and_fetch_table_example.data) > 0, "There should be data after fetch operation"
    assert after_change_and_fetch_table_example.has_example_data, "There should be example data after fetch operation"
    assert after_change_and_fetch_table_example.has_run_with_current_setup, \
        "There should be a run with current setup after fetch operation"
    after_second_fetch_data = [row["titleCity"] for row in after_change_and_fetch_table_example.data]

    assert after_second_fetch_data != after_fetch_data, 'Data should be different after run'

    # Run again
    graph.run_graph()

    # Fetch again
    graph.trigger_fetch_node(3)


def test_fetch_after_run_performance():
    graph = get_dependency_example()
    graph.flow_settings.execution_location = "remote"
    graph.flow_settings.execution_mode = "Performance"
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="titlecase([city])"),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=3))
    graph.run_graph()
    graph.trigger_fetch_node(3)

    node = graph.get_node(3)
    example_data = node.get_table_example(True)
    assert len(example_data.data) > 0, "There should be data after fetch operation"


def test_fetch_before_run_debug():
    """
    Test scenario in which the graph is set to debug mode, and a node is fetched before run, and afterwards,
    without changes to the node, the graph is run. The data should still be available after the run.
    """
    graph = get_dependency_example()
    graph.flow_settings.execution_location = "remote"
    graph.flow_settings.execution_mode = "Development"
    graph.add_formula(
        input_schema.NodeFormula(
            flow_id=1,
            node_id=3,
            function=transform_schema.FunctionInput(transform_schema.FieldInput(name="titleCity"),
                                                    function="titlecase([city])"),
        )
    )
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=3))

    graph.trigger_fetch_node(3)
    node = graph.get_node(3)
    example_data_before_run = node.get_table_example(True)

    assert len(example_data_before_run.data) > 0, "There should be data after fetch operation"
    graph.run_graph()
    example_data_after_run = node.get_table_example(True).data

    assert len(example_data_after_run) > 0, "There should be data after fetch operation"

