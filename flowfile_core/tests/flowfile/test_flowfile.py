from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.FlowfileFlow import EtlGraph, add_connection
from flowfile_core.schemas import input_schema, transform_schema, schemas
from typing import List, Dict
from flowfile_core.flowfile.flowfile_table.flowfile_table import FlowfileTable
from flowfile_core.flowfile.analytics.main import AnalyticsProcessor


def add_manual_input(graph: EtlGraph, data: List[Dict], node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id,
                                              raw_data=data)
    graph.add_manual_input(input_file)


def add_node_promise_on_type(graph: EtlGraph, node_type: str, node_id: int, flow_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    graph.add_node_promise(node_promise)
    return graph


def test_create_flowfile_handler():
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def test_import_flow(handler: FlowfileHandler):
    flow_path = "flowfile_core/tests/support_files/flows/airbyte_example.flowfile"
    flow_id = handler.import_flow(flow_path)
    return flow_id


def get_status_of_flow():
    handler = test_create_flowfile_handler()
    flow_id = test_import_flow(handler)
    flow = handler.get_flow(flow_id)


def test_create_graph():
    handler = test_create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=1, name='new_flow', path='.'))
    graph = handler.get_flow(1)
    assert graph.flow_id == 1, 'Flow ID should be 1'
    assert graph.__name__ == 'new_flow', 'Flow name should be new_flow'
    return graph


def test_add_node_promise_for_manual_input(node_type: str = 'manual_input'):
    graph = test_create_graph()
    node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type=node_type)
    graph.add_node_promise(node_promise)
    return graph


def test_add_two_input_nodes():
    graph = test_create_graph()
    add_manual_input(graph, data=[{'name': 'John', 'city': 'New York'}], node_id=1)
    add_manual_input(graph, data=[{'name': 'Jane', 'city': 'Los Angeles'}], node_id=2)
    graph.run_graph()


def test_add_manual_input():
    graph = test_add_node_promise_for_manual_input()
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=1,
                                              raw_data=[{'name': 'John', 'city': 'New York'},
                                                        {'name': 'Jane', 'city': 'Los Angeles'},
                                                        {'name': 'Edward', 'city': 'Chicago'},
                                                        {'name': 'Courtney', 'city': 'Chicago'}])
    graph.add_manual_input(input_file)
    assert len(graph.nodes) == 1, 'There should be 1 node in the graph'
    assert not graph.get_node(1).has_input, 'Node should not have input'
    return graph


def test_get_schema():
    graph = test_add_manual_input()
    schema = graph.get_node(1).get_predicted_schema()
    node = graph.get_node(1)
    columns = [s.column_name for s in node.schema]
    assert len(schema) == 2, 'There should be 2 columns in the schema'
    assert ['name', 'city'] == columns, 'Columns should be name and city'


def test_run_graph():
    graph = test_add_manual_input()
    graph.run_graph()
    node = graph.get_node(1)
    assert node.node_stats.has_run, 'Node should have run'
    assert node.results.resulting_data.collect().to_dicts() == node.setting_input.raw_data, 'Data should be the same'


def test_execute_manual_node_externally():
    graph = test_add_manual_input()
    node = graph.get_node(1)
    node.execute_remote()
    assert node.load_from_cache().collect().to_dicts() == node.setting_input.raw_data, 'Data should be the same'


def test_add_unique():
    graph = test_add_manual_input()
    node_promise = input_schema.NodePromise(flow_id=1, node_id=2, node_type='unique')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeUnique(flow_id=1, node_id=2,
                                         unique_input=transform_schema.UniqueInput(columns=['city'])
                                         )
    graph.add_unique(input_file)
    assert len(graph.nodes) == 2, 'There should be 2 nodes in the graph'
    return graph


def test_connect_node():
    graph = test_add_unique()
    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    assert graph.node_connections == [(1, 2)], 'Node connections should be [(1, 2)]'
    assert graph.get_node(1).leads_to_nodes[0] == graph.get_node(2), 'Node 1 should lead to node 2'
    assert graph.get_node(2).node_inputs.main_inputs[0] == graph.get_node(1), 'Node 2 should have node 1 as input'
    return graph


def test_running_unique():
    graph = test_connect_node()
    graph.run_graph()
    node = graph.get_node(2)
    assert node.node_stats.has_run, 'Node should have run'
    df = node.results.resulting_data.collect()
    assert len(df) == 3, 'There should be 3 rows in the data'
    assert (set(df.select('city').to_series(0).to_list()) ==
            {'New York', 'Los Angeles', 'Chicago'}), 'Cities should be unique'


def test_opening_parquet_file():
    graph = test_add_node_promise_for_manual_input(node_type='read_data')
    received_table = input_schema.ReceivedTable(file_type='parquet', name='parquet_file.parquet',
                                                path='/Users/username/Downloads/artist_match.parquet')
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received_table)
    graph.add_read(node_read)
    self = graph.get_node(1)
    self.execute_remote()


def test_running_performance_mode():
    graph = test_add_node_promise_for_manual_input(node_type='read_data')
    received_table = input_schema.ReceivedTable(file_type='parquet', name='parquet_file.parquet',
                                                path='//tests/data/parquet_file.parquet')
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received_table)
    graph.add_read(node_read)
    add_node_promise_on_type(graph, 'record_count', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node_number_of_records = input_schema.NodeRecordCount(flow_id=1, node_id=2)
    graph.add_record_count(node_number_of_records)
    graph.flow_settings.execution_mode = 'Performance'
    fast = graph.run_graph()
    graph.reset()
    slow = graph.run_graph()
    assert slow.node_step_result[1].run_time > fast.node_step_result[1].run_time, 'Performance mode should be faster'


def test_adding_graph_solver():
    graph = test_create_graph()
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
    graph = test_create_graph()
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
    graph.run_graph()
    output_data = graph.get_node(2).get_resulting_data()
    expected_data = FlowfileTable([{'name': 'eduward', 'fuzzy_score_0': 0.8571428571428572, 'right_name': 'edward'},
                                   {'name': 'edward', 'fuzzy_score_0': 1.0, 'right_name': 'edward'},
                                   {'name': 'eduward', 'fuzzy_score_0': 1.0, 'right_name': 'eduward'},
                                   {'name': 'edward', 'fuzzy_score_0': 0.8571428571428572, 'right_name': 'eduward'},
                                   {'name': 'courtney', 'fuzzy_score_0': 1.0, 'right_name': 'courtney'}]
                                  )
    output_data.assert_equal(expected_data)


def test_add_record_count():
    graph = test_create_graph()
    input_data = [{'name': 'eduward'},
                  {'name': 'edward'},
                  {'name': 'courtney'}]
    add_manual_input(graph, data=input_data)
    add_node_promise_on_type(graph, 'record_count', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node_number_of_records = input_schema.NodeRecordCount(flow_id=1, node_id=2)
    graph.add_record_count(node_number_of_records)
    r = graph.run_graph(performance_mode=True)
    expected_data = FlowfileTable(raw_data=[3], schema=['number_of_records'])
    d = graph.get_node(2).get_resulting_data()
    d.assert_equal(expected_data)


def test_add_read_excel():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': True, 'pos_x': 351.8727272727273,
                'pos_y': 270.5090909090909, 'is_setup': True, 'description': '',
                'received_file': {'id': None, 'name': 'rockstar_data_with_masters.xlsx',
                                  'path': '/Users/username/Downloads/data.xlsx',
                                  'directory': None, 'analysis_file_available': False, 'status': None,
                                  'file_type': 'excel', 'fields': [], 'reference': '', 'starting_from_line': 0,
                                  'delimiter': ',', 'has_headers': True, 'encoding': 'utf-8', 'parquet_ref': None,
                                  'row_delimiter': '\n', 'quote_char': '"', 'infer_schema_length': 1000,
                                  'truncate_ragged_lines': False, 'ignore_errors': False, 'sheet_name': 'Sheet1',
                                  'start_row': 0, 'start_column': 0, 'end_row': 0, 'end_column': 0,
                                  'type_inference': False}}
    graph = test_create_graph()
    add_node_promise_on_type(graph, node_type='read', node_id=1)
    input_schema.NodeRead(**settings)
    graph.add_read()


def test_add_cross_join():
    graph = test_create_graph()
    input_data = [{'Column 1': 'eduward'}]
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
            'is_setup': True, 'description': '', 'depending_on_ids': [-1], 'auto_generate_selection': True,
            'verify_integrity': True, 'cross_join_input': {'left_select': {'renames': [
            {'old_name': 'Column 1', 'new_name': 'Column 1', 'keep': True, 'data_type': None, 'data_type_change': False,
             'join_key': False, 'is_altered': False, 'position': None, 'is_available': True}]}, 'right_select': {
            'renames': [{'old_name': 'Column 1', 'new_name': 'right_Column 1', 'keep': True, 'data_type': None,
                         'data_type_change': False, 'join_key': False, 'is_altered': False, 'position': None,
                         'is_available': True}]}}, 'auto_keep_all': True, 'auto_keep_right': True,
            'auto_keep_left': True}
    graph.add_cross_join(input_schema.NodeCrossJoin(**data))
    graph.run_graph()
    output_data = graph.get_node(2).get_resulting_data()
    expected_data = FlowfileTable([{'name': 'eduward', 'right_name': 'eduward'},
                                   {'name': 'edward', 'right_name': 'eduward'},
                                   {'name': 'courtney', 'right_name': 'eduward'}]
                                  )
    output_data.assert_equal(expected_data)


def test_add_external_source():
    graph = test_create_graph()
    node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type='external_source')
    graph.add_node_promise(node_promise)
    external_source_input = input_schema.NodeExternalSource(
        **{'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 501.8727272727273, 'pos_y': 313.4,
           'is_setup': True, 'description': '', 'node_type': 'external_source',
           'source_settings': {'SAMPLE_USERS': True, 'size': 100, 'orientation': 'row', 'fields': []},
           'identifier': 'sample_users'})
    graph.add_external_source(external_source_input)
    graph.run_graph()


def test_airbyte():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 110.87272727272727, 'pos_y': 298.4,
                'is_setup': True, 'description': '', 'node_type': 'airbyte_reader', 'source_settings': {
            'parsed_config': [{'title': 'Count', 'type': 'integer', 'key': 'count', 'properties': [], 'required': False,
                               'description': 'How many users should be generated in total. The purchases table will be scaled to match, with 10 purchases created per 10 users. This setting does not apply to the products stream.',
                               'isOpen': False, 'airbyte_secret': False, 'input_value': 1000, 'default': 1000},
                              {'title': 'Seed', 'type': 'integer', 'key': 'seed', 'properties': [], 'required': False,
                               'description': 'Manually control the faker random seed to return the same values on subsequent runs (leave -1 for random)',
                               'isOpen': False, 'airbyte_secret': False, 'input_value': -1, 'default': -1},
                              {'title': 'Records Per Stream Slice', 'type': 'integer', 'key': 'records_per_slice',
                               'properties': [], 'required': False,
                               'description': 'How many fake records will be in each page (stream slice), before a state message is emitted?',
                               'isOpen': False, 'airbyte_secret': False, 'input_value': 1000, 'default': 1000},
                              {'title': 'Always Updated', 'type': 'boolean', 'key': 'always_updated', 'properties': [],
                               'required': False,
                               'description': 'Should the updated_at values for every record be new each sync?  Setting this to false will case the source to stop emitting records after COUNT records have been emitted.',
                               'isOpen': False, 'airbyte_secret': False, 'input_value': True, 'default': True},
                              {'title': 'Parallelism', 'type': 'integer', 'key': 'parallelism', 'properties': [],
                               'required': False,
                               'description': 'How many parallel workers should we use to generate fake data?  Choose a value equal to the number of CPUs you will allocate to this source.',
                               'isOpen': False, 'airbyte_secret': False, 'input_value': 4, 'default': 4}],
            'mapped_config_spec': {'count': 1000, 'seed': -1, 'records_per_slice': 1000, 'always_updated': True,
                                   'parallelism': 4}, 'config_mode': 'in_line', 'selected_stream': 'products',
            'source_name': 'faker', 'fields': []}}
    graph = test_create_graph()
    node_promise = input_schema.NodePromise(flow_id=1, node_id=1, node_type='external_source')
    graph.add_node_promise(node_promise)
    external_source_input = input_schema.NodeAirbyteReader(**settings)
    graph.add_external_source(external_source_input)
    fl = graph.get_node(1).get_resulting_data()


def test_read_excel():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': True, 'pos_x': 234.37272727272727,
                'pos_y': 271.5272727272727, 'is_setup': True, 'description': '',
                'received_file': {'id': None, 'name': 'fake_data.xlsx', 'path': 'backend/tests/data/fake_data.xlsx',
                                  'directory': None, 'analysis_file_available': False, 'status': None,
                                  'file_type': 'excel', 'fields': [], 'reference': '', 'starting_from_line': 0,
                                  'delimiter': ',', 'has_headers': True, 'encoding': 'utf-8', 'parquet_ref': None,
                                  'row_delimiter': '\n', 'quote_char': '"', 'infer_schema_length': 1000,
                                  'truncate_ragged_lines': False, 'ignore_errors': False, 'sheet_name': 'Sheet1',
                                  'start_row': 0, 'start_column': 0, 'end_row': 0, 'end_column': 0,
                                  'type_inference': False}}
    graph = test_create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    graph.run_graph()
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_read_csv():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': True, 'pos_x': 304.8727272727273,
                'pos_y': 549.5272727272727, 'is_setup': True, 'description': 'Test csv',
                'received_file': {'id': None, 'name': 'fake_data.csv', 'path': 'backend/tests/data/fake_data.csv',
                                  'directory': None, 'analysis_file_available': False, 'status': None,
                                  'file_type': 'csv', 'fields': [], 'reference': '', 'starting_from_line': 0,
                                  'delimiter': ',', 'has_headers': True, 'encoding': 'utf-8', 'parquet_ref': None,
                                  'row_delimiter': '', 'quote_char': '', 'infer_schema_length': 20000,
                                  'truncate_ragged_lines': False, 'ignore_errors': False, 'sheet_name': None,
                                  'start_row': 0, 'start_column': 0, 'end_row': 0, 'end_column': 0,
                                  'type_inference': False}}
    graph = test_create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    graph.run_graph()
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_read_parquet():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 421.8727272727273,
                'pos_y': 224.52727272727273, 'is_setup': True, 'description': '', 'node_type': 'read',
                'received_file': {'name': 'fake_data.parquet', 'path': 'backend/tests/data/fake_data.parquet',
                                  'file_type': 'parquet'}}
    graph = test_create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    graph.run_graph()
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_read_parquet_external():
    settings = {'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 421.8727272727273,
                'pos_y': 224.52727272727273, 'is_setup': True, 'description': '', 'node_type': 'read',
                'received_file': {'name': 'fake_data.parquet', 'path': 'backend/tests/data/fake_data.parquet',
                                  'file_type': 'parquet'}}
    graph = test_create_graph()
    add_node_promise_on_type(graph, 'read', 1)
    input_file = input_schema.NodeRead(**settings)
    graph.add_read(input_file)
    graph.flow_settings.execution_location = 'remote'
    graph.run_graph()
    assert graph.get_node(1).get_resulting_data().count() == 1000, 'There should be 1000 records'


def test_write_csv():
    settings = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 878.0685745258651,
                'pos_y': 566.9771241413704, 'is_setup': True, 'description': 'write csv',
                'output': {'id': None, 'name': 'output_csv.csv',
                           'path': '/Users//Flowfile/backend/tests/data/output_csv.csv',
                           'directory': '/Users//Flowfile/backend/tests/data',
                           'analysis_file_available': False, 'status': None, 'file_type': 'csv', 'fields': [],
                           'abs_file_path': '/Users//Flowfile/backend/tests/data/output_csv.csv',
                           'delimiter': ',', 'encoding': 'utf-8', 'sheet_name': 'Sheet1', 'write_mode': 'overwrite'}}
    graph = test_create_graph()
    add_manual_input(graph, data=[{'name': 'eduward'}, {'name': 'edward'}, {'name': 'courtney'}])
    add_node_promise_on_type(graph, 'output', 2)
    output_file = input_schema.NodeOutput(**settings)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.add_output(output_file)
    graph.flow_settings.execution_location = 'local'
    graph.run_graph()


def test_filter():
    settings = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 864.533596836909,
                'pos_y': 592.7749104575811, 'is_setup': True, 'description': '', 'depending_on_id': -1,
                'filter_input': {'advanced_filter': "[ID] = '50000'",
                                 'basic_filter': {'field': '', 'filter_type': '', 'filter_value': ''},
                                 'filter_type': 'advanced'}}
    graph = test_create_graph()
    add_manual_input(graph, data=[{'ID': 'eduward'}, {'ID': 'edward'}, {'ID': 'courtney'}])
    add_node_promise_on_type(graph, 'filter', 2)
    filter_settings = input_schema.NodeFilter(**settings)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.add_filter(filter_settings)
    graph.run_graph()


def test_analytics_processor():
    graph = test_add_manual_input()
    add_node_promise_on_type(graph, 'explore_data', 2, 1)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node = graph.get_node(2)
    try:
        AnalyticsProcessor.create_graphic_walker_input(node)
    except Exception as e:
        raise ValueError(f'Error in get_graphic_walker_input: {str(e)}')


def test_analytics_processor_after_run():
    graph = test_add_manual_input()
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
    graph = test_add_manual_input()
    add_node_promise_on_type(graph, 'text_to_rows', 2, 1)
    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    settings = {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 709.8727272727273, 'pos_y': 320.4,
                'is_setup': True, 'description': '', 'node_type': 'text_to_rows',
                'text_to_rows_input': {'column_to_split': 'Column 1', 'output_column_name': '',
                                       'split_by_fixed_value': True, 'split_fixed_value': ',', 'split_by_column': ''}}
    text_to_rows = input_schema.NodeTextToRows(**settings)
    graph.add_text_to_rows(text_to_rows)


def test_polars_code():
    graph = test_add_manual_input()
    add_node_promise_on_type(graph, 'polars_code', 2, 1)
    node_connection = input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=2)
    add_connection(graph, node_connection)
    settings = {'flow_id': 4, 'node_id': 2, 'pos_x': 668, 'pos_y': 450,
                'polars_code_input': {'polars_code': '# Add your polars code here\ninput_df.select(pl.col("name"))'},
                'cache_results': False, 'is_setup': True}
    polars_code = input_schema.NodePolarsCode(**settings)
    graph.add_polars_code(polars_code)
    node = graph.get_node(2)


def get_join_data(how: str = 'inner'):
    return {'flow_id': 1, 'node_id': 3, 'cache_results': False, 'pos_x': 788.8727272727273, 'pos_y': 186.4,
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


def test_add_join():
    graph = test_create_graph()
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
    graph.run_graph()
    graph.get_node(3).get_resulting_data().collect()

