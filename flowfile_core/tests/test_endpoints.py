from flowfile_core.routes import (add_node,
                                  flow_file_handler,
                                  register_flow,
                                  schemas,
                                  input_schema,
                                  connect_node,
                                  add_generic_settings,
                                  get_vue_flow_data,
                                  get_graphic_walker_input,
                                  get_instant_function_result)
import threading
import requests

register_flow(schemas.FlowSettings(flow_id=1, path='./'))


def test_add_node():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    assert len(flow_file_handler._flows) == 1, 'Node not added'


def test_connect_node():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(1, 2, node_type='select', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    connect_node(1, connection)
    assert flow_file_handler.get_node(1, 1).leads_to_nodes[0].node_id == 2, 'Node not connected'


def test_add_big_excel():
    add_node(1, 1, node_type='read', pos_x=0, pos_y=0)
    settings = input_schema.NodeRead(flow_id=1, node_id=1, cache_results=True, pos_x=466.57009116828044,
                                     pos_y=108.7232469702331, is_setup=True,
                                     description='',
                                     received_file=input_schema.ReceivedTable(file_type='excel', id=None,
                                                                              name='big_xlsx.xlsx',
                                                                              path='C:\\Users\\edwar\\Downloads\\big_xlsx.xlsx',
                                                                              directory=None,
                                                                              analysis_file_available=False,
                                                                              status=None,
                                                                              fields=[],
                                                                              abs_file_path='C:\\Users\\edwar\\Downloads\\big_xlsx.xlsx',
                                                                              reference='', starting_from_line=0,
                                                                              delimiter=',',
                                                                              has_headers=True, encoding='utf-8',
                                                                              parquet_ref=None,
                                                                              row_delimiter='\n', quote_char='"',
                                                                              infer_schema_length=1000,
                                                                              truncate_ragged_lines=False,
                                                                              ignore_errors=False,
                                                                              sheet_name='Sheet1', start_row=0,
                                                                              start_column=0, end_row=0,
                                                                              end_column=0,
                                                                              type_inference=False)).__dict__
    add_generic_settings(settings, 'read')
    flow = flow_file_handler.get_flow(1)
    flow.get_node_data(1)


def test_open_flowfile():
    flow_id = flow_file_handler.import_flow('C:/Users//edwar//big_excel_test.flowfile')
    flow = flow_file_handler.get_flow(flow_id)
    node = flow.get_node(1)
    thread = threading.Thread(target=flow.run_graph)
    thread.start()
    flow.cancel()


def test_remove_connection():
    ...


def test_get_flow_data_v2():
    test_connect_node()
    data = get_vue_flow_data(1)


def test_add_node_analysis():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id=1, node_id=2, node_type='explore_data', pos_x=0, pos_y=0)


def test_get_graphic_walker_input():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id=1, node_id=2, node_type='explore_data', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=1,
                                              raw_data=[{'name': 'John', 'city': 'New York'},
                                                        {'name': 'Jane', 'city': 'Los Angeles'},
                                                        {'name': 'Edward', 'city': 'Chicago'},
                                                        {'name': 'Courtney', 'city': 'Chicago'}]).__dict__
    add_generic_settings(input_file, 'manual_input')
    connect_node(1, connection)
    try:
        data = get_graphic_walker_input(1, 2)
    except Exception as e:
        print(e)
        assert False, 'Error in get_graphic_walker_input'


def test_add_generic_settings_polars_code():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id=1, node_id=2, node_type='explore_data', pos_x=0, pos_y=0)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=1,
                                              raw_data=[{'name': 'John', 'city': 'New York'},
                                                        {'name': 'Jane', 'city': 'Los Angeles'},
                                                        {'name': 'Edward', 'city': 'Chicago'},
                                                        {'name': 'Courtney', 'city': 'Chicago'}]).__dict__
    add_generic_settings(input_file, 'manual_input')
    add_node(1, 2, node_type='polars_code', pos_x=0, pos_y=0)
    connect_node(1, connection)
    settings = {'flow_id': 1, 'node_id': 2, 'pos_x': 668, 'pos_y': 450,
                'polars_code_input': {'polars_code': '# Add your polars code here\ninput_df.select(pl.col("name"))'},
                'cache_results': False, 'is_setup': True}
    add_generic_settings(settings, 'polars_code')


async def test_instant_function_result():
    # Setup nodes
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id=1, node_id=2, node_type='formula', pos_x=0, pos_y=0)

    # Create connection
    node_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)

    # Setup input data
    input_file = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data=[
            {'name': 'John', 'city': 'New York'},
            {'name': 'Jane', 'city': 'Los Angeles'},
            {'name': 'Edward', 'city': 'Chicago'},
            {'name': 'Courtney', 'city': 'Chicago'}
        ]
    ).__dict__

    # Add settings and connect nodes
    add_generic_settings(input_file, 'manual_input')
    connect_node(1, node_connection)

    # Await the result
    result = await get_instant_function_result(1, 2, '[name]')
    assert result.success, 'Instant function result failed'


async def test_instant_function_result_fail():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id=1, node_id=2, node_type='formula', pos_x=0, pos_y=0)

    # Create connection
    node_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)

    # Setup input data
    input_file = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data=[
            {'name': 'John', 'city': 'New York'},
            {'name': 'Jane', 'city': 'Los Angeles'},
            {'name': 'Edward', 'city': 'Chicago'},
            {'name': 'Courtney', 'city': 'Chicago'}
        ]
    ).__dict__

    add_generic_settings(input_file, 'manual_input')
    connect_node(1, node_connection)

    result = await get_instant_function_result(1, 2, 'name')
    assert not result.success, 'Instant function result did not fail'


async def test_instant_function_result_after_run():
    add_node(1, 1, node_type='manual_input', pos_x=0, pos_y=0)
    add_node(flow_id=1, node_id=2, node_type='formula', pos_x=0, pos_y=0)

    # Create connection
    node_connection = input_schema.NodeConnection.create_from_simple_input(1, 2)

    # Setup input data
    input_file = input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data=[
            {'name': 'John', 'city': 'New York'},
            {'name': 'Jane', 'city': 'Los Angeles'},
            {'name': 'Edward', 'city': 'Chicago'},
            {'name': 'Courtney', 'city': 'Chicago'}
        ]
    ).__dict__

    add_generic_settings(input_file, 'manual_input')
    connect_node(1, node_connection)
    flow = flow_file_handler.get_flow(1)
    flow.run_graph()
    result = await get_instant_function_result(1, 2, '[name]')
    assert result.success, 'Instant function result failed: ' + result.result


def test_get_stream_data():
    with requests.get("http://0.0.0.0:63578/logs/1", stream=True) as r:
        print('streaming')
        for chunk in r.iter_content(1024):  # or, for line in r.iter_lines():
            print('getting chunk')
            print(chunk)

test_get_stream_data()