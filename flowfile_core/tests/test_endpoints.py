from flowfile_core.routes import *


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
    settings = {'flow_id': 1, 'node_id': 2, 'pos_x': 668, 'pos_y': 450, 'polars_code_input': {'polars_code': '# Add your polars code here\ninput_df.select(pl.col("name"))'}, 'cache_results': False, 'is_setup': True}
    add_generic_settings(settings, 'polars_code')