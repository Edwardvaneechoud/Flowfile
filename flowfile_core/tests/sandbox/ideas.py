from flowfile_core.flowfile_core.routes import *

def doing_something_random():
    flow_name = '2'
    import_saved_flow(flow_name)
    flow = flow_file_handler.get_flow(1)
    self = flow
    flow.run_graph()
    add_node(1, 3, node_type='explore_data')
    from_ = input_schema.NodeInputConnection(node_id=1, connection_class='output_1')
    to_ = input_schema.NodeOutputConnection(node_id=3, connection_class='input_1')
    node_connection = input_schema.NodeConnection(input_connection=to_, output_connection=from_)
    connect_node(flow_id=1, node_connection=node_connection)
    flow.run_graph()
    input_data = {'flow_id': 1, 'node_id': 3, 'cache_results': False, 'pos_x': 977, 'pos_y': 356.109375, 'is_setup': True, 'description': '', 'graphic_walker_input': {'is_initial': False, 'data_model': {'datasets': [{'id': 'dst-1712958243787', 'name': 'context dataset', 'rawFields': [], 'dsId': 'dse-1712958243787'}], 'dataSources': [{'id': 'dse-1712958243787', 'data': []}], 'specList': [{'visId': 'gw_1tpq', 'name': 'Chart 1', 'encodings': {'dimensions': [{'dragId': 'gw_XR0M', 'fid': 'date', 'name': 'date', 'semanticType': 'temporal', 'analyticType': 'dimension'}], 'measures': [{'dragId': 'gw_count_fid', 'fid': 'gw_count_fid', 'name': 'Row count', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': True, 'expression': {'op': 'one', 'params': [], 'as': 'gw_count_fid'}}], 'rows': [], 'columns': [], 'color': [], 'opacity': [], 'size': [], 'shape': [], 'radius': [], 'theta': [], 'details': [], 'filters': [], 'text': []}, 'config': {'defaultAggregated': True, 'geoms': ['auto'], 'stack': 'stack', 'showActions': False, 'interactiveScale': False, 'sorted': 'none', 'zeroScale': True, 'size': {'mode': 'auto', 'width': 320, 'height': 200}, 'format': {}}}]}}}
    input_data = {'flow_id': 1, 'node_id': 6, 'cache_results': False, 'pos_x': 2327, 'pos_y': 490.5, 'is_setup': True, 'description': '', 'graphic_walker_input': {'has_run': False, 'is_initial': False, 'data_model': {'datasets': [{'id': 'dst-1713211861683', 'name': 'context dataset', 'rawFields': [{'fid': 'song', 'semanticType': 'nominal', 'analyticType': 'dimension'}, {'fid': 'Fuzzy match score', 'semanticType': 'quantitative', 'analyticType': 'measure'}, {'fid': 'Similar song', 'semanticType': 'nominal', 'analyticType': 'dimension'}, {'fid': 'Combination', 'semanticType': 'nominal', 'analyticType': 'dimension'}], 'dsId': 'dse-1713211861683'}], 'dataSources': [{'id': 'dse-1713211861683', 'data': []}], 'specList': [{'visId': 'gw_aLUa', 'name': 'Chart 1', 'encodings': {'dimensions': [{'dragId': 'gw_yQwa', 'fid': 'song', 'name': 'song', 'semanticType': 'nominal', 'analyticType': 'dimension'}, {'dragId': 'gw_UF40', 'fid': 'Similar song', 'name': 'Similar song', 'semanticType': 'nominal', 'analyticType': 'dimension'}, {'dragId': 'gw_K7Xd', 'fid': 'Combination', 'name': 'Combination', 'semanticType': 'nominal', 'analyticType': 'dimension'}], 'measures': [{'dragId': 'gw_-VPG', 'fid': 'Fuzzy match score', 'name': 'Fuzzy match score', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': None, 'expression': None}, {'dragId': 'gw_count_fid', 'fid': 'gw_count_fid', 'name': 'Row count', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': True, 'expression': {'op': 'one', 'params': [], 'as': 'gw_count_fid'}}], 'rows': [{'dragId': 'gw_S3RC', 'fid': 'Similar song', 'name': 'Similar song', 'semanticType': 'nominal', 'analyticType': 'dimension'}], 'columns': [], 'color': [], 'opacity': [], 'size': [], 'shape': [], 'radius': [], 'theta': [], 'details': [], 'filters': [], 'text': []}, 'config': {'defaultAggregated': True, 'geoms': ['auto'], 'stack': 'stack', 'showActions': False, 'interactiveScale': False, 'sorted': 'none', 'zeroScale': True, 'size': {'mode': 'auto', 'width': 320, 'height': 200}, 'format': {}}}]}}}
    node_type = 'explore_data'
    add_generic_settings(input_data=input_data, node_type=node_type)


    id(flow.get_node(1).get_resulting_data().data_frame)
    self = flow
    flow.delete_node(2)
    flow.run_graph()

    add_node(1, 12, 'external_source')
    external_source_input = input_schema.NodeExternalSource(flow_id=1, node_id=12, identifier='sample_users',
                                                            source_settings=input_schema.SampleUsers(fields=None))
    add_generic_settings(external_source_input.dict(), 'external_source')
    add_node(1, 13, 'select')
    connect_node(1, input_schema.NodeConnection(input_connection=input_schema.NodeInputConnection(node_id=13, connection_class='input_1'),
                                                output_connection=input_schema.NodeOutputConnection(node_id=12, connection_class='output_1')))


    flow = flow_file_handler.get_flow(1)
    self = flow.get_node(6)
    fm = flow.get_node(4)
    add_node(1, 2, 'formula')
    connect_node(1, input_schema.NodeConnection(input_connection=input_schema.NodeInputConnection(node_id=2, connection_class='input_1'),
                                                output_connection=input_schema.NodeOutputConnection(node_id=1, connection_class='output_1')))
    add_generic_settings({'flow_id': 1, 'node_id': '2', 'pos_x': 573, 'pos_y': 251.109375, 'cache_input': False, 'function': {'field': {'name': 'test', 'data_type': 'Utf8'}, 'function': '[RAW_SONG]'}, 'depending_on_id': 1, 'cache_results': False, 'is_setup': True},
                         'formula')


    node.get_cache_location()
    schema = node.schema
    flow.run_graph()
    self = flow.get_node(1)
    received_table = node.setting_input.received_file
    flow.get_node(9).setting_input.received_file.type_inference=False
    flow.run_graph()
    node = flow.get_node(3)
    node.schema
    fuzzy_match_input = node.setting_input.join_input
    self, other = [n.results.resulting_data for n in node.node_inputs.get_all_inputs()]

    self.needs_run()
    self.node_stats.has_run

    self.results.resulting_data
    self.results.errors
    fuzzy_match_input = self.setting_input.join_input
    self, other = [v.get_resulting_data() for v in self.all_inputs]
    input_connection = input_schema.NodeConnection(input_connection=input_schema.NodeInputConnection(node_id=3, connection_class='input_1'),
                                                   output_connection=input_schema.NodeOutputConnection(node_id=1, connection_class='output_1'))
    delete_connection(1,input_connection)
    self = flow_file_handler.get_flow(1)
    input_data = {'flow_id': 1, 'node_id': 4, 'cache_results': False, 'pos_x': 913.3999938964844, 'pos_y': 347.59999084472656, 'is_setup': True, 'depending_on_ids': [-1], 'auto_generate_selection': True, 'verify_integrity': True, 'join_input': {'join_mapping': [{'left_col': 'Country', 'right_col': 'Country', 'threshold_score': 50, 'fuzzy_type': 'levenshtein', 'perc_unique': 2}], 'left_select': {'renames': [{'old_name': 'Country', 'new_name': 'Country', 'keep': True, 'data_type': None, 'data_type_change': False, 'join_key': True, 'is_altered': False, 'is_available': True}, {'old_name': 'Units Sold', 'new_name': 'Units Sold', 'keep': True, 'data_type': None, 'data_type_change': False, 'join_key': False, 'is_altered': False, 'is_available': True}, {'old_name': 'Sale Price_max', 'new_name': 'Sale Price_max', 'keep': True, 'data_type': None, 'data_type_change': False, 'join_key': False, 'is_altered': False, 'is_available': True}]}, 'right_select': {'renames': [{'old_name': 'Country', 'new_name': 'right_Country', 'keep': True, 'data_type': None, 'data_type_change': False, 'join_key': True, 'is_altered': False, 'is_available': True}, {'old_name': 'Units Sold', 'new_name': 'right_Units Sold', 'keep': True, 'data_type': None, 'data_type_change': False, 'join_key': False, 'is_altered': False, 'is_available': True}, {'old_name': 'Sale Price_max', 'new_name': 'right_Sale Price_max', 'keep': True, 'data_type': None, 'data_type_change': False, 'join_key': False, 'is_altered': False, 'is_available': True}]}, 'how': 'inner'}, 'auto_keep_all': True, 'auto_keep_right': True, 'auto_keep_left': True}
    add_generic_settings(input_data, node_type='fuzzy_match')

    node_settings = input_schema.NodeGroupBy(**input_data)
    c = input_schema.NodeConnection(input_connection=input_schema.NodeInputConnection(node_id=3, connection_class='input_1'),
                                    output_connection = input_schema.NodeOutputConnection(node_id=1, connection_class='output_1'))

    connect_node(1, c)


    node_type = 'group_by'
    add_generic_settings(input_data, 'group_by')




    del_connection = input_schema.NodeConnection(input_connection=input_schema.NodeInputConnection(node_id=6, connection_class='input_1'),
                                                 output_connection=input_schema.NodeOutputConnection(node_id=2, connection_class='output_1'))
    delete_connection(1, del_connection)
    add_connection = input_schema.NodeConnection(input_connection=input_schema.NodeInputConnection(node_id=6, connection_class='input_1'),
                                                 output_connection=input_schema.NodeOutputConnection(node_id=1, connection_class='output_1'))
    connect_node(1, add_connection)

    input_connection_right = input_schema.NodeConnection(output_connection = input_schema.NodeOutputConnection(node_id=3, connection_class='output_1'),
                                                         input_connection = input_schema.NodeInputConnection(node_id=6, connection_class='input_2'))
    input_connection_main = input_schema.NodeConnection(output_connection = input_schema.NodeOutputConnection(node_id=1, connection_class='output_1'),
                                                        input_connection = input_schema.NodeInputConnection(node_id=6, connection_class='input_1'))
    connect_node(flow_id=1, node_connection=input_connection_right)
    connect_node(flow_id=1, node_connection=input_connection_main)
    setting_input = flow.get_node_data(6).setting_input
    add_generic_settings(input_data, 'join')
    add_generic_settings()

    self = flow.get_node(6)
    self.get_resulting_data()
    left = flow.get_node(1).get_resulting_data()
    right = flow.get_node(3).get_resulting_data()
    left_select = [transform_schema.SelectInput(c) for c in left.columns]
    right_select = [transform_schema.SelectInput(c) for c in right.columns]
    join_input = transform_schema.JoinInput(join_mapping='Column 1', left_select=left_select, right_select=right_select)
    r = left.do_join(join_input,auto_generate_selection=True, verify_integrity=True, other=right)
    input_schema.NodeJoin(flow_id = 1, node_id=4, join_input=join_input)


    df = flow.get_node(2).results.resulting_data.data_frame
    add_generic_settings(input_data={'flow_id': 1, 'node_id': 3, 'cache_results': False, 'pos_x': 872, 'pos_y': 247, 'is_setup': True, 'depending_on_id': 2,
                                     'function': {'field': {'name': 'NewField', 'data_type': 'Int32'}, 'function': '"1"+"1"'}, 'cache_input': False}, node_type='formula')

    flow.run_graph()
    flow.save_flow()

    n = flow.get_node(3)
    #flow_file_handler.register_flow(input_schema.FlowSettings(flow_id=1))
    # flow = flow_file_handler.get_flow(1)
    # flow.save_flow
    # node = flow.get_node(2)
    # select_settings = node.setting_input
    # self = flow.get_node(1).resulting_data
    add_node(1,1,'manual_input')
    add_generic_settings(input_data ={'flow_id': 1, 'node_id': 3, 'cache_results': False, 'pos_x': 904, 'pos_y': 264, 'is_setup': True, 'depending_on_id': 2, 'function': {'field': {'name': 'NewField', 'data_type': 'Utf8'}, 'function': 'contains("edward","ed")'}, 'cache_input': False}, node_type='manual_input')
    flow = flow_file_handler.get_flow(1)

    flow.get_frontend_data()
    flow.run_graph()
    add_generic_settings({'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 428, 'pos_y': 198, 'is_setup': True, 'depending_on_id': 1, 'select_input': [{'old_name': 'Column 1', 'new_name': 't', 'keep': True, 'data_type': 'Int32', 'data_type_change': True, 'join_key': False, 'is_altered': True, 'is_available': True}]},'select')

    add_node(1, 2, 'join')
    add_generic_settings({'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 529, 'pos_y': 196, 'depending_on_id': 1,'select_input': [
            {'old_name': 'Column 1', 'new_name': 'Column 1', 'keep': True, 'data_type': 'Utf8', 'data_type_change': False,'join_key': False, 'is_altered': False, 'is_available': True},
    {'old_name': 'Column 2', 'new_name': 'Surname', 'keep': True, 'data_type': 'Utf8', 'data_type_change': False,'join_key': False, 'is_altered': False, 'is_available': True}]},node_type='select')

    """
    self = flow.get_node(4)
    print(self.hash)
    # change the data input

    input_data = (
        {'flow_id': 1, 'node_id': 2, 'cache_results': False, 'pos_x': 529, 'pos_y': 196, 'depending_on_id': 1,
         'select_input': [
             {'old_name': 'Name', 'new_name': 'Surname', 'keep': True, 'data_type': 'Utf8', 'data_type_change': False,
              'join_key': False, 'is_altered': False, 'is_available': True},
             {'old_name': 'Age', 'new_name': 'Age', 'keep': True, 'data_type': 'Int16', 'data_type_change': True,
              'join_key': False, 'is_altered': True, 'is_available': True},
             {'old_name': 'Nationality', 'new_name': 'Nationality', 'keep': False, 'data_type': 'Utf8',
              'data_type_change': False, 'join_key': False, 'is_altered': False, 'is_available': True},
             {'old_name': 'Column 4', 'new_name': 'Column 4', 'keep': True, 'data_type': 'Utf8',
              'data_type_change': False, 'join_key': False, 'is_altered': False, 'is_available': True},
             {'old_name': 'Column 5', 'new_name': 'ID', 'keep': True, 'data_type': 'Utf8', 'data_type_change': False,
              'join_key': False, 'is_altered': False, 'is_available': True}]}
    )
    add_generic_settings(input_data=input_data,
                         node_type='select')"""
    # self = flow.get_node(3)

    #flow.run_graph()
    #self = flow.get_node(2)


    #setting_input = self.setting_input
    #setting_input.raw_data += setting_input.raw_data
    #self.setting_input = setting_input
    #print(self.function)


# flow_file_handler.create('flow_1')
