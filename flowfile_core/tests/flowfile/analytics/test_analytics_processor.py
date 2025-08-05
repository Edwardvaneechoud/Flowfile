from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor

from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine


def get_starting_gw_node_settings() -> input_schema.NodeExploreData:
    node_settings = {
        "flow_id": 1,
        "node_id": 2,
        "cache_results": False,
        "pos_x": 537.8727272727273,
        "pos_y": 584,
        "is_setup": True,
        "description": "",
        "user_id": None,
        "graphic_walker_input": {
            "dataModel": {"data": [], "fields": []},
            "is_initial": True,
            "specList": [
                {
                    "config": {
                        "defaultAggregated": True,
                        "geoms": ["auto"],
                        "coordSystem": "generic",
                        "limit": -1,
                    },
                    "encodings": {
                        "dimensions": [
                            {
                                "fid": "Column 1",
                                "name": "Column 1",
                                "basename": "Column 1",
                                "semanticType": "nominal",
                                "analyticType": "dimension",
                                "offset": None,
                            },
                            {
                                "fid": "gw_mea_key_fid",
                                "name": "Measure names",
                                "analyticType": "dimension",
                                "semanticType": "nominal",
                            },
                        ],
                        "measures": [
                            {
                                "fid": "gw_count_fid",
                                "name": "Row count",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                                "computed": True,
                                "expression": {
                                    "op": "one",
                                    "params": [],
                                    "as": "gw_count_fid",
                                },
                            },
                            {
                                "fid": "gw_mea_val_fid",
                                "name": "Measure values",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                            },
                        ],
                        "rows": [
                            {
                                "fid": "Column 1",
                                "name": "Column 1",
                                "basename": "Column 1",
                                "semanticType": "nominal",
                                "analyticType": "dimension",
                                "offset": None,
                            }
                        ],
                        "columns": [
                            {
                                "fid": "gw_count_fid",
                                "name": "Row count",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                                "computed": True,
                                "expression": {
                                    "op": "one",
                                    "params": [],
                                    "as": "gw_count_fid",
                                },
                            }
                        ],
                        "color": [],
                        "opacity": [],
                        "size": [],
                        "shape": [],
                        "radius": [],
                        "theta": [],
                        "longitude": [],
                        "latitude": [],
                        "geoId": [],
                        "details": [],
                        "filters": [],
                        "text": [],
                    },
                    "layout": {
                        "showActions": False,
                        "showTableSummary": False,
                        "stack": "stack",
                        "interactiveScale": False,
                        "zeroScale": True,
                        "size": {"mode": "auto", "width": 320, "height": 200},
                        "format": {},
                        "geoKey": "name",
                        "resolve": {
                            "x": False,
                            "y": False,
                            "color": False,
                            "opacity": False,
                            "shape": False,
                            "size": False,
                        },
                    },
                    "visId": "gw_f7_M",
                    "name": "Chart 1",
                }
            ],
        },
    }
    return input_schema.NodeExploreData.model_validate(node_settings)


def get_initiated_gw_node_settings() -> input_schema.NodeExploreData:
    node_settings = {
        "flow_id": 1,
        "node_id": 3,
        "cache_results": True,
        "pos_x": 855,
        "pos_y": 281,
        "is_setup": True,
        "description": "",
        "user_id": 1,
        "graphic_walker_input": {
            "dataModel": {"data": [], "fields": []},
            "is_initial": True,
            "specList": [
                {
                    "config": {
                        "defaultAggregated": True,
                        "geoms": ["auto"],
                        "coordSystem": "generic",
                        "limit": -1,
                    },
                    "encodings": {
                        "dimensions": [
                            {
                                "fid": "Column 1",
                                "name": "Column 1",
                                "basename": "Column 1",
                                "semanticType": "nominal",
                                "analyticType": "dimension",
                                "offset": None,
                            },
                            {
                                "fid": "gw_mea_key_fid",
                                "name": "Measure names",
                                "analyticType": "dimension",
                                "semanticType": "nominal",
                            },
                        ],
                        "measures": [
                            {
                                "fid": "Column 2",
                                "name": "Column 2",
                                "basename": "Column 2",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                                "offset": None,
                            },
                            {
                                "fid": "gw_count_fid",
                                "name": "Row count",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                                "computed": True,
                                "expression": {
                                    "op": "one",
                                    "params": [],
                                    "as": "gw_count_fid",
                                },
                            },
                            {
                                "fid": "gw_mea_val_fid",
                                "name": "Measure values",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                            },
                        ],
                        "rows": [
                            {
                                "fid": "Column 1",
                                "name": "Column 1",
                                "basename": "Column 1",
                                "semanticType": "nominal",
                                "analyticType": "dimension",
                                "offset": None,
                            }
                        ],
                        "columns": [
                            {
                                "fid": "Column 2",
                                "name": "Column 2",
                                "basename": "Column 2",
                                "analyticType": "measure",
                                "semanticType": "quantitative",
                                "aggName": "sum",
                                "offset": None,
                            }
                        ],
                        "color": [],
                        "opacity": [],
                        "size": [],
                        "shape": [],
                        "radius": [],
                        "theta": [],
                        "longitude": [],
                        "latitude": [],
                        "geoId": [],
                        "details": [],
                        "filters": [],
                        "text": [],
                    },
                    "layout": {
                        "showActions": False,
                        "showTableSummary": False,
                        "stack": "stack",
                        "interactiveScale": False,
                        "zeroScale": True,
                        "size": {"mode": "auto", "width": 320, "height": 200},
                        "format": {},
                        "geoKey": "name",
                        "resolve": {
                            "x": False,
                            "y": False,
                            "color": False,
                            "opacity": False,
                            "shape": False,
                            "size": False,
                        },
                    },
                    "visId": "gw_rSez",
                    "name": "Chart 1",
                }
            ],
        },
    }
    return input_schema.NodeExploreData.model_validate(node_settings)


def end_gw_node_settings() -> input_schema.NodeExploreData:
    node_settings = {
        'flow_id': 1872601619, 'node_id': 6, 'cache_results': False, 'pos_x': 1223.8727272727272, 'pos_y': 337, 'is_setup': True, 'description': '', 'user_id': None, 'graphic_walker_input': {'dataModel': {'data': [], 'fields': []}, 'is_initial': True, 'specList': [{'config': {'defaultAggregated': True, 'geoms': ['auto'], 'coordSystem': 'generic', 'limit': -1}, 'encodings': {'dimensions': [{'fid': 'Column 1', 'name': 'Column 1', 'basename': 'Column 1', 'semanticType': 'nominal', 'analyticType': 'dimension', 'offset': None}, {'fid': 'date', 'name': 'date', 'basename': 'date', 'semanticType': 'temporal', 'analyticType': 'dimension', 'offset': None}, {'fid': 'boolean_field', 'name': 'boolean_field', 'basename': 'boolean_field', 'semanticType': 'nominal', 'analyticType': 'dimension', 'offset': None}, {'fid': 'gw_mea_key_fid', 'name': 'Measure names', 'analyticType': 'dimension', 'semanticType': 'nominal'}], 'measures': [{'fid': 'Column 2', 'name': 'Column 2', 'basename': 'Column 2', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'offset': None}, {'fid': 'gw_count_fid', 'name': 'Row count', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': True, 'expression': {'op': 'one', 'params': [], 'as': 'gw_count_fid'}}, {'fid': 'gw_mea_val_fid', 'name': 'Measure values', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum'}], 'rows': [{'fid': 'gw_count_fid', 'name': 'Row count', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': True, 'expression': {'op': 'one', 'params': [], 'as': 'gw_count_fid'}}], 'columns': [{'fid': 'date', 'name': 'date', 'basename': 'date', 'semanticType': 'temporal', 'analyticType': 'dimension', 'offset': None}], 'color': [], 'opacity': [], 'size': [], 'shape': [], 'radius': [], 'theta': [], 'longitude': [], 'latitude': [], 'geoId': [], 'details': [], 'filters': [], 'text': []}, 'layout': {'showActions': False, 'showTableSummary': False, 'stack': 'stack', 'interactiveScale': False, 'zeroScale': True, 'size': {'mode': 'auto', 'width': 320, 'height': 200}, 'format': {}, 'geoKey': 'name', 'resolve': {'x': False, 'y': False, 'color': False, 'opacity': False, 'shape': False, 'size': False}}, 'visId': 'gw_L4rL', 'name': 'Chart 1'}]}
    }
    return input_schema.NodeExploreData.model_validate(node_settings)


def create_flowfile_handler():
    handler = FlowfileHandler()
    assert handler._flows == {}, 'Flow should be empty'
    return handler


def create_graph(flow_id: int = 1):
    handler = create_flowfile_handler()
    handler.register_flow(schemas.FlowSettings(flow_id=flow_id, name='new_flow', path='.'))
    graph = handler.get_flow(flow_id)
    return graph


def add_manual_input(graph: FlowGraph, data: input_schema.RawData, node_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=1, node_id=node_id, node_type='manual_input')
    graph.add_node_promise(node_promise)
    input_file = input_schema.NodeManualInput(flow_id=1, node_id=node_id, raw_data_format=data)
    graph.add_manual_input(input_file)
    return graph


def create_big_flow():
    graph = create_graph(1)
    raw_data: input_schema.RawData = input_schema.RawData.from_pylist([{'Column 1': 'edward', 'Column 2': '1'}, {'Column 1': 'eduward', 'Column 2': '2'}])
    input_data_settings = input_schema.NodeManualInput.model_validate({'flow_id': 1, 'node_id': 1, 'cache_results': False, 'pos_x': 494, 'pos_y': 336, 'is_setup': True, 'description': '', 'user_id': 1, 'raw_data_format': raw_data})
    select_settings = input_schema.NodeSelect.model_validate({
        "flow_id": 1872601619,
        "node_id": 2,
        "cache_results": False,
        "pos_x": 670,
        "pos_y": 361,
        "is_setup": True,
        "description": "",
        "user_id": 1,
        "depending_on_id": 3,
        "keep_missing": True,
        "select_input": [
            {
                "old_name": "Column 1",
                "original_position": 0,
                "new_name": "Column 1",
                "data_type": "String",
                "data_type_change": False,
                "join_key": False,
                "is_altered": False,
                "position": 0,
                "is_available": True,
                "keep": True,
            },
            {
                "old_name": "Column 2",
                "original_position": 1,
                "new_name": "Column 2",
                "data_type": "Float64",
                "data_type_change": True,
                "join_key": False,
                "is_altered": True,
                "position": 1,
                "is_available": True,
                "keep": True,
            },
        ],
        "sorted_by": "none",
    })
    field_with_date = input_schema.NodeFormula.model_validate({'flow_id': 1, 'node_id': 7, 'pos_x': 880.8727272727273, 'pos_y': 365.3272727272727, 'function': {'field': {'name': 'date', 'data_type': 'Datetime'}, 'function': 'today()'}, 'cache_results': False, 'depending_on_id': 2, 'is_setup': True})
    field_with_boolean = input_schema.NodeFormula.model_validate({'flow_id': 1, 'node_id': 8, 'cache_results': False, 'pos_x': 1020, 'pos_y': 369, 'is_setup': True, 'description': '', 'user_id': 1, 'depending_on_id': 4, 'function': {'field': {'name': 'boolean_field', 'data_type': 'Boolean'}, 'function': '1==1'}})
    explore_data_settings = get_initiated_gw_node_settings()
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type='manual_input'))
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type='select'))
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=3, node_type='explore_data'))
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=7, node_type='formula'))
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=8, node_type='formula'))
    graph.add_manual_input(input_data_settings)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_select(select_settings)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 7))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(7, 8))
    graph.add_formula(field_with_date)
    graph.add_formula(field_with_boolean)
    graph.add_explore_data(explore_data_settings)
    add_manual_input(graph, data=input_schema.RawData.from_pylist([{'Column 1': 'edward', 'Column 2': 'test'}, {'Column 1': 'eduward', 'Column 2': 'xre'}]), node_id=4)  # add another manual input with different data types
    add_manual_input(graph, data=input_schema.RawData.from_pylist([{'Column 3': 'edward', 'Column 4': 'test'}, {'Column 3': 'eduward', 'Column 4': 'xre'}]), node_id=5)  # add another manual input with different columns
    return graph


def add_node_promise_on_type(graph: FlowGraph, node_type: str, node_id: int, flow_id: int = 1):
    node_promise = input_schema.NodePromise(flow_id=flow_id, node_id=node_id, node_type=node_type)
    if node_type == 'explore_data':
        graph.add_initial_node_analysis(node_promise)
    else:
        graph.add_node_promise(node_promise)


def expected_graphic_walker_data_not_run():
    expected_data = {'dataModel': {'data': [], 'fields': [{'fid': 'Column 1', 'key': 'Column 1', 'name': 'Column 1', 'basename': 'Column 1', 'disable': False, 'semanticType': 'nominal', 'analyticType': 'dimension', 'path': None, 'offset': None}]}, 'is_initial': True, 'specList': [{'config': {'defaultAggregated': True, 'geoms': ['auto'], 'coordSystem': 'generic', 'limit': -1}, 'encodings': {'dimensions': [{'fid': 'Column 1', 'name': 'Column 1', 'basename': 'Column 1', 'semanticType': 'nominal', 'analyticType': 'dimension', 'offset': None}, {'fid': 'gw_mea_key_fid', 'name': 'Measure names', 'analyticType': 'dimension', 'semanticType': 'nominal'}], 'measures': [{'fid': 'gw_count_fid', 'name': 'Row count', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': True, 'expression': {'op': 'one', 'params': [], 'as': 'gw_count_fid'}}, {'fid': 'gw_mea_val_fid', 'name': 'Measure values', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum'}], 'rows': [{'fid': 'Column 1', 'name': 'Column 1', 'basename': 'Column 1', 'semanticType': 'nominal', 'analyticType': 'dimension', 'offset': None}], 'columns': [{'fid': 'gw_count_fid', 'name': 'Row count', 'analyticType': 'measure', 'semanticType': 'quantitative', 'aggName': 'sum', 'computed': True, 'expression': {'op': 'one', 'params': [], 'as': 'gw_count_fid'}}], 'color': [], 'opacity': [], 'size': [], 'shape': [], 'radius': [], 'theta': [], 'longitude': [], 'latitude': [], 'geoId': [], 'details': [], 'filters': [], 'text': []}, 'layout': {'showActions': False, 'showTableSummary': False, 'stack': 'stack', 'interactiveScale': False, 'zeroScale': True, 'size': {'mode': 'auto', 'width': 320, 'height': 200}, 'format': {}, 'geoKey': 'name', 'resolve': {'x': False, 'y': False, 'color': False, 'opacity': False, 'shape': False, 'size': False}}, 'visId': 'gw_f7_M', 'name': 'Chart 1'}]}
    return expected_data


def test_analytics_data_generator_when_run_development_no_settings():
    graph = create_graph()
    graph.flow_settings.execution_mode = "Development"
    input_data = (FlowDataEngine.create_random(1000)
                  .apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'Work']))
    add_manual_input(graph, data=input_data.to_raw_data())
    add_node_promise_on_type(graph, 'explore_data', 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.run_graph()
    node = graph.get_node(2)
    graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node)
    assert len(graphic_walker_input.dataModel.data) == 1_000, "Expected a data length of 1000"


def test_analytics_processor_create_graphic_walker_input_not_run():
    graph = create_graph()
    input_data = (FlowDataEngine.create_random(1000)
                  .apply_flowfile_formula('random_int(0, 4)', 'groups')
                  .select_columns(['groups', 'Country', 'Work']))
    add_manual_input(graph, data=input_data.to_raw_data())
    add_node_promise_on_type(graph, 'explore_data', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    node = graph.get_node(2)
    graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node)
    expected_data = {
        'dataModel':
            {'data': [],
             'fields':
                 [{'fid': 'groups', 'key': 'groups', 'name': 'groups', 'basename': 'groups', 'disable': False, 'semanticType': 'quantitative', 'analyticType': 'measure', 'path': None, 'offset': None},
                  {'fid': 'Country', 'key': 'Country', 'name': 'Country', 'basename': 'Country', 'disable': False, 'semanticType': 'nominal', 'analyticType': 'dimension', 'path': None, 'offset': None},
                  {'fid': 'Work', 'key': 'Work', 'name': 'Work', 'basename': 'Work', 'disable': False, 'semanticType': 'nominal', 'analyticType': 'dimension', 'path': None, 'offset': None}]},
        'is_initial': True, 'specList': None}
    assert graphic_walker_input.model_dump() == expected_data


def test_analytics_processor_existing_specs_not_run():
    graph = create_graph()
    gw_node_settings = get_starting_gw_node_settings()

    add_manual_input(graph, data=input_schema.RawData.from_pylist([{'Column 1': 'edward'}, {'Column 1': 'eduward'}, {'Column 1': 'edward'}]))
    add_node_promise_on_type(graph, 'explore_data', 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.add_explore_data(gw_node_settings)
    node = graph.get_node(2)
    graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node, node.setting_input.graphic_walker_input)
    expected_data = expected_graphic_walker_data_not_run()
    assert graphic_walker_input.model_dump() == expected_data


def test_analytics_processor_existing_specs_run():
    graph = create_graph()
    gw_node_settings = get_starting_gw_node_settings()
    data = input_schema.RawData.from_pylist([{"Column 1": "edward"}, {"Column 1": "eduward"}])
    add_manual_input(
        graph,
        data=data
    )
    add_node_promise_on_type(graph, "explore_data", 2)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.add_explore_data(gw_node_settings)
    node = graph.get_node(2)
    graph.run_graph()
    graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node, node.setting_input.graphic_walker_input)
    expected_data = expected_graphic_walker_data_not_run()
    expected_data['dataModel']['data'] = data.to_pylist()
    assert graphic_walker_input.model_dump() == expected_data


def test_analytics_changing_data_processor_existing_specs_run():
    # In this scenario, we want to reset the data types of the columns in the graphic walker settings
    graph = create_big_flow()
    # We want to run the graph in development mode
    graph.flow_settings.execution_mode = "Performance"
    # Shuffle things up by changing the data type input. Instead of float, we get string as input
    delete_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 3))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(8, 3))

    node_step = graph.get_node(3)
    graphic_walker_input = node_step.setting_input.graphic_walker_input

    graphic_walker_input = AnalyticsProcessor.create_graphic_walker_input(node_step, graphic_walker_input)
    assert len(graphic_walker_input.specList[0]['encodings']['dimensions']) == 4, 'There should be 4 dimensions'
    assert len(graphic_walker_input.specList[0]['encodings']['measures']) == 3
    assert graphic_walker_input.specList[0]['encodings']['dimensions'][3]['fid'] == 'boolean_field', 'The last dimension should be boolean_field'


def test_analytics_no_data_when_not_run_development():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Development"
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert not node_step.results.analysis_data_generator, 'The node should not have to run'
    assert not output.graphic_walker_input.dataModel.data, 'There should be no data in the graphic walker input'


def test_analytics_no_data_when_not_run_performance():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Performance"
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert not node_step.results.analysis_data_generator, 'The node should not have to run'
    assert not output.graphic_walker_input.dataModel.data, 'There should be no data in the graphic walker input'


def test_analytics_data_generator_when_run_development():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Development"
    graph.run_graph()
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert node_step.results.analysis_data_generator, 'The node should have to run'
    assert output.graphic_walker_input.dataModel.data


def test_analytics_data_generator_when_run_performance():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Performance"
    graph.run_graph()
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert node_step.results.analysis_data_generator, 'The node should have to run'
    assert output.graphic_walker_input.dataModel.data


def test_analytics_processor_from_parquet_file_run_performance():
    graph = create_graph()
    graph.flow_settings.execution_mode = "Performance"

    add_node_promise_on_type(graph, 'read', 1, 1)

    received_table = input_schema.ReceivedTable(file_type='parquet', name='table.parquet',
                                                path='flowfile_core/tests/support_files/data/table.parquet')
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received_table)
    graph.add_read(node_read)
    add_node_promise_on_type(graph, 'explore_data', 2, 1)
    connection = input_schema.NodeConnection.create_from_simple_input(1, 2)
    add_connection(graph, connection)
    graph.run_graph()
    node_step = graph.get_node(2)
    assert node_step.results.analysis_data_generator, 'The node should have to run'
    assert node_step.results.analysis_data_generator().__len__() == 1000, 'There should be 1000 rows in the data'
