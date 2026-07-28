from pathlib import Path

import polars as pl
import pytest

from flowfile_core.flowfile.analytics.analytics_processor import AnalyticsProcessor
from flowfile_core.flowfile.analytics.node_viz import has_result_to_visualize
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas


def find_parent_directory(target_dir_name, start_path=None):
    """Navigate up directories until finding the target directory"""
    current_path = Path(start_path) if start_path else Path.cwd()

    while current_path != current_path.parent:
        if current_path.name == target_dir_name:
            return current_path
        if current_path.name == target_dir_name:
            return current_path
        current_path = current_path.parent

    raise FileNotFoundError(f"Directory '{target_dir_name}' not found")


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
    add_manual_input(graph, data=input_schema.RawData.from_pylist([{'Column 1': 'edward', 'Column 2': 'test'}, {'Column 1': 'eduward', 'Column 2': 'xre'}]), node_id=4)
    add_manual_input(graph, data=input_schema.RawData.from_pylist([{'Column 3': 'edward', 'Column 4': 'test'}, {'Column 3': 'eduward', 'Column 4': 'xre'}]), node_id=5)
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


def test_analytics_ships_no_rows_after_run():
    """Rows never reach the browser: GraphicWalker aggregates on the worker."""
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
    assert graphic_walker_input.dataModel.data == [], "The setup payload must carry no rows"
    assert [f.fid for f in graphic_walker_input.dataModel.fields] == ['groups', 'Country', 'Work']


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
    # A run changes nothing about the setup payload — it is fields + specs either way.
    assert graphic_walker_input.model_dump() == expected_graphic_walker_data_not_run()


def test_analytics_changing_data_processor_existing_specs_run():
    graph = create_big_flow()

    graph.get_node(2).get_predicted_schema()
    graph.flow_settings.execution_mode = "Performance"
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
    assert not output.graphic_walker_input.dataModel.data, 'There should be no data in the graphic walker input'


def test_analytics_no_data_when_not_run_performance():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Performance"
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert not output.graphic_walker_input.dataModel.data, 'There should be no data in the graphic walker input'


def test_analytics_no_data_when_run_development():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Development"
    graph.run_graph()
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert not output.graphic_walker_input.dataModel.data
    assert output.graphic_walker_input.dataModel.fields


def test_analytics_no_data_when_run_performance():
    graph = create_big_flow()
    graph.flow_settings.execution_mode = "Performance"
    graph.run_graph()
    node_step = graph.get_node(3)
    output = AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert not output.graphic_walker_input.dataModel.data
    assert output.graphic_walker_input.dataModel.fields


def test_explore_data_node_is_pass_through():
    """The node no longer samples at run time; it forwards its input unchanged."""
    graph = create_graph()
    graph.flow_settings.execution_mode = "Performance"

    add_node_promise_on_type(graph, 'read', 1, 1)
    received_table = input_schema.ReceivedTable(file_type='parquet', name='table.parquet',
                                                path='flowfile_core/tests/support_files/data/table.parquet')
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, cache_data=False, received_file=received_table)
    graph.add_read(node_read)
    add_node_promise_on_type(graph, 'explore_data', 2, 1)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.run_graph()

    node_step = graph.get_node(2)
    assert has_result_to_visualize(node_step)
    # Full fidelity: every row is still there, where the old path sampled to 10k.
    assert node_step.get_resulting_data().data_frame.select(pl.len()).collect().item() == 1000
    assert node_step.get_resulting_data().columns == graph.get_node(1).get_resulting_data().columns


def test_explore_data_node_is_pass_through_local_execution():
    graph = create_graph()
    graph.flow_settings.execution_location = "local"
    add_node_promise_on_type(graph, 'read', 1, 1)

    received_table = input_schema.ReceivedTable(file_type='parquet', name='table.parquet',
                                                path=str(Path(find_parent_directory('Flowfile'))/'flowfile_core/tests/support_files/data/large_table.parquet'))
    node_read = input_schema.NodeRead(flow_id=1, node_id=1, received_file=received_table)
    graph.add_read(node_read)
    add_node_promise_on_type(graph, 'explore_data', 2, 1)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    node_step = graph.get_node(2)
    graph.run_graph()
    assert has_result_to_visualize(node_step)
    n_rows = node_step.get_resulting_data().data_frame.select(pl.len()).collect().item()
    assert n_rows == graph.get_node(1).get_resulting_data().data_frame.select(pl.len()).collect().item()
    assert n_rows > 10_000, 'The 10k sample cap is gone; the full result must survive the node'


@pytest.mark.parametrize("execution_mode", ["Development", "Performance"])
@pytest.mark.parametrize("execution_location", ["local", "remote"])
def test_readiness_signal_holds_in_every_execution_mode(execution_mode, execution_location):
    """The 422 gate must clear after a run in all four mode/location combinations.

    `node_stats.has_completed_last_run` does not: it is skipped in performance
    mode, so a Performance + local run would leave the explorer permanently
    reporting "not run".
    """
    graph = create_graph()
    graph.flow_settings.execution_mode = execution_mode
    graph.flow_settings.execution_location = execution_location
    add_manual_input(graph, data=FlowDataEngine.create_random(50).to_raw_data())
    add_node_promise_on_type(graph, 'explore_data', 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    node_step = graph.get_node(2)
    assert not has_result_to_visualize(node_step), 'Must report not-run before the flow runs'

    # Opening the drawer on an un-run node must not flip the gate by itself.
    AnalyticsProcessor.process_graphic_walker_input(node_step)
    assert not has_result_to_visualize(node_step), 'The setup route must not count as a run'

    graph.run_graph()
    assert has_result_to_visualize(graph.get_node(2)), (
        f'{execution_mode}/{execution_location} left the explorer gated after a successful run'
    )


def _worker_cache_files(flow) -> set:
    """Cache files for this flow. The dir is shared across runs, so compare deltas."""
    from shared.storage_config import storage
    return set((storage.cache_directory / str(flow.flow_id)).glob("*.arrow"))


def test_fetch_in_performance_mode_stores_nothing():
    """The Explore drawer's fetch must not materialise the node.

    The default (preview) fetch stores the whole result so the 100-row example
    grid has something to read. The chart builder charts through the worker and
    never reads those rows, so it pays that cost for nothing — on a 294 MB
    Parquet source the store was 4.9 GB.
    """
    graph = create_graph()
    graph.flow_settings.execution_location = "remote"
    add_manual_input(graph, data=FlowDataEngine.create_random(50).to_raw_data())
    add_node_promise_on_type(graph, 'explore_data', 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    before = _worker_cache_files(graph)
    graph.trigger_fetch_node(2, performance_mode=True, reset_cache=False)

    node = graph.get_node(2)
    assert has_result_to_visualize(node), 'the plan must still be built, so the drawer opens'
    assert _worker_cache_files(graph) - before == set(), 'a performance-mode fetch must write no IPC'


def test_default_fetch_still_stores_for_the_preview_grid():
    """Regression guard: the data-preview contract is unchanged."""
    graph = create_graph()
    graph.flow_settings.execution_location = "remote"
    add_manual_input(graph, data=FlowDataEngine.create_random(50).to_raw_data())
    add_node_promise_on_type(graph, 'explore_data', 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))

    before = _worker_cache_files(graph)
    graph.trigger_fetch_node(2)

    node = graph.get_node(2)
    assert node.results.example_data_generator is not None, 'the preview grid needs example rows'
    assert _worker_cache_files(graph) - before, 'the default fetch must still store the result'
