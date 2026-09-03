import datetime
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import polars as pl
import polars_gw
import pytest

from flowfile_core.flowfile.analytics.analytics_processor import (
    AnalyticsProcessor,
    validate_spec_list_with_data_model_types,
)
from flowfile_core.flowfile.analytics.graphic_walker import convert_ff_columns_to_gw_fields
from flowfile_core.flowfile.analytics.node_viz import has_result_to_visualize
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from flowfile_core.flowfile.flow_graph import FlowGraph, add_connection, delete_connection
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, schemas
from flowfile_core.schemas.analysis_schemas.graphic_walker_schemas import DataModel


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


def _explore_flow_for_fetch():
    graph = create_graph()
    graph.flow_settings.execution_location = "remote"
    add_manual_input(graph, data=FlowDataEngine.create_random(50).to_raw_data())
    add_node_promise_on_type(graph, 'explore_data', 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    return graph


def _fetch_recording_stores(graph, **fetch_kwargs) -> list:
    """Run a node fetch, recording every attempt to store the node's result.

    Asserting on the attempt rather than on the resulting file keeps this
    independent of whether a worker happens to be running: what changed is
    whether the store is *reached*, and that is what must stay pinned.
    """
    from flowfile_core.flowfile.flow_node import flow_node as flow_node_module

    attempts = []
    real = flow_node_module.ExternalDfFetcher

    def spy(*args, **kwargs):
        attempts.append(kwargs.get("file_ref"))
        return real(*args, **kwargs)

    with patch.object(flow_node_module, "ExternalDfFetcher", side_effect=spy):
        graph.trigger_fetch_node(2, **fetch_kwargs)
    return attempts


def test_fetch_in_performance_mode_stores_nothing():
    """The Explore drawer's fetch must not materialise the node.

    The default (preview) fetch stores the whole result so the 100-row example
    grid has something to read. The chart builder charts through the worker and
    never reads those rows, so it pays that cost for nothing — on a 294 MB
    Parquet source the store was 4.9 GB.
    """
    graph = _explore_flow_for_fetch()

    attempts = _fetch_recording_stores(graph, performance_mode=True, reset_cache=False)

    assert attempts == [], 'a performance-mode fetch must never reach the store'
    assert has_result_to_visualize(graph.get_node(2)), 'the plan must still be built, so the drawer opens'


def test_default_fetch_still_stores_for_the_preview_grid():
    """Regression guard: the data-preview contract is unchanged."""
    graph = _explore_flow_for_fetch()

    attempts = _fetch_recording_stores(graph)

    assert attempts, 'the default fetch must still store the result for the preview grid'


def _spec_field(fid: str, semantic_type: str = 'nominal', analytic_type: str = 'dimension') -> dict:
    return {
        'fid': fid,
        'name': fid,
        'basename': fid,
        'semanticType': semantic_type,
        'analyticType': analytic_type,
        'offset': None,
    }


def _all_encoded_fids(encodings: dict) -> set[str]:
    fids = set()
    for channel in encodings.values():
        for field in channel:
            fids.add(field['fid'])
    return fids


def _find_field(encodings: dict, channel: str, fid: str) -> dict | None:
    return next((f for f in encodings[channel] if f['fid'] == fid), None)


def test_saved_spec_follows_a_changed_upstream_schema():
    """A saved chart spec must track the data: retyped, dropped and added columns.

    The explorer shows `currentVis.encodings`, not the fresh field list, so a
    spec that keeps stale entries is exactly what the user ends up looking at.
    """
    graph = create_graph()
    graph.flow_settings.execution_location = "local"
    gw_node_settings = get_starting_gw_node_settings()
    encodings = gw_node_settings.graphic_walker_input.specList[0]['encodings']
    encodings['dimensions'].insert(1, _spec_field('Old'))
    encodings['color'].append(_spec_field('Old'))

    add_manual_input(
        graph,
        data=input_schema.RawData.from_pylist(
            [{'Column 1': 'edward', 'Old': 'x'}, {'Column 1': 'eduward', 'Old': 'y'}]
        ),
    )
    add_node_promise_on_type(graph, 'explore_data', 2)
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 2))
    graph.add_explore_data(gw_node_settings)
    node = graph.get_node(2)

    graph.run_graph()
    AnalyticsProcessor.process_graphic_walker_input(node)

    graph.add_manual_input(
        input_schema.NodeManualInput(
            flow_id=1,
            node_id=1,
            raw_data_format=input_schema.RawData.from_pylist(
                [{'Column 1': 1, 'New': 'a'}, {'Column 1': 2, 'New': 'b'}]
            ),
        )
    )
    graph.run_graph()
    node_explore_data = AnalyticsProcessor.process_graphic_walker_input(node)
    encodings = node_explore_data.graphic_walker_input.specList[0]['encodings']

    retyped = _find_field(encodings, 'measures', 'Column 1')
    assert retyped is not None, 'Column 1 became an integer, so it must live in measures'
    assert retyped['semanticType'] == 'quantitative'
    assert retyped['analyticType'] == 'measure'
    assert _find_field(encodings, 'dimensions', 'Column 1') is None, 'the stale dimension entry must be gone'

    row_field = _find_field(encodings, 'rows', 'Column 1')
    assert row_field is not None, 'the placed field must survive the retype'
    assert row_field['semanticType'] == 'quantitative'
    assert row_field['analyticType'] == 'measure'

    assert 'Old' not in _all_encoded_fids(encodings), 'a dropped column must vanish from every encoding list'
    assert _find_field(encodings, 'dimensions', 'New') is not None, 'a new string column must show up as a dimension'

    pseudo_fields = {'gw_count_fid', 'gw_mea_key_fid', 'gw_mea_val_fid'}
    assert pseudo_fields <= _all_encoded_fids(encodings), 'GW pseudo-fields must survive reconciliation'


def test_unchanged_data_leaves_the_users_own_choices_alone():
    """Reconciliation runs on every drawer open, so it must not undo GW field-menu edits.

    Moving a numeric field to Dimensions ('to_dim') rewrites only analyticType, and
    setting a semantic type rewrites only semanticType — neither is a schema change.
    """
    spec_list = {
        'encodings': {
            'dimensions': [_spec_field('year', 'quantitative', 'dimension'), _spec_field('gw_mea_key_fid')],
            'measures': [_spec_field('score', 'ordinal', 'measure')],
            'rows': [_spec_field('year', 'quantitative', 'dimension')],
        }
    }
    data_model = DataModel(
        data=[],
        fields=convert_ff_columns_to_gw_fields(
            FlowDataEngine(pl.DataFrame({'year': [2024], 'score': [1]})).schema
        ),
    )

    validate_spec_list_with_data_model_types(spec_list, data_model)

    encodings = spec_list['encodings']
    assert _find_field(encodings, 'dimensions', 'year') is not None, "the user's shelf choice must survive"
    assert _find_field(encodings, 'measures', 'year') is None
    assert _find_field(encodings, 'rows', 'year')['analyticType'] == 'dimension'
    assert 'aggName' not in _find_field(encodings, 'rows', 'year')
    assert _find_field(encodings, 'measures', 'score')['semanticType'] == 'ordinal', 'manual override must survive'


_PARITY_FRAME = pl.DataFrame(
    {
        'text': ['a'],
        'tiny_int': [1],
        'unsigned_int': [1],
        'big_int': [1],
        'float': [1.0],
        'decimal': [1.0],
        'date': [datetime.date(2024, 1, 1)],
        'datetime': [datetime.datetime(2024, 1, 1)],
        'duration': [1],
        'time': [1],
        'boolean': [True],
        'categorical': ['x'],
        'list': [[1, 2]],
    },
    schema_overrides={
        'tiny_int': pl.Int8,
        'unsigned_int': pl.UInt32,
        'float': pl.Float32,
        'decimal': pl.Decimal(10, 2),
        'duration': pl.Duration('us'),
        'time': pl.Time,
        'categorical': pl.Categorical,
        'list': pl.List(pl.Int64),
    },
)


def test_semantic_types_agree_with_polars_gw():
    """The backend classifier must agree with polars_gw, which owns the browser's field schema.

    Reconciliation compares a saved spec entry (built from polars_gw's fields) against a
    backend-derived field, so any disagreement retypes columns on every drawer open.
    """
    core_fields = convert_ff_columns_to_gw_fields(FlowDataEngine(_PARITY_FRAME).schema)
    gw_fields = polars_gw.get_fields(_PARITY_FRAME, classify_integers='measure')

    assert [(f.fid, f.semanticType) for f in core_fields] == [(f['fid'], f['semanticType']) for f in gw_fields]

    spec_fields = deepcopy(gw_fields)
    spec_list = {
        'encodings': {
            'dimensions': [f for f in spec_fields if f['analyticType'] != 'measure'],
            'measures': [f for f in spec_fields if f['analyticType'] == 'measure'],
        }
    }
    untouched = deepcopy(spec_list)

    validate_spec_list_with_data_model_types(spec_list, DataModel(data=[], fields=core_fields))

    assert spec_list == untouched, 'a spec built from polars_gw fields must survive reconciliation unchanged'
