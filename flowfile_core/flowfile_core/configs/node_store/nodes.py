from typing import List
from flowfile_core.schemas.schemas import NodeTemplate, NodeDefault, TransformTypeLiteral, NodeTypeLiteral

nodes_list: List[NodeTemplate] = [
    NodeTemplate(name='Read Airbyte', item='airbyte_reader', input=0, output=1, image='airbyte.png',
                 node_group='input'),
    NodeTemplate(name='Google sheets', item='google_sheet', input=0, output=1,
                 image='google_sheet.png', node_group='input', prod_ready=False),
    NodeTemplate(name='External source', item='external_source', input=0, output=1,
                 image='external_source.png', node_group='input', prod_ready=False),
    NodeTemplate(name='Manual input', item='manual_input', input=0, output=1,
                 image='manual_input.png', node_group='input'),
    NodeTemplate(name='Read data', item='read', input=0, output=1, image='input_data.png', node_group='input'),
    NodeTemplate(name='Join', color='#49494970', item='join', input=2, output=1, image='join.png',
                 node_group='combine'),
    NodeTemplate(name='Formula', color='blue', item='formula', input=1, output=1, image='formula.png',
                 node_group='transform'),
    # NodeTemplate(name='Sql editor', color='blue', item='sql_editor', input=1, output=1, image='sql.png'),
    NodeTemplate(name='Write data', item='output', input=1, output=0, image='output.png', node_group='output'),
    NodeTemplate(name='Select data', item='select', input=1, output=1, image='select.png', node_group='transform'),
    NodeTemplate(name='Filter data', item='filter', input=1, output=1, image='filter.png', node_group='transform'),
    NodeTemplate(name='Group by', item='group_by', input=1, output=1, image='group_by.png', node_group='aggregate'),
    NodeTemplate(name='Fuzzy match', item='fuzzy_match', input=2, output=1, image='fuzzy_match.png',
                 node_group='combine'),
    NodeTemplate(name='Sort data', item='sort', input=1, output=1, image='sort.png', node_group='transform'),
    NodeTemplate(name='Add record Id', item='record_id', input=1, output=1, image='record_id.png',
                 node_group='transform'),
    NodeTemplate(name='Take Sample', item='sample', input=1, output=1, image='sample.png', node_group='transform'),
    NodeTemplate(name='Explore data', item='explore_data', input=1, output=0,
                 image='explore_data.png', node_group='output'),
    NodeTemplate(name='Pivot data', item='pivot', input=1, output=1, image='pivot.png', node_group='aggregate'),
    NodeTemplate(name='Unpivot data', item='unpivot', input=1, output=1, image='unpivot.png', node_group='aggregate'),
    NodeTemplate(name='Union data', item='union', input=10, output=1, image='union.png', multi=True,
                 node_group='combine'),
    NodeTemplate(name='Drop duplicates', item='unique', input=1, output=1, image='unique.png', node_group='transform'),
    NodeTemplate(name='Graph solver', item='graph_solver', input=1, output=1, image='graph_solver.png',
                 node_group='combine'),
    NodeTemplate(name='Count records', item='record_count', input=1, output=1, image='record_count.png',
                 node_group='aggregate'),
    NodeTemplate(name='Cross join', item='cross_join', input=2, output=1, image='cross_join.png', node_group='combine'),
    NodeTemplate(name='Text to rows', item='text_to_rows', input=1, output=1, image='text_to_rows.png',
                 node_group='transform'),
    NodeTemplate(name="Polars code", item="polars_code", input=10, output=1, image='polars_code.png',
                 node_group='transform', multi=True, can_be_start=True),
    NodeTemplate(name="Read from Database", item="database_reader", input=0, output=1, image='database_reader.svg',
                 node_group='input'),
    NodeTemplate(name='Write to Database', item='database_writer', input=1, output=0, image='database_writer.svg',
                 node_group='output'),
]

nodes_list.sort(key=lambda x: x.name)

output = ['Explore data', 'Write data', 'Write to Database']
_input = ['Read Airbyte', 'Google sheets', 'Manual input', 'Read data', 'External source', 'Read from Database']
transform = ['Join', 'Formula', 'Select data', 'Filter data', 'Group by', 'Fuzzy match', 'Sort data', 'Add record Id',
             'Take Sample', 'Pivot data', 'Unpivot data', 'Union data', 'Drop duplicates', 'Graph solver',
             'Count records', 'Cross join', 'Text to rows', 'Polars code']
narrow = ['Select data', 'Filter data', 'Take Sample', 'Formula', 'Read data', 'Union data', 'Polars code']
wide = ['Join', 'Group by', 'Fuzzy match', 'Sort data', 'Pivot data', 'Unpivot data', 'Add record Id',
        'Graph solver', 'Drop duplicates', 'Count records', 'Cross join', 'Text to rows']
other = ['Explore data', 'Write data', 'Read Airbyte', 'Google sheets', 'Manual input', 'Read data', 'External source',
         'Read from Database', 'Write to Database']
nodes_with_defaults = {'sample', 'sort', 'union', 'select', 'record_count'}


def get_node_type(node_name: str) -> NodeTypeLiteral:
    if node_name in output:
        return 'output'
    if node_name in _input:
        return 'input'
    if node_name in transform:
        return 'process'
    else:
        raise ValueError(f'Node name {node_name} not found in any of the node types')


def check_if_has_default_setting(node_item: str):
    return node_item in nodes_with_defaults


def get_transform_type(node_name: str) -> TransformTypeLiteral:
    if node_name in narrow:
        return 'narrow'
    if node_name in wide:
        return 'wide'
    if node_name in other:
        return 'other'
    else:
        raise ValueError(f'Node name {node_name} not found in any of the transform types')


node_defaults = {node.item: NodeDefault(node_name=node.name,
                                        node_type=get_node_type(node.name),
                                        transform_type=get_transform_type(node.name),
                                        has_default_settings=check_if_has_default_setting(node.item)
                                        ) for node in nodes_list}

node_dict = {n.item: n for n in nodes_list}
