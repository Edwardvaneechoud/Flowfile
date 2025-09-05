from typing import List
from flowfile_core.schemas.schemas import NodeTemplate, NodeDefault, TransformTypeLiteral, NodeTypeLiteral

from typing import List
from flowfile_core.schemas.schemas import NodeTemplate, NodeDefault, TransformTypeLiteral, NodeTypeLiteral

nodes_list: List[NodeTemplate] = [
    NodeTemplate(
        name='External source',
        item='external_source',
        input=0,
        output=1,
        image='external_source.png',
        node_group='input',
        prod_ready=False,
        drawer_title="External Source",
        drawer_intro="Connect to external data sources and APIs"
    ),

    NodeTemplate(
        name='Manual input',
        item='manual_input',
        input=0,
        output=1,
        image='manual_input.png',
        node_group='input',
        drawer_title="Manual Input",
        drawer_intro="Create data directly"
    ),

    NodeTemplate(
        name='Read data',
        item='read',
        input=0,
        output=1,
        image='input_data.png',
        node_group='input',
        drawer_title="Read Data",
        drawer_intro="Load data from CSV, Excel, or Parquet files"
    ),

    NodeTemplate(
        name='Join',
        item='join',
        input=2,
        output=1,
        image='join.png',
        node_group='combine',
        drawer_title="Join Datasets",
        drawer_intro="Merge two datasets based on matching column values"
    ),

    NodeTemplate(
        name='Formula',
        item='formula',
        input=1,
        output=1,
        image='formula.png',
        node_group='transform',
        drawer_title="Formula Editor",
        drawer_intro="Create or modify columns using custom expressions"
    ),

    NodeTemplate(
        name='Write data',
        item='output',
        input=1,
        output=0,
        image='output.png',
        node_group='output',
        drawer_title="Write Data",
        drawer_intro="Save your data as CSV, Excel, or Parquet files"
    ),

    NodeTemplate(
        name='Select data',
        item='select',
        input=1,
        output=1,
        image='select.png',
        node_group='transform',
        drawer_title="Select Columns",
        drawer_intro="Choose, rename, and reorder columns to keep"
    ),

    NodeTemplate(
        name='Filter data',
        item='filter',
        input=1,
        output=1,
        image='filter.png',
        node_group='transform',
        drawer_title="Filter Rows",
        drawer_intro="Keep only rows that match your conditions"
    ),

    NodeTemplate(
        name='Group by',
        item='group_by',
        input=1,
        output=1,
        image='group_by.png',
        node_group='aggregate',
        drawer_title="Group By",
        drawer_intro="Aggregate data by grouping and calculating statistics"
    ),

    NodeTemplate(
        name='Fuzzy match',
        item='fuzzy_match',
        input=2,
        output=1,
        image='fuzzy_match.png',
        node_group='combine',
        drawer_title="Fuzzy Match",
        drawer_intro="Join datasets based on similar values instead of exact matches"
    ),

    NodeTemplate(
        name='Sort data',
        item='sort',
        input=1,
        output=1,
        image='sort.png',
        node_group='transform',
        drawer_title="Sort Data",
        drawer_intro="Order your data by one or more columns"
    ),

    NodeTemplate(
        name='Add record Id',
        item='record_id',
        input=1,
        output=1,
        image='record_id.png',
        node_group='transform',
        drawer_title="Add Record ID",
        drawer_intro="Generate unique identifiers for each row"
    ),

    NodeTemplate(
        name='Take Sample',
        item='sample',
        input=1,
        output=1,
        image='sample.png',
        node_group='transform',
        drawer_title="Take Sample",
        drawer_intro="Work with a subset of your data"
    ),

    NodeTemplate(
        name='Explore data',
        item='explore_data',
        input=1,
        output=0,
        image='explore_data.png',
        node_group='output',
        drawer_title="Explore Data",
        drawer_intro="Interactive data exploration and analysis"
    ),

    NodeTemplate(
        name='Pivot data',
        item='pivot',
        input=1,
        output=1,
        image='pivot.png',
        node_group='aggregate',
        drawer_title="Pivot Data",
        drawer_intro="Convert data from long format to wide format"
    ),

    NodeTemplate(
        name='Unpivot data',
        item='unpivot',
        input=1,
        output=1,
        image='unpivot.png',
        node_group='aggregate',
        drawer_title="Unpivot Data",
        drawer_intro="Transform data from wide format to long format"
    ),

    NodeTemplate(
        name='Union data',
        item='union',
        input=10,
        output=1,
        image='union.png',
        multi=True,
        node_group='combine',
        drawer_title="Union Data",
        drawer_intro="Stack multiple datasets by combining rows"
    ),

    NodeTemplate(
        name='Drop duplicates',
        item='unique',
        input=1,
        output=1,
        image='unique.png',
        node_group='transform',
        drawer_title="Drop Duplicates",
        drawer_intro="Remove duplicate rows based on selected columns"
    ),

    NodeTemplate(
        name='Graph solver',
        item='graph_solver',
        input=1,
        output=1,
        image='graph_solver.png',
        node_group='combine',
        drawer_title="Graph Solver",
        drawer_intro="Group related records in graph-structured data"
    ),

    NodeTemplate(
        name='Count records',
        item='record_count',
        input=1,
        output=1,
        image='record_count.png',
        node_group='aggregate',
        drawer_title="Count Records",
        drawer_intro="Calculate the total number of rows"
    ),

    NodeTemplate(
        name='Cross join',
        item='cross_join',
        input=2,
        output=1,
        image='cross_join.png',
        node_group='combine',
        drawer_title="Cross Join",
        drawer_intro="Create all possible combinations between two datasets"
    ),

    NodeTemplate(
        name='Text to rows',
        item='text_to_rows',
        input=1,
        output=1,
        image='text_to_rows.png',
        node_group='transform',
        drawer_title="Text to Rows",
        drawer_intro="Split text into multiple rows based on a delimiter"
    ),

    NodeTemplate(
        name="Polars code",
        item="polars_code",
        input=10,
        output=1,
        image='polars_code.png',
        node_group='transform',
        multi=True,
        can_be_start=True,
        drawer_title="Polars Code",
        drawer_intro="Write custom Polars DataFrame transformations"
    ),

    NodeTemplate(
        name="Read from Database",
        item="database_reader",
        input=0,
        output=1,
        image='database_reader.svg',
        node_group='input',
        drawer_title="Database Reader",
        drawer_intro="Load data from database tables or queries"
    ),

    NodeTemplate(
        name='Write to Database',
        item='database_writer',
        input=1,
        output=0,
        image='database_writer.svg',
        node_group='output',
        drawer_title="Database Writer",
        drawer_intro="Save data to database tables"
    ),

    NodeTemplate(
        name='Read from cloud provider',
        item='cloud_storage_reader',
        input=0,
        output=1,
        image='cloud_storage_reader.png',
        node_group='input',
        drawer_title="Cloud Storage Reader",
        drawer_intro="Read data from AWS S3 and other cloud storage"
    ),

    NodeTemplate(
        name='Write to cloud provider',
        item='cloud_storage_writer',
        input=1,
        output=0,
        image='cloud_storage_writer.png',
        node_group='output',
        drawer_title="Cloud Storage Writer",
        drawer_intro="Save data to AWS S3 and other cloud storage"
    ),
]

nodes_list.sort(key=lambda x: x.name)

output = ['Explore data', 'Write data', 'Write to Database', 'Write to cloud provider',]
_input = [ 'Manual input', 'Read data', 'External source', 'Read from Database', "Read from cloud provider",]
transform = ['Join', 'Formula', 'Select data', 'Filter data', 'Group by', 'Fuzzy match', 'Sort data', 'Add record Id',
             'Take Sample', 'Pivot data', 'Unpivot data', 'Union data', 'Drop duplicates', 'Graph solver',
             'Count records', 'Cross join', 'Text to rows', 'Polars code']
narrow = ['Select data', 'Filter data', 'Take Sample', 'Formula', 'Read data', 'Union data', 'Polars code']
wide = ['Join', 'Group by', 'Fuzzy match', 'Sort data', 'Pivot data', 'Unpivot data', 'Add record Id',
        'Graph solver', 'Drop duplicates', 'Count records', 'Cross join', 'Text to rows']
other = ['Explore data', 'Write data', 'Manual input', 'Read data', 'External source',
         'Read from Database', 'Write to Database', "Read from cloud provider", 'Write to cloud provider',]
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
node_dict["polars_lazy_frame"] = NodeTemplate(name='LazyFrame node', item='polars_lazy_frame', input=0, output=1, node_group="special", image="",)