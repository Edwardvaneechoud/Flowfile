
from typing import List, Dict
from flowfile_core.schemas.schemas import NodeTemplate, NodeDefault


def get_all_standard_nodes() -> tuple[List[NodeTemplate], Dict[str, NodeTemplate], Dict[str, NodeDefault]]:
    """
    Initializes and returns the complete list, dict, and defaults for all nodes.
        """
    nodes_list: List[NodeTemplate] = [
        NodeTemplate(
            name='External source',
            item='external_source',
            input=0,
            output=1,
            image='external_source.png',
            node_type="input",
            transform_type="other",
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
            transform_type="other",
            node_type="input",
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
            transform_type="other",
            node_type="input",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="narrow",
            node_type="process",
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
            transform_type="other",
            image='output.png',
            node_type="output",
            node_group='output',
            drawer_title="Write Data",
            drawer_intro="Save your data as CSV, Excel, or Parquet files"
        ),

        NodeTemplate(
            name='Select data',
            item='select',
            input=1,
            output=1,
            transform_type="narrow",
            node_type="process",
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
            transform_type="narrow",
            node_type="process",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="wide",
            image='fuzzy_match.png',
            node_type="process",
            node_group='combine',
            drawer_title="Fuzzy Match",
            drawer_intro="Join datasets based on similar values instead of exact matches"
        ),

        NodeTemplate(
            name='Sort data',
            item='sort',
            input=1,
            output=1,
            transform_type="wide",
            node_type="process",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="narrow",
            node_type="process",
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
            transform_type="other",
            node_type="output",
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
            transform_type="wide",
            image='pivot.png',
            node_type="process",
            node_group='aggregate',
            drawer_title="Pivot Data",
            drawer_intro="Convert data from long format to wide format"
        ),

        NodeTemplate(
            name='Unpivot data',
            item='unpivot',
            input=1,
            output=1,
            transform_type="wide",
            node_type="process",
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
            transform_type="narrow",
            node_type="process",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="other",
            node_type="process",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="wide",
            node_type="process",
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
            transform_type="narrow",
            image='polars_code.png',
            node_group='transform',
            node_type="process",
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
            node_type="input",
            transform_type="other",
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
            transform_type="other",
            node_type="output",
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
            transform_type="other",
            node_type="input",
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
            transform_type="other",
            node_type="output",
            image='cloud_storage_writer.png',
            node_group='output',
            drawer_title="Cloud Storage Writer",
            drawer_intro="Save data to AWS S3 and other cloud storage"
        ),
    ]
    nodes_list.sort(key=lambda x: x.name)
    nodes_with_defaults = {'sample', 'sort', 'union', 'select', 'record_count'}

    def check_if_has_default_setting(node_item: str):

        return node_item in nodes_with_defaults

    node_defaults = {node.item: NodeDefault(node_name=node.name,
                                            node_type=node.node_type,
                                            transform_type=node.transform_type,
                                            has_default_settings=check_if_has_default_setting(node.item)
                                            ) for node in nodes_list}
    node_dict = {n.item: n for n in nodes_list}

    node_dict["polars_lazy_frame"] = NodeTemplate(name='LazyFrame node', item='polars_lazy_frame', input=0, output=1,
                                                  node_group="special", image="", node_type="input", transform_type="other",)

    return nodes_list, node_dict, node_defaults
