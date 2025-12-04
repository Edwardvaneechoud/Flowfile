import pytest
from typing import Dict, Any
from flowfile_core.flowfile.handler import FlowfileHandler
from flowfile_core.schemas import input_schema, transform_schema, schemas
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
from pl_fuzzy_frame_match.models import FuzzyMapping
from flowfile_core.flowfile.flow_graph import (FlowGraph, add_connection, RunInformation)

@pytest.fixture
def complex_elaborate_flow() -> FlowGraph:
    """
    Creates a super complex elaborate flow using all available node types.

    This fixture demonstrates the full capabilities of the Flowfile system by:
    - Starting with multiple data sources (manual input and read nodes)
    - Applying various transformations (filter, formula, select, sort, sample)
    - Performing joins (regular join, cross join, fuzzy match)
    - Aggregating data (group by, pivot, unpivot)
    - Utilizing text operations (text to rows)
    - Applying graph algorithms (graph solver)
    - Finding unique records
    - Adding record IDs and counting records
    - Executing custom Polars code
    - Creating unions of multiple streams
    - Outputting to multiple destinations
    - Including data exploration nodes

    Returns:
        FlowGraph: A fully configured complex flow graph
    """

    # Initialize the flow
    handler = FlowfileHandler()
    handler.register_flow(schemas.FlowSettings(
        flow_id=1,
        name='complex_elaborate_flow',
        path='.',
        execution_mode='Development'
    ))
    graph = handler.get_flow(1)
    # ============================================================
    # SECTION 1: DATA SOURCES (Nodes 1-10)
    # ============================================================

    # Node 1: Manual Input - Customer Data
    customer_data = [
        {'customer_id': 'C001', 'name': 'John Smith', 'city': 'New York', 'segment': 'Premium'},
        {'customer_id': 'C002', 'name': 'Jane Doe', 'city': 'Los Angeles', 'segment': 'Standard'},
        {'customer_id': 'C003', 'name': 'Edward Jones', 'city': 'Chicago', 'segment': 'Premium'},
        {'customer_id': 'C004', 'name': 'Courtney Brown', 'city': 'Chicago', 'segment': 'Standard'},
        {'customer_id': 'C005', 'name': 'Michael Davis', 'city': 'Houston', 'segment': 'Premium'},
    ]
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=1, node_type='manual_input'))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=1,
        node_id=1,
        raw_data_format=input_schema.RawData.from_pylist(customer_data)
    ))

    # Node 2: Manual Input - Sales Transactions
    sales_data = (FlowDataEngine.create_random(100).drop_columns(["City"])
                  .apply_flowfile_formula("if random_int(1,4) == 1 then 'New York' "
                                          "elseif random_int(1,4) == 2 then 'Chicago' "
                                          "else 'Los Angeles'", col_name="City").to_pylist())
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=2, node_type='manual_input'))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=1,
        node_id=2,
        raw_data_format=input_schema.RawData.from_pylist(sales_data)
    ))
    # Node 3: Manual Input - Product Catalog
    product_data = [
        {'product_id': 'P001', 'category': 'Electronics', 'price': 299.99, 'tags': 'laptop,computer,portable'},
        {'product_id': 'P002', 'category': 'Electronics', 'price': 599.99, 'tags': 'phone,mobile,smart'},
        {'product_id': 'P003', 'category': 'Furniture', 'price': 899.99, 'tags': 'desk,office,wood'},
        {'product_id': 'P004', 'category': 'Furniture', 'price': 399.99, 'tags': 'chair,office,ergonomic'},
    ]
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=3, node_type='manual_input'))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=1,
        node_id=3,
        raw_data_format=input_schema.RawData.from_pylist(product_data)
    ))

    # Node 4: Manual Input - Customer Addresses (for fuzzy matching)
    address_data = [
        {'name': 'Jon Smith', 'address': '123 Main St'},  # Fuzzy match with John Smith
        {'name': 'Jane Do', 'address': '456 Oak Ave'},     # Fuzzy match with Jane Doe
        {'name': 'Eduard Jones', 'address': '789 Elm St'},  # Fuzzy match with Edward Jones
    ]
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=4, node_type='manual_input'))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=1,
        node_id=4,
        raw_data_format=input_schema.RawData.from_pylist(address_data)
    ))

    # Node 5: Manual Input - Network Connections (for graph solver)
    network_data = [
        {'from': 'A', 'to': 'B'},
        {'from': 'B', 'to': 'C'},
        {'from': 'C', 'to': 'D'},
        {'from': 'X', 'to': 'Y'},
        {'from': 'Y', 'to': 'Z'},
    ]
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=5, node_type='manual_input'))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=1,
        node_id=5,
        raw_data_format=input_schema.RawData.from_pylist(network_data)
    ))

    # ============================================================
    # SECTION 2: BASIC TRANSFORMATIONS (Nodes 11-25)
    # ============================================================

    # Node 11: Filter - Keep only Premium customers
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=11, node_type='filter'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 11))
    graph.add_filter(input_schema.NodeFilter(
        flow_id=1,
        node_id=11,
        filter_input=transform_schema.FilterInput(
            filter_type='advanced',
            advanced_filter='[segment] = "Premium"'
        )
    ))

    # Node 12: Formula - Add computed fields to customers
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=12, node_type='formula'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(11, 12))
    graph.add_formula(input_schema.NodeFormula(
        flow_id=1,
        node_id=12,
        function=transform_schema.FunctionInput(
            field=transform_schema.FieldInput(name='full_name_upper'),
            function='uppercase([name])'
        )
    ))

    # Node 13: Select - Reorder and rename columns
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=13, node_type='select'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(12, 13))
    graph.add_select(input_schema.NodeSelect(
        flow_id=1,
        node_id=13,
        select_input=[
            transform_schema.SelectInput(old_name='customer_id', new_name='id', keep=True),
            transform_schema.SelectInput(old_name='full_name_upper', new_name='customer_name', keep=True),
            transform_schema.SelectInput(old_name='city', new_name='city', keep=True),
        ],
        keep_missing=False
    ))

    # Node 14: Sort - Sort by city and name
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=14, node_type='sort'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(13, 14))
    graph.add_sort(
        input_schema.NodeSort(
            flow_id=1,
            node_id=14,
            sort_input=[
                    transform_schema.SortByInput(column='city', how="ascending"),
                    transform_schema.SortByInput(column='customer_name',  how="ascending"),
                ]
        )
    )

    # Node 15: Record ID - Add unique row IDs
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=15, node_type='record_id'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(14, 15))
    graph.add_record_id(input_schema.NodeRecordId(
        flow_id=1,
        node_id=15,
        record_id_input=transform_schema.RecordIdInput(output_column_name='row_number')
    ))

    # Node 16: Sample - Take sample of sales data
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=16, node_type='sample'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(2, 16))
    graph.add_sample(input_schema.NodeSample(
        flow_id=1,
        node_id=16,
        sample_size=50
    ))

    # Node 17: Unique - Find unique countries in sales data
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=17, node_type='unique'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(16, 17))
    graph.add_unique(input_schema.NodeUnique(
        flow_id=1,
        node_id=17,
        unique_input=transform_schema.UniqueInput(columns=['Country'])
    ))

    # ============================================================
    # SECTION 3: ADVANCED TRANSFORMATIONS (Nodes 26-40)
    # ============================================================

    # Node 26: Group By - Aggregate sales by country
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=26, node_type='group_by'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(16, 26))
    graph.add_group_by(input_schema.NodeGroupBy(
        flow_id=1,
        node_id=26,
        groupby_input=transform_schema.GroupByInput([
            transform_schema.AggColl('Country', 'groupby'),
            transform_schema.AggColl('sales_data', 'sum', 'total_sales'),
            transform_schema.AggColl('sales_data', 'mean', 'avg_sales'),
            transform_schema.AggColl('sales_data', 'count', 'num_transactions'),
        ])
    ))

    # Node 27: Pivot - Pivot sales by Work category
    input_data_for_pivot = (
        FlowDataEngine.create_random(100)
        .apply_flowfile_formula('random_int(0, 3)', 'quarter')
        .select_columns(['quarter', 'Country', 'sales_data'])
    )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=27, node_type='manual_input'))
    graph.add_manual_input(input_schema.NodeManualInput(
        flow_id=1,
        node_id=27,
        raw_data_format=input_schema.RawData.from_pylist(input_data_for_pivot.to_pylist())
    ))

    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=28, node_type='pivot'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(27, 28))
    graph.add_pivot(input_schema.NodePivot(
        flow_id=1,
        node_id=28,
        pivot_input=transform_schema.PivotInput(
            pivot_column='quarter',
            value_col='sales_data',
            index_columns=['Country'],
            aggregations=['sum']
        )
    ))

    # Node 29: Unpivot - Unpivot the pivoted data

    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=9999, node_type="select"))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(28, 9999))

    graph.add_select(input_schema.NodeSelect(flow_id=1, node_id=9999,
                                             select_input=[transform_schema.SelectInput("0", "first_quarter"),
                                                           transform_schema.SelectInput("1", "second_quarter"),
                                                           transform_schema.SelectInput("2", "third_quarter"),])
                     )
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=29, node_type='unpivot'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(9999, 29))

    graph.add_unpivot(input_schema.NodeUnpivot(
        flow_id=1,
        node_id=29,
        unpivot_input=transform_schema.UnpivotInput(
            index_columns=['Country'],
            value_columns=["first_quarter", "second_quarter", "third_quarter"],
        )
    ))

    # Node 30: Text to Rows - Split product tags
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=30, node_type='text_to_rows'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(3, 30))
    graph.add_text_to_rows(input_schema.NodeTextToRows(
        flow_id=1,
        node_id=30,
        text_to_rows_input=transform_schema.TextToRowsInput(
            column_to_split='tags',
            split_fixed_value=','
        )
    ))
    # Node 31: Graph Solver - Find connected components
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=31, node_type='graph_solver'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(5, 31))
    graph.add_graph_solver(input_schema.NodeGraphSolver(
        flow_id=1,
        node_id=31,
        graph_solver_input=transform_schema.GraphSolverInput(
            col_from='from',
            col_to='to',
            output_column_name='component_id'
        )
    ))

    # Node 32: Polars Code - Custom transformation
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=32, node_type='polars_code'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(26, 32))
    polars_code = """
# Custom Polars code to add derived columns
output_df = input_df.with_columns([
    (pl.col('total_sales') / pl.col('num_transactions')).alias('sales_per_transaction'),
    (pl.col('total_sales') * 1.1).alias('total_sales_with_tax')
])
"""
    graph.add_polars_code(input_schema.NodePolarsCode(
        flow_id=1,
        node_id=32,
        polars_code_input=transform_schema.PolarsCodeInput(polars_code=polars_code)
    ))

    # ============================================================
    # SECTION 4: JOINS (Nodes 41-50)
    # ============================================================
    # Node 41: Regular Join - Join customers with sales
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=41, node_type='join'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(15, 41))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(16, 41, "right"))
    graph.add_join(input_schema.NodeJoin(
        flow_id=1,
        node_id=41,
        join_input=transform_schema.JoinInput(
            join_mapping=[transform_schema.JoinMap(left_col='city', right_col='City')],
            left_select=[
                transform_schema.SelectInput(old_name='id', new_name='customer_id', keep=True),
                transform_schema.SelectInput(old_name='customer_name', new_name='customer_name', keep=True),
                transform_schema.SelectInput(old_name='city', new_name='city', keep=True, join_key=True),
            ],
            right_select=[
                transform_schema.SelectInput(old_name='sales_data', new_name='sale_amount', keep=True),
                transform_schema.SelectInput(old_name='Work', new_name='work_type', keep=True),
            ],
            how='inner'
        ),
        auto_keep_all=True,
        depending_on_ids=[15, 16]
    ))
    graph.get_node(41).get_resulting_data()
    # Node 42: Cross Join - Create all combinations
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=42, node_type='cross_join'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(17, 42))

    right_connection = input_schema.NodeConnection.create_from_simple_input(30, 42, "right")
    add_connection(graph, right_connection)

    graph.add_cross_join(input_schema.NodeCrossJoin(
        flow_id=1,
        node_id=42,
        cross_join_input=transform_schema.CrossJoinInput(
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='Country', new_name='country', keep=True),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='tags', new_name='product_tag', keep=True),
            ])
        ),
        auto_keep_all=True,
        depending_on_ids=[17, 30]
    ))

    # Node 43: Fuzzy Match - Match customer names with addresses
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=43, node_type='fuzzy_match'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(1, 43))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(4, 43, "right"))
    graph.add_fuzzy_match(input_schema.NodeFuzzyMatch(
        flow_id=1,
        node_id=43,
        join_input=transform_schema.FuzzyMatchInput(
            join_mapping=[FuzzyMapping(
                left_col='name',
                right_col='name',
                threshold_score=75,
                fuzzy_type='levenshtein',
                valid=True
            )],
            left_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='customer_id', new_name='customer_id', keep=True),
                transform_schema.SelectInput(old_name='name', new_name='customer_name', keep=True, join_key=True),
            ]),
            right_select=transform_schema.JoinInputs(renames=[
                transform_schema.SelectInput(old_name='name', new_name='address_name', keep=True, join_key=True),
                transform_schema.SelectInput(old_name='address', new_name='address', keep=True),
            ]),
            how='inner'
        ),
        auto_keep_all=True,
        depending_on_ids=[1, 4]
    ))
    # ============================================================
    # SECTION 5: AGGREGATIONS & ANALYTICS (Nodes 51-60)
    # ============================================================

    # Node 51: Record Count - Count records after join
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=51, node_type='record_count'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(41, 51))
    graph.add_record_count(input_schema.NodeRecordCount(
        flow_id=1,
        node_id=51
    ))
    # Node 52: Explore Data - Add analytics node
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=52, node_type='explore_data'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(41, 52))
    graph.add_explore_data(input_schema.NodeExploreData(
        flow_id=1,
        node_id=52
    ))
    # ============================================================
    # SECTION 6: UNIONS (Nodes 61-70)
    # ============================================================

    # Node 61: Union - Combine multiple aggregated streams
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=61, node_type='union'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(26, 61))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(32, 61))

    graph.add_union(input_schema.NodeUnion(
        flow_id=1,
        node_id=61,
        depending_on_ids=[26, 32]
    ))

    # ============================================================
    # SECTION 7: OUTPUTS (Nodes 71-80)
    # ============================================================
    # Node 71: Output - Final customer analysis
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=71, node_type='output'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(41, 71))
    graph.add_output(input_schema.NodeOutput(
        flow_id=1,
        node_id=71,
        output_settings=input_schema.OutputSettings(
            name='customer_sales_analysis.csv',
            directory='.',
            file_type='csv',
            write_mode='overwrite',
            output_csv_table=input_schema.OutputCsvTable(
                delimiter=',',
                encoding='utf-8'
            )
        )
    ))

    # Node 72: Output - Fuzzy match results
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=72, node_type='output'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(43, 72))
    graph.add_output(input_schema.NodeOutput(
        flow_id=1,
        node_id=72,
        output_settings=input_schema.OutputSettings(
            name='fuzzy_matched_customers.parquet',
            directory='.',
            file_type='parquet',
            write_mode='overwrite',
            output_parquet_table=input_schema.OutputParquetTable()
        )
    ))

    # Node 73: Output - Graph solver results
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=73, node_type='output'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(31, 73))
    graph.add_output(input_schema.NodeOutput(
        flow_id=1,
        node_id=73,
        output_settings=input_schema.OutputSettings(
            name='network_components.csv',
            directory='.',
            file_type='csv',
            write_mode='overwrite',
            output_csv_table=input_schema.OutputCsvTable(
                delimiter=',',
                encoding='utf-8'
            )
        )
    ))

    # Node 74: Output - Union results
    graph.add_node_promise(input_schema.NodePromise(flow_id=1, node_id=74, node_type='output'))
    add_connection(graph, input_schema.NodeConnection.create_from_simple_input(61, 74))
    graph.add_output(input_schema.NodeOutput(
        flow_id=1,
        node_id=74,
        output_settings=input_schema.OutputSettings(
            name='combined_aggregations.csv',
            directory='.',
            file_type='csv',
            write_mode='overwrite',
            output_csv_table=input_schema.OutputCsvTable(
                delimiter=',',
                encoding='utf-8'
            )
        )
    ))

    return graph


@pytest.fixture
def complex_flow_summary(complex_elaborate_flow: FlowGraph) -> Dict[str, Any]:
    """
    Provides a summary of the complex elaborate flow for testing and documentation.

    Returns:
        Dict containing flow statistics and node information
    """
    graph = complex_elaborate_flow

    return {
        'total_nodes': len(graph.nodes),
        'total_connections': len(graph.node_connections),
        'starting_nodes': len(graph._flow_starts),
        'node_types': {
            'manual_input': len([n for n in graph.nodes if n.node_type == 'manual_input']),
            'filter': len([n for n in graph.nodes if n.node_type == 'filter']),
            'formula': len([n for n in graph.nodes if n.node_type == 'formula']),
            'select': len([n for n in graph.nodes if n.node_type == 'select']),
            'sort': len([n for n in graph.nodes if n.node_type == 'sort']),
            'sample': len([n for n in graph.nodes if n.node_type == 'sample']),
            'unique': len([n for n in graph.nodes if n.node_type == 'unique']),
            'group_by': len([n for n in graph.nodes if n.node_type == 'group_by']),
            'pivot': len([n for n in graph.nodes if n.node_type == 'pivot']),
            'unpivot': len([n for n in graph.nodes if n.node_type == 'unpivot']),
            'join': len([n for n in graph.nodes if n.node_type == 'join']),
            'cross_join': len([n for n in graph.nodes if n.node_type == 'cross_join']),
            'fuzzy_match': len([n for n in graph.nodes if n.node_type == 'fuzzy_match']),
            'union': len([n for n in graph.nodes if n.node_type == 'union']),
            'text_to_rows': len([n for n in graph.nodes if n.node_type == 'text_to_rows']),
            'graph_solver': len([n for n in graph.nodes if n.node_type == 'graph_solver']),
            'record_id': len([n for n in graph.nodes if n.node_type == 'record_id']),
            'record_count': len([n for n in graph.nodes if n.node_type == 'record_count']),
            'polars_code': len([n for n in graph.nodes if n.node_type == 'polars_code']),
            'explore_data': len([n for n in graph.nodes if n.node_type == 'explore_data']),
            'output': len([n for n in graph.nodes if n.node_type == 'output']),
        },
        'flow_name': graph.__name__,
        'execution_mode': graph.execution_mode,
        'node_ids': [n.node_id for n in graph.nodes],
    }


def test_complex_flow_structure(complex_elaborate_flow: FlowGraph, complex_flow_summary):
    """Test that the complex flow has the expected structure."""
    summary = complex_flow_summary
    # Verify we have a substantial number of nodes
    assert summary['total_nodes'] > 29, "Complex flow should have more than 30 nodes"

    # Verify we have multiple starting points
    assert summary['starting_nodes'] >= 5, "Complex flow should have at least 5 starting nodes"

    # Verify connections exist
    assert summary['total_connections'] > 25, "Complex flow should have more than 25 connections"

    # Verify all major node types are represented
    assert summary['node_types']['manual_input'] >= 5
    assert summary['node_types']['filter'] >= 1
    assert summary['node_types']['formula'] >= 1
    assert summary['node_types']['join'] >= 1
    assert summary['node_types']['group_by'] >= 1
    assert summary['node_types']['pivot'] >= 1
    assert summary['node_types']['output'] >= 4


def handle_run_info(run_info: RunInformation):
    if not run_info.success:
        errors = 'errors:'
        for node_step in run_info.node_step_result:
            if not node_step.success:
                errors += f'\n node_id:{node_step.node_id}, error: {node_step.error}'
        raise ValueError(f'Graph should run successfully:\n{errors}')


def test_execution_complex_flow_remote_development(complex_elaborate_flow: FlowGraph):
    """Test executing the complex flow to ensure all nodes run without error."""
    complex_elaborate_flow.flow_settings.execution_mode = "Development"
    complex_elaborate_flow.flow_settings.execution_location = "remote"
    # Execute the entire flow
    results = complex_elaborate_flow.run_graph()
    handle_run_info(results)


def test_execution_complex_flow_remote_performance(complex_elaborate_flow: FlowGraph):
    """Test executing the complex flow to ensure all nodes run without error."""
    complex_elaborate_flow.flow_settings.execution_mode = "Performance"
    complex_elaborate_flow.flow_settings.execution_location = "remote"
    # Execute the entire flow
    results = complex_elaborate_flow.run_graph()
    handle_run_info(results)


def test_execution_complex_flow_local_performance(complex_elaborate_flow: FlowGraph):
    """Test executing the complex flow to ensure all nodes run without error."""
    complex_elaborate_flow.flow_settings.execution_mode = "Performance"
    complex_elaborate_flow.flow_settings.execution_location = "local"
    # Execute the entire flow
    results = complex_elaborate_flow.run_graph()
    handle_run_info(results)


def test_execution_complex_flow_local_development(complex_elaborate_flow: FlowGraph):
    """Test executing the complex flow to ensure all nodes run without error."""
    complex_elaborate_flow.flow_settings.execution_mode = "Development"
    complex_elaborate_flow.flow_settings.execution_location = "local"
    # Execute the entire flow
    results = complex_elaborate_flow.run_graph()
    handle_run_info(results)


def test_store_flow(complex_elaborate_flow: FlowGraph):
    # breakpoint()
    complex_elaborate_flow.get_flowfile_data()
