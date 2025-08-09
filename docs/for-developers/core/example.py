from flowfile_core.flowfile.flow_graph import  FlowGraph
from flowfile_core.schemas.schemas import FlowGraphConfig

def test_case():
    # Config of how and where the flow should execute, what it is named and where it is stored
    flow_settings_config = FlowGraphConfig(flow_id=1,
        name="My ETL Pipeline",
        execution_location='local',  # 'local', 'remote', or 'auto'
        execution_mode='Development'
                                           )
    graph = FlowGraph(flow_settings=flow_settings_config)

    print(graph)


    print(graph.run_graph())

    from flowfile_core.schemas import input_schema
    from flowfile_core.schemas.transform_schema import FilterInput, BasicFilter, FunctionInput

    # Add a data source
    manual_input = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData.from_pylist([
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
            {"name": "Charlie", "age": 35, "city": "NYC"}
        ])
    )
    graph.add_manual_input(manual_input)

    node = graph.get_node(1)
    print(type(node))


    # Add a filter node
    filter_node_config = input_schema.NodeFilter(
        flow_id=graph.flow_id,
        node_id=2,
        filter_input=FilterInput(
            filter_type="advanced",
            advanced_filter="[age] > 28"
        )
    )
    graph.add_filter(filter_node_config)

    filter_node = graph.get_node(2)  # Get the filter node to check its configuration
    print(graph.run_graph())  # This will only run the manual input node THERE IS NO CONNECTION YET
    print(graph.get_node(1).results.get_example_data()) # Provides example data from the manual input node
    print(filter_node.results.get_example_data())  # prints none, since there is no data filtered

    from flowfile_core.flowfile.flow_graph import add_connection

    # Connect node 1 (manual input) to node 2 (filter)
    connection = input_schema.NodeConnection.create_from_simple_input(
        from_id=1,
        to_id=2,
        input_type="main"  # "main", "left", or "right"
    )
    add_connection(graph, connection)

    print(graph.run_graph())
    print(graph.get_node(2).results.get_example_data())  # Now there is data!


    from flowfile_core.schemas.transform_schema import FunctionInput, FieldInput

    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=3,
        depending_on_id=1,
        function=FunctionInput(
            field=FieldInput(name="total", data_type="Auto"),
            function="[price] * [quantity]"  # The columns price and quantity do not exist in the manual input data,
            # so it should throw an error when running the graph
        )
    )

    graph.add_formula(formula_node)

    add_connection(flow=graph,
                   node_connection=input_schema.NodeConnection.create_from_simple_input(from_id=1, to_id=3)
                   )

    run_stats = graph.run_graph()
    print(f"The graph run with success: {run_stats.success}")

    print(f"Number of nodes completed: {run_stats.nodes_completed}")
    print(f"Node 3 give the following error: {run_stats.node_step_result[2].error}")
    # We can also access the result of the formula node via the FlowNode object
    print(graph.get_node(3).results.errors)
    # price
    # Resolved plan until failure:
    #     ---> FAILED HERE RESOLVING 'with_columns' <---
    # DF ["name", "age", "city"]; PROJECT */3 COLUMNS

    graph.delete_node(3)

    # Now let's add a new formula node that works

    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=3,
        depending_on_id=2,
        function=FunctionInput(
            field=FieldInput(name="today_value", data_type="Auto"),
            function="today()"  # Just some random text
        )
    )
    graph.add_formula(formula_node)
    add_connection(flow=graph,
                   node_connection=input_schema.NodeConnection.create_from_simple_input(from_id=2, to_id=3)
                   )


    # Let's explain the caching mechanism when in mode debugging.
    # Let's also add a long running function, to mimick an expensive function
    from flowfile_core.schemas.transform_schema import PolarsCodeInput

    polars_node = input_schema.NodePolarsCode(
        flow_id=1,
        node_id=4,
        polars_code_input=PolarsCodeInput("time.sleep(5)\noutput_df=input_df.with_columns(pl.lit(1221).alias('new'))")
    )

    graph.add_polars_code(polars_node)
    breakpoint()
    add_connection(flow=graph,
                   node_connection=input_schema.NodeConnection.create_from_simple_input(from_id=3, to_id=4)
                   )
    graph.run_graph()  # this will take at least 5 seconds to execute since we've added the sleep in polars code

    # 2025-08-09 17:18:22,323 - PipelineHandler - INFO - Log file cleared for flow 1
    #     2025-08-09 17:18:22,324 - PipelineHandler - INFO - Starting topological sort to determine execution order
    # 2025-08-09 17:18:22,324 - PipelineHandler - INFO - execution order:
    # [Node id: 1 (manual_input), Node id: 2 (filter), Node id: 3 (formula), Node id: 4 (polars_code)]
    # 2025-08-09 17:18:22,324 - PipelineHandler - INFO - Starting to run: node 1, start time: 1754752702.3246248
    # 2025-08-09 17:18:22,325 - PipelineHandler - INFO - Starting to run: node 2, start time: 1754752702.3250299
    # 2025-08-09 17:18:22,325 - PipelineHandler - INFO - Starting to run: node 3, start time: 1754752702.325375
    # 2025-08-09 17:18:22,325 - PipelineHandler - INFO - Starting to run: node 4, start time: 1754752702.3257039
    # RunInformation(flow_id=1, start_time=datetime.datetime(2025, 8, 9, 17, 18, 22, 323929), end_time=datetime.datetime(2025, 8, 9, 17, 18, 22, 326096), success=True, nodes_completed=4, number_of_nodes=4, node_step_result=[NodeResult(node_id=1, node_name='manual_input', start_timestamp=1754752702.3246248, end_timestamp=1754752702.324918, success=True, error='None', run_time=0, is_running=False), NodeResult(node_id=2, node_name='_func', start_timestamp=1754752702.3250299, end_timestamp=1754752702.325272, success=True, error='None', run_time=0, is_running=False), NodeResult(node_id=3, node_name='_func', start_timestamp=1754752702.325375, end_timestamp=1754752702.3256042, success=True, error='None', run_time=0, is_running=False), NodeResult(node_id=4, node_name='_func', start_timestamp=1754752702.3257039, end_timestamp=1754752702.325921, success=True, error='None', run_time=0, is_running=False)])



    # the next time it will not take nearly as long, since all the nodes have executed and there is no need for them
    # to recalculate (the situation has not changed)

    graph.run_graph()
    # [Node id: 1 (manual_input), Node id: 2 (filter), Node id: 3 (formula), Node id: 4 (polars_code)]
    # 2025-08-09 17:20:22,521 - PipelineHandler - INFO - Starting to run: node 1, start time: 1754752822.5216188
    # 2025-08-09 17:20:22,521 - PipelineHandler - INFO - Starting to run: node 2, start time: 1754752822.5219378
    # 2025-08-09 17:20:22,522 - PipelineHandler - INFO - Starting to run: node 3, start time: 1754752822.522172
    # 2025-08-09 17:20:22,522 - PipelineHandler - INFO - Starting to run: node 4, start time: 1754752822.522379
    # RunInformation(flow_id=1, start_time=datetime.datetime(2025, 8, 9, 17, 20, 22, 520594), end_time=datetime.datetime(2025, 8, 9, 17, 20, 22, 522736), success=True, nodes_completed=4, number_of_nodes=4, node_step_result=[NodeResult(node_id=1, node_name='manual_input', start_timestamp=1754752822.5216188, end_timestamp=1754752822.521854, success=True, error='None', run_time=0, is_running=False), NodeResult(node_id=2, node_name='_func', start_timestamp=1754752822.5219378, end_timestamp=1754752822.5220978, success=True, error='None', run_time=0, is_running=False), NodeResult(node_id=3, node_name='_func', start_timestamp=1754752822.522172, end_timestamp=1754752822.522316, success=True, error='None', run_time=0, is_running=False), NodeResult(node_id=4, node_name='_func', start_timestamp=1754752822.522379, end_timestamp=1754752822.522582, success=True, error='None', run_time=0, is_running=False)])

    # now let's update the formula in the graph

    formula_node = input_schema.NodeFormula(
        flow_id=1,
        node_id=3,
        depending_on_id=2,
        function=FunctionInput(
            field=FieldInput(name="today_value", data_type="Auto"),
            function="add_years(today(), 2)"  # We add years now to it so the data will be different
        )
    )
    breakpoint()
    graph.get_node(4).results.resulting_data  # contains the data so it does not need to reset
    graph.add_formula(formula_node)
    graph.get_node(4).results.resulting_data  # data is gone, since the outcome of one of the previous steps is different


    ## Explain the performance mode.
    # When running in performance mode, the graph actually works with a pull. We pull that off with the fact that you can
    # always just do a pull of the execution plan
    graph.reset()
    graph.get_node(3).get_resulting_data().data_frame.explain() # Is just all formulas nested in eachother

    node2 = graph.get_node(2)
    original_hash = node2.hash
    print(f"Original hash of filter node: {original_hash[:10]}...")
    node2.setting_input.filter_input.advanced_filter = "[age] > 20"

    graph.flow_settings.execution_mode = 'Performance'

    # When putting the execution mode to performance, it will only collect the nodes that need to run
    # However, in our case we've added a slow running job in the main process (Sleep blocks the main process, and
    # we have to complete it before we can go to the next step and know our data structure. Therefore, you should
    # always try to work with lazyframe functionalities since they can be offloaded easily result in an lazyframe,
    # which we can track.
    graph.run_graph()

    graph.get_node(3).results.resulting_data  # This now does not contain any data since the



def test_new():

    flow_settings_config = FlowGraphConfig(flow_id=1,
        name="My ETL Pipeline",
        execution_location='local',  # 'local', 'remote', or 'auto'
        execution_mode='Development'
                                           )
    graph = FlowGraph(flow_settings=flow_settings_config)

    print(graph)


    print(graph.run_graph())

    from flowfile_core.schemas import input_schema
    from flowfile_core.schemas.transform_schema import FilterInput, BasicFilter, FunctionInput

    # Add a data source
    manual_input = input_schema.NodeManualInput(
        flow_id=graph.flow_id,
        node_id=1,
        raw_data_format=input_schema.RawData.from_pylist([
            {"name": "Alice", "age": 30, "city": "NYC"},
            {"name": "Bob", "age": 25, "city": "LA"},
            {"name": "Charlie", "age": 35, "city": "NYC"}
        ])
    )
    graph.add_manual_input(manual_input)


def test_another_implementation():
    import flowfile as ff
    raw_data = [
        {"id": 1, "region": "North", "quantity": 10, "price": 150},
        {"id": 2, "region": "South", "quantity": 5, "price": 300},
        {"id": 3, "region": "East", "quantity": 8, "price": 200},
        {"id": 4, "region": "West", "quantity": 12, "price": 100},
        {"id": 5, "region": "North", "quantity": 20, "price": 250},
        {"id": 6, "region": "South", "quantity": 15, "price": 400},
        {"id": 7, "region": "East", "quantity": 18, "price": 350},
        {"id": 8, "region": "West", "quantity": 25, "price": 500},]

    from flowfile_core.flowfile.flow_graph import FlowGraph
    graph: FlowGraph = ff.create_flow_graph(1)
    df_1 = ff.FlowFrame(
    raw_data, flow_graph=graph)
    df_2 = df_1.with_columns(flowfile_formulas=['[quantity] * [price]'], output_column_names=["total"])
    df_3 = df_2.filter(flowfile_formula="[total]>1500")
    df_4 = df_3.group_by(['region']).agg([
        ff.col("total").sum().alias("total_revenue"),
        ff.col("total").mean().alias("total_quantity"),
    ])
    print(df_4.get_node_settings().setting_input)
    # flow_id=1 node_id=5 cache_results=False pos_x=200.0 pos_y=200.0 is_setup=True description='Aggregate after grouping by "region"' user_id=None is_flow_output=False depending_on_id=4 groupby_input=GroupByInput(agg_cols=[AggColl(old_name='region', agg='groupby', new_name='region', output_type=None), AggColl(old_name='total', agg='sum', new_name='total_revenue', output_type=None), AggColl(old_name='total', agg='mean', new_name='total_quantity', output_type='Float64')])
    df_5 = df_3.group_by([(ff.col("region").str.to_uppercase() + ff.lit("test"))]).agg(
                         ff.col("total").sum().alias("total_revenue"),
                         ff.col("total").mean().alias("total_quantity"),
                         )
    df_5.collect()
    print(df_5.get_node_settings())
    # Node id: 6 (polars_code)
    print(df_5.get_node_settings().setting_input)
    # flow_id=1 node_id=6 cache_results=False pos_x=0 pos_y=0 is_setup=True description="Aggregate after grouping by (pl.col('region').str.to_uppercase() + pl.lit('test'))" user_id=None is_flow_output=False depending_on_ids=[4] polars_code_input=PolarsCodeInput(polars_code="input_df.group_by([(pl.col('region').str.to_uppercase() + pl.lit('test'))], maintain_order=False).agg(pl.col('total').sum().alias('total_revenue'), pl.col('total').mean().alias('total_quantity'))")
    # THIS IS THE OUTPUT OF THE NODE SETTINGS,
    # IT CONVERTED TO POLARS CODE SINCE THERE IS NO MAPPING TO A UI COMPONENT


def test_another():
    import flowfile as ff
    import polars as pl
    breakpoint()
    test = ff.col("signup_date").str.strptime(ff.Date, "%Y-%m-%d")
    test.get_polars_code()
    df =  ff.FlowFrame({
            "customer_id": [1, 2, 3, 4, 5],
            "status": ["active", "inactive", "active", "active", "inactive"],
            "signup_date": ["2024-01-15", "2023-12-10", "2024-02-20", "2023-11-05", "2024-03-01"],
            "customer_segment": ["premium", "basic", "premium", "basic", "premium"],
            "text": ["This is a sample text with pattern", "Another text without it",
                     "Pattern is here too", "No pattern here", "Just some random text"],
            "revenue": [1000, 500, 1500, 300, 2000],
            "quantity": [10, 5, 15, 3, 20],
            "price": [100, 100, 100, 100, 100],
            "total": [1000, 500, 1500, 300, 2000],
            "count": [1, 2, 12, 112, 5]},)
    df = df.with_columns(
        flowfile_formulas=[
            "[price] * [quantity]",
            "[price] * 1.1",
            "[total] / [count]"
        ],
        output_column_names=["revenue", "price_with_tax", "average"]
    )
    df = df.with_columns([
        ff.col("text").str.to_uppercase().alias("name_upper"),
        ff.col("text").str.slice(0, 3).alias("prefix"),
        ff.col("text").str.contains("pattern").alias("has_pattern")
    ])



    raw_data = [
        {"id": 1, "region": "North", "quantity": 10, "price": 150, "total": 1500},
        {"id": 2, "region": "South", "quantity": 5, "price": 300, "total": 1500},
        {"id": 3, "region": "East", "quantity": 8, "price": 200, "total": 1600},
        {"id": 4, "region": "West", "quantity": 12, "price": 100, "total": 1200},
        {"id": 5, "region": "North", "quantity": 20, "price": 250, "total": 5000},
        {"id": 6, "region": "South", "quantity": 15, "price": 400, "total": 6000},
        {"id": 7, "region": "East", "quantity": 18, "price": 350, "total": 6300},
        {"id": 8, "region": "West", "quantity": 25, "price": 500, "total": 12500},
    ]
    df = ff.FlowFrame(
        raw_data, flow_graph=df.flow_graph
    )

    df = df.with_columns([
        ff.when(ff.col("price") > 100)
        .then(ff.lit("Premium"))
        .when(ff.col("price") > 50)
        .then(ff.lit("Standard"))
        .otherwise(ff.lit("Budget"))
        .alias("tier")
    ])

    df.collect()

    df = ff.FlowFrame({
        "category": ["A", "B", "A", "B", "A"],
        "value": [10, 20, 30, 40, 50],
        "quantity": [1, 2, 3, 4, 5]
    })

    # Simple aggregation
    result = df.group_by("category").agg([
        ff.col("value").sum().alias("total_value"),
        ff.col("value").var().alias("avg_value"),
        ff.col("quantity").count().alias("count")
    ])
