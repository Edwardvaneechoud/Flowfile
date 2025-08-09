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