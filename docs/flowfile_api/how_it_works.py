import flowfile as ff
# Create a flow graph

raw_data = [
    {"id": 1, "region": "North", "quantity": 10, "price": 150},
    {"id": 2, "region": "South", "quantity": 5, "price": 300},
    {"id": 3, "region": "East", "quantity": 8, "price": 200},
    {"id": 4, "region": "West", "quantity": 12, "price": 100},
    {"id": 5, "region": "North", "quantity": 20, "price": 250},
    {"id": 6, "region": "South", "quantity": 15, "price": 400},
    {"id": 7, "region": "East", "quantity": 18, "price": 350},
    {"id": 8, "region": "West", "quantity": 25, "price": 500},]




def test_flow_graph_implementation():

    from flowfile_core.schemas import node_interface, transformation_settings, RawData
    from flowfile_core.flowfile.flow_graph import add_connection

    flow = ff.create_flow_graph()
    node_manual_input = node_interface.NodeManualInput(flow_id=flow.flow_id, node_id=1,
                                                       raw_data_format=RawData.from_pylist(raw_data))
    flow.add_manual_input(node_manual_input)
    # 2. Add formula for total
    formula_node = node_interface.NodeFormula(
        flow_id=1,
        node_id=2,
        function=transformation_settings.FunctionInput(
            field=transformation_settings.FieldInput(name="total", data_type="Double"),
            function="[quantity] * [price]"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_connection=node_interface.NodeConnection.create_from_simple_input(1, 2))
    # 3. Filter high value transactions
    filter_node = node_interface.NodeFilter(
        flow_id=1,
        node_id=3,
        filter_input=transformation_settings.FilterInput(
            filter_type="advanced",
            advanced_filter="[total]>1500"
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, node_connection=node_interface.NodeConnection.create_from_simple_input(2, 3))

    # 4. Group by region and calculate total revenue and quantity
    group_by_node = node_interface.NodeGroupBy(
        flow_id=1,
        node_id=4,
        groupby_input=transformation_settings.GroupByInput(
            agg_cols=[
                transformation_settings.AggColl("region", "groupby"),
                transformation_settings.AggColl("total", "sum", "total_revenue"),
                transformation_settings.AggColl("total", "mean", "total_quantity")
            ]
        )
    )
    flow.add_group_by(group_by_node)
    add_connection(flow, node_connection=node_interface.NodeConnection.create_from_simple_input(3, 4))
    print([s.get_minimal_field_info() for s in flow.get_node(4).schema])
    # [MinimalFieldInfo(name='region', data_type='String'), MinimalFieldInfo(name='total_revenue', data_type='Float64'), MinimalFieldInfo(name='total_quantity', data_type='Float64')]
    output_node = node_interface.NodeOutput(
        flow_id=1,
        node_id=5,
        output_settings=node_interface.OutputSettings(
            name="output.parquet",
            directory="",
            file_type="parquet",
        )
    )
    flow.add_output(output_node)
    add_connection(flow, node_connection=node_interface.NodeConnection.create_from_simple_input(4, 5))
    print(flow.get_node(4).get_resulting_data().data_frame.explain())


def flow_frame_implementation():

    from flowfile_core.flowfile.flow_graph import FlowGraph
    graph: FlowGraph = ff.create_flow_graph()
    df_1 = ff.FlowFrame([
    {"id": 1, "region": "North", "quantity": 10, "price": 150},
    {"id": 2, "region": "South", "quantity": 5, "price": 300},
    {"id": 3, "region": "East", "quantity": 8, "price": 200},
    ], flow_graph=graph)
    df_2 = df_1.with_columns(flowfile_formulas=['[quantity] * [price]'], output_column_names=["total"])
    df_3 = df_2.filter(flowfile_formula="[total]>1500")
    df_4 = df_3.group_by(['region']).agg([
        ff.col("total").sum().alias("total_revenue"),
        ff.col("total").mean().alias("total_quantity"),
    ])
    #  now we can access all the nodes that were created in the graph

    print(graph._node_db)
    #  {1: Node id: 1 (manual_input), 3: Node id: 3 (formula), 4: Node id: 4 (filter), 5: Node id: 5 (group_by)}

    # you can also find the starting node(s) of the graph:
    print(graph._flow_starts)
    #  [Node id: 1 (manual_input)]

    # and from every node, you can access the next node that has a dependency on it:
    print(graph.get_node(1).leads_to_nodes)
    # [Node id: 3 (formula)]

    # the other way around, works also:
    print(graph.get_node(3).node_inputs)
    # NodeStepInputs(Left Input: None, Right Input: None, Main Inputs: [Node id: 1 (manual_input)])

    # you can also access the settings of the node and it's type:
    print(graph.get_node(4).setting_input)
    print(graph.get_node(4).node_type)


""" Note the execution plan is exactly the same:
   AGGREGATE[maintain_order: false]
    [col("total").sum().alias("total_revenue"), col("total").mean().alias("total_quantity")] BY [col("region")]
    FROM
    FILTER [(col("total")) > (1500)]
    FROM
    WITH_COLUMNS:
    [[(col("quantity")) * (col("price"))].alias("total")]
    DF ["id", "region", "quantity", "price"]; PROJECT["region", "quantity", "price"] 3/4 COLUMNS"""
