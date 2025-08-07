# FlowFrame for Developers

## The Best of Both Worlds

Data platforms have an exciting opportunity: combining the introspection and accessibility of visual tools with the expressiveness and speed of code. Visual interfaces make pipelines understandable at a glance, while code interfaces enable rapid development and complex logic.

FlowFrame embraces both approaches simultaneously. Developers can write fluent Python code while visual users can drag and drop nodes—both creating the exact same underlying data pipeline. It's not about choosing one or the other; it's about having the right tool for every situation.

## A Happy Accident

The architecture emerged organically from practical needs. The team initially built a frontend UI with drag-and-drop functionality, which required well-structured node configurations that clearly exposed settings through Pydantic models. This design choice—made purely for UI clarity—turned out to be the perfect foundation for integrating with Polars' lazy evaluation model.

Since Polars handles 90% of the compute engine anyway, the settings-based approach naturally became the perfect bridge between visual and code interfaces. Each transformation is simply a configuration object that can be created either through code or through the UI. What started as a UI requirement became the foundation for something much more powerful.

## Building Pipelines: Two Approaches, Same Result

Let's look at a concrete example using this sample data:

```python
import flowfile as ff

raw_data = [
    {"id": 1, "region": "North", "quantity": 10, "price": 150},
    {"id": 2, "region": "South", "quantity": 5, "price": 300},
    {"id": 3, "region": "East", "quantity": 8, "price": 200},
    {"id": 4, "region": "West", "quantity": 12, "price": 100},
    {"id": 5, "region": "North", "quantity": 20, "price": 250},
    {"id": 6, "region": "South", "quantity": 15, "price": 400},
    {"id": 7, "region": "East", "quantity": 18, "price": 350},
    {"id": 8, "region": "West", "quantity": 25, "price": 500},
]
```

### Method 1: The FlowFrame API

This is what developers use day-to-day. It feels like working with Pandas or Polars:

```python
def flow_frame_implementation():
    from flowfile_core.flowfile.flow_graph import FlowGraph
    
    graph: FlowGraph = ff.create_flow_graph()
    
    df_1 = ff.FlowFrame([
        {"id": 1, "region": "North", "quantity": 10, "price": 150},
        {"id": 2, "region": "South", "quantity": 5, "price": 300},
        {"id": 3, "region": "East", "quantity": 8, "price": 200},
    ], flow_graph=graph)
    
    df_2 = df_1.with_columns(
        flowfile_formulas=['[quantity] * [price]'], 
        output_column_names=["total"]
    )
    
    df_3 = df_2.filter(flowfile_formula="[total]>1500")
    
    df_4 = df_3.group_by(['region']).agg([
        ff.col("total").sum().alias("total_revenue"),
        ff.col("total").mean().alias("total_quantity"),
    ])
```

### Method 2: Direct Graph Construction

This is what happens behind the scenes. The visual editor creates the exact same structure:

```python
def test_flow_graph_implementation():
    from flowfile_core.schemas import node_interface, transformation_settings, RawData
    from flowfile_core.flowfile.flow_graph import add_connection

    flow = ff.create_flow_graph()
    
    # Node 1: Manual input
    node_manual_input = node_interface.NodeManualInput(
        flow_id=flow.flow_id, 
        node_id=1,
        raw_data_format=RawData.from_pylist(raw_data)
    )
    flow.add_manual_input(node_manual_input)
    
    # Node 2: Add formula for total
    formula_node = node_interface.NodeFormula(
        flow_id=1,
        node_id=2,
        function=transformation_settings.FunctionInput(
            field=transformation_settings.FieldInput(name="total", data_type="Double"),
            function="[quantity] * [price]"
        )
    )
    flow.add_formula(formula_node)
    add_connection(flow, node_interface.NodeConnection.create_from_simple_input(1, 2))
    
    # Node 3: Filter high value transactions
    filter_node = node_interface.NodeFilter(
        flow_id=1,
        node_id=3,
        filter_input=transformation_settings.FilterInput(
            filter_type="advanced",
            advanced_filter="[total]>1500"
        )
    )
    flow.add_filter(filter_node)
    add_connection(flow, node_interface.NodeConnection.create_from_simple_input(2, 3))
    
    # Node 4: Group by region
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
    add_connection(flow, node_interface.NodeConnection.create_from_simple_input(3, 4))
```

Both approaches generate the exact same Polars execution plan:

```
AGGREGATE[maintain_order: false]
[col("total").sum().alias("total_revenue"), col("total").mean().alias("total_quantity")] BY [col("region")]
FROM
FILTER [(col("total")) > (1500)]
FROM
WITH_COLUMNS:
[[(col("quantity")) * (col("price"))].alias("total")]
DF ["id", "region", "quantity", "price"]; PROJECT["region", "quantity", "price"] 3/4 COLUMNS
```

## Understanding the Graph Structure

Once a pipeline is built (using either method), the graph structure is fully accessible and introspectable:

```python
# Access all nodes in the graph
print(graph._node_db)
# {1: Node id: 1 (manual_input), 3: Node id: 3 (formula), 4: Node id: 4 (filter), 5: Node id: 5 (group_by)}

# Find starting nodes
print(graph._flow_starts)
# [Node id: 1 (manual_input)]

# Navigate forward through the graph
print(graph.get_node(1).leads_to_nodes)
# [Node id: 3 (formula)]

# Navigate backward through the graph
print(graph.get_node(3).node_inputs)
# NodeStepInputs(Left Input: None, Right Input: None, Main Inputs: [Node id: 1 (manual_input)])

# Access node settings and type
print(graph.get_node(4).setting_input)
print(graph.get_node(4).node_type)
```

## Core Concepts

### The DAG (Directed Acyclic Graph)

Every FlowFrame pipeline is a DAG where:
- **Nodes** represent transformations (filter, group_by, formula, etc.)
- **Edges** represent data flow between transformations
- **Settings** are Pydantic models that configure each transformation

### Settings-Based Architecture

Each node type has a corresponding Pydantic model that defines its configuration:

```python
# Example: FilterInput for filter nodes
class FilterInput(BaseModel):
    filter_type: str = "advanced"
    advanced_filter: str  # e.g., "[total]>1500"

# Example: GroupByInput for group_by nodes  
class GroupByInput(BaseModel):
    agg_cols: List[AggColl]
```

These models aren't just for validation—they drive the UI forms, serialization, and schema prediction.

### FlowNode: The Orchestrator

The `FlowNode` is the heart of the system. Each FlowNode:
- **Holds the settings** (the Pydantic configuration model)
- **Contains the `_func`** (the NodeFlow function that's a closure capturing those settings)
- **Manages caching** (storing and retrieving intermediate results when needed)

When you chain methods in the FlowFrame API, you're actually creating FlowNodes that compose together. Each node knows how to transform data based on its settings, and can cache its results for debugging or inspection.

### FlowDataEngine: The Data Carrier

The `FlowDataEngine` wraps a Polars LazyFrame or DataFrame and carries data between nodes. The separation is clean:

- **FlowNode** holds the configuration and the transformation function
- **NodeFlow** functions (the `_func` inside each FlowNode) apply transformations to FlowDataEngine
- **FlowDataEngine** carries the actual data through the pipeline

## Schema Prediction Without Execution

FlowFrame can predict the output schema instantly without processing any data. This happens through two mechanisms:

### Lazy Evaluation with Closures

When you call a method like `.filter()`, no data is filtered. Instead, a node is created containing a NodeFlow function—a closure that remembers its settings. As methods chain together, these closures compose into a Polars LazyFrame query plan.

```python
# Get the schema from a node without executing
print([s.get_minimal_field_info() for s in flow.get_node(4).schema])
# [MinimalFieldInfo(name='region', data_type='String'), 
#  MinimalFieldInfo(name='total_revenue', data_type='Float64'), 
#  MinimalFieldInfo(name='total_quantity', data_type='Float64')]
```

The beauty is that Polars tracks the schema through the entire plan without touching the actual data.

### Schema Callbacks

For nodes that can't use lazy evaluation (like external data sources), explicit `schema_callback` functions calculate the output schema using only:
- The node's settings
- The input schema(s)

## Execution Modes

### Performance Mode: Lazy Pull

The default high-performance mode uses pull-based execution:

1. Request for final result "pulls" on the last node
2. Each node recursively pulls from its parents
3. Polars builds an optimized query plan
4. Data flows through once at the end

### Development Mode: Eager Push with Caching

For debugging and inspection, the system uses push-based execution:

1. Topologically sort the graph into execution order
2. For each node in order:
   - Load cached inputs from parent nodes
   - Execute and materialize the transformation
   - Save results to cache

The cache files (like `cache_Node_1.arrow`) allow inspection of intermediate results in the UI.

## System Architecture

FlowFrame consists of three services:

- **Designer** (Vue/Electron): The frontend UI for visual pipeline building
- **Core** (FastAPI): Manages the DAG, orchestration, and schema prediction  
- **Worker** (FastAPI): Executes Polars transformations and manages caching

The Worker is isolated so heavy data operations don't block the main application and can scale independently.

## Contributing

The codebase is organized as:

```
flowfile/
├── flowfile_core/       # DAG management and node definitions
├── flowfile_worker/      # Polars execution and caching
└── flowfile_frontend/    # Vue/Electron UI
```

Start by looking at existing node implementations in `flowfile_core/nodes/`. Each node is just a Pydantic settings model paired with a NodeFlow transformation function—simple and self-contained.

The beauty of FlowFrame's architecture is that adding new transformations is straightforward: define a settings model, implement the transformation, and both the UI and code API automatically support it. Welcome aboard!