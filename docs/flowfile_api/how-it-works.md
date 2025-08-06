# How Flowfile Works

After the initial version of Flowfile was completed, it resulted in the backend in an expressive settings based object in which 
every action was captured in settings and relations between nodes were explicitly set. The expressive nature 
provided high understanding of the settings, good interaction with external systems.

However, due to style of separation of concerns (settings, function, table), it did not lend itself for a nice quick and concise way of writing
Python code as we are used in to in Polars, Pyspark or Pandas. In these frameworks, there is usually a tight coupling
between the object(the dataframe or table) and the transformation one does. Leading to a more interactive way of 
programming. The fact that the table is the starting point increases understanding and the sequence of operations is
automatically determined and easy to understand. 
However, with the introduction of LazyFrame/Lazy evaluated transformations, conceptually there is less of a coupling 
between the object and the transformations: a transformation in Polars will only fail _after_ it started collecting. 

This principle sparked our idea for combining best of both worlds: go from the concise and clean way of Polars and transform
it actually builds the same separated settings <-> object style. This way it would not matter if a user programs in python 
or decides to pick the ui. They _both_ result in the same object. 

We'll use the following example to explain how Flowfile's different parts work together.

```python
import flowfile as ff

raw_data = [
    {"id": 1, "region": "North", "quantity": 10, "price": 150},
    {"id": 2, "region": "South", "quantity": 5, "price": 300},
    # ... more data
]

# Simple filter example in Python
df = ff.FlowFrame(raw_data)
df = df.filter(flowfile_formula="[price] > 200")
```

## The Flowfile Architecture

Data pipelines in Flowfile can be built through visual drag-and-drop or Python code. Both approaches create the same underlying graph structure.

In traditional data tools, visual interfaces and code are completely separate. You either get the accessibility of visual tools or the power of code, but not both.

We wanted to unify this in Flowfile by having visual operations and Python code be two views of the exact same system. Every drag-and-drop action has an equivalent in code, and every line of code can be visualized.

### TLDR

Under the hood, Flowfile builds a Flow Graph where each node represents a data transformation. Each node contains settings (configuration), dependencies (connections to other nodes), and a step function (the transformation logic). Whether you use the visual editor or write Python code, you're building the same graph that compiles down to optimized Polars operations.

## Two Ways to Build, Same Pipeline

Let's look at a complete pipeline that calculates regional sales metrics:

### 1. Visual Editor (Drag & Drop)

When you drag nodes onto the canvas and connect them, you're building this flow:

```
[Manual Input] → [Formula: total] → [Filter: total>1500] → [Group By: region] → [Output: parquet]
    Node 1           Node 2              Node 3                Node 4              Node 5
```

**Behind the scenes**, when you drag and drop nodes in the visual editor, here's what actually happens:

```python
# This is what happens internally when you use the visual editor
def flow_graph_implementation():
    from flowfile_core.schemas import nodes, transformations, RawData
    from flowfile_core.flowfile.flow_graph import add_connection

    flow = ff.create_flow_graph()
    
    # User drags a "Manual Input" node onto canvas
    node_manual_input = nodes.NodeManualInput(
        flow_id=flow.flow_id, 
        node_id=1,
        raw_data_format=RawData.from_pylist(raw_data)
    )
    flow.add_manual_input(node_manual_input)

    # User drags a "Formula" node onto canvas
    formula_node = nodes.NodeFormula(
        flow_id=1,
        node_id=2,
        function=transformations.FunctionInput(
            field=transformations.FieldInput(name="total", data_type="Double"),
            function="[quantity] * [price]"
        )
    )
    flow.add_formula(formula_node)
    
    # User connects Node 1 to Node 2
    add_connection(flow, nodes.NodeConnection.create_from_simple_input(1, 2))

    # User drags a "Filter" node onto canvas
    filter_node = nodes.NodeFilter(
        flow_id=1,
        node_id=3,
        filter_input=transformations.FilterInput(
            filter_type="advanced",
            advanced_filter="[total]>1500"
        )
    )
    flow.add_filter(filter_node)
    
    # User connects Node 2 to Node 3
    add_connection(flow, nodes.NodeConnection.create_from_simple_input(2, 3))

    # User drags a "Group By" node onto canvas
    group_by_node = nodes.NodeGroupBy(
        flow_id=1,
        node_id=4,
        groupby_input=transformations.GroupByInput(
            agg_cols=[
                transformations.AggColl("region", "groupby"),
                transformations.AggColl("total", "sum", "total_revenue"),
                transformations.AggColl("total", "mean", "total_quantity")
            ]
        )
    )
    flow.add_group_by(group_by_node)
    
    # User connects Node 3 to Node 4
    add_connection(flow, nodes.NodeConnection.create_from_simple_input(3, 4))
    
    # User drags an "Output" node onto canvas
    output_node = nodes.NodeOutput(
        flow_id=1,
        node_id=5,
        output_settings=nodes.OutputSettings(
            name="output.parquet",
            file_type="parquet"
        )
    )
    flow.add_output(output_node)
    
    # User connects Node 4 to Node 5
    add_connection(flow, nodes.NodeConnection.create_from_simple_input(4, 5))
```

### 2. FlowFrame API (Writing Python Code)

```python
def flow_frame_implementation():
    df_1 = ff.FlowFrame(raw_data)
    df_2 = df_1.with_columns(
        flowfile_formulas=['[quantity] * [price]'], 
        output_column_names=["total"]
    )
    df_3 = df_2.filter(flowfile_formula="[total]>1500")
    df_4 = df_3.group_by(['region']).agg([
        ff.col("total").sum().alias("total_revenue"),
        ff.col("total").mean().alias("total_quantity"),
    ])
    df_4.sink_parquet("output.parquet")
```

### The Key Insight

Whether you use the visual editor or write Python code, you're building the exact same Flow Graph. The visual editor is essentially creating the node structures shown above, while the FlowFrame API is a more concise way to build the same graph.

### The Magic: Identical Execution Plans

Both approaches produce **exactly** the same Polars execution plan:

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

## The Four Moving Parts

### 1. Nodes = Operations

Each transformation is a FlowNode that contains:
- **Settings**: Configuration for the operation (stored in `_setting_input`)
- **Dependencies**: Which other nodes feed into this one (`node_inputs` and `leads_to_nodes`)
- **Step Function**: The actual code that transforms the data (`_function`)
- **Schema Callback**: Logic to predict output structure (`_schema_callback`)
- **Hash**: Unique identifier for caching results (`_hash`)

```python
# When you write this...
df.filter(flowfile_formula="[total]>1500")

# Or drag a filter node in the visual editor...

# Both create this node:
NodeFilter(
    node_id=3,
    filter_input=FilterInput(
        filter_type="advanced",
        advanced_filter="[total]>1500"
    )
)

# The node's setting_input contains all the configuration
node.setting_input  # FilterInput(filter_type="advanced", advanced_filter="[total]>1500")
```

### 2. Settings = Configuration Models

Every node has a Pydantic settings model that defines what the operation does:

```python
class FilterInput(BaseModel):
    filter_type: str = "advanced"
    advanced_filter: str = "[total]>1500"

class GroupByInput(BaseModel):
    agg_cols: List[AggColl] = [
        AggColl("region", "groupby"),
        AggColl("total", "sum", "total_revenue"),
        AggColl("total", "mean", "total_quantity")
    ]
```

These settings are the single source of truth that drives:
- **Python API**: `df.filter()` creates these settings
- **Visual Editor**: Forms are auto-generated from the Pydantic model
- **Execution**: Step functions use settings as parameters
- **Export**: Settings translate directly to Polars code

### 3. Schema Prediction = Instant Feedback

Flowfile shows you how your data structure changes without processing any data:

```python
# After Node 4 (Group By), we can predict the schema
print([s.get_minimal_field_info() for s in flow.get_node(4).schema])

# Output:
[MinimalFieldInfo(name='region', data_type='String'), 
 MinimalFieldInfo(name='total_revenue', data_type='Float64'), 
 MinimalFieldInfo(name='total_quantity', data_type='Float64')]
```

This enables real-time feedback:
- See new columns appear as you configure transforms
- Catch schema errors before execution
- Understand data flow through complex pipelines

### 4. Graph = Your Complete Pipeline

The graph maintains:
- **Node Dependencies**: Which operations feed into others
- **Execution Order**: Topological sort determines run sequence
- **State Management**: Track progress, errors, and caching
- **Lineage**: Complete history of transformations

## The Settings-Driven Architecture

Everything flows from the Pydantic settings models:

```
User Input → Settings Model → Step Function → Schema Prediction
     ↓              ↓              ↓              ↓
Visual Form    Configuration   Execution    UI Feedback
```

Example: Adding a new operation
1. Define settings: `class NewOpSettings(BaseModel): ...`
2. Write step function: `def new_op_step(lazy_frame, settings): ...`
3. Add schema logic: Auto-predicted or custom callback
4. Visual forms are automatically generated from the Pydantic model

## Two Execution Paths

### Development Mode: Step-by-Step
- Execute each node individually
- Cache intermediate results for inspection
- Perfect for debugging and exploration
- Slower but provides full visibility

### Performance Mode: Optimized Pipeline
- Build complete Polars execution plan
- Execute everything at once
- Leverage full lazy evaluation benefits
- Faster for production workflows

## Why This Works

### Always in Sync

The visual editor and Python code share the exact same infrastructure:

```python
# Code → Visual
pipeline = df.filter(...).group_by(...)
ff.open_graph_in_editor(pipeline.flow_graph)  # Same settings, same nodes

# Visual → Code
# Settings models export directly to equivalent Polars operations
```

### Pure Functions

Each operation is isolated and predictable:
- Same input + same settings = same output
- No hidden state or side effects
- Complete reproducibility

### Lazy Everything

Build complex pipelines without memory overhead:
- Graph construction doesn't process data
- Only execution triggers data movement
- Polars optimizes the entire plan

## Three-Service Architecture

1. **Designer (Vue)**: Visual interface that translates Pydantic schemas to UI components
2. **Core (FastAPI)**: Graph management, schema prediction, settings validation  
3. **Worker (FastAPI)**: Heavy computation, data materialization, caching

The result: You get the accessibility of visual tools with the power and performance of code-first approaches, unified by a settings-driven architecture that keeps everything in perfect sync.