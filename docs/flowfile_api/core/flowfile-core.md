# Flowfile Core for Developers

The core functionality of Flowfile contains the HTTP interface, Graph, Nodes, Engine, and workers. 
Here it is described how the engine works, how you can integrate it with your system and where you can find what.

## FlowGraph

A flow graph is an object that manages the execution, connection of nodes, and addition of nodes. 
It ensures that a process starts, is executing in the right order and in case of cancelling every process is cancelled.

### Creating a Flow Graph

You can construct a graph and start adding settings to it to see it evolve:

```python
from flowfile_core.flowfile.flow_graph import FlowGraph
from flowfile_core.schemas.schemas import FlowSettings

# Config of how and where the flow should execute, what it is named and where it is stored
flow_settings = FlowSettings(
    flow_id=1,
    name="My ETL Pipeline",
    execution_location='local',  # 'local', 'remote', or 'auto'
    execution_mode='Development'  # 'Development' or 'Performance'
)

graph = FlowGraph(flow_settings=flow_settings)
```

### Adding Nodes to the Graph

The FlowGraph provides methods to add different types of nodes. Each node represents a transformation or data operation:

```python
from flowfile_core.schemas import input_schema
from flowfile_core.schemas.transform_schema import FilterInput, BasicFilter

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

# Add a filter node
filter_node = input_schema.NodeFilter(
    flow_id=graph.flow_id,
    node_id=2,
    filter_input=FilterInput(
        filter_type="advanced",
        advanced_filter="[age] > 28"
    )
)
graph.add_filter(filter_node)
```

### Connecting Nodes

Nodes must be connected to define the data flow:

```python
from flowfile_core.flowfile.flow_graph import add_connection

# Connect node 1 (manual input) to node 2 (filter)
connection = input_schema.NodeConnection.create_from_simple_input(
    from_id=1,
    to_id=2,
    input_type="main"  # "main", "left", or "right"
)
add_connection(graph, connection)
```

### Executing the Graph

Once your graph is built, you can execute it:

```python
# Run the entire graph
graph.run_graph()

# Check execution status
run_info = graph.get_run_info()
print(f"Success: {run_info.success}")
print(f"Nodes completed: {run_info.nodes_completed}/{run_info.number_of_nodes}")

# Cancel execution if needed
if graph.flow_settings.is_running:
    graph.cancel()
```

### Inspecting the Graph

The FlowGraph provides various methods to inspect its structure:

```python
# Get all nodes
nodes = graph.nodes  # List of FlowNode objects

# Get a specific node
node = graph.get_node(node_id=2)

# Get node connections
connections = graph.node_connections  # List of (source_id, target_id) tuples

# Get starting nodes
start_nodes = graph._flow_starts

# Visualize the graph structure
for node in graph.nodes:
    print(f"Node {node.node_id}: {node.node_type}")
    if node.leads_to_nodes:
        for target in node.leads_to_nodes:
            print(f"  → Node {target.node_id}")
```

### Saving and Loading Graphs

Graphs can be persisted and loaded:

```python
# Save the graph
graph.save_flow(flow_path="my_pipeline.flowfile")

# Load a graph
import pickle
with open("my_pipeline.flowfile", "rb") as f:
    flow_info = pickle.load(f)
    # Reconstruct the graph from flow_info
```

## FlowNode

FlowNodes are the building blocks of a FlowGraph. Each node represents a single transformation or operation in your data pipeline.

### Node Structure

Every FlowNode contains:

```python
class FlowNode:
    # Identity
    node_id: Union[str, int]           # Unique identifier
    node_type: str                      # Type of operation (filter, join, etc.)
    
    # Configuration
    setting_input: Any                  # Pydantic model with node settings
    _function: Callable                 # The transformation function
    
    # Connections
    node_inputs: NodeStepInputs         # Input nodes (main, left, right)
    leads_to_nodes: List[FlowNode]      # Output nodes
    
    # State
    results: NodeResults                # Execution results and cache
    node_stats: NodeStepStats           # Execution statistics
    schema: List[FlowfileColumn]        # Output schema
```

### Creating Custom Nodes

You can create custom nodes by defining a transformation function and settings:

```python
from flowfile_core.flowfile.flow_node.flow_node import FlowNode
from pydantic import BaseModel

# Define settings for your custom node
class CustomTransformSettings(BaseModel):
    flow_id: int
    node_id: int
    multiplier: float = 2.0
    column_name: str = "value"

# Define the transformation function
def custom_transform(df: FlowDataEngine) -> FlowDataEngine:
    return df.apply_flowfile_formula(
        func=f"[{settings.column_name}] * {settings.multiplier}",
        col_name=f"{settings.column_name}_scaled"
    )

# Add the custom node to a graph
settings = CustomTransformSettings(
    flow_id=graph.flow_id,
    node_id=3,
    multiplier=1.5,
    column_name="age"
)

node = graph.add_node_step(
    node_id=settings.node_id,
    function=custom_transform,
    node_type="custom_transform",
    setting_input=settings,
    input_node_ids=[2]  # Connect to node 2
)
```

### Node Execution

Nodes execute in a specific order based on their dependencies:

```python
# Individual node execution
node = graph.get_node(2)

# Execute the node
node.execute_node(
    run_location='local',
    performance_mode=False,
    node_logger=graph.flow_logger.get_node_logger(2)
)

# Check results
if node.results.errors:
    print(f"Error: {node.results.errors}")
else:
    data = node.get_resulting_data()
    print(f"Output shape: {data.number_of_records} rows, {data.number_of_fields} columns")
```

### Schema Prediction

Nodes can predict their output schema without executing:

```python
# Get predicted schema
node = graph.get_node(2)
schema = node.get_predicted_schema()

for column in schema:
    print(f"Column: {column.column_name}, Type: {column.data_type}")

# Schema callbacks for dynamic schema
def schema_callback():
    input_schema = node.singular_main_input.schema
    # Calculate output schema based on input
    return [
        FlowfileColumn.from_input("new_column", "Float64"),
        *input_schema  # Keep all input columns
    ]

node.schema_callback = schema_callback
```

### Node Caching

Nodes can cache their results for faster re-execution:

```python
# Enable caching for a node
node.node_settings.cache_results = True

# Check if cache exists
if results_exists(node.hash):
    print("Cache available")

# Force cache refresh
node.reset(deep=True)
```

## FlowDataEngine

The FlowDataEngine is the data carrier that wraps Polars DataFrames/LazyFrames and provides additional functionality for the Flowfile ecosystem.

### Creating a FlowDataEngine

```python
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine
import polars as pl

# From a dictionary
data = FlowDataEngine({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "score": [95, 87, 92]
})

# From a list of dictionaries
data = FlowDataEngine([
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 87}
])

# From a Polars DataFrame/LazyFrame
df = pl.DataFrame({"col1": [1, 2, 3]})
data = FlowDataEngine(df)

# From a file
data = FlowDataEngine.create_from_path(
    input_schema.ReceivedCsvTable(
        path="data.csv",
        delimiter=",",
        has_headers=True
    )
)
```

### Data Transformations

FlowDataEngine provides a rich API for transformations:

```python
# Filtering
filtered = data.do_filter("[score] > 90")

# Selecting columns
selected = data.select_columns(["id", "name"])

# Adding computed columns
with_computed = data.apply_flowfile_formula(
    func="[score] * 1.1",
    col_name="adjusted_score"
)

# Grouping and aggregation
from flowfile_core.schemas.transform_schema import GroupByInput, AggColl

grouped = data.do_group_by(
    GroupByInput(
        agg_cols=[
            AggColl("department", "groupby"),
            AggColl("score", "mean", "avg_score"),
            AggColl("id", "count", "count")
        ]
    )
)

# Joining
join_result = data.join(
    join_input=JoinInput(
        join_mapping=[JoinMap("id", "user_id")],
        left_select=["id", "name"],
        right_select=["user_id", "department"],
        how="left"
    ),
    other=other_data,
    auto_generate_selection=True,
    verify_integrity=True
)
```

### Lazy vs Eager Evaluation

FlowDataEngine supports both lazy and eager evaluation:

```python
# Check if lazy
if data.lazy:
    print("Data is lazy (not materialized)")

# Convert between lazy and eager
data.lazy = True   # Make lazy
data.lazy = False  # Materialize

# Collect data (materialize if lazy)
df = data.collect(n_records=100)  # Get first 100 records
df_all = data.collect()            # Get all records

# Get sample without full materialization
sample = data.get_sample(n_rows=10, random=True)
```

### Schema Management

FlowDataEngine provides rich schema information:

```python
# Get schema
schema = data.schema
for col in schema:
    print(f"{col.column_name}: {col.data_type}")
    print(f"  Unique values: {col.number_of_unique_values}")
    print(f"  Null values: {col.number_of_empty_values}")
    
# Calculate detailed schema statistics
data._calculate_schema_stats = True
detailed_schema = data.calculate_schema()

# Access specific column info
col_info = data.get_schema_column("score")
print(f"Is unique: {col_info.is_unique}")
print(f"Percentage unique: {col_info.perc_unique}")
```

### Cloud Storage Integration

FlowDataEngine seamlessly integrates with cloud storage:

```python
from flowfile_core.schemas.cloud_storage_schemas import (
    CloudStorageReadSettingsInternal,
    CloudStorageWriteSettingsInternal,
    FullCloudStorageConnection, CloudStorageWriteSettings, CloudStorageReadSettings
)

# Read from S3
connection = FullCloudStorageConnection(
    storage_type="s3",
    auth_method="access_key",
    aws_access_key_id="your_key",
    aws_secret_access_key="your_secret",
    aws_region="us-east-1"
)

settings = CloudStorageReadSettingsInternal(
    read_settings=CloudStorageReadSettings(
        resource_path="s3://bucket/data.parquet",
        file_format="parquet"
    ),
    connection=connection
)

data = FlowDataEngine.from_cloud_storage_obj(settings)

# Write to cloud storage
write_settings = CloudStorageWriteSettingsInternal(
    write_settings=CloudStorageWriteSettings(
        resource_path="s3://bucket/output.parquet",
        file_format="parquet",
        write_mode="overwrite"
    ),
    connection=connection
)

data.to_cloud_storage_obj(write_settings)
```

### External Processing

For large operations, FlowDataEngine can offload processing to workers:

```python
from flowfile_core.flowfile.flow_data_engine.subprocess_operations import (
    ExternalDfFetcher,
    ExternalSampler
)

# Offload heavy computation to worker
fetcher = ExternalDfFetcher(
    lf=data.data_frame,
    flow_id=1,
    node_id=1,
    wait_on_completion=False
)

# Check status
status = fetcher.status
print(f"Processing status: {status.status}")

# Get result when ready
result = fetcher.get_result()
```

## Advanced Topics

### Custom Node Types

Create a complete custom node type with UI support:

```python
from flowfile_core.configs.node_store import nodes
from pydantic import BaseModel

# 1. Define the settings model
class MyCustomSettings(BaseModel):
    flow_id: int
    node_id: int
    threshold: float
    operation: str = "filter"

# 2. Register the node template
nodes.add_node_template(
    nodes.NodeTemplate(
        name="My Custom Node",
        item="custom_operation",
        input=1,
        output=1,
        image="custom.svg",
        node_group="transform",
        can_be_start=False
    )
)

# 3. Add the transformation method to FlowGraph
def add_custom_operation(graph, settings: MyCustomSettings):
    def transform(df: FlowDataEngine) -> FlowDataEngine:
        if settings.operation == "filter":
            return df.do_filter(f"[value] > {settings.threshold}")
        else:
            return df
    
    graph.add_node_step(
        node_id=settings.node_id,
        function=transform,
        node_type="custom_operation",
        setting_input=settings
    )

# Monkey-patch the method onto FlowGraph
FlowGraph.add_custom_operation = add_custom_operation
```

### Execution Strategies

Different execution strategies for different scenarios:

```python
# Development mode: Step-by-step with caching
graph.flow_settings.execution_mode = 'Development'
graph.flow_settings.execution_location = 'local'
graph.run_graph()  # Each node caches results

# Performance mode: Optimized lazy execution
graph.flow_settings.execution_mode = 'Performance'
graph.flow_settings.execution_location = 'remote'
graph.run_graph()  # Builds complete Polars plan

# Hybrid: Auto-select based on operation
graph.flow_settings.execution_location = 'auto'
# Wide operations (joins, sorts) → remote
# Narrow operations (filters, projections) → local
```

### Error Handling and Recovery

Robust error handling patterns:

```python
# Node-level error handling
node = graph.get_node(2)
try:
    node.execute_node(
        run_location='local',
        retry=True,  # Auto-retry on failure
        node_logger=logger
    )
except Exception as e:
    # Check node state
    if node.results.errors:
        print(f"Node error: {node.results.errors}")
    
    # Attempt recovery
    node.reset(deep=True)
    node.execute_node(run_location='local', retry=False)

# Graph-level error handling
try:
    graph.run_graph()
except Exception as e:
    # Get detailed run info
    run_info = graph.get_run_info()
    for node_result in run_info.node_step_result:
        if not node_result.success:
            print(f"Failed node {node_result.node_id}: {node_result.error}")
```

### Memory Management

Optimize memory usage for large datasets:

```python
# Use streaming execution for large datasets
data = FlowDataEngine(large_dataset, streamable=True)

# Process in batches
for batch in data.iter_batches(batch_size=10000):
    processed = batch.apply_flowfile_formula(
        func="[value] * 2",
        col_name="doubled"
    )
    processed.save("output_batch.parquet")

# Clear caches when done
for node in graph.nodes:
    node.remove_cache()
```

## Testing Your Pipelines

### Unit Testing Nodes

```python
import pytest
from flowfile_core.flowfile.flow_data_engine.flow_data_engine import FlowDataEngine

def test_custom_transform():
    # Arrange
    input_data = FlowDataEngine({
        "value": [1, 2, 3, 4, 5]
    })
    
    # Act
    result = input_data.apply_flowfile_formula(
        func="[value] * 2",
        col_name="doubled"
    )
    
    # Assert
    output = result.collect()
    assert output["doubled"].to_list() == [2, 4, 6, 8, 10]

def test_schema_prediction():
    # Test that schema is correctly predicted
    node = create_test_node()
    schema = node.get_predicted_schema()
    
    assert len(schema) == 3
    assert schema[0].column_name == "id"
    assert schema[0].data_type == "Int64"
```

### Integration Testing Graphs

```python
def test_complete_pipeline():
    # Build test graph
    graph = FlowGraph(FlowSettings(flow_id=1))
    
    # Add test data
    test_data = create_test_data()
    graph.add_manual_input(test_data)
    
    # Add transformations
    graph.add_filter(create_filter_node())
    graph.add_group_by(create_groupby_node())
    
    # Connect nodes
    add_connection(graph, NodeConnection.create_from_simple_input(1, 2))
    add_connection(graph, NodeConnection.create_from_simple_input(2, 3))
    
    # Execute
    run_info = graph.run_graph()
    
    # Verify
    assert run_info.success
    assert run_info.nodes_completed == 3
    
    # Check output
    final_node = graph.get_node(3)
    result = final_node.get_resulting_data()
    assert result.number_of_records == expected_count
```

## Performance Optimization

### Query Plan Optimization

```python
# Inspect the Polars query plan
node = graph.get_node(5)
data = node.get_resulting_data()
print(data.data_frame.explain())  # Show optimized plan

# Force optimization hints
data.data_frame = data.data_frame.with_optimizations(
    projection_pushdown=True,
    predicate_pushdown=True,
    type_coercion=True,
    simplify_expression=True
)
```

### Parallel Processing

```python
from concurrent.futures import ThreadPoolExecutor

# Process multiple graphs in parallel
graphs = [create_graph(i) for i in range(5)]

with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(g.run_graph) for g in graphs]
    results = [f.result() for f in futures]
```

## Debugging

### Enable Detailed Logging

```python
import logging
from flowfile_core.configs import logger

# Set log level
logger.setLevel(logging.DEBUG)

# Add custom handler
handler = logging.FileHandler('flowfile_debug.log')
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Node-specific logging
node_logger = graph.flow_logger.get_node_logger(node_id=2)
node_logger.info("Starting custom transformation")
```

### Inspect Intermediate Results

```python
# Enable caching for all nodes
for node in graph.nodes:
    node.node_settings.cache_results = True

# Run graph
graph.run_graph()

# Inspect cached results
for node in graph.nodes:
    if node.results.example_data_path:
        # Read cached Arrow file
        import pyarrow.parquet as pq
        table = pq.read_table(node.results.example_data_path)
        df = table.to_pandas()
        print(f"Node {node.node_id}: {df.shape}")
        print(df.head())
```

## Best Practices

1. **Always validate inputs**: Use Pydantic models for settings validation
2. **Handle schema changes explicitly**: Use schema callbacks for dynamic schemas
3. **Cache judiciously**: Cache expensive operations, not simple filters
4. **Use lazy evaluation**: Keep data lazy as long as possible
5. **Test schema prediction**: Verify schemas before execution
6. **Monitor memory**: Use streaming for large datasets
7. **Log appropriately**: Use node loggers for debugging

## Next Steps

- Explore the [node catalog](./node-catalog.md) for all available transformations
- Learn about [custom node development](./custom-nodes.md)
- Understand [worker architecture](./workers.md) for distributed processing
- Review [API endpoints](./api-reference.md) for HTTP integration

# Reference
::: flowfile_core.flowfile.flow_graph.FlowGraph
::: flowfile_core.flowfile.flow_graph
::: flowfile_core.flowfile.flow_node.flow_node.FlowNode