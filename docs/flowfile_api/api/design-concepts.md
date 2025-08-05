# FlowFrame and FlowGraph Design Concepts

Understanding how FlowFrame and FlowGraph work together is key to mastering Flowfile. This guide explains the core design principles that make Flowfile both powerful and intuitive.

## FlowFrame: Always Lazy, Always Connected

### What is FlowFrame?

FlowFrame is Flowfile's version of a Polars DataFrame with a crucial difference: **it's always lazy and always connected to a graph**.

```python
import flowfile as ff

# This creates a FlowFrame, not a regular DataFrame
df = ff.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "amount": [100, 250, 80, 300, 150],
    "category": ["A", "B", "A", "C", "B"]
})
print(type(df))  # <class 'FlowFrame'>
print(type(df.data))  # <class 'polars.LazyFrame'>
```

### Key Properties of FlowFrame

#### 1. Always Lazy Evaluation
FlowFrame never loads your actual data into memory until you explicitly call `.collect()`. This means you can build complex transformations on massive datasets without consuming memory:

```python
# None of this processes any data yet
df = (
    ff.FlowFrame({
        "id": [1, 2, 3, 4, 5],
        "amount": [500, 1200, 800, 1500, 900], 
        "category": ["A", "B", "A", "C", "B"]
    })                                # Creates manual input node
    .filter(ff.col("amount") > 1000)  # No filtering happens yet
    .group_by("category")             # No grouping happens yet
    .agg(ff.col("amount").sum())     # No aggregation happens yet
)

# Only now does the data get processed
result = df.collect()  # Everything executes at once, optimized
```

#### 2. Connected to a DAG (Directed Acyclic Graph)
Every FlowFrame has a reference to a FlowGraph that tracks every operation as a node:

```python
df = ff.FlowFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "active": [True, False, True]
})
print(df.flow_graph)  # Shows the graph this FlowFrame belongs to
print(df.node_id)     # Shows which node in the graph this FlowFrame represents
```

#### 3. Linear Operation Tracking
Each operation creates a new node in the graph, even if you repeat the same operation:

```python
df = ff.FlowFrame({
    "id": [1, 2, 3, 4],
    "amount": [50, 150, 75, 200],
    "status": ["active", "inactive", "active", "active"]
})
print(f"Initial graph has {len(df.flow_graph.nodes)} nodes")

# First filter - creates node 1
df1 = df.filter(ff.col("amount") > 100)
print(f"After first filter: {len(df1.flow_graph.nodes)} nodes")

# Second identical filter - creates node 2 (not reused!)
df2 = df.filter(ff.col("amount") > 100)  
print(f"After second filter: {len(df2.flow_graph.nodes)} nodes")

# Both operations are tracked separately in the graph
```

## FlowGraph: The Operation History

### What is FlowGraph?

FlowGraph is the "brain" behind FlowFrame - it's a Directed Acyclic Graph (DAG) that tracks every step in your data transformation pipeline.

```python
# Access the graph from any FlowFrame
df = ff.FlowFrame({
    "product": ["Widget", "Gadget", "Tool"],
    "price": [10.99, 25.50, 8.75],
    "quantity": [100, 50, 200]
})
graph = df.flow_graph

print(f"Graph ID: {graph.flow_id}")
print(f"Number of operations: {len(graph.nodes)}")
print(f"Node connections: {graph.node_connections}")
```

### How the DAG Works

Each operation becomes a node with dependencies:

```python
df = ff.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "category": ["A", "B", "A", "C", "B"],
    "amount": [100, 200, 150, 300, 250],
    "active": [True, False, True, True, False]
}, description="Load source data")
# Graph: [Manual Input Node]

filtered = df.filter(ff.col("active") == True, description="Keep active records") 
# Graph: [Manual Input Node] → [Filter Node]

aggregated = filtered.group_by("category").agg(ff.col("amount").sum())
# Graph: [Manual Input Node] → [Filter Node] → [Group By Node]

# Each FlowFrame points to its node in the graph
print(f"Source node: {df.node_id}")
print(f"Filter node: {filtered.node_id}")  
print(f"Aggregation node: {aggregated.node_id}")
```

### Linear Execution Model

**Important**: Flowfile tracks operations linearly, meaning identical operations create separate nodes:

```python
source = ff.FlowFrame({
    "id": [1, 2, 3, 4],
    "amount": [50, 150, 75, 200],
    "category": ["A", "B", "A", "B"]
})

# Scenario 1: Running the same operation twice
df1 = source.filter(ff.col("amount") > 100)  # Creates filter node A
df2 = source.filter(ff.col("amount") > 100)  # Creates filter node B (different!)

print(df1.node_id != df2.node_id)  # True - different nodes!

# The graph now looks like:
# [Manual Input] → [Filter A]
#              ↘ [Filter B]
```

This is especially important when debugging or experimenting:

```python
df = ff.FlowFrame({
    "product": ["Widget", "Gadget"],
    "amount": [100, 200]
})

# In a Jupyter notebook, running this cell multiple times:
df = df.with_columns([
    (ff.col("amount") * 1.1).alias("amount_with_tax")
])

# Each execution adds a new node to the graph!
# Graph after 3 runs: [Manual Input] → [Transform] → [Transform] → [Transform]
```

## Visual Integration

### Viewing Your Graph
Every FlowFrame can show its complete operation history in the visual editor:

```python
# Build a pipeline
result = (
    ff.FlowFrame({
        "region": ["North", "South", "North", "East", "South"],
        "amount": [1000, 0, 1500, 800, 1200],
        "product": ["A", "B", "A", "C", "B"]
    }, description="Load sales data")
    .filter(ff.col("amount") > 0, description="Remove invalid amounts")
    .group_by("region", description="Group by sales region")
    .agg(ff.col("amount").sum().alias("total_sales"))
)

# Open the entire pipeline in the visual editor
ff.open_graph_in_editor(result.flow_graph)
```

This opens a visual representation showing:
- Each operation as a node
- Data flow between operations  
- Descriptions you added for documentation
- Schema changes at each step

### Real-time Schema Prediction
The DAG enables instant schema prediction without processing data:

```python
df = ff.FlowFrame({
    "product": ["Widget", "Gadget"],
    "price": [10.50, 25.00],
    "quantity": [2, 3]
})
print("Original schema:", df.schema)

# Schema is predicted instantly, no data processed
transformed = df.with_columns([
    (ff.col("price") * ff.col("quantity")).alias("total")
])
print("New schema:", transformed.schema)  # Shows new 'total' column immediately
```

## Practical Implications

### Memory Efficiency
Since FlowFrame is always lazy:

```python
# This can handle large datasets efficiently through lazy evaluation
large_pipeline = (
    ff.FlowFrame({
        "id": list(range(10000)),
        "quality_score": [0.1, 0.9, 0.8, 0.95] * 2500,  # Simulating large data
        "value": list(range(10000)),
        "category": ["A", "B", "C", "D"] * 2500
    })
    .filter(ff.col("quality_score") > 0.95)  # Reduces data early
    .select(["id", "value", "category"])     # Reduces columns early  
    .group_by("category")
    .agg(ff.col("value").mean())
)

# Only processes what's needed when you collect
result = large_pipeline.collect()  # Optimized execution plan
```

### Development vs Production
The linear tracking is perfect for development but needs consideration for production:

```python
# Development: Experiment freely
df = ff.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "value": [10, 20, 30, 40, 50],
    "category": ["A", "B", "A", "C", "B"]
})

debugging = True
if debugging:
    df = df.inspect()  # Adds inspection node
    df = df.head(100)  # Adds sample node for testing

# Production: Build clean pipelines
def create_production_pipeline():
    return (
        ff.FlowFrame({
            "region": ["North", "South", "East", "West"],
            "sales": [1000, 1500, 800, 1200],
            "status": ["active", "active", "inactive", "active"]
        })
        .filter(ff.col("status") == "active")
        .group_by("region")
        .agg(ff.col("sales").sum())
    )
```

### Graph Reuse and Copying
You can work with the same graph across multiple FlowFrames:

```python
# Start with common base
base = ff.FlowFrame({
    "region": ["North", "South", "East"],
    "year": [2024, 2024, 2023],
    "sales": [1000, 1500, 800],
    "product": ["Widget", "Gadget", "Tool"],
    "quantity": [10, 15, 8]
}).filter(ff.col("year") == 2024)

# Create different branches (same graph, different endpoints)
sales_summary = base.group_by("region").agg(ff.col("sales").sum())
product_summary = base.group_by("product").agg(ff.col("quantity").sum())

# Both share the same underlying graph
assert sales_summary.flow_graph is product_summary.flow_graph
```

## Best Practices

### 1. Use Descriptions for Complex Pipelines
```python
pipeline = (
    ff.FlowFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "status": ["active", "inactive", "active", "active", "inactive"],
        "signup_date": ["2024-01-15", "2023-12-10", "2024-02-20", "2023-11-05", "2024-03-01"],
        "customer_segment": ["premium", "basic", "premium", "basic", "premium"],
        "revenue": [1000, 500, 1500, 300, 2000]
    }, description="Load raw customer data")
    .filter(ff.col("status") == "active", description="Keep only active customers")
    .with_columns([
        ff.col("signup_date").str.strptime(ff.Date, "%Y-%m-%d").alias("signup_date")
    ], description="Parse signup dates")
    .group_by("customer_segment", description="Aggregate by customer segment")
    .agg([
        ff.col("revenue").sum().alias("total_revenue"),
        ff.col("customer_id").count().alias("customer_count")
    ], description="Calculate segment metrics")
)
```

### 2. Visualize During Development
```python
# Check your pipeline structure frequently
ff.open_graph_in_editor(pipeline.flow_graph)
```

### 3. Avoid Redundant Operations in Loops
```python
# ❌ Bad: Creates many duplicate nodes
df = ff.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "value": [1, 2, 3, 4, 5]
})
for i in range(10):
    df = df.filter(ff.col("value") > i)  # Creates 10 filter nodes!

# ✅ Good: Build the condition first
df = ff.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "value": [1, 2, 3, 4, 5]
})
final_threshold = 5
df = df.filter(ff.col("value") > final_threshold)  # One filter node
```

## Summary

FlowFrame and FlowGraph work together to provide:

- **Lazy evaluation**: No memory waste, optimal performance
- **Complete lineage**: Every operation is tracked and visualizable  
- **Real-time feedback**: Instant schema prediction and error detection
- **Seamless integration**: Switch between code and visual editing
- **Polars compatibility**: 95% identical API with additional features

Understanding this design helps you build efficient, maintainable data pipelines that scale from quick analyses to production ETL workflows.