# FlowFrame and FlowGraph

FlowFrame and FlowGraph are the two objects behind every Python pipeline: the always-lazy frame you call methods on, and the graph that records each operation as a node. Understanding how they interact explains almost everything else about the API — why nothing executes until `.collect()`, why every pipeline can open in the visual editor, and why schemas resolve instantly.

## FlowFrame: always lazy, always connected

A FlowFrame looks like a Polars DataFrame but differs in two ways: it is always lazy, and it always belongs to a graph.

```python
import flowfile as ff

df = ff.FlowFrame({
    "id": [1, 2, 3, 4, 5],
    "amount": [100, 250, 80, 300, 150],
    "category": ["A", "B", "A", "C", "B"]
})
print(type(df))       # <class 'flowfile_frame.flow_frame.FlowFrame'>
print(type(df.data))  # <class 'polars.lazyframe.frame.LazyFrame'>
```

### Nothing executes until `.collect()`

Method calls only build the plan. Data is read and processed once, when you collect — with the whole plan visible to the Polars optimizer:

```python
df = (
    ff.FlowFrame({
        "id": [1, 2, 3, 4, 5],
        "amount": [500, 1200, 800, 1500, 900],
        "category": ["A", "B", "A", "C", "B"]
    })
    .filter(ff.col("amount") > 1000)
    .group_by("category")
    .agg(ff.col("amount").sum())   # still nothing has executed
)

result = df.collect()              # everything runs here, optimized as one plan
```

### Every operation becomes a graph node

Each FlowFrame knows its graph and its own position in it:

```python
df = ff.FlowFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
print(df.flow_graph)  # the graph this frame belongs to
print(df.node_id)     # the node this frame represents
```

Operations always append — an identical operation applied twice creates two nodes, and because both frames share one graph, the node count is cumulative:

```python
df = ff.FlowFrame({"id": [1, 2, 3, 4], "amount": [50, 150, 75, 200]})
print(len(df.flow_graph.nodes))   # 1

df1 = df.filter(ff.col("amount") > 100)
print(len(df1.flow_graph.nodes))  # 2

df2 = df.filter(ff.col("amount") > 100)   # identical filter, new node
print(len(df2.flow_graph.nodes))  # 3
```

!!! note "Not every method gets its own node type"
    Operations with a visual-node equivalent (formula-based filters, group-by, joins) appear as that node type; anything else lands in a generic `polars_code` node. The pipeline still works identically — the difference is only how editable the step is in the visual editor. See [Expressions](expressions.md) and [Formulas in Python](formulas.md) for which form produces which.

## FlowGraph: the pipeline's record

The FlowGraph is the DAG all of a pipeline's frames share:

```python
graph = df.flow_graph
print(graph.flow_id)           # graph id
print(len(graph.nodes))        # operations so far
print(graph.node_connections)  # how they're wired
```

### Branching shares the graph

Branches created from a common base are endpoints in one graph, not copies:

```python
base = ff.FlowFrame({
    "region": ["North", "South", "East"],
    "year": [2024, 2024, 2023],
    "sales": [1000, 1500, 800],
    "product": ["Widget", "Gadget", "Tool"],
    "quantity": [10, 15, 8]
}).filter(ff.col("year") == 2024)

sales_summary = base.group_by("region").agg(ff.col("sales").sum())
product_summary = base.group_by("product").agg(ff.col("quantity").sum())

assert sales_summary.flow_graph is product_summary.flow_graph
```

### Schema prediction without execution

Because the graph knows every operation, output schemas resolve immediately — no data is read:

```python
df = ff.FlowFrame({"product": ["Widget", "Gadget"], "price": [10.50, 25.00], "quantity": [2, 3]})
transformed = df.with_columns((ff.col("price") * ff.col("quantity")).alias("total"))
print(transformed.schema)  # Schema([('product', String), ('price', Float64), ('quantity', Int64), ('total', Float64)])
```

`df.schema` returns a Polars `Schema` — a mapping keyed by column name (`transformed.schema["total"]`), not a list.

## Opening a pipeline in the visual editor

Any pipeline can be inspected on the canvas. The `description=` argument most methods accept becomes the node's description there, which is what makes a code-built graph readable to someone else:

```python
result = (
    ff.FlowFrame({
        "region": ["North", "South", "North", "East", "South"],
        "amount": [1000, 0, 1500, 800, 1200]
    }, description="Load sales data")
    .filter(ff.col("amount") > 0, description="Remove invalid amounts")
    .group_by("region")
    .agg(ff.col("amount").sum().alias("total_sales"))
)

ff.open_graph_in_editor(result.flow_graph)
```

See [Visual UI Integration](../reference/visual-ui.md) for how the editor is launched and controlled from Python, and [Export to Python](../../visual-editor/tutorials/code-generator.md) for the reverse direction.

## Related

- [FlowFrame Operations](../reference/flowframe-operations.md) — the transformation methods
- [Expressions](expressions.md) — Polars-style column operations
- [Formulas in Python](formulas.md) — methods that accept Flowfile formula strings
- [Developers guide](../../../for-developers/index.md) — how these objects are implemented
