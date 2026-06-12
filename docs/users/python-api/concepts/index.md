# Core Concepts

Understanding the key concepts behind Flowfile's Python API will help you build better pipelines.

## Available Guides

### [FlowFrame and FlowGraph](design-concepts.md)
The fundamental building blocks of Flowfile pipelines.

**You'll learn:**
- What FlowFrame is and how it differs from DataFrames
- How FlowGraph tracks your operations
- Why everything is lazy by default
- How visual and code representations connect

**Key takeaways:**
- FlowFrame = Your data + its transformation history
- FlowGraph = The complete pipeline blueprint
- Every operation creates a node in the graph

---

### [Expressions](expressions.md)
Polars-style column operations — the default way to express transformations.

**You'll learn:**
- Column references, arithmetic, and conditional logic with `ff.col` / `ff.when`
- Filtering with expression predicates
- How expressions compose into one lazy query plan

---

### [Formulas in Python](formulas.md)
The FlowFrame methods that accept Flowfile formula strings.

**You'll learn:**
- Creating columns with `with_columns(flowfile_formulas=...)`
- Filtering with `filter(flowfile_formula=...)` and `filter_split`
- When to reach for a formula instead of an expression

**Key takeaway:** the formula language itself (syntax, operators, all functions) is documented separately in the [Formula Language guide](../../formulas/index.md).

## Quick Overview

### FlowFrame vs DataFrame

| DataFrame (Pandas/Polars) | FlowFrame (Flowfile) |
|--------------------------|---------------------|
| Holds data in memory | Always lazy (data not loaded) |
| Operations execute immediately | Operations build a plan |
| No operation history | Full operation history in graph |
| Can't visualize workflow | Can open in visual editor |

### The Lazy Advantage

```python
# This doesn't load the 10GB file!
df = ff.read_csv("huge_file.csv")

# Still no data loaded - just building the plan
df = df.filter(ff.col("country") == "USA")
df = df.select(["id", "amount"])

# NOW it loads only what's needed
result = df.collect()  # Might only read 100MB!
```

### Visual Integration

Every FlowFrame knows its history:

```python
# Build a complex pipeline
pipeline = (
    ff.read_csv("input.csv")
    .filter(ff.col("active") == True)
    .group_by("category")
    .agg(ff.col("revenue").sum())
)

# See the entire pipeline visually
ff.open_graph_in_editor(pipeline.flow_graph)

# The graph shows all 4 operations as connected nodes
```

## Why These Concepts Matter

Understanding these concepts helps you:

1. **Write efficient code** - Leverage lazy evaluation
2. **Debug effectively** - Visualize your pipeline
3. **Collaborate better** - Share visual representations
4. **Optimize performance** - Understand what executes when

## Learn More

- **Deep dive:** Read the full [FlowFrame and FlowGraph guide](design-concepts.md)
- **Expressions:** Learn [Polars-style expressions](expressions.md)
- **Formulas:** Use the [formula-accepting methods](formulas.md)
- **Practice:** Try the [tutorials](../tutorials/index.md)

---

*These concepts are the foundation of Flowfile. Understanding them will make everything else click!*