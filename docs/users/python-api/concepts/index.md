# Core Concepts

Understanding the key concepts behind Flowfile's Python API will help you build better pipelines.

**New here? Read in this order:** [Quick Start](../quickstart.md) → [FlowFrame & FlowGraph](design-concepts.md) → [Formula Syntax](expressions.md).

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

### [Formula Syntax](expressions.md)
Flowfile's Excel-like formula syntax for expressions.

**You'll learn:**
- When to use `[column]` vs `ff.col("column")`
- Supported operations and functions
- How formulas translate to Polars
- Best practices for each syntax

**Key takeaways:**
- Formulas make simple operations more readable
- Great for users coming from Excel/Tableau
- Both syntaxes can be mixed in the same pipeline

## Quick Overview

### FlowFrame vs DataFrame

| DataFrame (Pandas/Polars) | FlowFrame (Flowfile) |
|--------------------------|---------------------|
| Holds data in memory | Always lazy (data not loaded) |
| Operations execute immediately | Operations build a plan |
| No operation history | Full operation history in graph |
| Can't visualize workflow | Can open in visual editor |

For lazy evaluation, graph tracking, and visual integration in depth, see the
[FlowFrame and FlowGraph guide](design-concepts.md).

## Learn More

- **Deep dive:** Read the full [FlowFrame and FlowGraph guide](design-concepts.md)
- **Expressions:** Master the [Formula Syntax](expressions.md)
- **Practice:** Try the [tutorials](../tutorials/index.md)

---

*These concepts are the foundation of Flowfile. Understanding them will make everything else click!*