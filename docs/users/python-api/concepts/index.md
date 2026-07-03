# Core Concepts

These guides explain the model behind Flowfile's Python API: what a FlowFrame is, how expressions and formulas differ, and why every operation is lazy and graph-connected.

## Guides

### [FlowFrame and FlowGraph](design-concepts.md)

The fundamental building blocks. Covers how a FlowFrame differs from a DataFrame, how the FlowGraph tracks each operation as a node, why everything is lazy, and how the code and visual representations connect.

### [Expressions](expressions.md)

Polars-style column operations — the default way to express transformations. Column references, arithmetic, conditional logic with `ff.when`, filtering, and the `.str` / `.dt` / `.list` namespaces.

### [Formulas in Python](formulas.md)

The FlowFrame methods that accept Flowfile formula strings: `with_columns(flowfile_formulas=...)`, `filter(flowfile_formula=...)`, and `filter_split`. The formula language itself (syntax, operators, functions) is documented in the [Formula Language guide](../../formulas/index.md).

## The lazy model

A FlowFrame is always lazy: operations build a plan and append a node to the FlowGraph, and nothing runs until `.collect()`. See [FlowFrame and FlowGraph](design-concepts.md) for a worked example.
