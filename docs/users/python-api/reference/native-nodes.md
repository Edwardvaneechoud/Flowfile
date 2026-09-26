# Native Node Classes

This page covers the canvas node types that have no fluent `FlowFrame` method, and the Python classes that place them: `Gate`, `FlowInput` / `to_flow_output`, `RunFlow`, `custom_node` / `CustomNode`, `python_script` / `PythonScript` and the generic `Node`, plus the helpers they use (flow parameters, flow references, flow registration, the `custom_nodes` registry). Each call adds one node to the same `FlowGraph` the fluent methods build, so the flow opens in the designer like any other.

The examples use `import flowfile as ff`. The tested ones run in CI on every commit and share these imports:

```python
--8<-- "docs/examples/native_nodes.py:imports"
```

## Common surface

Every class returns an object with the same accessors:

| Accessor | Returns |
|---|---|
| `.output` | The only output frame (on a `Gate`, the `.then` exit). Raises `NativeNodeError` naming the outputs when the node has more than one. |
| `node["name"]`, `node.get_output(name)` | The output frame with that name; `name` may also be a `ff.FlowOutput`. An unknown name raises `NativeNodeError` listing the outputs. |
| `.outputs` | The output names, in handle order (`output-0` first). |
| `.node_id`, `.node`, `.flow_graph` | The node id, the placed core `FlowNode`, and the graph it lives on. |

The `custom_node(...)` factory and a `@ff.python_script` function are callables instead: calling one returns the output frame, and its `.node(...)` method returns the node object with these accessors.

Every class takes `description: str | None = None`, the label shown on the canvas. Classes that can be built without input frames take `flow_graph: FlowGraph | None = None`, the graph to place the node on; when it is omitted a new graph is created, as the readers do. Input frames that live on different graphs are merged onto one graph first, as `join` does.

## Deferred frames

Most nodes produce their output lazily in-process, so building a chain predicts each step's schema without reading data. Some nodes cannot: a subflow call, code that runs in a kernel container, a source that reads an external system. Their output frames are **deferred**.

- A deferred frame holds a typed, zero-row placeholder with the node's predicted columns. **Building never runs the node**; its output exists once the flow runs.
- Operations on a deferred frame build nodes as usual and return deferred frames. Their schemas are predicted from the placeholder.
- A writer, `output`, `api_response`, `flow_output`, `explore_data` or model node (`train_model`, `apply_model`, `evaluate_model`) below a deferred frame is placed, but it does not write, publish or train until the flow runs.
- `collect()` on a deferred frame runs **this frame's lineage** with `flow_graph.run_graph(node_ids=...)`, then returns the node's output; every call runs it again. `describe()`, `profile()`, `fetch()` and `collect_async()` do the same.
- A deferred node stays deferred across runs: once a change upstream or `flow_graph.reset()` drops its result, building below it serves the placeholder again instead of running the node.

| Deferred output | Why |
|---|---|
| `ff.RunFlow` | The child flow runs with the parent. |
| `ff.PythonScript`, a `@ff.python_script` function | The code runs in a kernel container. |
| `ff.CustomNode` with `kernel=`, a custom node whose class declares `node_type="output"`, a custom node whose schema needs data, or an installed custom node on a graph that does not execute locally | The node runs in a kernel or the worker, is an output node, or its columns are only known after it ran. |
| `ff.Node("google_analytics_reader" / "external_source", ...)` | The node reads an external system. |
| `ff.Node(..., deferred=True)`, `ff.CustomNode(..., deferred=True)` | Requested. |
| Any frame built from a deferred frame | Inherited. |

What `collect()` on a deferred frame does:

1. Runs this node and its ancestors only; writers, output nodes and subflows on other branches do not run.
2. Judges the result on those nodes: a failed or unconfigured ancestor raises `NativeNodeError` with the node errors.
3. Returns the zero-row frame with the predicted columns when a gate routed this frame away (the node was deliberately skipped, or the frame is a gate's dead exit).
4. Otherwise returns the node's real output.

!!! warning "Kernel nodes need Docker at run time"
    When the graph holds a kernel node (`PythonScript` or a `@ff.python_script` function, a kernel `CustomNode`), running it contacts Docker to reach the kernel container. Building those nodes needs neither Docker nor a running kernel.

A frame that is neither deferred nor below a [gate](#gate) keeps its plain behaviour: `collect()` evaluates its lazy plan in-process.

## `Gate`

A pass-through node whose downstream only runs when its condition holds. See [Gate](../../visual-editor/nodes/combine.md#gate) for the canvas node.

```python
ff.Gate(
    frame: FlowFrame,
    formula: str | Expr | None = None,
    *,
    parameter: str | Parameter | None = None,
    operator: GateOperatorLiteral | GateOperator = "equals",
    value: Any = None,
    control: FlowFrame | None = None,
    else_output: bool = True,
    description: str | None = None,
)
```

Give exactly one condition:

- **Formula** (positional): a [flowfile formula](../../formulas/index.md), or an expression with a formula form, such as `ff.col("amount") > 500`, which is converted as `filter` converts it; an expression without one raises. The gate is open when at least one row of `control`, else of `frame`, matches. `control` is wired to the gate's control input, the bottom pip on the canvas.
- **Parameter**: a flow parameter, by name or as an [`ff.Parameter`](#flow-parameters), compared with `operator` and `value`. The parameter must be declared first with [`add_flow_parameter`](#flow-parameters); an undeclared name raises at build.

| `operator` | `ff.GateOperator` | Open when the parameter |
|---|---|---|
| `"equals"` | `EQUALS` | equals `value` |
| `"not_equals"` | `NOT_EQUALS` | differs from `value` |
| `"in"` | `IN` | is one of `value` (a list) |
| `"not_in"` | `NOT_IN` | is none of `value` (a list) |
| `"is_true"` | `IS_TRUE` | reads as boolean true (`true`, `1`, `yes`, `on`) |
| `"is_false"` | `IS_FALSE` | does not read as true |
| `"is_set"` | `IS_SET` | is not empty |

The string and the enum member are interchangeable:

=== "Literal"

    ```python
    gate = ff.Gate(orders, parameter="mode", operator="equals", value="full")
    ```

=== "Enum"

    ```python
    gate = ff.Gate(orders, parameter="mode", operator=ff.GateOperator.EQUALS, value="full")
    ```

`value` is stored in the form the canvas stores: booleans as `"true"` / `"false"`, lists comma-joined, a `ff.Parameter` as its `${name}` reference. A list with any operator other than `in` / `not_in` raises.

| Accessor | Returns |
|---|---|
| `.then` | The exit that is live when the condition holds (`output-0`). |
| `.otherwise`, `.else_` | The exit that is live when it does not (`output-1`). |
| `.output` | Same as `.then`. |
| `.outputs` | `["then", "else"]`, or `["main"]` with `else_output=False`. |
| `.is_open` | Whether the condition holds now. |

`else_output=False` builds a single-exit gate; `.otherwise` then raises. Both exits of one gate cannot feed the same node; that raises `NativeNodeError`.

`.is_open` on a parameter gate evaluates the graph's current parameter values. On a formula gate it evaluates the probed frame lazily plus a one-row collect; when that frame is deferred it raises, because the placeholder has no rows.

**Routing.** `.then` and `.otherwise` are pass-through frames while you build. `collect()` on any frame below a gate runs the flow and returns only the live side, and so do `describe()`, `profile()` and `fetch()`: a frame on the dead side collects to a zero-row frame with its columns. In the run, the dead exit's downstream is deliberately skipped (`NodeResult.skipped` is `True`, the run stays green). Each such call runs the frame's lineage, not the rest of the graph.

A writer below a gate (a `write_*` method's Output node, `to_flow_output`, `ff.Node("output", ...)` and the other writer, output and model nodes listed under [Deferred frames](#deferred-frames)) is placed without writing and writes when the flow runs, on the live side only; so does `sink_csv`, `sink_ipc` or `sink_ndjson`, each an alias of its `write_*` method. The other `sink_*` methods and the Polars-code fallbacks of `write_*` (extra writer options) write when they are built, so they raise `NativeNodeError` below a gate.

To bring the two sides back together, use a `union` node, which runs when at least one input survived: `ff.concat([...], how="diagonal_relaxed")` places one. Every other node below a dead exit is skipped too, including the Polars-code node the default `ff.concat` places.

This diamond routes on a parameter, checks which side the run skipped, and collects only the live side:

```python
--8<-- "docs/examples/native_nodes.py:gate"
```

## Flow parameters

Parameters are the `${name}` values that gates and node settings read. They are what **Flow settings** lists in the designer and what `flowfile run flow --param` overrides on a [headless run](../../deployment/cli.md).

```python
ff.Parameter(
    name: str,
    *,
    default: Any = "",
    type: ParamTypeLiteral | ParamType = "string",
    description: str = "",
    enum_values: list[str] | None = None,
)

ff.add_flow_parameter(flow: FlowGraph | FlowFrame, parameter: Parameter) -> Parameter

ff.set_flow_parameter(flow: FlowGraph | FlowFrame, name: str | Parameter, value: Any) -> None
```

A `Parameter` is the declaration: `type` is one of `"string"`, `"integer"`, `"float"`, `"boolean"`, `"enum"` (or the `ff.ParamType` member of the same name), and `"enum"` needs `enum_values`. The default is stored as a string (booleans lowercase) and checked against the type when the `Parameter` is created. It exposes `name`, `type`, `default` (typed), `dtype` (the Polars dtype: `String`, `Int64`, `Float64` or `Boolean`) and `ref` (`"${name}"`), is equal to another `Parameter` with the same name, and can key a dict.

`add_flow_parameter` gives the graph its own copy of the declaration and returns the `Parameter`, so `set_flow_parameter` changes that graph only. Declaring a name twice on one graph raises, and so does setting a name that was never declared.

### Referencing parameters in expressions

A `Parameter` is a value. Use it directly in an expression, `ff.col("amount") >= min_amount`, or wrap it with `ff.lit(min_amount)`; it is typed from its declaration, so it needs no cast. Such a predicate still becomes a native Filter or Formula node that stores the bare `${min_amount}`, as the canvas does. Plain strings keep working: a bare `${name}` inside a formula or filter expression, `flowfile_formula="[amount] >= ${min_amount}"`, is rendered as a typed literal, and inside Polars code a reference sits in a string literal, `pl.lit("${min_amount}")`.

Building a node uses the parameter's current value, so a frame collected while you build reflects the default at that moment. A run (`run_graph()`, `collect()` on a deferred frame or one below a gate, a `RunFlow` call) substitutes the values of that run. A reference to a parameter the graph does not declare raises `NativeNodeError` when the node is built, also on a graph that declares no parameters.

**Not supported.** Parameters are values, never column names. A `Parameter`, or a string holding `${name}`, in a column-name position raises `NativeNodeError`:

- `ff.col(...)`, `alias`, `.name.prefix`, `.name.suffix`, `rename`, `select`, `drop`, `sort`, `group_by`, `unique(subset=)`, `pivot`, `unpivot`, `with_row_index(name=)`, `text_to_rows` and the keyword constraints of `filter` / `filter_split`
- join keys (`on=`, `left_on=`, `right_on=`)
- `FlowInput(schema=)` and `FlowInput(sample=)` keys, `to_flow_output` names and `RunFlow` input names (keywords or `inputs=` keys)
- a `[${name}]` column reference in a formula: `filter(flowfile_formula=...)`, `with_columns(flowfile_formulas=...)` and a `Gate` formula

A `Parameter` passed as an argument to an expression method, such as `clip(p)`, `fill_null(p)` or `is_in([p])`, also raises `NativeNodeError`, and the message names what works instead: `ff.lit(p)` for methods that take an expression, such as `clip`. A parameter's value cannot be another parameter: `ff.set_flow_parameter(flow, name, other_parameter)` raises.

!!! note "Pass a frame after a merge"
    Joining frames from two graphs, or a native node over them, merges them into a new graph object. An older `FlowGraph` handle no longer holds the nodes. Pass a frame to the parameter helpers, or re-read `frame.flow_graph`.

## Catalog navigation

Registered flows are reached through the same catalog handles as tables. Each call looks the name up immediately and raises when it is missing:

```python
clean = ff.get_catalog("Demo").get_schema("sales").get_flow("Clean orders")   # FlowRef
flows = ff.get_catalog("Demo").get_schema("sales").list_flows()               # list[FlowRef]
```

The `get_catalog(...).get_schema(...)` chain reads the same as `flowfile_ctx.get_catalog(...).get_schema(...)` inside a kernel. See [Catalog References](catalog-references.md) for `get_catalog`, `get_flow`, `list_flows` and `register_flow` on a schema.

## `FlowInput` and `to_flow_output`

A child flow receives data through Flow Input nodes and returns it through Flow Output nodes. See [Subflows](../../visual-editor/subflows.md) for how a parent calls it.

```python
ff.FlowInput(
    name: str,
    *,
    schema: Mapping[str, PolarsDataType] | Sequence[tuple[str, PolarsDataType]] | None = None,
    sample: Mapping[str, Sequence[Any]] | pl.DataFrame | None = None,
    flow_graph: FlowGraph | None = None,
    description: str | None = None,
) -> FlowFrame

ff.FlowOutput(name: str, *, description: str | None = None)

FlowFrame.to_flow_output(name: str | FlowOutput, *, description: str | None = None) -> FlowFrame
```

`FlowInput` places a Flow Input node and returns its frame. Give at most one of `schema` and `sample`. `schema` (a dict, `pl.Schema` or list of `(name, dtype)` pairs) declares the columns as a zero-row typed frame; write nested dtypes in full, for example `ff.List(ff.Int64)`. `sample` (a dict of columns or a DataFrame) stores rows that a standalone run of the child reads. With neither, the input is empty. A duplicate input name raises, and so does one of `RunFlow`'s own parameter names (`name`, `schema`, `inputs`, `params`, ...).

`to_flow_output` places a Flow Output node below the frame and returns the **same frame**: the Flow Output node has no outgoing handle, so nothing chains from it.

The name can also be declared once, `large_orders = ff.FlowOutput("large_orders")`, and passed both to `to_flow_output` and to the caller's `run.get_output(large_orders)` (see the [`RunFlow`](#runflow) example). Its `description` labels the Flow Output node when `to_flow_output` gives none. A `FlowOutput` is equal to another with the same name and can key a dict; an empty name raises.

**Port order is creation order.** A caller sees the child's inputs and outputs in the order the `FlowInput` and `to_flow_output` calls were made.

## `register_flow`

```python
ff.register_flow(
    flow_or_frame: FlowGraph | FlowFrame,
    *,
    name: str,
    schema: SchemaReference | None = None,
    overwrite: bool = False,
) -> FlowRef
```

Saves the flow as a YAML file and registers it in the catalog **when it is called**, then returns a [`FlowRef`](#flowref-and-flow_ref). `schema` defaults to the `General > Python Editor` schema. The graph is laid out first so it opens cleanly on the canvas; the file lands in the Python-editor flows folder (`~/.flowfile/flows/python_editor_flows/` by default), and the graph's `flow_settings.path` points at it afterwards.

Re-running a script is idempotent: an existing registration with the same name in the same schema whose file is in that folder is rewritten in place and keeps its id and uuid. A same-name registration whose file is elsewhere (a flow saved from the designer) raises `NativeNodeError`; `overwrite=True` writes over that file instead.

## `FlowRef` and `flow_ref`

`FlowRef` is an immutable, hashable and picklable handle to one registered flow. Get one from `flow_ref`, `register_flow` or a schema's `get_flow`; the constructor only holds values and checks nothing.

| Attribute | Meaning |
|---|---|
| `registration_id` | Catalog registration id. |
| `flow_uuid` | Stable uuid of the flow. |
| `flow_path` | Absolute path of the flow file. |
| `name` | Registration name. |
| `schema` | The `SchemaReference` it is registered under (`None` outside a schema), so `ref.schema.read_table(...)` works from the same handle. |
| `namespace_full_name` | `"catalog.schema"`, or `None` outside a schema. |
| `to_subflow_reference()` | The reference a Run Flow node stores. |

```python
ff.flow_ref(
    namespace: str | SchemaReference | CatalogReference | None = None,
    name: str | None = None,
    *,
    uuid: str | None = None,
    registration_id: int | None = None,
) -> FlowRef
```

`flow_ref` resolves a flow by name, by `uuid`, or by `registration_id`, looking it up immediately. `namespace` narrows a name lookup: a `"catalog.schema"` string (a bare catalog name selects the catalog itself), a schema or catalog handle, or `None` for every namespace. A `uuid` or `registration_id` is exact and is cross-checked against `name` and `namespace` when those are given too. The registration must point at an existing flow file by absolute path.

```python
ff.flow_ref("Demo.sales", "Clean orders")
ff.flow_ref(uuid="4c1f...")
ff.flow_ref(registration_id=12)
```

Flow names are unique neither across schemas nor within one. A name that matches more than one registration raises instead of picking one, listing the candidates. A flow shared with nobody you belong to is left out of name matches, and an exact `uuid` / id for one raises. Every failed lookup raises `NativeNodeError` with the fix, chained from the `flowfile_core.catalog` error it stands for (`AmbiguousFlowError`, `FlowNotFoundError`, `NotAuthorizedError`, `NamespaceNotFoundError`).

## `RunFlow`

Calls a registered flow as a subflow, like the [Run Flow](../../visual-editor/nodes/combine.md#run-flow) node on the canvas.

```python
ff.RunFlow(
    flow: FlowRef | int | FlowGraph | FlowFrame,
    *,
    name: str | None = None,
    schema: SchemaReference | None = None,
    overwrite: bool = False,
    inputs: Mapping[str, FlowFrame] | None = None,
    params: Mapping[str | Parameter, Any] | None = None,
    param_frame: FlowFrame | None = None,
    iterate: bool = False,
    append_metadata: bool = True,
    description: str | None = None,
    flow_graph: FlowGraph | None = None,
    **input_frames: FlowFrame,
)
```

- `flow`: a `FlowRef`, a registration id, or an unregistered `FlowGraph` / `FlowFrame`. An unregistered flow needs `name=`; it is then registered with [`register_flow`](#register_flow) at build time, under `schema=` and with `overwrite=` as `register_flow` takes them. Passing `name`, `schema` or `overwrite=True` with a `FlowRef` or an id raises.
- `inputs`, `**input_frames`: `inputs` maps the child's Flow Input names to frames; a keyword per input, `orders=frame`, is the short form. An input given both ways raises, and so does a name the child does not have, listing the child's inputs. An input left out falls back to the child's sample data.
- `params`: keyed by child parameter name or `ff.Parameter`; a name the child does not declare raises, and an omitted parameter keeps the child's default. A constant is stored as a string (booleans lowercase) and checked against the parameter's type at build. A `ff.Parameter` of the calling flow, or its `"${name}"` string, forwards that parameter: the child receives the value of each run. A plain column (`ff.col("region")`) binds the parameter to that column of `param_frame`, which is then required and must have the column. `param_frame` without any column binding raises.
- `iterate=True` runs the child once per row of `param_frame` and concatenates the outputs; it needs a parameter bound to a column, and a `param_frame` of more than 1000 rows fails the run. `False` uses the first row. With `iterate` and `append_metadata`, each output gets a `run_index` column and a `param_<name>` column per bound parameter.

Outputs are [deferred](#deferred-frames) and named after the child's Flow Outputs. Read one with `run["large_orders"]` or `run.get_output("large_orders")`, by name or by `ff.FlowOutput`, or with `run.output` when there is exactly one. A child without Flow Outputs has one output, `"main"`: a summary row per run (`run_index`, `success`, and a `param_<name>` column per bound parameter). `run.flow` is the `FlowRef` that runs.

The node stores the flow's uuid, registration id, `catalog.schema` and name. A run finds the flow by uuid, then by registration id when the reference has no uuid, then by schema and name, so a flow saved on another install still resolves; an id that holds a different flow there is refused. Subflows nest at most 5 deep; a deeper call fails the run.

```python
--8<-- "docs/examples/native_nodes.py:subflow"
```

## `custom_node` and `CustomNode`

Places a [custom node](../../visual-editor/node-designer.md) authored with `node_designer`. Writing the node class itself is covered in [Custom Nodes in Code](../../visual-editor/creating-custom-nodes.md).

### `custom_node(...)` factory

```python
ff.custom_node(node: type[CustomNodeBase] | CustomNodeBase | str) -> CustomNodeFactory
```

Resolves the node class once and returns a callable whose keyword arguments are the node's settings components, flat:

```python
trim = ff.custom_node(TrimNode)
cleaned = trim(orders, upper=True)                      # FlowFrame
kept = ff.custom_node("deduper").node(a, b)["kept"]     # CustomNode, for several outputs
```

- `factory(*inputs, kernel=None, deferred=None, schemas=None, description=None, settings=None, flow_graph=None, **components) -> FlowFrame` returns the single output frame. A node with several outputs raises and points at `.node(...)`.
- `factory.node(*inputs, ...) -> CustomNode` takes the same arguments and returns the node object.
- An unknown keyword raises and lists the valid names. A component name that appears in two sections, or that clashes with `kernel`, `deferred`, `schemas`, `description`, `settings` or `flow_graph`, is only reachable as `section__component`. Naming a component both in `settings=` and as a keyword raises.
- `help(factory)` and `inspect.signature(factory)` list the components with their defaults.

### `CustomNode`

The canonical form, one-to-one with what the node stores: settings nested per section.

```python
ff.CustomNode(
    node: type[CustomNodeBase] | CustomNodeBase | str,
    *inputs: FlowFrame,
    settings: dict[str, dict[str, Any]] | None = None,
    kernel: str | Any | None = None,
    deferred: bool | None = None,
    schemas: Mapping[str, Mapping[str, PolarsDataType]] | None = None,
    description: str | None = None,
    flow_graph: FlowGraph | None = None,
)
```

`node` is a class, an instance (its settings values are the base; an instance that overrides any other field raises, since only settings are saved), or the type name of an installed node. An unknown section or component raises, and so does a class whose node name collides with a built-in node or a different installed node. The number of input frames must match the node's `number_of_inputs`. `.node_class`, `.settings` (the stored envelope) and `.kernel` hold what was placed.

A class defined in your script is registered for the session: a flow saved with it opens elsewhere as "not installed" until you [install it](#custom_nodes). `ff.register_flow` and `open_graph_in_editor` warn about such classes.

Kernel rules: an `environment="kernel"` node needs `kernel=` (a kernel id or an object with an `.id`, as for `PythonScript`; not validated at build), and `kernel=` on an `environment="local"` node raises.

- **Build.** A local node's `process()` runs at build to produce its lazy plan. The output is [deferred](#deferred-frames) for a kernel node, a `node_type="output"` node, a node whose schema needs data, and an installed node on a graph that does not execute locally. `deferred=` overrides this, as on [`ff.Node`](#node).
- **`schemas=`** gives a deferred node without a `predict_output_schema` hook its placeholder columns, `{output: {column: dtype}}`, as on `PythonScript`.
- **Parameters.** A setting may be a [`ff.Parameter`](#flow-parameters) or a `"${name}"` string; it is stored as `${name}`, must be declared on the graph, and reaches `process()` resolved (numbers and booleans typed by the component).
- **Secrets.** Building reads a `SecretSelector` secret, so it must exist where you build; otherwise pass `deferred=True`.

The same settings two ways, built but not run:

```python
--8<-- "docs/examples/native_nodes.py:custom-node"
```

A parameter as a setting, a deferred placement, and the same node by key:

```python
--8<-- "docs/examples/native_nodes.py:custom-node-options"
```

### `custom_nodes`

`ff.custom_nodes` holds the custom nodes this process can place, by node key:

```python
ff.custom_nodes.list()                      # CustomNodeInfo per node file (with any load error) and session class
ff.custom_nodes.get("Trim Text")            # the factory, by key or display name
ff.custom_nodes.trim_text(orders)           # attribute form
ff.custom_nodes.install(TrimNode)           # or a path to a .py file; overwrite=False by default
```

`install` writes `<key>.py` to the custom-nodes directory and registers it. A class is written with the `NodeSettings` classes and imports it uses; a class that reads other module-level names is refused, so install its file instead. The written file must load the way the designer loads it; one that does not is removed again, a file it replaced is put back, and `install` raises. A running designer shows a new node after **Settings → Extensions → Custom Nodes → Rescan**.

An unknown key raises `NativeNodeError`; in the attribute form the error is also an `AttributeError`, so `hasattr(ff.custom_nodes, name)` is `False`.

## `python_script` and `PythonScript`

Places a [Python Script](../../visual-editor/kernels.md#python-script-node) node, whose code runs in a kernel container. The `@ff.python_script` decorator turns a function into the node's notebook; `ff.PythonScript` takes the cells as strings.

### `python_script(...)` decorator

```python
ff.python_script(
    fn: Callable | None = None,
    /,
    *,
    kernel: str | Any | None = None,
    outputs: list[str] | None = None,
    returns: Mapping[str, PolarsDataType] | Mapping[str, Mapping[str, PolarsDataType]] | None = None,
    description: str | None = None,
    flow_graph: FlowGraph | None = None,
) -> PythonScriptFunction | Callable[[Callable], PythonScriptFunction]
```

The decorator works bare, `@ff.python_script`, or with the options above. The function's parameters are the node's inputs, each a `pl.LazyFrame` in the kernel; its body is the notebook, and its final `return` is what the node publishes:

```python
--8<-- "docs/examples/native_nodes.py:script-decorator"
```

- Calling the function, `forecast(monthly)`, places the node and returns its output frame, [deferred](#deferred-frames) like every Python Script output. Pass one `FlowFrame` per parameter, in order; a wrong count raises and lists the parameter names.
- `forecast.node(monthly)` places the node and returns the `PythonScript` object with the [common accessors](#common-surface). A function with several outputs is placed this way.
- `forecast.fn` is the undecorated function. The cell markers are comments, so it runs locally on Polars frames, without a kernel.
- `forecast.cells` holds the generated notebook cells, built once when the function is decorated.
- `kernel` is stored as given, as for [`PythonScript`](#pythonscript); `description` defaults to the function name. `flow_graph` is used only by a function without parameters, which has no input frame to take a graph from.

The function's source is read from the file or notebook cell that defines it, or from PyCharm's Python console. That console runs a selection through Python's `code` module, as `code.interact()` does, and does not keep its text. flowfile reads it from the selection being run, so a selection that imports flowfile and defines the function works, and in an interactive session or PyCharm's console it records every selection run after the import (any other console needs `FLOWFILE_CONSOLE_SOURCE=1` set before flowfile is imported). A function whose definition ran before flowfile was imported cannot be recovered; run its definition again. The plain `python` prompt before Python 3.13 keeps no source, and neither does code run with `exec()`; decorating a function defined there raises `NativeNodeError`.

The function is a module-level `def`. Lambdas, nested functions, methods, generators, `async` functions and functions wrapped by another decorator raise, and so do `*args`, `**kwargs`, keyword-only and positional-only parameters, and parameters with a default. These checks, and the rules below, run when the function is decorated and raise `NativeNodeError` there.

The node stores only the cells. A saved flow opens in the designer as an ordinary Python Script notebook; edits made there do not change the function.

### Cells and notes

The body is split at [Jupytext percent-format](https://jupytext.readthedocs.io/en/latest/formats-scripts.html) markers, the cell syntax PyCharm, VS Code and Spyder run cell by cell:

| In the body | In the notebook |
|---|---|
| The docstring | The first note. |
| Code before the first marker | The first code cell. |
| `# %%` | Starts a code cell. Text after the marker is the cell's title, kept as its first line: `# %% Forecast` starts the cell with `# Forecast`. |
| `# %% [markdown]` (or `[md]`) | Starts a note: the `#` comment lines right below it. The first line that is not a comment, a blank line included, ends the note and starts a code cell. |

A note is a cell of commented text, the form the editor gives the markdown cells of an imported notebook. The kernel runs the cells as one script, so cell boundaries only matter in the designer. Each cell is dedented by the body's indentation, so nested blocks keep their relative indentation; empty cells are dropped. Markers sit between top-level statements of the body: a marker inside an `if`, `for`, `with` or `try` block raises. A `# %%` inside a string is not a marker.

`forecast.cells` from the example above, one block per cell (the `# ----` lines mark the boundaries here and are not part of the cells):

```python
# ---- 0: prelude, the names the body reads from its module
import polars as pl
# ---- 1: inputs, one line per parameter
# flowfile: inputs
monthly = flowfile_ctx.read_inputs()["main"][0]
# ---- 2: the docstring
# Revenue trend: a least-squares line through monthly revenue, extended three months.
# ---- 3: the code before the first marker
import numpy as np

df = monthly.collect()
# ---- 4: the note
# ## Fit
# One slope for the whole period; good enough for a demo, not for a quarter close.
# ---- 5: a code cell
slope, intercept = np.polyfit(df["month"], df["revenue"], deg=1)
ahead = np.arange(df["month"].max() + 1, df["month"].max() + 4)
# ---- 6: the titled last cell, its return rewritten into the publish
# Forecast
# flowfile: outputs
_result = pl.DataFrame({"month": ahead, "revenue_forecast": slope * ahead + intercept})
flowfile_ctx.publish_output(_result, "main")
```

The prelude, the inputs cell (a function without parameters has none) and the docstring note are present only when they have content. `read_inputs()["main"]` lists the inputs in wiring order, so parameter *i* reads input *i*.

**One `return`, at the end.** The body returns exactly once, as its last top-level statement, never inside an `if`, loop, `with` or `try`. A single frame (`pl.DataFrame` or `pl.LazyFrame`) is published to the one output, `"main"` unless `outputs=["name"]` names it. A dict of frames is published one output per key: `outputs=` lists the keys, a dict literal whose keys differ from it raises, and the last cell checks that `_result` is a dict with exactly those keys when the flow runs. A dict held in a variable is not visible at decoration: it is published per key only when `outputs=` names two or more outputs, and as one frame otherwise. Without `outputs=`, a returned value that is not a call, such as a variable, is checked when the flow runs, and a dict fails the run, naming `outputs=` as the fix. A tuple or list of frames raises. Two inputs and two outputs, placed with `.node(...)`:

```python
--8<-- "docs/examples/native_nodes.py:script-outputs"
```

### Names from outside the function

The body runs in the kernel without the rest of its module, and the kernel imports nothing on its own. A name the body reads from its module goes into the prelude cell:

- A **module** becomes an import line under the name the body uses: `pl` gives `import polars as pl`, which is why the forecast notebook starts with it.
- A **plain constant** (a number, string, bytes, `None`, or a list, tuple, dict or set of those, whose `repr` reads back as the same value) becomes an assignment, such as `WINDOW = 3`.
- **Anything else** raises: a function or class, a name imported with `from numpy import polyfit`, a `FlowFrame`, a flowfile module such as `ff` (the kernel has no flowfile package), `__file__` (the kernel runs the cells without a file), or a name not defined yet where the function is decorated (a helper further down the file). The message names it: `` `polyfit` is used inside `forecast` but lives outside it; move it into the function or pass it as an input ``.

Imports come first, in order of first use, then constants. Imports written inside the body stay in the body, as `import numpy as np` does above. Builtins are left alone, and so are `flowfile_ctx`, `display` and `explore`, which the kernel defines; a parameter with one of those three names raises.

### Declaring the output columns

While you build, the output is a placeholder. Without `returns=`, it has the first input's columns (none without inputs), so a step that names a returned column, such as `revenue_forecast` above, fails at build. `returns=` declares the columns:

- `{column: dtype}` for a function with one output, as in the forecast example.
- `{output: {column: dtype}}` for several. An output left out keeps the first input's columns, as `matched` does above.

The values are Polars dtypes (`ff.Int64`, `pl.Float64`); anything else raises, and so does an output that `outputs=` does not list. The declaration only shapes the placeholder: a run publishes whatever the function returns, and the declaration is not saved with the flow.

### `PythonScript`

The low-level form, one-to-one with what the node stores: the cells as strings. Use it for code that is not a function, such as a notebook copied from the designer.

```python
ff.PythonScript(
    *inputs: FlowFrame,
    code: str | None = None,
    cells: list[str] | None = None,
    kernel: str | Any | None = None,
    outputs: list[str] | None = None,
    schemas: Mapping[str, Mapping[str, PolarsDataType]] | None = None,
    description: str | None = None,
    flow_graph: FlowGraph | None = None,
)
```

- Give exactly one of `code` or `cells`. The node stores both forms: the cells, and `code` as the non-empty cells joined by blank lines (what the kernel executes).
- `kernel` is a kernel id, or an object with an `.id`, stored as given. It is **not** checked at build: a missing or unknown kernel fails when the flow runs.
- `outputs` names the output handles (default `["main"]`); publish to them with `flowfile_ctx.publish_output(df, "name")`.
- `schemas` declares output columns as `{output: {column: dtype}}`, the nested form of `returns=`, with the same checks.
- Inputs are wired in order. Inside the kernel, `flowfile_ctx.read_input()` reads all of them; each is also readable by name, which is the upstream node's reference if set, else `df_<node_id>`.

Outputs are [deferred](#deferred-frames); an output not declared in `schemas` has the first input's schema (no columns without inputs). `.code`, `.cells` and `.kernel` hold what was stored. See the [`flowfile_ctx` API](../../visual-editor/kernel-api.md) for the code side.

```python
--8<-- "docs/examples/native_nodes.py:script"
```

## `Node`

Places any built-in node type from its type name and settings.

```python
ff.Node(
    node_type: NodeTypeLiteral | NodeType,
    *inputs: FlowFrame,
    settings: dict[str, Any] | BaseModel | None = None,
    deferred: bool | None = None,
    description: str | None = None,
    flow_graph: FlowGraph | None = None,
)
```

- `node_type` is a type name (`"api_response"`) or its `ff.NodeType` member (`ff.NodeType.API_RESPONSE`).
- `settings` is the node's settings model or a dict of its fields; the models live in `flowfile_core.schemas.input_schema`. An unknown top-level key raises. The node sets `flow_id`, `node_id`, the position, `is_setup`, `user_id` and `depending_on_id(s)` itself and refuses them in `settings`.
- Multi-input nodes (`union`, `polars_code`, `sql_query`, `python_script`) take every frame on one input, in order; `sql_query` reads them as `input_1`, `input_2`, .... Other nodes take frame *i* on input *i*, at most three.
- `deferred` overrides whether the output is deferred; `deferred=False` with a deferred input frame raises, since building would run the node on placeholder rows.
- `"promise"`, `"polars_lazy_frame"` and custom node types are refused; use `ff.FlowFrame(lazy_frame)` or [`ff.CustomNode`](#customnode) for those. `run_flow` is refused too, because its inputs are keyed by slot; use [`ff.RunFlow`](#runflow). `flow_input` and `flow_output` work, but the dedicated helpers above are the normal route, as is [`ff.sql`](flowframe-operations.md#sql-queries) for `sql_query`, which also names its tables.

An API Response node, which has no fluent method, with its settings as a dict:

```python
--8<-- "docs/examples/native_nodes.py:node"
```

## Errors

Every build or materialisation failure raises `ff.NativeNodeError`, a subclass of `ValueError`: bad arguments, wrong input counts, refused connections, a failed ancestor during `collect()`, and the checks `@ff.python_script` runs when it decorates a function. `flow_ref`, `register_flow` and `RunFlow` raise it for catalog failures too, chained from the `flowfile_core.catalog` error (`NamespaceNotFoundError`, `FlowNotFoundError`, `AmbiguousFlowError`, `FlowExistsError`, `NotAuthorizedError`); the `get_catalog(...).get_schema(...)` handles raise `NamespaceNotFoundError` itself. A node that fails to build is removed from the graph again.

## Known limits

- **`pivot` below a deferred frame raises at build.** Its columns come from the pivot column's values, which the zero-row placeholder does not have: `ValueError: Failed to fetch unique values`. When a `flowfile_worker` process answers that probe instead, the pivot builds with only its index columns. Collect the input first.
- **Build-time effects are refused on deferred frames and below a gate.** `inspect`, the `sink_*` methods that are not `write_*` aliases and the Polars-code fallbacks of `write_parquet` / `write_csv` / `write_excel` raise `NativeNodeError` on both; expressions without a code form raise on deferred frames. Use a `write_*` method with a native writer node, or collect first.
- **Three inputs at most** on nodes with fixed inputs, custom nodes included; the canvas has the same limit.
- **Local custom nodes run `process()` at build** to build their lazy plan, as canvas schema prediction does. Pass `deferred=True` to place one without running it.
- **Kernel ids are not validated at build.** `collect()` on a deferred frame re-runs its lineage, and contacts Docker when that lineage holds a kernel node.
- **A bare `${param}` token in Polars code fails at build.** Polars code is checked as Python when the node is added, before parameters are substituted. A reference inside a string literal, `pl.lit("${min_amount}")`, builds; a bare `${min_amount}` token outside one is a syntax error, so `ff.Node("polars_code", ...)` raises `NativeNodeError`.
- **Reader paths do not resolve `${name}` at build.** `ff.read_csv("${dir}/x.csv")` and the other readers open the path as written when they are called, so a reference in it fails with `FileNotFoundError`. Pass the resolved path from Python.
- **Registration writes at build time.** `register_flow` and `RunFlow(graph, name=...)` write a YAML file and a catalog row when called.
- **`train_model(publish_to_catalog=True)` needs a registered flow.** On a graph built in Python, call `ff.register_flow(flow, name=...)` first.
- **Reopened flow names.** A registered flow keeps its registration name when reopened only if its file is in the Python-editor flows folder; elsewhere the designer names it after the file.
- **`FlowFrame(data, flow)` treats the second positional argument as `schema`.** Pass `flow_graph=` by keyword.
- **`custom_node(...)` keywords are checked at call time** from the node's settings schema; IDEs do not complete them.
- **`@ff.python_script` needs the function's source.** Files, notebook cells and PyCharm's Python console provide it; in the console, a definition run before flowfile was imported has to be run again. The plain `python` prompt before Python 3.13 keeps none.
- **Notebook variables are shared across a flow's scripts.** The body runs at the top level of the kernel's namespace for the flow, so a name it assigns (even one that shadows a builtin, such as `max`) is visible to the flow's other Python Script nodes.
- **An upstream node referenced as `main` fails a script's run.** `read_inputs()["main"]` holds every input in wiring order, so running the script refuses an input of that name. Give the node another reference.
- **Code export emits `ff.sql`, not these classes.** The FlowFrame export writes a SQL Query node as `ff.sql(...)`, a gate as `if` blocks and a custom node as its inlined `process()`; a Python Script node does not become a `@ff.python_script` function.
- **No typed class per built-in node.** Node types without a dedicated class above are placed with `ff.Node` and a settings dict or model.

---
[← Previous: Catalog References](catalog-references.md)
