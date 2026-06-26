# Implementation guide: "Run Flow" node + REST API `${param}` support

Reconstruction of commit **`b5783afb` — "Allow for params in flow and run flow in flows"**
(the single non-merge commit on `feature/improve-params`). This document lets you redo the
work on a clean branch off `main` without the original branch.

- **Commit:** `b5783afbcb35d9a94187439a5309f9cdb1090e97`
- **Scope:** 22 files, **+1143 / −1**
- **Author/date:** edwardvaneechoud, Sat Jun 13 2026

> Shortcut: if you keep the commit around, `git cherry-pick b5783afb` (or
> `git format-patch -1 b5783afb`) reproduces it exactly. The rest of this doc is the
> *understanding* + verbatim code so you can re-apply it by hand on a clean branch.

---

## 1. What the commit actually does (two features in one)

### Feature A — new **Run Flow** node (`run_flow`)
A node that runs a **saved sub-flow once per input row**, mapping input columns to the
sub-flow's `${param}` references. Per-row outputs are unioned (diagonal concat) and a
`__param_value__` (or `__param_<name>__` when >1 param) column records the value used.
The sub-flow's single `api_response` node defines what data is returned. Designed to fan a
pipeline (e.g. a REST API call) across a list of inputs (tickers, ids, …).

Key design points:
- **Sequential** execution (v1) — sub-flow runs share `flow_id`-keyed scratch dirs, so
  rows cannot run in parallel yet. Optional `delay_seconds` between runs (rate limiting).
- **`max_rows` cap** (default 1000); overflow rows are skipped with a logged warning.
- **Recursion guard** — a sub-flow that (transitively) runs the flow containing it is refused.
- **Worker offload per row** — each row's output is materialised on the worker under a unique
  `file_ref` so core never collects; falls back to in-core collect in local mode / no worker.
- **Schema prediction** — opens the sub-flow, reads the single `api_response` node's predicted
  schema, appends the param column(s). Never returns empty (empty would make the engine run
  the function during prediction).

### Feature B — `${param}` support for the **REST API reader** at *sample* time
The flow-level parameter system already existed and already resolved `${param}` at **run**
time for every node. This commit only extends it to the **"Fetch sample"** editor path:
`fetch_rest_api_sample` now resolves `${param}` in url/headers/query/body against each
parameter's *default value* before sampling, and 422s on undefined params. Plus a UI hint.

---

## 2. Pre-existing infrastructure you can rely on (do NOT rebuild)

Branch off **current `main`** and these are already present (verified at merge-base
`34e47871` / current tree). The new code only *imports/calls* them:

| Dependency | Location | Note |
|---|---|---|
| `FlowParameter`, `FlowSettings.parameters` | `schemas/schemas.py` | flow-level params model |
| `apply_parameters_in_place`, `find_unresolved_in_model`, `restore_parameters` | `flowfile/parameter_resolver.py` | `${param}` substitution helpers |
| Run-time param resolution for **all** nodes | `flowfile/flow_graph.py` (`run_graph` path) | already applied per node |
| `ExternalDfFetcher(lf, file_ref, flow_id, node_id, wait_on_completion)` + `.has_error/.error_code/.error_description/.get_result()` | `flowfile/flow_data_engine/subprocess_operations/subprocess_operations.py` | worker offload |
| `_first_error(run_info)`, `_flow_run_lock(flow_id)` | `flowfile/api_runner.py` | sub-flow run helpers |
| `open_flow(path, user_id=...)` | `flowfile/manage/io_flowfile.py` | load a saved flow |
| `find_registration_by_registration_id(id) -> snapshot.flow_path` | `flowfile/catalog_helpers.py` | catalog id → path |
| `FlowRegistration` cols `id/owner_id/is_api_compatible/name/flow_path` | `database/models.py` | runnable-flows query |
| `api_response` node: `.node_type`, `.get_predicted_schema()`, `.get_resulting_data()`, `.results.errors`, `.is_setup` | `flowfile/flow_node/flow_node.py` | sub-flow output node |
| `FlowGraph.add_node_step(...)`, `with_history_capture`, `HistoryActionType.UPDATE_SETTINGS` | `flowfile/flow_graph.py` | node registration |
| `FlowfileColumn.from_input(name, dtype)` | `flowfile/flow_data_engine/flow_file_column/main.py` | schema cols |
| `GET /flow-api/flows/{id}/parameters` + `FlowParamInfo` | `routes/flow_api.py`, `schemas/flow_api_schema.py` | param picker |
| Frontend `FlowApiApi.getFlowParameters`, `FlowParamInfo`, `useNodeSettings`, `genericNodeSettings`, `node-store.getNodeData` | `flowfile_frontend/...` | settings panel plumbing |

---

## 3. How node wiring works (why there are no "hidden" registry edits)

Both the palette and the settings drawer are **data/convention-driven**, so adding a node type
does **not** require editing any component map:

- **Backend dispatch is generic:** routes call `add_func = getattr(flow, "add_" + node_type)`
  (`routes/routes.py:579` and `:1130`; also `flow_graph.py`, `io_flowfile.py`). `run_flow`
  → `FlowGraph.add_run_flow` automatically. Settings class is resolved generically through
  `NODE_TYPE_TO_SETTINGS_CLASS` (`schemas/schemas.py`).
- **Frontend palette is backend-driven:** the node list comes from `get_all_standard_nodes()`
  (`configs/node_store/nodes.py`). There is **no** static frontend node list.
- **Frontend settings component is convention-resolved:** `GenericNode.vue` does
  `import.meta.glob("./node-types/elements/**/*.vue")` then builds the path from the item name:
  `toTitleCase("run_flow")="RunFlow"`, `toCamelCase("run_flow")="runFlow"`
  → `./node-types/elements/runFlow/RunFlow.vue`. Same convention in `composables/useDragAndDrop.ts`.

**Net effect:** the only registries to touch for a new node type are the 4 the commit edits
(`NODE_TYPE_TO_SETTINGS_CLASS`, `_NODE_CLASS_MAP`, `NODE_*_DESCRIPTIONS/INSTRUCTIONS`,
`NodeTemplate` list) plus the icon's `BUILTIN_ICONS` set. Everything else resolves by name.

---

## 4. File-by-file changes

Recommended application order is dependency-first (schemas → guard → graph → registries →
routes → frame → frontend → tests).

### 4.1 `flowfile_core/flowfile_core/schemas/input_schema.py` — new settings models
Insert after `class NodeRestApiReader` (before `class NodeFormula`):

```python
class ParameterMapping(BaseModel):
    """Maps an input column to a sub-flow parameter for the Run-flow node."""

    param_name: str
    input_column: str = ""


class NodeRunFlow(NodeSingleInput):
    """Settings for a node that runs a saved sub-flow once per input row.

    Each input row supplies string values for the sub-flow's ``${param}`` references
    (``parameter_mappings``); the sub-flow runs once per row (sequentially, with an
    optional ``delay_seconds`` between runs) and the per-row outputs are unioned. The
    sub-flow's single ``api_response`` node defines what data is returned.

    ``flow_reference`` (the saved flow's filesystem path) is canonical; a
    ``flow_registration_id`` is resolved to a path at run time when set.
    """

    flow_reference: str | None = None
    flow_registration_id: int | None = None
    parameter_mappings: list[ParameterMapping] = Field(default_factory=list)
    delay_seconds: float = 0.0
    max_rows: int = 1000

    def get_default_description(self) -> str:
        """Describes which sub-flow runs per input row."""
        if self.flow_reference:
            return f"Run {Path(self.flow_reference).stem} per input row"
        return "Run a sub-flow per input row"
```

`NodeSingleInput` supplies `depending_on_id` / `user_id` / base node fields. `Field` and
`Path` are already imported in this module.

### 4.2 `flowfile_core/flowfile_core/schemas/schemas.py` — settings-class registry
Add one entry to `NODE_TYPE_TO_SETTINGS_CLASS` (next to `rest_api_reader`):

```python
    "run_flow": input_schema.NodeRunFlow,
```

### 4.3 `flowfile_core/flowfile_core/schemas/flow_api_schema.py` — picker DTO
Add after `class PublishableFlow`:

```python
class RunnableFlow(BaseModel):
    """A flow that can be run as a sub-flow (has an api_response node).

    Surfaced by the Run-flow node's picker. Unlike ``PublishableFlow`` this carries the
    flow's path (stored as the node's canonical ``flow_reference``) and does not exclude
    flows already published as endpoints — a flow can be both an API and a sub-flow.
    """

    registration_id: int
    name: str
    flow_path: str
    file_exists: bool = True
```

### 4.4 `flowfile_core/flowfile_core/flowfile/run_flow_guard.py` — NEW (recursion guard)
Whole file:

```python
"""Cycle detection for the Run-flow node.

A sub-flow that (directly or transitively) runs the flow already executing would
loop forever. This tracks the resolved flow paths currently on the execution
stack via a ``ContextVar`` (which nested synchronous ``run_graph`` calls share)
and refuses to re-enter one.
"""

from __future__ import annotations

import contextvars
from collections.abc import Iterator
from contextlib import contextmanager

_active_flow_paths: contextvars.ContextVar[frozenset[str]] = contextvars.ContextVar(
    "run_flow_active_paths", default=frozenset()
)


class RecursiveSubFlowError(Exception):
    """Raised when a Run-flow node would re-enter a flow already executing."""


@contextmanager
def guard_sub_flow(flow_path: str) -> Iterator[None]:
    """Mark *flow_path* as executing for the duration of the block.

    Raises:
        RecursiveSubFlowError: if *flow_path* is already on the execution stack.
    """
    active = _active_flow_paths.get()
    if flow_path in active:
        raise RecursiveSubFlowError(
            f"Detected recursive sub-flow execution: '{flow_path}' is already running. "
            "A Run-flow node cannot (directly or transitively) run the flow that contains it."
        )
    token = _active_flow_paths.set(active | {flow_path})
    try:
        yield
    finally:
        _active_flow_paths.reset(token)
```

### 4.5 `flowfile_core/flowfile_core/flowfile/flow_graph.py` — the engine (largest change)

**Imports** (top of file):
```python
from time import sleep, time          # was: from time import time
from flowfile_core.flowfile.run_flow_guard import guard_sub_flow   # add after parameter_resolver import block
```

**Methods** — insert into `class FlowGraph` (the commit puts them right before
`add_cloud_storage_writer`). Verbatim:

```python
    @with_history_capture(HistoryActionType.UPDATE_SETTINGS)
    def add_run_flow(self, node_run_flow: input_schema.NodeRunFlow) -> None:
        """Adds a node that runs a saved sub-flow once per input row.

        For each input row, the mapped input-column values are injected into the
        sub-flow's ``${param}`` references; the sub-flow runs (sequentially, with an
        optional inter-run delay) and its single ``api_response`` node's output is
        captured. Per-row outputs are unioned (diagonal concat) and a
        ``__param_value__`` / ``__param_<name>__`` column records the value used.

        Each per-row result is materialised on the worker under a unique ref and
        concatenated lazily, so the core never holds full datasets. v1 runs rows
        sequentially because sub-flow runs share flow_id-keyed scratch dirs.
        """

        def _func(fl: FlowDataEngine) -> FlowDataEngine:
            return self._execute_run_flow_node(node_run_flow, fl)

        def schema_callback() -> list[FlowfileColumn]:
            return self._predict_run_flow_schema(node_run_flow)

        self.add_node_step(
            node_id=node_run_flow.node_id,
            function=_func,
            node_type="run_flow",
            setting_input=node_run_flow,
            schema_callback=schema_callback,
            input_node_ids=[node_run_flow.depending_on_id],
        )

    @staticmethod
    def _run_flow_param_specs(
        settings: input_schema.NodeRunFlow,
    ) -> list[tuple[str, str, str]]:
        """Active (param_name, input_column, output_column) tuples for the mappings.

        The output column is ``__param_value__`` for a single mapping, else one
        ``__param_<name>__`` per mapped parameter.
        """
        mappings = [m for m in settings.parameter_mappings if m.param_name and m.input_column]
        single = len(mappings) == 1
        return [
            (m.param_name, m.input_column, "__param_value__" if single else f"__param_{m.param_name}__")
            for m in mappings
        ]

    @staticmethod
    def _resolve_run_flow_path(settings: input_schema.NodeRunFlow) -> str | None:
        """Resolve the sub-flow's filesystem path (own ``flow_reference``, else registration)."""
        if settings.flow_reference:
            return settings.flow_reference
        if settings.flow_registration_id is not None:
            from flowfile_core.flowfile.catalog_helpers import find_registration_by_registration_id

            snap = find_registration_by_registration_id(settings.flow_registration_id)
            if snap is not None:
                return snap.flow_path
        return None

    def _predict_run_flow_schema(self, settings: input_schema.NodeRunFlow) -> list[FlowfileColumn]:
        """Predict output columns: the sub-flow's api_response schema plus the param column(s).

        Never returns empty — an empty schema_callback makes the engine fall back to
        running ``_func`` during prediction, which would execute the sub-flow.
        """
        specs = self._run_flow_param_specs(settings)
        param_cols = [FlowfileColumn.from_input(out_col, "String") for (_, _, out_col) in specs]
        sub_cols: list[FlowfileColumn] = []
        flow_path = self._resolve_run_flow_path(settings)
        if flow_path:
            from flowfile_core.flowfile.manage.io_flowfile import open_flow

            # The guard also breaks self-referential flows: predicting the sub-flow's
            # api_response schema traverses back into the nested run_flow node, which
            # would otherwise open the same flow forever.
            try:
                with guard_sub_flow(str(Path(flow_path).resolve())):
                    sub_flow = open_flow(Path(flow_path), user_id=settings.user_id)
                    api_nodes = [n for n in sub_flow.nodes if n.node_type == "api_response"]
                    if len(api_nodes) == 1:
                        predicted = api_nodes[0].get_predicted_schema()
                        sub_cols = list(predicted) if predicted else []
            except Exception as e:  # noqa: BLE001 - incl. recursion; prediction is best-effort
                logger.warning(f"Run-flow schema prediction failed: {e}")
        schema = sub_cols + param_cols
        if not schema:
            schema = [FlowfileColumn.from_input("__param_value__", "String")]
        return schema

    def _materialize_run_flow_row(self, lf: pl.LazyFrame, node_id: int | str, index: int) -> pl.LazyFrame:
        """Materialise one row's sub-flow output to a stable, uniquely-named location.

        Sequential sub-flow runs reuse the sub-flow's flow_id-keyed Arrow IPC output
        path, so a lazy reference captured this iteration would be overwritten by the
        next run. Offloading to the worker with a per-iteration ``file_ref`` writes a
        distinct file and returns a lazy scan over it (core never collects). Falls
        back to an in-core collect only in local mode or when no worker is reachable.
        """
        if self.execution_location == "local":
            return lf.collect().lazy()
        try:
            fetcher = ExternalDfFetcher(
                lf=lf,
                file_ref=f"__runflow_{node_id}_{index}",
                flow_id=self.flow_id,
                node_id=node_id,
                wait_on_completion=True,
            )
        except Exception as exc:  # noqa: BLE001 - worker unreachable; degrade to in-core
            logger.warning("Run-flow worker offload unavailable (%s); materializing row %s in core", exc, index)
            return lf.collect().lazy()
        if fetcher.has_error:
            if fetcher.error_code == -1:
                logger.warning("Run-flow worker materialize was killed; materializing row %s in core", index)
                return lf.collect().lazy()
            raise ValueError(fetcher.error_description or "Run-flow worker materialize failed")
        return fetcher.get_result()

    def _execute_run_flow_node(self, settings: input_schema.NodeRunFlow, fl: FlowDataEngine) -> FlowDataEngine:
        """Run the sub-flow once per input row and return the unioned, lazily-built result."""
        from flowfile_core.flowfile.api_runner import _first_error, _flow_run_lock
        from flowfile_core.flowfile.manage.io_flowfile import open_flow

        flow_path = self._resolve_run_flow_path(settings)
        if not flow_path:
            raise ValueError("Run-flow node has no sub-flow selected.")

        specs = self._run_flow_param_specs(settings)
        max_rows = max(0, settings.max_rows)

        # Bounded, columns-limited collect to drive iteration (head+1 detects overflow).
        input_lf = fl.data_frame.lazy() if isinstance(fl.data_frame, pl.DataFrame) else fl.data_frame
        mapped_cols = list(dict.fromkeys(input_col for (_, input_col, _) in specs))
        if mapped_cols:
            driver_df = input_lf.select(mapped_cols).head(max_rows + 1).collect()
        else:
            total = input_lf.select(pl.len()).collect().item()
            driver_df = pl.DataFrame({"__row__": range(min(total, max_rows + 1))})

        if driver_df.height > max_rows:
            self.flow_logger.warning(
                f"Run-flow node {settings.node_id}: input exceeds max_rows={max_rows}; "
                f"running the first {max_rows} rows and skipping the rest."
            )
            driver_df = driver_df.head(max_rows)

        sub_flow = open_flow(Path(flow_path), user_id=settings.user_id)
        api_nodes = [n for n in sub_flow.nodes if n.node_type == "api_response"]
        if len(api_nodes) == 0:
            raise ValueError("Run-flow sub-flow has no API Response node to define its output.")
        if len(api_nodes) > 1:
            raise ValueError("Run-flow sub-flow has more than one API Response node.")
        api_node = api_nodes[0]

        sub_flow.flow_settings.execution_mode = "Performance"
        sub_flow.flow_settings.execution_location = self.execution_location
        params_by_name = {p.name: p for p in sub_flow.flow_settings.parameters}

        results: list[pl.LazyFrame] = []
        n_iter = driver_df.height
        with guard_sub_flow(str(Path(flow_path).resolve())):
            for i, row in enumerate(driver_df.iter_rows(named=True)):
                for param_name, input_col, _ in specs:
                    param = params_by_name.get(param_name)
                    if param is not None:
                        value = row.get(input_col)
                        param.default_value = "" if value is None else str(value)
                with _flow_run_lock(sub_flow.flow_id):
                    run_info = sub_flow.run_graph()
                if run_info is None or not run_info.success:
                    raise ValueError(_first_error(run_info) or f"Run-flow sub-flow failed on row {i}")
                data = api_node.get_resulting_data()
                if data is None:
                    unconfigured = [n.node_id for n in sub_flow.nodes if not n.is_setup]
                    if unconfigured:
                        raise ValueError(
                            f"Run-flow sub-flow has unconfigured node(s) {sorted(unconfigured)} that were "
                            "saved without settings. Open the sub-flow, finish configuring/connecting every "
                            "node, run it once to confirm it produces data, then re-save it."
                        )
                    detail = api_node.results.errors or _first_error(run_info)
                    raise ValueError(
                        "Run-flow sub-flow API Response node produced no data"
                        + (
                            f": {detail}"
                            if detail
                            else ". Check that the API Response node is connected to the branch whose "
                            "data you want returned, and that the sub-flow runs and produces rows on its own."
                        )
                    )
                row_lf = data.data_frame
                if isinstance(row_lf, pl.DataFrame):
                    row_lf = row_lf.lazy()
                for _param_name, input_col, out_col in specs:
                    value = row.get(input_col)
                    row_lf = row_lf.with_columns(pl.lit("" if value is None else str(value)).alias(out_col))
                results.append(self._materialize_run_flow_row(row_lf, settings.node_id, i))
                if settings.delay_seconds and i < n_iter - 1:
                    sleep(settings.delay_seconds)

        if not results:
            return FlowDataEngine()
        return FlowDataEngine(pl.concat(results, how="diagonal_relaxed"))
```

Notes: `ExternalDfFetcher`, `FlowfileColumn`, `FlowDataEngine`, `logger`, `pl`, `Path` are
already imported in this module. `self.execution_location` / `self.flow_id` /
`self.flow_logger` are existing `FlowGraph` members.

### 4.6 `flowfile_core/flowfile_core/configs/node_store/nodes.py` — palette template
Add to the standard-nodes list (the commit places it in the `combine` group):

```python
        NodeTemplate(
            name="Run flow",
            item="run_flow",
            input=1,
            output=1,
            transform_type="other",
            node_type="process",
            image="run_flow.svg",
            node_group="combine",
            drawer_title="Run Flow",
            drawer_intro="Run a saved sub-flow once per input row, mapping columns to its parameters",
            laziness="eager",
        ),
```

### 4.7 `flowfile_core/flowfile_core/ai/tools/classification.py` — AI node class
Add to `_NODE_CLASS_MAP`:
```python
    "run_flow": "static",
```

### 4.8 `flowfile_core/flowfile_core/ai/tools/node_docs.py` — AI docs (2 entries)
Add to `NODE_LONG_DESCRIPTIONS`:
```python
    "run_flow": (
        "Run a saved sub-flow once per input row, mapping input columns to the "
        "sub-flow's ${parameters}; the per-row outputs are unioned with a "
        "__param_value__ column. Use to fan a pipeline (e.g. a REST API call) "
        "across a list of inputs like tickers or ids. The sub-flow must have "
        "exactly one API Response node defining its output. Don't use for a single "
        "static call (build the flow directly) or when no per-row iteration is needed."
    ),
```
Add to `NODE_USER_INSTRUCTIONS`:
```python
    "run_flow": (
        "Settings panel: pick a saved flow (it must have one API Response node), "
        "then map each of its ${parameters} to an input column, and optionally set "
        "a delay between runs and a max-rows cap. Worked example: 'call Finnhub for "
        "every ticker in my table' → drag 'Run Flow' from Combine Operations, select "
        "your quote sub-flow whose REST API url is "
        "https://finnhub.io/api/v1/quote?symbol=${ticker}, map its 'ticker' parameter "
        "to your 'symbol' column, set a 1-second delay to respect rate limits, and "
        "run — you get one output row per input row plus a __param_value__ column. "
        "Pitfall: rows run sequentially (no parallelism yet) and inputs beyond max "
        "rows are skipped, so raise max rows for large inputs and expect longer "
        "runtimes when a delay is set."
    ),
```

### 4.9 `flowfile_core/flowfile_core/routes/flow_api.py` — `/runnable-flows` route
Add `RunnableFlow` to the `flow_api_schema` import, then add the endpoint (the commit puts it
just before `get_flow_parameters`):

```python
@management_router.get("/runnable-flows", response_model=list[RunnableFlow])
def list_runnable_flows(
    current_user=Depends(get_current_active_user),
    db: Session = Depends(get_db),
):
    """The current user's API-compatible flows, for the Run-flow node's picker.

    A flow qualifies when it has exactly one ``api_response`` node
    (``is_api_compatible``). Unlike ``publishable-flows`` this does not exclude flows
    already published as endpoints — a flow can be both an API and a sub-flow.
    """
    regs = (
        db.query(db_models.FlowRegistration)
        .filter(
            db_models.FlowRegistration.owner_id == current_user.id,
            db_models.FlowRegistration.is_api_compatible.is_(True),
        )
        .order_by(db_models.FlowRegistration.name)
        .all()
    )
    return [
        RunnableFlow(
            registration_id=r.id,
            name=r.name,
            flow_path=r.flow_path or "",
            file_exists=bool(r.flow_path) and Path(r.flow_path).exists(),
        )
        for r in regs
    ]
```

### 4.10 `flowfile_core/flowfile_core/routes/routes.py` — REST `${param}` at sample time
Add the import:
```python
from flowfile_core.flowfile.parameter_resolver import apply_parameters_in_place, find_unresolved_in_model
```
In `fetch_rest_api_sample`, right after the `flow is None` 404 check and **before** the
auth/credential resolution, insert:
```python
    # Resolve ${param} references (in url/headers/query/body) using the flow's
    # parameter defaults so a parameterized URL can be sampled. ``node`` is a
    # throwaway parse of the request body, so no restoration is needed. The
    # editor sample path is trusted (the flow owner), so the api_runner string
    # safety gate does not apply here.
    params = {p.name: p.default_value for p in flow.flow_settings.parameters}
    try:
        apply_parameters_in_place(node, params)
    except ValueError as e:
        raise HTTPException(422, str(e)) from e
    unresolved = find_unresolved_in_model(node)
    if unresolved:
        raise HTTPException(
            422,
            f"REST API settings reference undefined flow parameter(s): {sorted(unresolved)}. "
            "Define them on the flow before sampling.",
        )
```

### 4.11 `flowfile_frame/flowfile_frame/flow_frame.py` — programmatic API
Add a `run_flow` method to `class FlowFrame` (the commit places it after the `pipe`/function
helper near line 417):

```python
    def run_flow(
        self,
        flow_reference: str | None = None,
        *,
        flow_registration_id: int | None = None,
        parameter_mappings: dict[str, str] | list[Any] | None = None,
        delay_seconds: float = 0.0,
        max_rows: int = 1000,
        description: str | None = None,
    ) -> FlowFrame:
        """Run a saved sub-flow once per input row, mapping input columns to its ${parameters}.

        For each input row the mapped column values are injected into the sub-flow's
        ``${param}`` references; the sub-flow runs (sequentially, with an optional
        ``delay_seconds`` between runs) and its single ``api_response`` node's output is
        captured. Per-row outputs are unioned and a ``__param_value__`` /
        ``__param_<name>__`` column records the value(s) used.

        Args:
            flow_reference: Filesystem path to the saved sub-flow (``.flowfile`` /
                ``.yaml`` / ``.json``). Canonical reference.
            flow_registration_id: Catalog registration id of the sub-flow; resolved to a
                path at run time when ``flow_reference`` is not given.
            parameter_mappings: Maps the sub-flow's ${parameters} to input columns —
                either a dict ``{"ticker": "symbol"}`` (param -> column) or a list of
                ``{"param_name": ..., "input_column": ...}`` dicts.
            delay_seconds: Optional delay between per-row runs (rate-limit friendly).
            max_rows: Cap on the number of input rows processed.
            description: Optional node description.

        Returns:
            FlowFrame: A FlowFrame backed by the Run-flow node's unioned output.
        """
        from flowfile_core.schemas.input_schema import NodeRunFlow, ParameterMapping
        from flowfile_frame.rest_api import get_current_user_id

        if isinstance(parameter_mappings, dict):
            mappings = [ParameterMapping(param_name=str(k), input_column=str(v)) for k, v in parameter_mappings.items()]
        elif parameter_mappings:
            mappings = [m if isinstance(m, ParameterMapping) else ParameterMapping(**m) for m in parameter_mappings]
        else:
            mappings = []

        new_node_id = generate_node_id()
        settings = NodeRunFlow(
            flow_id=self.flow_graph.flow_id,
            node_id=new_node_id,
            user_id=get_current_user_id(),
            depending_on_id=self.node_id,
            description=description,
            flow_reference=flow_reference,
            flow_registration_id=flow_registration_id,
            parameter_mappings=mappings,
            delay_seconds=delay_seconds,
            max_rows=max_rows,
        )
        self.flow_graph.add_run_flow(settings)
        return self._create_child_frame(new_node_id)
```

### 4.12 `flowfile_frame/flowfile_frame/flow_frame.pyi` — stub
Add inside `class FlowFrame` (the commit places it before `save_graph`):
```python
    # Run a saved sub-flow once per input row, mapping input columns to its ${parameters}.
    def run_flow(self, flow_reference: str | None = None, flow_registration_id: int | None = None, parameter_mappings: dict[str, str] | list[Any] | None = None, delay_seconds: float = 0.0, max_rows: int = 1000, description: str | None = None) -> 'FlowFrame': ...
```
> Or regenerate with `make stubs` after adding the method (CI gates on `make check_stubs`).

### 4.13 Frontend — types: `flowfile_frontend/src/renderer/app/types/node.types.ts`
Add after `interface NodeRestApiReader`:
```ts
// Run-flow node: run a saved sub-flow once per input row, mapping columns to its ${params}.
export interface ParameterMapping {
  param_name: string;
  input_column: string;
}

export interface NodeRunFlow extends NodeSingleInput {
  flow_reference: string | null;
  flow_registration_id: number | null;
  parameter_mappings: ParameterMapping[];
  delay_seconds: number;
  max_rows: number;
}
```

### 4.14 Frontend — API: `flowfile_frontend/src/renderer/app/api/flowApi.api.ts`
Add the interface (after `PublishableFlow`) and the method (in `class FlowApiApi`):
```ts
// An API-compatible flow that can be run as a sub-flow, for the Run-flow node's picker.
// Carries the flow path (stored as the node's flow_reference); includes published flows.
export interface RunnableFlow {
  registration_id: number;
  name: string;
  flow_path: string;
  file_exists: boolean;
}
```
```ts
  static async listRunnableFlows(): Promise<RunnableFlow[]> {
    const res = await axios.get<RunnableFlow[]>("/flow-api/runnable-flows");
    return res.data;
  }
```

### 4.15 Frontend — NEW component `.../node-types/elements/runFlow/RunFlow.vue`
Resolved by convention (`run_flow` → `runFlow/RunFlow.vue`). Whole file:

```vue
<template>
  <div v-if="dataLoaded && nodeRunFlow" class="listbox-wrapper">
    <generic-node-settings
      v-model="nodeRunFlow"
      @update:model-value="handleGenericSettingsUpdate"
      @request-save="saveSettings"
    >
      <div class="list-wrapper">
        <div class="listbox-subtitle">Flow to run</div>
        <el-select
          v-model="nodeRunFlow.flow_registration_id"
          size="small"
          style="width: 100%"
          placeholder="Select a saved flow"
          filterable
          @change="onFlowSelected"
        >
          <el-option
            v-for="flow in runnableFlows"
            :key="flow.registration_id"
            :label="flow.file_exists ? flow.name : `${flow.name} (file missing)`"
            :value="flow.registration_id"
            :disabled="!flow.file_exists"
          />
        </el-select>
        <div class="hint-text">
          The sub-flow runs once per input row. Its single API Response node defines the output.
        </div>
      </div>

      <div v-if="nodeRunFlow.flow_registration_id" class="list-wrapper">
        <div class="listbox-subtitle">Map parameters to columns</div>
        <div v-if="flowParams.length === 0" class="hint-text">
          The selected flow has no parameters. Add ${name} parameters to the flow to map them here.
        </div>
        <table v-else class="mapping-table">
          <thead>
            <tr>
              <th>Parameter</th>
              <th>Input column</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="param in flowParams" :key="param.name">
              <td class="param-name">${{ param.name }}</td>
              <td>
                <el-select
                  v-model="mappingByParam[param.name]"
                  size="small"
                  style="width: 100%"
                  clearable
                  filterable
                  placeholder="Select column"
                >
                  <el-option
                    v-for="column in inputColumns"
                    :key="column"
                    :label="column"
                    :value="column"
                  />
                </el-select>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="list-wrapper">
        <div class="listbox-subtitle">Execution</div>
        <div class="field-row">
          <span class="field-label">Delay between runs (seconds)</span>
          <el-input-number
            v-model="nodeRunFlow.delay_seconds"
            :min="0"
            :step="0.5"
            size="small"
            controls-position="right"
          />
        </div>
        <div class="field-row">
          <span class="field-label">Max rows</span>
          <el-input-number
            v-model="nodeRunFlow.max_rows"
            :min="1"
            :step="100"
            size="small"
            controls-position="right"
          />
        </div>
        <div class="hint-text">Rows run sequentially. Inputs beyond max rows are skipped.</div>
      </div>
    </generic-node-settings>
  </div>
  <CodeLoader v-else />
</template>

<script lang="ts" setup>
import { ref, computed } from "vue";
import { CodeLoader } from "vue-content-loader";
import { NodeData } from "../../../baseNode/nodeInterfaces";
import { useNodeStore } from "../../../../../stores/node-store";
import { useNodeSettings } from "../../../../../composables/useNodeSettings";
import GenericNodeSettings from "../../../baseNode/genericNodeSettings.vue";
import type { NodeRunFlow } from "../../../../../types/node.types";
import { FlowApiApi, type RunnableFlow, type FlowParamInfo } from "../../../../../api/flowApi.api";

const nodeStore = useNodeStore();
const dataLoaded = ref(false);
const nodeRunFlow = ref<null | NodeRunFlow>(null);
const nodeData = ref<null | NodeData>(null);
const runnableFlows = ref<RunnableFlow[]>([]);
const flowParams = ref<FlowParamInfo[]>([]);
const mappingByParam = ref<Record<string, string>>({});

const inputColumns = computed<string[]>(() => nodeData.value?.main_input?.columns ?? []);

const rebuildMappingState = () => {
  const existing = new Map(
    (nodeRunFlow.value?.parameter_mappings ?? []).map((m) => [m.param_name, m.input_column]),
  );
  const next: Record<string, string> = {};
  for (const param of flowParams.value) {
    next[param.name] = existing.get(param.name) ?? "";
  }
  mappingByParam.value = next;
};

const loadFlowParameters = async (registrationId: number) => {
  try {
    flowParams.value = await FlowApiApi.getFlowParameters(registrationId);
  } catch (error) {
    console.error("Failed to load sub-flow parameters:", error);
    flowParams.value = [];
  }
  rebuildMappingState();
};

const onFlowSelected = async (registrationId: number | null) => {
  if (!nodeRunFlow.value) return;
  const flow = runnableFlows.value.find((f) => f.registration_id === registrationId);
  nodeRunFlow.value.flow_registration_id = registrationId;
  nodeRunFlow.value.flow_reference = flow?.flow_path ?? null;
  if (registrationId == null) {
    flowParams.value = [];
    mappingByParam.value = {};
    return;
  }
  await loadFlowParameters(registrationId);
};

const syncMappings = () => {
  if (!nodeRunFlow.value) return;
  nodeRunFlow.value.parameter_mappings = flowParams.value
    .map((param) => ({
      param_name: param.name,
      input_column: mappingByParam.value[param.name] ?? "",
    }))
    .filter((mapping) => mapping.input_column !== "");
};

const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({
  nodeRef: nodeRunFlow,
  onBeforeSave: () => {
    syncMappings();
    return true;
  },
});

const loadData = async (nodeId: number) => {
  nodeData.value = await nodeStore.getNodeData(nodeId, false);
  nodeRunFlow.value = nodeData.value?.setting_input;
  try {
    runnableFlows.value = await FlowApiApi.listRunnableFlows();
  } catch (error) {
    console.error("Failed to load runnable flows:", error);
    runnableFlows.value = [];
  }
  if (nodeRunFlow.value?.flow_registration_id != null) {
    await loadFlowParameters(nodeRunFlow.value.flow_registration_id);
  }
  dataLoaded.value = true;
};

const loadNodeData = async (nodeId: number) => {
  await loadData(nodeId);
};

defineExpose({
  loadNodeData,
  pushNodeData,
  saveSettings,
});
</script>

<style scoped>
.hint-text {
  font-size: 11px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}

.mapping-table {
  width: 100%;
  border-collapse: collapse;
}

.mapping-table th {
  text-align: left;
  font-size: 11px;
  color: var(--el-text-color-secondary);
  font-weight: 500;
  padding: 2px 4px;
}

.mapping-table td {
  padding: 2px 4px;
  vertical-align: middle;
}

.mapping-table .param-name {
  font-family: var(--el-font-family-monospace, monospace);
  font-size: 12px;
  white-space: nowrap;
}

.field-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  margin-bottom: 6px;
}

.field-label {
  font-size: 12px;
}
</style>
```

### 4.16 Frontend — NEW `.../node-types/elements/runFlow/utils.ts`
```ts
import type { NodeRunFlow } from "../../../../../types/node.types";

export const createNodeRunFlow = (flowId: number, nodeId: number): NodeRunFlow => {
  return {
    flow_id: flowId,
    node_id: nodeId,
    pos_x: 0,
    pos_y: 0,
    cache_results: false,
    flow_reference: null,
    flow_registration_id: null,
    parameter_mappings: [],
    delay_seconds: 0,
    max_rows: 1000,
  };
};
```

### 4.17 Frontend — NEW icon `.../features/designer/assets/icons/run_flow.svg`
```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="#8b5cf6" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
  <circle cx="4.5" cy="6" r="2"/>
  <circle cx="4.5" cy="18" r="2"/>
  <path d="M6.5 6 C10 6 9 12 12 12"/>
  <path d="M6.5 18 C10 18 9 12 12 12"/>
  <path d="M13 8 L19 12 L13 16 Z" fill="#8b5cf6" stroke="none"/>
</svg>
```

### 4.18 Frontend — register icon: `.../features/designer/utils.ts`
Add to the `BUILTIN_ICONS` set:
```ts
  "run_flow.svg",
```

### 4.19 Frontend — REST helper text: `.../elements/restApiReader/RestApiReader.vue`
After the URL input `<input … placeholder="https://api.example.com/v1/items" />`, inside its
form-group `<div>`, add:
```html
            <div class="helper-text">
              <i class="fa-solid fa-info-circle"></i>
              <span
                >Supports <code>${param}</code> references to flow parameters (resolved at run time;
                Fetch sample uses each parameter's default value).</span
              >
            </div>
```

---

## 5. Tests added (copy verbatim — they're the acceptance criteria)

- **`flowfile_core/tests/test_run_flow_node.py`** (NEW, ~327 lines): builds a hermetic
  `manual_input → polars_code(echo ${x}) → api_response` sub-flow, saves it, drives it from a
  parent flow. Covers: per-row run + `__param_value__` correlation; `max_rows` cap + warning;
  **recursion guard** (a flow whose `run_flow` references itself fails with "recursive");
  no-sub-flow-selected error; 0-row sub-flow runs tolerated (dropped silently). Uses an
  `execution_location` fixture (local + worker).
- **`flowfile_frame/tests/test_run_flow.py`** (NEW, ~60 lines): `FlowFrame.run_flow` end-to-end
  via `from_dict(...).run_flow(path, parameter_mappings={"x": "ticker"})`.
- **`flowfile_core/tests/test_parameter_integration.py`** (append): a test asserting
  `${ticker}` in a REST url resolves into the worker settings after `apply_parameters_in_place`.

Because these are long and mechanical, the safest reproduction is
`git show b5783afb -- flowfile_core/tests/test_run_flow_node.py flowfile_frame/tests/test_run_flow.py > /tmp/runflow_tests.diff`
(or `git checkout b5783afb -- <those test paths>` onto your clean branch).

---

## 6. Redo checklist (suggested order)

1. Branch off current `main`.
2. Backend schemas: 4.1 `input_schema.py`, 4.2 `schemas.py`, 4.3 `flow_api_schema.py`.
3. 4.4 `run_flow_guard.py` (new file).
4. 4.5 `flow_graph.py` (imports + 6 methods).
5. Registries: 4.6 `nodes.py`, 4.7 `classification.py`, 4.8 `node_docs.py`.
6. Routes: 4.9 `flow_api.py` (route), 4.10 `routes.py` (REST sample params).
7. Frame API: 4.11 `flow_frame.py`, then `make stubs` (or hand-edit 4.12 `.pyi`).
8. Frontend: 4.13–4.19 (types, api, RunFlow.vue, utils.ts, icon, BUILTIN_ICONS, REST hint).
9. Tests: section 5.
10. `poetry run pytest flowfile_core/tests/test_run_flow_node.py flowfile_frame/tests/test_run_flow.py flowfile_core/tests/test_parameter_integration.py`
11. `poetry run ruff check . && make check_stubs`; frontend `cd flowfile_frontend && npm run lint && npx vue-tsc --noEmit`.

---

## 7. Known wart to fix while redoing (not a blocker)

`runFlow/utils.ts` exports `createNodeRunFlow`, but **`RunFlow.vue` never imports it** — unlike
`RestApiReader.vue`, which falls back to its factory when `setting_input` is absent. In practice
the backend `NodePromise` seeds default `NodeRunFlow` settings, so this rarely bites, but to match
the established pattern, change `RunFlow.vue`'s `loadData`:

```ts
// import { createNodeRunFlow } from "./utils";
const hasValidSetup = Boolean(nodeData.value?.setting_input?.is_setup);
nodeRunFlow.value = hasValidSetup
  ? (nodeData.value!.setting_input as NodeRunFlow)
  : createNodeRunFlow(nodeStore.flow_id, nodeId);
```

---

## 8. Gotchas worth remembering

- **Core never collects on the hot path:** per-row outputs go through `ExternalDfFetcher` (worker)
  except in `execution_location == "local"`. Keep that branch.
- **Schema callback must never return empty** — an empty schema makes the engine execute `_func`
  during prediction (running the sub-flow). `_predict_run_flow_schema` guards this with a
  `__param_value__` fallback.
- **Recursion guard uses a `ContextVar`** keyed on the resolved flow path; nested synchronous
  `run_graph` calls share it. It also protects schema prediction (self-referential traversal).
- **Sub-flow contract:** exactly one `api_response` node. Zero ⇒ error; >1 ⇒ error.
- **Params half is small:** flow-level parameters + run-time resolution already exist on `main`;
  the commit only adds *sample-time* resolution for the REST node + a UI hint + a test.
