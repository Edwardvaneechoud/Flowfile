# Notebook UX: implementation plan

Status: Changes 1 and 2 shipped; Changes 3 and 4 not started.
Updated: 2026-09-20.

## What we are building

Deliver four changes, in the order below, as one release. Apply each to both catalog notebooks and Python Script node notebooks unless explicitly stated otherwise.

| Change | Deliverable | Depends on | Status |
| --- | --- | --- | --- |
| 1 | Drag cells to reorder; keyboard equivalent; undo cell actions | — | Shipped |
| 2 | Duplicate/collapse cells; reliable focus and run-and-advance | 1 | Shipped |
| 3 | Mark old results and prevent execution races | 1 | Not started |
| 4 | Suggest columns and types for the actual dataframe being edited | 3 | Not started |

Each change below carries a Status section recording what is in the tree, so the
numbered Implement steps stay readable as the original specification.

Do not include reactive execution, multiplayer editing, AI generation, SQL cells, notebook-to-app publishing, or notebook-to-flow conversion in this release. Do not rewrite the two notebook stores into one store.

## File aliases used below

All paths are relative to the repository root. A name marked NEW was a file to
create when this plan was written; the Status sections say which of them now exist.

- `APP` = `flowfile_frontend/src/renderer/app`
- `NB` = `APP/components/notebook`
- `PY` = `APP/components/nodes/node-types/elements/pythonScript`
- `CORE` = `flowfile_core/flowfile_core`
- `RUNTIME` = `kernel_runtime/kernel_runtime`

## Change 1 — Reorder cells and undo structural edits

### Status

Shipped. `NB/cellOperations.ts`, `NB/useCellHistory.ts` and `NB/useCellDrag.ts`
all exist with unit tests beside them, both cell components carry the handle,
and `flowfile_frontend/tests/notebook-interactions.spec.ts` covers drag, undo
and persistence on both surfaces. That spec runs in CI as part of
`.github/workflows/e2e-tests.yml`.

### User behavior

Each cell gets a six-dot handle on its left. Dragging displays a horizontal insertion line. Releasing moves the complete cell, including its output. Escape cancels. Up/down actions remain available; Alt+Up/Down on the focused handle performs the same move.

The notebook toolbar gets Undo cell action and Redo cell action buttons. These affect insert/delete/move actions. Cmd/Ctrl+Z inside code continues to undo code edits.

### Implement

1. NEW `NB/cellOperations.ts`: implement pure `moveCell(cells, cellId, targetIndex)`, where targetIndex is the final zero-based index, and insert/delete helpers. Return the new ordered array and inverse operation. No-op moves return no operation.
2. NEW `NB/useCellHistory.ts`: keep the last 50 structural operations per open notebook. Store affected cell references and positions, not full notebook snapshots or deep copies of outputs. New actions clear redo history. Dispose history when its notebook closes.
3. NEW `NB/useCellDrag.ts`: implement pointer capture on the handle, compute drop position from rendered cell bounds, and commit only on pointer-up. Start after a 5px movement threshold. Use requestAnimationFrame for edge scrolling within 40px of the scroll container boundary. Cancel on Escape, pointercancel, or unmount. Do not physically reorder editor DOM during the gesture.
4. Add handles and drag events to `NB/CatalogNotebookCell.vue` and `PY/NotebookCell.vue`.
5. Add drop indicator and operation wiring in `APP/views/CatalogView/NotebookPanel.vue` and `PY/NotebookEditor.vue`.
6. Replace catalog store `moveCell` internals with the shared helper; add `moveCellToIndex`. Route insert/delete through history. Keep its current dirty flag and persistence calls.
7. In the node editor, continue emitting `update:cells`; apply shared operations before emitting. Keep history local to the mounted node editor and clear it when node identity changes.
8. Disable structural actions during a run, including gaps between cells in Run All. Keep Vue keys based on cell ID. Announce completed keyboard moves through an aria-live region.

### Done when

- [x] Drag first-to-last and last-to-first in both surfaces; code, output, caret, and text undo history remain with the same cell. (output verified via rendered Markdown; no kernel available in the run)
- [x] Dragging code selections and table cells does not move a cell. (code selections verified; AG Grid table drags not exercised — no kernel available in the run)
- [x] Escape leaves order unchanged; no-op drops do not dirty the notebook.
- [x] Move → delete → undo → undo restores the original order and source.
- [x] Save/reopen preserves the order.
- [x] Keyboard users can perform every reorder and hear the resulting position.

Tests: NEW `NB/cellOperations.test.ts`, `NB/useCellHistory.test.ts`; NEW `flowfile_frontend/tests/notebook-interactions.spec.ts` for drag/focus/persistence checks.

## Change 2 — Useful cell actions and correct focus

### Status

Shipped. All six steps are in the tree: `duplicateCell` in `NB/cellOperations.ts`
is reached from both surfaces, `NB/editorViews.ts` is owner-keyed
(`ownerIdForNotebook`, `ownerIdForNode`) and its `focusCell(ownerId, cellId, host?)`
is called after insert, duplicate, delete and run-and-advance, `NB/cellPresentation.ts`
holds the session-local code/output collapse state, and `NB/CellActionMenu.vue` is
the one menu both cell components render. Note that `PY/useCollapsedSections.ts`
is unrelated — it collapses the kernel/outputs/artifacts panels of the Python
Script drawer, not a cell's code or output.

### User behavior

Each cell menu contains Insert above, Insert below, Duplicate, Collapse code, Collapse output, and Delete. The active cell has a visible border. Running with advance moves the caret to the next cell, creating a blank trailing cell when needed.

Shift+Enter = run and advance; Cmd/Ctrl+Enter = run in place, matching Jupyter and Databricks. Update all tooltips/help to match. Markdown follows the same advancement behavior in catalog notebooks.

### Implement

1. Add duplicate to `cellOperations.ts`: copy source/type, generate a new ID, clear output and execution state, insert immediately below. Record one history operation.
2. Extend `NB/editorViews.ts` from cell-only keys to notebook/node identity plus cell ID. Register node notebook editors as well as catalog editors.
3. Add `focusCell(ownerId, cellId)`; invoke after Vue nextTick following insert, duplicate, or run-and-advance. On deletion focus the next surviving cell, otherwise the previous cell. Keep at least one blank cell.
4. Wire catalog `onRunAdvance` through `notebookEditor.ts` to a new parent action. Replace the node editor's current run-and-advance placeholder with actual editor focus.
5. Add session-local presentation state keyed by owner/cell ID for code/output collapse. Hiding code must preserve its EditorView; hiding output must not clear its data. Do not change the saved notebook format for this step.
6. Add a shared cell action menu in NEW `NB/CellActionMenu.vue`; use it in both cell components. Retain surface-specific type selection.

### Done when

- [x] Duplicate has identical code and no old result.
- [x] Run-and-advance focuses the next editor in both surfaces, including after the last cell and after rendering Markdown.
- [x] Collapsing and expanding retains cursor and text undo history.
- [x] Delete and undo restore source without stealing focus from another notebook tab.
- [x] Toolbar labels, NotebookHelp, and notebook user docs describe the same shortcuts.

Tests: extend `notebook-interactions.spec.ts`; add duplicate cases to `cellOperations.test.ts`.

## Change 3 — Outdated outputs and execution identity

### Status

Shipped. `NB/notebookRuntimeState.ts` holds the owner-keyed runtime state (source/submitted revisions, execution tickets, session epochs, the batch coordinator) and `NB/CellStatusBadge.vue` renders the labels; both surfaces route every run through a batch (a single run is a batch of one), mark downstream cells at submission, settle responses against their ticket, and expose Reset session. Verified against a real kernel in the browser and by mocked-kernel Playwright races.
### User behavior

Changing code keeps the previous output visible with “Code changed — rerun”. Affected later outputs show “Earlier cells changed — rerun”. Switching/resetting the kernel marks retained results “Previous session”. No result silently becomes current when undoing an edit.

Only one execution batch can be active per notebook. Repeated Run All clicks or shortcuts cannot start overlapping batches. Changing tabs does not redirect results to the new tab.

### Implement

1. NEW `NB/notebookRuntimeState.ts`: transient map keyed by owner/cell ID. Store source revision, last submitted revision, request ID, session epoch, status, and stale reason. Keep it out of notebook YAML and node serialization.
2. Increment source revision on code edits. Mark the edited result stale and every later Python result potentially stale. Moves invalidate from min(oldIndex, newIndex); executable insert/delete/type changes invalidate from the affected position. Markdown-only edits affect only their rendered preview.
3. Before every execution, capture owner ID, cell ID, source revision, session epoch, and a new request ID. Apply the response only to that owner and latest request. If source changed while executing, keep the result but label it stale. Discard responses from a previous session epoch or removed cell.
4. Increment session epoch on kernel selection change, successful namespace reset, and notebook session recreation. Clear schema caches from Change 4 at the same boundary.
5. Capture the originating catalog notebook once at Run All start. Iterate a snapshot of its cell IDs and source revisions, not `this.active`. Continue through tab switches. Stop before executing a pending cell whose source revision changed.
6. Add batch-level busy state in both surfaces; retain it through the entire loop. Stop on the first error. Running an earlier cell also marks later results stale because its values may have changed.
7. Render status badges in both cell components. Rename namespace-clear controls to “Reset session” rather than implying a kernel process restart. Surface clearNamespace failures; do not silently declare a reset successful.

### Done when

- [x] Run A then B; edit A: both results are labelled outdated. Rerun A: A is current and B remains outdated.
- [x] Edit a slow-running cell before completion: returned output is outdated.
- [x] Start Run All, switch notebook tabs: results stay with the original notebook and the batch continues there.
- [x] Reset/switch kernel while a response is in flight: that response cannot restore current state.
- [x] Rapid duplicate execution requests do not execute a batch twice.

Tests: NEW `NB/notebookRuntimeState.test.ts`; extend `APP/stores/notebook-store.test.ts`; browser test edit-during-run and tab-switch races using controlled API responses.

## Change 4 — Dataframe-aware column names and types

### Status

Shipped. Kernel 0.5.5 serves `POST /lsp/dataframe_schemas` over a generation/revision-stamped namespace, core bridges it at `POST /kernels/{id}/lsp/dataframe_schemas`, and `PY/dataframeColumnCompletions.ts` resolves a column position against static source inference (catalog refs, node inputs, literal frames, the transfer rules) first and the kernel's last-run schemas second. `PY/useUpstreamColumns.ts` now feeds only the unresolved-receiver fallback rows. Verified against a live 0.5.5 kernel in the browser on both surfaces.

### Exact initial support

Start with Polars. Reuse Jedi for Python symbols/methods; add a separate schema completion source for column strings.

| Typed expression | Schema used |
| --- | --- |
| `orders.select("…")` | orders |
| `orders.group_by('…')` | orders |
| `orders.sort("…")` | orders |
| `orders.filter(pl.col("…"))` | orders |
| `orders.with_columns(pl.col('…'))` | orders before this operation |
| `orders["…"]` | orders, when known to be a DataFrame |
| `pl.col("…")` with no resolvable enclosing dataframe | labelled upstream candidates in node notebooks; no guessed receiver in catalog notebooks |

Support string lists in select/group_by/sort, multiline calls, escaped column names, and incomplete string literals. Each suggestion shows `column name`, `dtype`, and `dataframe/source`. Two different sources with the same column name must not silently collapse into the first source's type.

### Backend contract

Add `POST /kernels/{kernel_id}/lsp/dataframe_schemas` in Core and `POST /lsp/dataframe_schemas` in the runtime. Request: `{flow_id, node_id?}`. Response:

```json
{
  "namespace_generation": "server-generated-id",
  "revision": 12,
  "dataframes": [
    {
      "name": "orders",
      "kind": "DataFrame",
      "state": "ready",
      "columns": [{"name": "amount", "dtype": "Float64"}],
      "truncated": false
    },
    {"name": "lazy_orders", "kind": "LazyFrame", "state": "unresolved", "columns": [], "truncated": false}
  ]
}
```

The runtime mints a generation when a namespace is created, including after LRU eviction, and increments revision after every execution attempt, including failures. Expose these as optional fields on execution responses so the frontend can reject metadata from a different runtime generation.

### Implement

1. NEW `RUNTIME/lsp/dataframe_schemas.py`: inspect supported Polars objects in `_peek_namespace`. For DataFrames read column dtypes without reading row values. For LazyFrames report unresolved; do not call collect or collect_schema in this endpoint. Return at most 100 dataframe entries and 2,000 columns per entry, with truncation flags.
2. Update `RUNTIME/main.py` for generation/revision bookkeeping on create, execute, clear, and eviction; register the endpoint. Do not inspect mutable objects during active execution: return cached metadata marked busy or an unavailable result.
3. Add matching response models in `RUNTIME/lsp/models.py` and `CORE/lsp/models.py`. Add the owner-checked Core route in `CORE/kernel/routes.py`, following `_lsp_forward` feature-flag/old-runtime fallback behavior but using the new request model. Extend `CORE/kernel/manager.py` forwarding as needed.
4. Add `dataframeSchemas()` to `APP/api/lsp.api.ts`. NEW `NB/useDataframeSchemas.ts` caches per kernel/session/generation. Refresh on notebook open with a live session and after execution completion. Deduplicate in-flight requests; never request metadata on every keystroke.
5. NEW `PY/dataframeColumnContext.ts`: use CodeMirror's Python syntax tree to find the enclosing method receiver, including recovery from an unfinished string. Resolve simple variable aliases from preceding source. Do not use regex alone to resolve nested calls.
6. NEW `PY/dataframeColumnCompletions.ts`: consume the context and cache. Register in `buildNotebookCompletionSources`. Remove overlapping emission from the old upstream source; retain it as the labelled unresolved-context fallback for node inputs.
7. For lazy catalog reads, resolve literal table references using existing catalog metadata through `APP/api/catalog.api.ts`. Seed node input schemas from `useUpstreamColumns.ts`. Treat ambiguous names as unresolved; do not execute reads to resolve them.
8. Add explicit transfer rules for simple assignment/alias, select by literal names, rename by literal mapping, and drop by literal names. This lets common LazyFrame chains work without execution. Unsupported transforms stop inference rather than preserving an incorrect schema.
9. Mark metadata outdated immediately on source edits. Prefer a resolved current-source schema over old runtime state; otherwise label suggestions “last run” and suppress missing-column diagnostics.

### Explicit limits

No pre-execution dtype inference for arbitrary with_columns expressions, aggregations, joins, Python functions, dynamic table names, or dynamic selectors in this change. After an eager result has executed, its observed schema is available. Unresolved LazyFrames remain visibly unresolved until supported catalog/input inference supplies their schema. No missing-column squiggles in this release.

### Design (decided 2026-09-20)

This section is the build contract for Change 4. Where it is more specific than
"Implement" above, it wins; where it widens a rule, it says so and why.

#### What the user experiences

1. **Column names appear the moment you open a quote in a column position** —
   `orders.select("`, `orders.group_by('`, `orders.filter(pl.col("`, `orders["` —
   and each row shows the column name, its dtype and where that knowledge came
   from. Nothing has to run first: a frame read from a catalog table or a node
   input already has a schema, and a plain chain of `select` / `rename` / `drop`
   (and row-only operations such as `filter`, `sort`, `head`, `unique`) keeps it.
2. **Running a cell adds what the kernel actually observed.** An eager
   `DataFrame` (anything `.collect()`ed) reports its real columns and dtypes after
   the run, labelled `last run`. If the cell that produced the frame has changed
   since (or its badge says it is stale), the label becomes `last run, outdated`
   — the same story the amber badges from Change 3 already tell.
3. **When the editor cannot know, it says nothing rather than guessing.** After
   a `join`, `agg`, `with_columns`, a dynamic table name, or an uncollected
   `LazyFrame` past an unsupported transform, the popup does not appear. In a
   Python Script node the one exception is deliberate: `pl.col("` with an
   unresolved (or absent) receiver lists every input's columns, each row labelled
   with its input, so `id · Int64 · input main` and `id · String · input lookup`
   stay two rows. A catalog notebook never guesses a receiver.
4. **Both quote styles, string lists, multiline calls and unfinished strings all
   work.** A column name containing the active quote or a backslash is escaped on
   insert. Accepting a row replaces the whole string content, not just the prefix.
5. **Nothing reads data.** No `collect`, no LazyFrame plan resolution, no
   user-defined property or `repr`. The kernel reports names and dtypes of eager
   frames only.
6. **Reset session, kernel switch or restart forget the `last run` knowledge;
   the static knowledge stays.** An old kernel image or a disabled LSP behaves
   the same: static column completions keep working, `last run` rows are absent.
7. **Typing never waits on the kernel.** Schema metadata is fetched when a
   notebook opens on a live kernel and after a run settles, never per keystroke.

#### One resolution engine

The receiver of a column position is an **expression**, not a name. The engine
infers the schema of an expression by walking it:

- `VariableName` → the last top-level assignment to that name *before this
  point* (prior cells in notebook order, then the current cell up to the
  cursor), and the RHS is inferred *as of that assignment*, so `df = df.select(...)`
  refers to the previous `df`. Resolution indices strictly decrease, so it
  terminates without a visited set (a depth cap of 32 guards pathological chains).
  Tuple/starred/augmented assignments and nested (indented) assignments are ignored.
- `CallExpression` on a `MemberExpression` → infer the object, then apply the
  method rule below.
- A **root** call produces a schema from outside the source (next section).
- Anything else → unresolved.

Every result carries `columns` (`{name, dtype}`; dtype may be `""` when only
names are known), `kind` (`DataFrame | LazyFrame | unknown`), `provenance`
(`source | runtime`), a `sourceLabel`, and `outdated`.

**Method rules** (applied to a resolved receiver schema; any other method, or a
supported method with non-literal arguments, stops inference → unresolved):

| Method | Effect on schema |
| --- | --- |
| `select(args)` | subset in argument order. Accepted args: `"a"`, `["a", "b"]`, `pl.col("a")`, `pl.col("a").alias("x")` (dtype of `a`), keyword `x=pl.col("a")`. A name missing from the input stops. |
| `rename({"a": "b"})` | renamed, dtype kept. Missing key or non-dict stops. |
| `drop("a", ["b"])` | removed. Missing name stops. |
| `collect()` / `lazy()` | schema kept; kind becomes `DataFrame` / `LazyFrame`. |
| `filter`, `sort`, `head`, `tail`, `limit`, `slice`, `unique`, `drop_nulls`, `reverse`, `clone`, `cache`, `rechunk` | schema kept (row-only; can never be wrong). *Widens Decision 4: the risk that decision guarded against — offering an incorrect schema — cannot occur for these.* |
| `group_by(...)` | schema kept, flagged `grouped` so `pl.col` inside the following `.agg(` sees the pre-group columns; `.agg(...)` itself stops. |
| `with_columns`, `join`, `agg`, `pivot`, `explode`, `unnest`, `pipe`, `map_*`, `__getitem__`, everything else | stop. |

**Roots:**

| Root expression (literal arguments only) | Schema source | kind |
| --- | --- | --- |
| `flowfile_ctx.read_input("x")`, `read_input()` (= `"main"`), `read_first("x")` (`flowfile.` alias too) | node input `x` from `useUpstreamColumns` (columns grouped by `source_input`) | LazyFrame |
| `flowfile_ctx.read_catalog_table("t")`, `(..., schema="s")`, `(..., namespace_id=N)` | catalog table metadata (strict resolve, below) | LazyFrame |
| `flowfile_ctx.get_catalog("c").get_schema("s").read_table("t")` and `.get_table_ref("t").read()` | same | LazyFrame |
| `pl.DataFrame({"a": ..., "b": ...})`, `pl.LazyFrame({...})`, `pl.from_dict({...})` | names from the literal string keys, dtype `""` | as constructed |
| a name the kernel knows (last executed namespace) | runtime metadata, `provenance: runtime` | as reported |

Precedence for a variable: a resolved current-source chain wins; otherwise the
runtime entry (`last run`); otherwise unresolved. The one exception is a chain
that knows only names and no dtypes (a literal `pl.DataFrame({...})` root): when
the runtime knows the same variable with exactly the same column-name set, the
runtime entry wins, so the literal frame picks up the dtypes the kernel observed.
The runtime entry is
**outdated** when the cell holding the variable's last assignment (prior cell or
the current cell) is unrun, stale (any `staleReason`), or edited since submission
(`sourceRevision !== submittedRevision`). A runtime name with no assignment in
the visible source (another node on the same flow, a deleted cell) is plain
`last run`.

#### Column positions (where the popup fires)

Located on the CodeMirror syntax tree (`syntaxTree` / `ensureSyntaxTree`, never a
regex): `resolveInner(pos, -1)` must land on a `String` that starts with a single
`'` or `"` (no prefix letter, no triple quote; `FormatString` rejected). An
unterminated string is still a `String` node, so one path covers finished and
unfinished literals. Climb: `String` → (`ArrayExpression`)? → `ArgList` → `CallExpression`.

| Enclosing call | Receiver expression | Fires when |
| --- | --- | --- |
| `<expr>.select / with_columns / filter / sort / group_by / agg / drop / unique / drop_nulls / explode / partition_by ("…")` | `<expr>` | always (the receiver's schema *before* the call is what `with_columns` needs) |
| `<expr>.rename({"…"` (dict **key** position only) | `<expr>` | always |
| `<expr>["…"]` (`MemberExpression` subscript) | `<expr>` | only when the receiver resolves with `kind === "DataFrame"` |
| `pl.col / col / pl.exclude / pl.sum / pl.mean / pl.min / pl.max / pl.first / pl.last / pl.median / pl.n_unique / pl.std / pl.var ("…")` | the nearest enclosing call from the first row (climb ≤ 8 levels from the `col` call) | with a receiver: that schema; without one → **bare-col** |

Bare-col and an **unresolved receiver** behave the same: node surface → every
upstream column, one row per input, `detail: "<dtype> · input <name>"`; catalog
surface → null. Subscript never falls back.

Result shape: `from` = first content offset, `to` = end of the string content
(the closing quote excluded, or `pos` when unfinished), `validFor` =
`/^[^"\\]*$/` or `/^[^'\\]*$/` by quote, `apply` set only when the label contains
the active quote or a backslash, options `{label, type: "property", detail, boost: 6}`.
`detail` carries dtype and source because `withoutInfo` strips `info`.

**Labels** (`detail`): `Float64 · orders` (static, receiver variable name);
`Float64 · orders (catalog)` / `Int64 · main (input)` when the receiver is a
root call rather than a variable; `Float64 · orders (last run)`;
`Float64 · orders (last run, outdated)`; `Int64 · input main` (fallback rows);
` · ` alone precedes the source when dtype is `""`.

#### Runtime metadata lifecycle

Kernel (`RUNTIME/main.py`): `_namespace_generation[flow_id]` (uuid4 hex, minted
in `_get_namespace` on creation — the only creation site, so LRU recreation
mints a new one), `_namespace_revision[flow_id]` (0 on creation, +1 after every
execution attempt including failures and interrupts, stamped onto
`ExecuteResponse.namespace_generation/revision` through one exit path in
`_execute_sync`), `_executing_flow_ids[flow_id]` (refcount held for the whole of
`_execute_sync`). `_evict_oldest_namespace`, `_clear_namespace` and the
no-`flow_id` `/clear` branch drop the bookkeeping with the namespace.

`POST /lsp/dataframe_schemas` `{flow_id, node_id?}` → `{namespace_generation,
revision, state, dataframes}` with top-level `state`:
`unavailable` (no namespace → never allocates one; or timeout),
`busy` (refcount > 0: generation/revision present, `dataframes` empty),
`ready`. Collection runs in `asyncio.to_thread` under `_LSP_TIMEOUT_S` over
`_peek_namespace`'s copy. Per frame: `state: ready` for eager frames with
`columns`, `state: unresolved` with no columns for `LazyFrame`s. Caps: 100
frames (alphabetical), 2 000 columns per frame with `truncated: true`.

Frontend (`NB/useDataframeSchemas.ts`, module cache keyed by owner id, read
synchronously by the completion source): entry `{kernelId, flowId, generation,
revision, frames: Map<name, schema>}`. Refresh points: attach (notebook opened
with a kernel selected), execution settled with any verdict but `discard`,
session epoch bump (invalidate, forget the last seen generation, then refresh —
the new kernel may already hold this flow's namespace, under a generation of its
own), explicit call. **Batch-aware:** a settle schedules the
refresh on a macrotask and skips it while `isBatchActive(ownerId)`, so Run All
produces one request at the end instead of one per cell. A response is accepted
only when the owner's kernel/flow are unchanged, its generation matches the last
generation seen on a settle (when one is known), and its revision is not older
than the cached one. `busy` keeps the current entry and retries once after
1.5 s; `unavailable` drops it. A settle whose `namespace_generation` differs from
the cached entry drops the entry before refreshing.

#### Catalog and input roots

`NB/catalogRefResolver.ts` scans sources for the literal catalog root forms and
resolves each distinct reference **once** through `GET /catalog/tables/resolve`
with `strict=true` (`namespace_id=N` → `q=t&namespace_id=N`; `schema="s"` or a
3-part chain → `q="s.t"`, using `catalogStore.tree` to map `catalog → schema →
namespace_id` when loaded; bare `t` → `q=t`). 404 and 409 (ambiguous) both mean
unresolved. Results are memoized per reference key with in-flight dedupe;
negative results expire after 30 s so a table created later becomes visible.
Refs found in prior cells are pre-resolved on attach and after each settle; a
ref typed in the current cell resolves lazily the first time the completion
source needs it (the source returns a promise for that one call, synchronous
otherwise). Never a read, never a collect. Node inputs come from
`useUpstreamColumns` (dtypes as the flow reports them).

#### Safety rules (backend)

- Type dispatch uses `type(obj)` and `issubclass`, never `isinstance` on the
  object (a `__class__` property could lie) and never `getattr` on unknown objects.
- Eager schemas are read through the base-class getter
  (`pl.DataFrame.schema.__get__(obj)`), so a user subclass overriding `schema`
  is never invoked; `LazyFrame`s get no attribute access at all; `pl.Series`,
  pandas and everything else are ignored.
- Names starting with `_`, plus `flowfile_ctx` and `flowfile`, are skipped;
  non-`str` keys ignored; each frame's body is wrapped in `try/except Exception:
  continue`.
- The response never contains a row value; a test plants a sentinel string
  value in a frame and asserts it is absent from `resp.text`.

#### Degradation matrix

| Situation | Static (catalog/input/literal) rows | `last run` rows |
| --- | --- | --- |
| No kernel selected | yes | no |
| Kernel stopped / starting | yes | no (bridge returns `{}` → unavailable) |
| Kernel image < 0.5.5 | yes | no (404 → `{}`; core logs once per `(kernel, op)`) |
| `FLOWFILE_LSP_ENABLED` off | yes (no request is made) | no |
| Kernel busy for this flow | yes | previous entry kept, one retry |
| Reset session / kernel switch | yes | dropped until the next run |
| Namespace evicted (LRU) | yes | dropped on the next settle (generation changed) |

#### Module map

Backend: `RUNTIME/lsp/dataframe_schemas.py` (pure collector), `RUNTIME/main.py`
(bookkeeping + endpoint), `RUNTIME/lsp/models.py` ≡ `CORE/lsp/models.py`
(four new models, byte-identical field lists), `RUNTIME/lsp/analysis.py` and
`CORE/lsp/routes.py` `_FEATURES += "dataframe_schemas"`, `CORE/kernel/routes.py`
(`_lsp_forward` widened to `BaseModel`, new route), `CORE/kernel/manager.py`
(`_lsp_unsupported_warned: set[tuple[str, str]]`, op-aware warning text),
`CORE/kernel/models.py` (`ExecuteResult` + two optional fields), kernel 0.5.5
(`make bump-version-kernel VERSION=0.5.5` + `images.py` pins).

Frontend: `APP/api/lsp.api.ts` (`dataframeSchemas`), `APP/types/kernel.types.ts`,
`APP/api/catalog.api.ts` (`resolveTableStrict`), `PY/dataframeColumnContext.ts`
(positions), `PY/dataframeSchemaInference.ts` (engine, roots, method rules,
memoized per-cell assignment index), `NB/catalogRefResolver.ts`,
`NB/useDataframeSchemas.ts`, `PY/dataframeColumnCompletions.ts` (the
CompletionSource), `PY/notebookEditor.ts` (options `getOwnerId`, `getCellId`,
`getPriorCells` `{id, code}[]`, `getSurface`; registration; removal of
`createUpstreamColumnCompletions`), wiring in `CatalogNotebookCell.vue`,
`NotebookCell.vue`, `NotebookEditor.vue`, `NotebookPanel.vue`, `PythonScript.vue`.

### Done when

- [x] Two frames have `id` with different types; each receiver shows the correct type.
- [x] All expressions in the support table work with single/double quotes and multiline code.
- [x] Renaming a catalog-backed LazyFrame column offers the new name without collecting data.
- [x] An unsupported transform never offers its input schema as a certain output schema.
- [x] Reassigning a variable, failed execution, kernel reset, and LRU recreation invalidate old metadata. (Reassignment and kernel reset verified live; failure-still-increments-revision verified against the live kernel; LRU recreation by `kernel_runtime/tests/test_dataframe_schemas.py`.)
- [x] An old kernel image or disabled LSP leaves existing static completions usable. (0.5.4 image verified live — the bridge answers `unavailable` and core warns once per `(kernel, op)`; the LSP-disabled path by `flowfile_core/tests/kernel/test_dataframe_schemas_route.py`.)
- [x] No endpoint returns dataframe row values or invokes user-defined properties/repr.

Tests: NEW `kernel_runtime/tests/test_dataframe_schemas.py`, NEW `flowfile_core/tests/kernel/test_dataframe_schemas_route.py`, NEW `PY/dataframeColumnContext.test.ts`, NEW `PY/dataframeColumnCompletions.test.ts`; extend `notebookEditorCompletions.test.ts` and model-sync tests.

## Release check

Changes 1–4 have all shipped on their branches; nothing in this plan is
outstanding.

The release is complete only when Changes 1–4 work in both surfaces. Run the focused new tests, existing notebook/store/completion tests, Vue type checking, and existing kernel LSP/model-sync tests. Use non-fixing lint on touched frontend files; the repository-wide lint script includes --fix.

Demo fixture:

1. Create a catalog notebook with a heading, an orders dataframe, a customers dataframe with different column types, and two analysis cells.
2. Reorder and undo; duplicate and advance; collapse and restore an output.
3. Run, edit upstream code, and confirm stale labels.
4. Type column references against both frames and confirm correctly scoped types.
5. Repeat the applicable actions in a Python Script node with two differently typed inputs.
6. Save and reopen; reset the kernel and repeat completions before/after execution.

Performance checks: use 100 cells and a 1,000-column schema. Cached completions should appear within 100ms p95 on a recorded development machine. Typing must not wait on metadata network calls. A reorder must preserve EditorViews, not recreate all 100 editors. Record measured results in the implementing PRs.
