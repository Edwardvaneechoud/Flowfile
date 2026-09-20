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

Not started. There is no `NB/notebookRuntimeState.ts`, no source-revision,
session-epoch or request-id bookkeeping anywhere in the notebook code, and none
of the status copy described below exists.

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

- [ ] Run A then B; edit A: both results are labelled outdated. Rerun A: A is current and B remains outdated.
- [ ] Edit a slow-running cell before completion: returned output is outdated.
- [ ] Start Run All, switch notebook tabs: results stay with the original notebook and the batch continues there.
- [ ] Reset/switch kernel while a response is in flight: that response cannot restore current state.
- [ ] Rapid duplicate execution requests do not execute a batch twice.

Tests: NEW `NB/notebookRuntimeState.test.ts`; extend `APP/stores/notebook-store.test.ts`; browser test edit-during-run and tab-switch races using controlled API responses.

## Change 4 — Dataframe-aware column names and types

### Status

Not started. `dataframe_schemas` appears nowhere in `kernel_runtime/` or
`flowfile_core/`, and none of the frontend modules below exist.
`PY/useUpstreamColumns.ts` remains the only column source for node notebooks.

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

### Done when

- [ ] Two frames have `id` with different types; each receiver shows the correct type.
- [ ] All expressions in the support table work with single/double quotes and multiline code.
- [ ] Renaming a catalog-backed LazyFrame column offers the new name without collecting data.
- [ ] An unsupported transform never offers its input schema as a certain output schema.
- [ ] Reassigning a variable, failed execution, kernel reset, and LRU recreation invalidate old metadata.
- [ ] An old kernel image or disabled LSP leaves existing static completions usable.
- [ ] No endpoint returns dataframe row values or invokes user-defined properties/repr.

Tests: NEW `kernel_runtime/tests/test_dataframe_schemas.py`, NEW `flowfile_core/tests/kernel/test_dataframe_schemas_route.py`, NEW `PY/dataframeColumnContext.test.ts`, NEW `PY/dataframeColumnCompletions.test.ts`; extend `notebookEditorCompletions.test.ts` and model-sync tests.

## Release check

Outstanding as of the date above: Changes 3 and 4. Changes 1 and 2 need no
further work.

The release is complete only when Changes 1–4 work in both surfaces. Run the focused new tests, existing notebook/store/completion tests, Vue type checking, and existing kernel LSP/model-sync tests. Use non-fixing lint on touched frontend files; the repository-wide lint script includes --fix.

Demo fixture:

1. Create a catalog notebook with a heading, an orders dataframe, a customers dataframe with different column types, and two analysis cells.
2. Reorder and undo; duplicate and advance; collapse and restore an output.
3. Run, edit upstream code, and confirm stale labels.
4. Type column references against both frames and confirm correctly scoped types.
5. Repeat the applicable actions in a Python Script node with two differently typed inputs.
6. Save and reopen; reset the kernel and repeat completions before/after execution.

Performance checks: use 100 cells and a 1,000-column schema. Cached completions should appear within 100ms p95 on a recorded development machine. Typing must not wait on metadata network calls. A reorder must preserve EditorViews, not recreate all 100 editors. Record measured results in the implementing PRs.
