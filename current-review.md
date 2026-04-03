# Code Review: CRUD Operations for Catalog

**Branch:** `feature/implement-crud-operations-catalog`
**Commit:** `ae748884`
**Reviewer notes:** Review of the diff adding upsert, update, delete, and append write modes to the catalog system.

---

## Summary

This PR extends the catalog writer from a simple overwrite/error model to full CRUD support (overwrite, error, append, upsert, update, delete) using Delta Lake merge operations. It also adds version-pinned reading for the catalog reader. The change touches backend (core + worker), frontend (reader + writer components), schemas, types, and tests.

**Overall impression:** Well-structured change. The architecture is sound -- merge operations are cleanly separated into a dedicated `merge_delta` worker function, the Pydantic validation catches missing merge keys early, and the frontend UX is thoughtful (mode descriptions, key column selector gated on merge modes, version picker). A few issues worth addressing below.

---

## Issues

### Bug: `merge_keys` can be `None` when building the predicate (worker)

**File:** `flowfile_worker/flowfile_worker/funcs.py:558`

```python
predicate = " AND ".join(f'target."{k}" = source."{k}"' for k in merge_keys)
```

The function signature declares `merge_keys: list[str] | None = None`. If `merge_keys` is `None` and the table exists, this line will raise `TypeError: 'NoneType' is not iterable`. The Pydantic `_validate_merge_keys` validator on `CatalogWriteSettings` protects the normal flow-graph path, but the worker function is a public interface called via dynamic dispatch (`getattr(funcs, operation)`). Any caller bypassing Pydantic validation (direct worker API call, future internal use) would hit this.

**Suggestion:** Add an early guard at the top of the `else` (table exists) branch:
```python
if not merge_keys:
    raise ValueError("merge_keys is required for merge operations on existing tables")
```

### Semantic concern: `update`/`delete` on non-existent tables

**File:** `flowfile_worker/flowfile_worker/funcs.py:535-543`

When the target table doesn't exist:
- **`update` mode** writes all source rows as a new table. But "update" semantics means "only update matching rows, no inserts." If there's no table, there are no rows to match -- writing everything contradicts the mode's meaning.
- **`delete` mode** creates an empty table with the source schema. A delete on a non-existent table producing an empty table is surprising behavior.

**Suggestion:** Consider either raising an error for update/delete on non-existent tables, or at minimum logging a warning. The current behavior could silently produce unexpected results.

### Missing test coverage for `merge_delta` worker function

There are no unit tests for the `merge_delta` function in `flowfile_worker/tests/`. The existing tests only cover `resolve_write_destination` in the catalog service. The merge logic (predicate building, schema evolution, upsert/update/delete branches) is non-trivial and would benefit from direct testing.

### Legacy parquet + merge modes: unclear cleanup path

**File:** `flowfile_core/flowfile_core/catalog/service.py:1363-1365`

When an existing table is a legacy parquet file and the write mode is a merge mode (upsert/update/delete), `resolve_write_destination` returns a new directory path at the same stem. The `merge_delta` worker will create a brand-new Delta table there (since it doesn't exist). This means:
1. The old parquet file is left behind (no cleanup)
2. The "merge" becomes a plain write (no existing data to merge against)

This may be intentional (merge into a non-existent table falls back to write), but it's worth documenting or warning the user that merging into a legacy-format table won't actually merge with existing data.

---

## Minor / Nits

### Frontend: `merge_keys` not cleared when switching away from merge modes

**File:** `CatalogWriter.vue`

When a user selects "upsert", picks key columns, then switches to "overwrite", the `merge_keys` array retains stale values. This doesn't cause backend errors (keys are ignored for non-merge modes), but it could confuse users if they switch back and see previously selected keys. Consider adding a watcher:
```typescript
watch(() => nodeData.value?.catalog_write_settings.write_mode, (newMode) => {
  if (!['upsert', 'update', 'delete'].includes(newMode)) {
    nodeData.value.catalog_write_settings.merge_keys = [];
  }
});
```

### `schema_mode: "merge"` for append might be too permissive

**File:** `flowfile_worker/flowfile_worker/funcs.py:604-605`

```python
elif mode == "append":
    delta_write_options["schema_mode"] = "merge"
```

Setting `schema_mode: "merge"` during append allows schema evolution (adding new columns). This is a design choice -- but it means appending data with extra columns silently widens the table schema rather than failing. If this is intentional, a comment explaining why would help. If not, `"strict"` might be safer.

### f-string in logger calls

**Files:** `flowfile_worker/flowfile_worker/funcs.py:528, 593, 598`

```python
flowfile_logger.info(f"Starting merge_delta ({merge_mode}) to: {output_path}")
```

Using f-strings in logger calls evaluates the string even when the log level is disabled. Prefer lazy formatting:
```python
flowfile_logger.info("Starting merge_delta (%s) to: %s", merge_mode, output_path)
```

This is a minor performance consideration and is consistent-ish with patterns seen elsewhere in the codebase, so low priority.

### Test name could be more descriptive

**File:** `flowfile_core/tests/test_catalog_delta.py:228`

`test_new_table_with_merge_mode` -- consider naming it `test_new_table_passthrough_upsert_mode` to make it clear what behavior is being tested (that the mode is passed through unchanged for new tables).

---

## What looks good

- **Pydantic `model_validator`** for `merge_keys` is the right place to catch this early -- clean and declarative.
- **Schema evolution handling** in `merge_delta` (detecting new columns and doing an empty append with `schema_mode: "merge"` before the actual merge) is a nice touch that prevents merge failures when source has new columns.
- **Version picker** on the catalog reader is well implemented -- clearable, defaults to latest, backward-compatible with existing saved flows.
- **Mode descriptions** in the writer UI are helpful for discoverability.
- **CSS-only tooltip** for truncated params in `TableDetailPanel.vue` is lightweight and avoids adding a tooltip library dependency.
- **`resolve_write_destination` simplification** -- the old logic that hard-coded "error" for new tables was confusing; passing through `write_mode` is clearer and lets the worker handle mode semantics.
- **Backward compatibility** is handled in both reader (`delta_version === undefined`) and writer (`merge_keys` fallback).
- **Test coverage** for `resolve_write_destination` with the new modes (append, upsert, new table with merge mode) is solid.
