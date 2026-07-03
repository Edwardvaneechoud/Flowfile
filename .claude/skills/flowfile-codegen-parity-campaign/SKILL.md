---
name: flowfile-codegen-parity-campaign
description: An executable, decision-gated campaign to close the parity gap between a visual Flowfile flow, its exported Python code (Polars and FlowFrame), and the FlowFrame API — reproduce the baseline, work the divergence inventory, fix by the ranked solution menu, and promote through change control. Use when asked to "fix code generation", "export to Python is wrong", "flow result differs from generated code", "close the codegen xfails", "delete a stale xfail/XPASS", work on `flowfile_core/flowfile_core/flowfile/code_generator/`, or touch `test_code_generator*` parity tests.
---

# Flowfile codegen parity campaign

**Read this first.** Flowfile ships *two executors of the same node settings*: the live DAG engine
(what the Designer runs) and the **code generator** (`export_flow_to_polars` /
`export_flow_to_flowframe`), which emits standalone Python. When they disagree, the exported code
runs clean but produces a *different answer* — silent, order-dependent, data-corrupting bugs. The
sort bug in incident #544 (a `"desc"` vs `"Descending"` string mismatch) shipped exactly because
generation was never tested against execution. This skill is the runbook to find and close those
gaps **measurably** — never "by eye".

The maintainer named this the single hardest live problem in the repo (2026-07-03).

## What "parity" means (the only success criterion)

Parity holds for a flow when, for every output node:

```
collect( execute_flow(flow).node[N] )  ==  collect( exec(export_code(flow))["run_etl_pipeline"]() )
```

i.e. **same input flow → exported code → executed result equals the in-engine result**, compared with
`polars.testing.assert_frame_equal(..., check_column_order=False, check_row_order=False)`. The repo
encodes this exact check in `assert_flow_result_matches_generated(flow, output_node_id, code)`
(`flowfile_core/tests/flowfile/test_code_generator_edge_cases.py:65`). Every parity test builds a
flow, runs both executors, and asserts frame-equality.

**Campaign success is binary and machine-checked:** zero `xfailed` and zero `xpassed` in the codegen
edge-case suite, plus the full `test_code_generator.py` corpus (parameterized over *both* exporters)
still green, plus `make check_stubs` clean. If you cannot state the pass/xfail numbers, you are not done.

## When NOT to use this skill

- **Adding a FlowFrame method / understanding how the emitter chooses native-node vs polars-code /
  `Expr._repr_str` mechanics** → use `flowfile-frame-and-codegen` (that is the home for emitter
  internals; this skill only *fixes divergences*).
- **Building a brand-new visual node** → `flowfile-node-development`.
- **The narrative history of the #544 sort bug and its sibling codegen bugs** →
  `flowfile-failure-archaeology` (this skill uses #544 only as the cautionary rule).
- **PR mechanics, drift gates, branch policy** → `flowfile-change-control` (routed to, not duplicated).
- **Isolated-DB / env-flag rationale** → `flowfile-config-and-flags` and
  `flowfile-testing-and-validation` (this skill states the one command it needs and cross-refs there).
- **`make stubs` internals / build orchestration** → `flowfile-build-and-env`.

---

## Phase 0 — Environment (do this once, every session)

The code generator lives entirely in core. Importing `flowfile_core` auto-runs Alembic against the
catalog DB, so **isolate the DB on every test invocation** or you will corrupt the shared catalog.

```bash
# Pick a scratch DB path OUTSIDE the repo, one per parallel run. NEVER omit this.
export PARITY_DB=/tmp/ff_parity_$$.db
```

Hard rules (see `flowfile-config-and-flags` for why):
- **Always** prefix pytest with `FLOWFILE_DB_PATH=$PARITY_DB`.
- **Never** combine a *fresh* `FLOWFILE_DB_PATH` with `FLOWFILE_SKIP_STARTUP_MIGRATION=1` — the DB
  will have no tables and you get `no such table: users` cascades.
- Run from the repo root with `poetry run`.

---

## Phase 1 — BASELINE (reproduce, then freeze the numbers)

Run these two commands **now, before changing anything**, and confirm you see the embedded numbers.
If your numbers differ, the inventory below is stale — STOP and re-verify against the source (Phase 2
line refs) before proceeding.

### 1a. The codegen edge-case suite (the campaign scoreboard)

```bash
FLOWFILE_DB_PATH=$PARITY_DB poetry run pytest \
  flowfile_core/tests/flowfile/test_code_generator_edge_cases.py \
  -q -p no:cacheprovider -rX
```

**Expected baseline as of 2026-07-03 (v0.12.7), re-verified by running (~1s):**

```
XPASS ...::TestBasicFilterOperators::test_in_operator_numeric - BUG: Flow's IN filter quotes numeric values incorrectly. ...
46 passed, 2 xfailed, 1 xpassed, 18 warnings in ~1s
```

- The **2 xfailed** are the two live divergences (Phase 2 rows X1, X2).
- The **1 xpassed** is a **stale marker** — the bug it guards was already fixed. `-rX` prints it.
  **The campaign rule: an XPASS in this repo means "delete the marker".** None of these markers are
  `strict=True`, so CI stays green and nobody notices the rot.

Branches:
- `no such table: users` (or similar OperationalError cascade) → `FLOWFILE_SKIP_STARTUP_MIGRATION=1`
  is set in your environment alongside a fresh DB. `unset FLOWFILE_SKIP_STARTUP_MIGRATION`, pick a new
  `$PARITY_DB`, re-run. Never combine that flag with a fresh test DB.
- **More than 1 `xpassed`** → additional markers went stale since 2026-07-03; each one is a Phase 3.1
  delete-the-marker row. `-rX` names them all.
- **Any `failed` or fewer than 46 passed** → the tree drifted from this frozen baseline; re-verify the
  Phase 2 anchors with the Provenance greps before touching anything.

### 1b. The full parity corpus (regression floor — must stay green)

```bash
FLOWFILE_DB_PATH=${PARITY_DB}.main poetry run pytest \
  flowfile_core/tests/flowfile/test_code_generator.py \
  -q -p no:cacheprovider
# Expected baseline 2026-07-03 (v0.12.7), re-verified by running: 653 passed  (~77s, no skips locally)
```

The round-trip flow-builders are parameterized `@pytest.mark.parametrize("export_func",
[export_flow_to_polars, export_flow_to_flowframe], ids=["polars", "flowframe"])` — 88 parametrize
sites producing the bulk of the 653 collected — so the corpus is the sample-flow set for round-trip
equivalence across *both* export frameworks. (The un-parameterized remainder are exporter-specific
tests such as `test_flowframe_*` at `:1326+`.) **This 653 is your floor.** Any fix that drops it has
regressed parity.

Branches:
- Any `failed` → re-run with `-rf` to name them; a fix that breaks the corpus is a parity regression,
  full stop — revert or fix before proceeding.
- **More than 653 passed, no failures** → a merge added parity tests; re-freeze the number in your
  notes and use the observed count as the new floor.

> There is no directory of `.flowfile` fixtures for parity — the corpus is these programmatic
> flow-builders. Add new sample flows *there*, next to `create_sample_dataframe_node`
> (`test_code_generator.py:318`), not as loose files.
>
> A third file exists: `test_code_generator_custom_nodes.py` (baseline: `5 passed`, <1s). It covers
> custom-node emission on the **Polars exporter only**; run it too whenever you touch
> `code_generator.py`'s custom-node path.

---

## Phase 2 — INVENTORY (the worklist)

Every known divergence, with a status column. Work top-down; each row links to a fix phase.

| ID | Divergence | Buggy side | Location (verify before editing) | Guard test | Status 2026-07-03 |
|----|-----------|-----------|----------------------------------|-----------|-------------------|
| S1 | **Stale XPASS**: IN-filter numeric quoting — flow now splits on `,` before the numeric check, matching the emitter | (already fixed in `filter_expressions.py:_build_in_expression`, splits at `:155`) | marker `test_code_generator_edge_cases.py:201` | `TestBasicFilterOperators::test_in_operator_numeric` | **XPASS — delete marker** |
| X1 | `unique(columns=[])` (all columns): flow's unique uses `group_by` internally → "at least one key is required in a group_by operation"; emitted `.unique(keep='first')` is correct | **flow engine (executor)** | fix site `flow_data_engine.py:2686` (`make_unique`, called from `flow_graph.py:2415`); marker `test_code_generator_edge_cases.py:459` | `TestUniqueOperationVariations::test_unique_without_columns` | **live xfail** |
| X2 | group_by concat aggregation: emitter emits `str.concat` with default `-` delimiter; flow uses `,` → `['x-y']` vs `['x,y']` | **emitter** | map `expression_helpers.py:152` (`"concat": "str.concat"`), emitted with no args at `transform_handlers.py:28`; engine truth: `transform_schema.py:119` (`string_concat`, `delimiter=","`) | `TestGroupByEdgeCases::test_groupby_with_concat_aggregation` | **live xfail** |
| D1 | Emitter emits deprecated `str.concat` (Polars → `str.join`; note default delim differs: `-` vs `""`) | emitter | `expression_helpers.py:152` | `test_groupby_concat_uses_deprecated_str_concat:1004` (asserts deprecated form; TODO@:1054) | **passing, documents debt** |
| D2 | Emitter emits deprecated `with_row_count` (Polars → `with_row_index`) | emitter | `transform_handlers.py:370` | `test_record_id_uses_deprecated_with_row_count:960` (TODO@:1002) | **passing, documents debt** |
| F1 | FlowFrame export: right joins emit `.collect().lazy()` → returns a `pl.LazyFrame`, breaks the FlowFrame chain | emitter (FlowFrame framework) | `join_handlers.py:477` `TODO(FlowFrame)` | (no dedicated round-trip guard) | **open** |
| F2 | FlowFrame export: formula nodes emit `pl.col`/`pl.lit` without `import polars as pl` when `framework == "ff"` | emitter (FlowFrame framework) | `transform_handlers.py:54` `TODO(FlowFrame)` (+ `:326`) | (partial via `test_flowframe_formula_*`) | **open** |
| F3 | FlowFrame export: fuzzy-match serializes Polars `Expr` via `repr` → invalid code `pl.lit(<Expr ['len()'] at 0x...>)` | emitter (FlowFrame framework) | `transform_handlers.py:259` `TODO(FlowFrame)` | (none) | **open** |
| F4 | FlowFrame export: polars-code nodes reference `ff.LazyFrame`, which the `flowfile` package does not export | emitter (FlowFrame framework) | `code_generator.py:567` `TODO(FlowFrame)` | (none) | **open** |
| S2 | **Stale XPASS (adjacent subsystem, not codegen)**: node-designer `"numeric"` string alias | (fixed) | marker `node_designer/test_node_designer.py:516` | `TestNumericStringAliasBug` | **XPASS — delete marker** (out of campaign scope; clean up if touching node_designer) |

Two secondary emitter defects that are not yet guarded by a parity test (open, lower priority): param
codegen accepts Python keywords as parameter names → invalid signature (`param_codegen.py:38`); typed
flow-parameter empty default exports `""` for int/float params
(`flowfile_core/flowfile_core/flowfile/param_types.py:96` — note: one level above `code_generator/`).
Add a failing parity test before fixing either.

> **The FlowFrame framework-prefix trap** underlies F1–F4: the same converter emits both `pl.`-prefixed
> Polars and `ff.`-prefixed FlowFrame code. `export_flow_to_flowframe` is newer and less complete than
> `export_flow_to_polars`. When a bug is FlowFrame-only, fix the FlowFrame converter path — do not
> "fix" it by making the Polars path emit FlowFrame syntax.

---

## Phase 3 — Fix phases (ordered by leverage ÷ risk)

Do them in order. Each phase names the command, the **exact expected observation**, and a
`if you see X → do Y` branch. Do not batch phases; land and verify one at a time.

### Phase 3.1 — Delete the stale XPASS markers (highest leverage, lowest risk)

Removing a stale `xfail` is pure debt-payoff: the test already passes, so deleting the decorator makes
it a normal green test that will *now* catch regressions.

1. Edit `test_code_generator_edge_cases.py:201` — delete the `@pytest.mark.xfail(...)` decorator on
   `test_in_operator_numeric` (row S1). Leave the test body and docstring; optionally trim the
   "will fail until fixed" line in the docstring.
2. (If working near node_designer) delete the decorator at `node_designer/test_node_designer.py:516`
   (row S2).

Verify:

```bash
FLOWFILE_DB_PATH=$PARITY_DB poetry run pytest \
  flowfile_core/tests/flowfile/test_code_generator_edge_cases.py \
  -q -p no:cacheprovider -rX
```

- **Expect: `47 passed, 2 xfailed` and NO `xpassed` line.**
- If you see `1 xpassed` still → you deleted the wrong decorator; the XPASS is `test_in_operator_numeric`.
- If you see `1 failed` → the underlying fix regressed between baseline and now; re-run Phase 1a, and
  if it reproduces, that IN-filter split (`filter_expressions.py:155`) has been reverted — treat as a
  new bug, not a stale marker.

### Phase 3.2 — X2 + D1: group_by concat delimiter (emitter fix)

The emitter must emit the flow's *actual* concat delimiter. Row X2 is a correctness bug; D1 is the
paired deprecation. Fix them together (the delimiter must be explicit either way, because
`str.concat` defaults to `-` and `str.join` defaults to `""`, but the flow uses `,`).

1. The delimiter is **not** a stored node setting. The engine hard-codes it in
   `transform_schema.string_concat` (`transform_schema.py:117-119`: `.str.concat(delimiter=",")`),
   selected by `AggColl.agg_func` when `agg == "concat"` (`transform_schema.py:886`). The emitter maps
   `"concat" → "str.concat"` in `_get_agg_function` (`expression_helpers.py:152`) and the call site
   emits the mapped name with **no arguments** (`transform_handlers.py:28`:
   `f"{self.framework}.col({old}).{agg_func}().alias({new})"`) — so Polars' default `-` wins. Fix: make the
   concat aggregation emit `str.join(",")` (modern name, delimiter explicit to match the engine). The
   map returns bare method names that get `()` appended, so special-case concat at the emit site or
   let the map carry an argument string.
2. Update D1's guard `test_groupby_concat_uses_deprecated_str_concat` (`:1004`): flip its TODO at
   `:1054` — assert `uses_modern and not uses_deprecated`.
3. Delete the `@pytest.mark.xfail` on `test_groupby_with_concat_aggregation` (`:766`).

Verify (both classes — the D1 guard lives in `TestDeprecatedMethodUsage`, not `TestGroupByEdgeCases`):

```bash
FLOWFILE_DB_PATH=$PARITY_DB poetry run pytest \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestGroupByEdgeCases" \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestDeprecatedMethodUsage" \
  -q -p no:cacheprovider -rX
```

- **Expect: `3 passed`, 0 xfailed, 0 xpassed.** (Baseline for this pair is `2 passed, 1 xfailed`;
  `TestGroupByEdgeCases` holds exactly one test today.)
- If `test_groupby_with_concat_aggregation` still `xfailed` → your emitted call still lacks the
  explicit delimiter. Print `export_flow_to_polars(flow)` inside the test to see the emitted
  `str.join`/`str.concat` call.
- If `test_groupby_concat_uses_deprecated_str_concat` fails → you changed the emission but did not
  flip the guard's assertion at `:1054` (or vice versa) — the two must land together.
- If `assert_frame_equal` fails with values like `'y,x'` vs `'x,y'` → the *element order inside the
  concatenated string* differs (the helper already ignores frame row/column order); ensure your emitted
  expression preserves the flow's group-member order — do not add a sort the flow doesn't have.

### Phase 3.3 — D2: `with_row_count` → `with_row_index` (emitter fix, semantics-identical)

`with_row_index(name=, offset=)` is the drop-in modern replacement. Swap it at
`transform_handlers.py:370`, then flip the D2 guard TODO at `:1002`.

Verify:

```bash
# D1 and D2 guards both live in TestDeprecatedMethodUsage (edge-case file:953).
# The round-trip guard for record-id semantics is TestRecordIdDeprecation (file:416).
FLOWFILE_DB_PATH=$PARITY_DB poetry run pytest \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestDeprecatedMethodUsage" \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestRecordIdDeprecation" \
  -q -p no:cacheprovider
```

- **Expect: `3 passed`, 0 failed** (2 guards in `TestDeprecatedMethodUsage` + 1 round-trip in
  `TestRecordIdDeprecation`; the pair is also `3 passed` at baseline — the swap must keep it so).
- If `test_record_id_uses_deprecated_with_row_count` fails → you swapped the emission but did not flip
  the guard TODO at `:1002` (they must land together).
- If the round-trip in `TestRecordIdDeprecation` fails → `with_row_index` defaults differ in your
  Polars pin — check `offset` handling before shipping.

### Phase 3.4 — X1: `unique(columns=[])` (executor fix — the flow is wrong)

This is the **only** row where the emitter is correct and the *flow engine* is buggy. The unique node
calls `FlowDataEngine.make_unique` (`flow_graph.py:2415` → `flow_data_engine.py:2686`). Its guard only
special-cases `columns is None`; an **empty list** `columns=[]` falls into the subset branch and emits
`.unique([], keep=...)`, which Polars rejects ("at least one key is required in a group_by operation").
Fix the **executor**: treat `columns in (None, [])` as all-columns → `.unique(keep=unique_input.strategy)`
(mirror the emitted code; note the current `None` branch also drops `keep=` — preserve the strategy).
Then delete the xfail at `:459`.

Because you are changing runtime engine semantics, you **must** keep the flow-vs-generated
equivalence test as the regression guard (it already exists — that is the point). Do not fix the
executor without it.

Verify:

```bash
FLOWFILE_DB_PATH=$PARITY_DB poetry run pytest \
  "flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestUniqueOperationVariations" \
  -q -p no:cacheprovider -rX
```

- **Expect: `2 passed`, 0 xfailed** (baseline for this class: `1 passed, 1 xfailed`).
- If the other `unique` test in the class now fails → your executor change altered the non-empty-columns
  path. Scope the fix to the `columns == []` branch only.
- **Also re-run the whole engine suite** before promoting — the exact command is Phase 5 step 3; an
  executor change touches every flow, not just codegen.

### Phase 3.5 — F1–F4: FlowFrame-export framework bugs (largest surface, do last)

These are the `TODO(FlowFrame)` markers. They have *no round-trip guard today*, so the first step for
each is **write a failing parity test** parameterized on `export_flow_to_flowframe`, modeled on the
existing `test_flowframe_*` tests (`test_code_generator.py:1326+`). Only then fix the converter.

Recommended sub-order (leverage first): F2 (formula `pl.` import — most common node) → F4 (polars-code
`ff.LazyFrame` signature) → F1 (right-join `.collect().lazy()`) → F3 (fuzzy-match Expr repr — rarest).
Each `TODO(FlowFrame)` comment enumerates 2–3 sanctioned fix options; pick the one that keeps the
Polars path untouched (see the framework-prefix trap in Phase 2).

Verify per fix: run the new parity test (must go red→green) **and** the full corpus (Phase 1b, must
stay 653+). Do not delete any `TODO(FlowFrame)` comment until its parity test is green and committed.

---

## Phase 4 — Solution menu (which fix is correct, and its obligation)

For any divergence, pick exactly one and honor its obligation. Ranked by preference.

1. **Fix the emitter** — *default and preferred* when the flow engine produces the correct result and
   the generated code is wrong (rows X2, D1, D2, F1–F4). Obligation: a round-trip parity test that
   builds the flow, exports, executes, and `assert_frame_equal`s against the engine. Cheapest, lowest
   blast radius — you change only exported text.

2. **Fix the FlowFrame executor / engine semantics** — correct only when the *engine* is the buggy
   side and the emitted code is right (row X1). Obligation: (a) the flow-vs-generated equivalence test
   as the permanent guard, **and** (b) a full engine-suite run, because you altered runtime behavior
   for every flow, not just export. **Never** change engine semantics purely to make a codegen test
   green without a flow-level regression test — that is precisely how #544 shipped.

3. **Quarantine with `xfail(strict=True)` + a real issue** — only when the fix is genuinely out of
   scope for this change (needs upstream Polars, or a large refactor). Obligation: the marker MUST be
   `strict=True` (so a later fix that makes it pass fails CI and forces marker deletion — no more silent
   XPASS rot), and the `reason=` MUST link a filed GitHub issue, not a `flow_graph.py:1055` line ref
   (those rot — the current stale reasons cite lines that have since moved). This is a last resort, not
   a way to defer work quietly.

**Enum-drift corollary (the #544 lesson):** if a divergence is a settings string parsed two ways
(`"desc"` vs `"Descending"`, `"asc"` vs `"Ascending"`, filter-operator spellings), the fix is a *single
shared parser* consumed by both the emitter and the engine — not two patched copies. The canonical
example is `transform_schema.is_descending` + `SortByInput.descending` (`transform_schema.py:952-970`),
which replaced the drifted `how == "desc"` comparison in both `code_generator.py` and the engine's
`do_sort`. Grep for other hand-rolled direction/enum comparisons before adding a third copy.

---

## FENCED WRONG PATHS (do not do these)

- **Never hand-edit the generated `.pyi` stubs** (`flowfile_frame/flowfile_frame/*.pyi`,
  `**/*.pyi`). They are machine-generated by `make stubs` and gated by `make check_stubs` in CI; hand
  edits get clobbered and fail the drift gate. If your fix changes the FlowFrame public surface,
  regenerate — see `flowfile-frame-and-codegen` / `flowfile-build-and-env`.
- **Never loosen a round-trip assertion to make a test green.** Do not add
  `check_dtypes=False`/`check_exact=False`, do not swap `assert_frame_equal` for a shape check, do not
  down-scope `assert_flow_result_matches_generated`. A weakened assertion is a parity regression that
  reads as a pass — worse than the original bug.
- **Never fix codegen by changing engine runtime semantics without a flow-level regression test.**
  This is the #544 failure mode verbatim: generation was untested against execution, so a silent
  Descending→ascending flip shipped. Solution-menu option 2 lists the two mandatory guards.
- **Never leave an XPASS in place.** XPASS = stale marker = delete it (Phase 3.1). If the bug is
  genuinely still open, the marker was mis-set — replace it with `xfail(strict=True)` + an issue link
  (menu option 3), never a plain non-strict xfail.
- **Never "fix" a FlowFrame-only bug by editing the Polars export path**, and vice-versa. They are two
  branches of one converter keyed on `self.framework`; keep the working path untouched.
- **Never delete a `TODO(FlowFrame)` comment before its parity test is green.** The comment is the only
  marker of a known-broken export.
- **Never run parity pytest without `FLOWFILE_DB_PATH` isolation** — you will migrate/corrupt the
  shared catalog DB and burn a later session.

---

## Phase 5 — VALIDATION AND PROMOTION (routed through change control)

Do not open a PR until all of this passes. Success is the numbers, never a visual read.

1. **Codegen edge-case suite is clean** (the scoreboard — must show zero xfailed/xpassed for rows you
   closed):
   ```bash
   FLOWFILE_DB_PATH=$PARITY_DB poetry run pytest \
     flowfile_core/tests/flowfile/test_code_generator_edge_cases.py \
     -q -p no:cacheprovider -rX
   # Target after closing S1+X1+X2: 49 passed, 0 xfailed, 0 xpassed
   # (46 baseline passed + 3 formerly-xfail/xpass now passing). Restate the exact number you observe.
   ```

2. **Full parity corpus still green over both exporters** (regression floor):
   ```bash
   FLOWFILE_DB_PATH=${PARITY_DB}.main poetry run pytest \
     flowfile_core/tests/flowfile/test_code_generator.py \
     -q -p no:cacheprovider
   # Must be ≥ 653 passed (baseline). A drop = parity regression; do not promote.
   ```

3. **Core engine suite** — mandatory whenever you took solution-menu option 2 (executor change; row
   X1), advisable otherwise:
   ```bash
   FLOWFILE_DB_PATH=${PARITY_DB}.core poetry run pytest flowfile_core/tests -m "not kernel" \
     -q -p no:cacheprovider
   # Success criterion: 0 failed. ~5,079 tests collected as of 2026-07-03 (76 kernel-marked are
   # deselected); long run. Autouse fixtures spawn the worker + Docker DB services — prerequisites
   # and the SKIP_WORKER_TESTS=1 worker-less form are in flowfile-testing-and-validation.
   ```

4. **Frame suite** — mandatory whenever you worked F-rows or touched anything under
   `flowfile_frame/` (exported FlowFrame code executes against this API):
   ```bash
   FLOWFILE_DB_PATH=${PARITY_DB}.frame poetry run pytest flowfile_frame/tests \
     -q -p no:cacheprovider
   # Success criterion: 0 failed (~620 collected as of 2026-07-03). Cloud tests skip without
   # MinIO on :9000 — prerequisites in flowfile-testing-and-validation.
   ```

5. **Stub drift gate** — mandatory if you touched any FlowFrame public surface (F1–F4 can):
   ```bash
   make check_stubs   # regenerates and fails on any .pyi git diff; see flowfile-build-and-env
   ```

6. **Lint:**
   ```bash
   poetry run ruff check flowfile_core flowfile_frame
   ```

7. **PR through `flowfile-change-control`** (branch off `main`, never force-push, never commit —
   stage the diff and hand the maintainer the exact `git add`/`git commit` commands per
   `flowfile-change-control` §6). The PR body MUST carry a **baseline-vs-after table**:

   | Suite | Baseline (2026-07-03) | After |
   |-------|----------------------|-------|
   | edge-case | 46 passed / 2 xfailed / 1 xpassed | (yours) |
   | corpus | 653 passed | (yours) |

   List each inventory ID you closed (S1, X2, D1…) and, for any deferred, the `xfail(strict=True)` +
   issue link you added.

**Definition of done:** zero xfailed AND zero xpassed in the codegen edge-case suite for the rows in
scope, corpus pass-count not regressed, `make check_stubs` clean, PR shows baseline-vs-after. Anything
short of numbers is not done.

---

## Provenance and maintenance

Everything below was verified by reading source or running the command on 2026-07-03 (v0.12.7,
commit `f6963c77`, branch `feature/claude-skills`). Re-verify volatile facts before trusting them.

| Fact | Re-verify command |
|------|-------------------|
| Baseline `46 passed, 2 xfailed, 1 xpassed` | `FLOWFILE_DB_PATH=/tmp/v.db poetry run pytest flowfile_core/tests/flowfile/test_code_generator_edge_cases.py -q -rX \| tail -1` |
| Corpus `653 passed` | `FLOWFILE_DB_PATH=/tmp/v2.db poetry run pytest flowfile_core/tests/flowfile/test_code_generator.py -q \| tail -1` |
| Custom-nodes file `5 passed` (Polars exporter only) | `FLOWFILE_DB_PATH=/tmp/v5.db poetry run pytest flowfile_core/tests/flowfile/test_code_generator_custom_nodes.py -q \| tail -1` |
| Per-class gate baselines: GroupBy `1 xfailed`, Deprecated `2 passed`, RecordId `1 passed`, Unique `1 passed, 1 xfailed` | class-scoped pytest exactly as written in each Phase 3 gate |
| Corpus dual-exporter parametrization (88 sites) | `grep -c 'parametrize("export_func"' flowfile_core/tests/flowfile/test_code_generator.py` |
| Engine concat delimiter is hard-coded `,` (`string_concat`) | `sed -n '113,120p' flowfile_core/flowfile_core/schemas/transform_schema.py` |
| Emitter emits agg names with no args (X2 mechanism) | `grep -n '_get_agg_function' -A4 flowfile_core/flowfile_core/flowfile/code_generator/transform_handlers.py` |
| X1 executor fix site (`make_unique`, empty-list branch) | `grep -n 'def make_unique' flowfile_core/flowfile_core/flowfile/flow_data_engine/flow_data_engine.py; grep -n 'make_unique' flowfile_core/flowfile_core/flowfile/flow_graph.py` |
| The 3 xfail markers (edge-case) at lines 201/459/766 | `grep -n 'pytest.mark.xfail' flowfile_core/tests/flowfile/test_code_generator_edge_cases.py` |
| Stale XPASS = `test_in_operator_numeric` | `FLOWFILE_DB_PATH=/tmp/v3.db poetry run pytest 'flowfile_core/tests/flowfile/test_code_generator_edge_cases.py::TestBasicFilterOperators::test_in_operator_numeric' -q -rX` |
| Node-designer stale XPASS (row S2) | `FLOWFILE_DB_PATH=/tmp/v4.db poetry run pytest 'flowfile_core/tests/flowfile/node_designer/test_node_designer.py::TestNumericStringAliasBug' -q -rX` |
| 5 `TODO(FlowFrame)` markers (F1–F4 + `:326`) | `grep -rn 'TODO(FlowFrame)' flowfile_core/flowfile_core/flowfile/code_generator/` |
| Deprecated emissions: `str.concat` / `with_row_count` | `grep -rn 'str.concat\|with_row_count' flowfile_core/flowfile_core/flowfile/code_generator/` |
| IN-filter fix that made S1 stale (splits on `,` first) | `sed -n '144,160p' flowfile_core/flowfile_core/flowfile/filter_expressions.py` |
| Shared enum parser (#544 pattern) | `grep -n 'def is_descending\|def descending' flowfile_core/flowfile_core/schemas/transform_schema.py` |
| Export entry points | `grep -n 'def export_flow_to_polars\|def export_flow_to_flowframe' flowfile_core/flowfile_core/flowfile/code_generator/code_generator.py` |
| Round-trip helper | `grep -n 'def assert_flow_result_matches_generated' flowfile_core/tests/flowfile/test_code_generator_edge_cases.py` |
| Stub gate | `grep -n '^stubs:\|^check_stubs:' Makefile` |

Volatile caveats:
- **Line numbers drift** — every table above gives a grep to relocate the anchor; trust the grep, not
  the number. (The current xfail `reason=` strings themselves cite stale lines like
  `code_generator.py:1564` and `flow_graph.py:1055` — the real sites are now `expression_helpers.py:152`
  and `filter_expressions.py:155`.)
- **Baseline counts** are for `main`/`feature/claude-skills` at `f6963c77`; a merge that adds parity
  tests moves them. Re-run Phase 1 to re-freeze before starting a fix.
- `export_flow_to_flowframe` is newer and less complete than `export_flow_to_polars`; the F-rows are
  where that gap concentrates. Do not assume corpus-green means FlowFrame export is complete — F3/F4
  have no guard test yet.
- Emitter internals (native-node vs polars-code decision, `Expr._repr_str` contract) are owned by
  `flowfile-frame-and-codegen`; if the emission *path* changed, re-read that skill before editing here.
