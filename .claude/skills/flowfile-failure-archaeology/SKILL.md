---
name: flowfile-failure-archaeology
description: Chronicles 25+ major incidents in Flowfile's git history (worker/kernel deadlocks and races, reverted or self-reverted branches, Electron/Tauri packaging breakage, codegen correctness bugs, CI flakiness, security-fix lineage, in-flight save/open and flow-naming churn) with symptom, root cause, evidence commit/PR, and current status, plus a live/stalled-branch inventory and a verified-and-rejected list of already-settled debates — use BEFORE re-fighting a settled battle, before proposing a fix that touches worker transport, kernel lifecycle, sort/join/group_by codegen, flow save/open/naming, Tauri packaging or CSP, CI test ordering/coverage, or version/release tooling, and before trusting an old branch, a `git tag`, or a CLAUDE.md line as current truth without checking here first.
---

# Flowfile Failure Archaeology

**Purpose:** this is the "have we been here before" file. Scan the quick
index below *before* writing a fix for something that feels familiar — a
race condition, a silent revert, a packaging break, a codegen bug. Several
incidents below have a matching "verified and rejected" fix that someone
already tried and walked back; re-proposing it wastes a review cycle.

All facts here were re-verified against the working tree / git history on
2026-07-03 at commit `f6963c77` (repo version 0.12.7). SHAs are
copy-pasteable into `git show <sha>` or `git log -1 <sha>`.

## When NOT to use this skill

- Need the *current* non-historical contract for a subsystem → `flowfile-architecture-contract`.
- Actively debugging a live failure ("has this happened before" is a means, not the goal) → `flowfile-debugging-playbook`.
- Need build/CI/testing mechanics, not *why* they're shaped this way → `flowfile-build-and-env` / `flowfile-testing-and-validation`.
- Need env var / feature-flag semantics, not the incident behind them → `flowfile-config-and-flags`.
- Adding a new node type, want current conventions not history → `flowfile-node-development`.
- Want the open TODO/debt backlog rather than closed incidents → not this file.

## Quick index (symptom keyword → incident #)

`1` worker hangs forever / no timeout / `p.join()` / result queue ·
`2` Kafka/Redpanda node or scheduler sync · `3` app version wrong/blank on
Windows, `importlib.metadata` · `4` SQL viz 504, `VizSessionRegistry`,
`response_q` · `5` `"File must end with PAR1"`, kernel parquet I/O ·
`6` kernel won't start / double-starts on fast click · `7` Tauri sidecar
spawn/shutdown ordering · `8` Windows/macOS E2E always green ·
`9` macOS codesign / Python.framework notarization · `10` DMG missing
files, huge asar, electron-builder bump · `11` anything still says
"Electron" · `12` icons/fonts missing only in the desktop app, not web ·
`13` output path defaults, "." resolves next to the binary · `14` sort/
node output order wrong only in generated code · `15` flow tab title /
display name vs filename · `16` save-as / open overwriting an existing
flow · `17` upgrading `fastapi` · `18` adding/renumbering an Alembic
migration · `19` CI job slow/flaky, or want pytest-xdist ·
`20` file-explorer path traversal / sandbox escape · `21` worker transport
(HTTP polling vs WebSocket) · `22` GA4 / any connection failing only when
flow is opened · `23` Airbyte, in-house fuzzy match, `website/` ·
`24` draggable panels, z-index, overlay resize · `25` run history /
flow-version linkage · a `git tag` or "since v0.9.4" claim → **Tag/release
gotchas** below · an old/unmerged branch that "already does this" →
**Branch inventory** below · lowering the worker poll, the catalog sleep,
coverage `parallel=true`, re-trying the FastAPI bump →
**Verified-and-rejected** below.

---

## The 25 incidents

**1. Worker result-queue deadlock (#564) — the flagship.** Symptom: worker
tasks hang forever, no timeout (wide-table `calculate_schema`, deep query
plans trigger it). Root cause: both transports (`spawner.handle_task`,
`streaming.ws_submit`) joined the subprocess **before** reading its result
queue — a child `put()`ing a payload bigger than the OS pipe buffer (~64KB)
blocks in its feeder thread until the parent reads, so `p.join()` first
deadlocks parent and child. Fix:
`flowfile_worker/flowfile_worker/spawner.py:20`
`def drain_result_queue(q, p, timeout=...)` polls and bails the moment the
child exits without a result; `streaming.py:316` calls it via
`asyncio.to_thread`; `apply_model_task` now `put()`s before signalling
progress 100 so `progress == 100` always implies the result is already
queued. Evidence: `3857aced` "fix(worker): drain result queue before
joining child to avoid deadlock (#564)", 2026-07-02; regression test at
`flowfile_worker/tests/test_result_queue_deadlock.py` pushes a >64KB
payload through both paths — self-guarded since **pytest-timeout isn't
installed** here. **Status: merged. Rule: never `p.join()` before
draining `q`; any new task func must `put()` before signalling
`progress==100`.**

**2. Kafka pushed straight to main, reverted, re-landed as PRs.** Kafka
connection management lands via PR `572012e1` (#380, 2026-04-02). Next day
two commits go **direct to main, no PR**: `fa86d940` "Adding kafka to
flowgraph" (also accidentally committed `diff.txt`, 4144 lines, and
`current-review.md`, 119 lines, straight to main) and `b7e0be80` "Adding
kafka sync solutions to the scheduler". Both reverted same day (`84392a6c`,
`3ab65033`). Work re-landed properly as `434980b3` (#400) and `7be19166`
(#404). **Status: shipped** (own CI workflow, `kafka` pytest marker).
Watch for stray plan/diff files landing on main — recurred (`775fda49`,
`f670c126`).

**3. Windows `app_version` hotfix chain → root-cause fix.** Three
escalating hotfixes to `init_db.py:update_db_info` in ~36h: `ff2644d8`
(2026-06-23, `PackageNotFoundError` fallback → hardcoded `"0.12.3"`) →
`ebeca9a4` (same day, add bare `except Exception` too) → `95f60158`
(2026-06-24, `importlib.metadata.version()` was returning **`None`**, no
exception, in the PyInstaller Windows binary — added explicit `is None`
guard). Root-cause fix same day: `b21f518c` "Centralize version management
across all manifests (#547)" — `shared/_version.py` (`__version__ =
"0.12.7"`, `get_version()`) is now the single source of truth, plus
`tools/bump_version.py` / `tools/check_version_sync.py` (CI drift gate).
**Rule: never read app version via `importlib.metadata` in frozen
builds — use `shared/_version.py::get_version()`.**

**4. VizSessionRegistry response-queue race → 504s under SQL viz.**
`969dd74d` (2026-04-26): two parents sharing a `session_key` both blocked
on the same `response_q` and stole each other's responses; the correct
response for parent B got discarded by parent A's "stale, draining"
branch, so B timed out at 115s and 504'd. Fix: per-`SessionHandle`
`parent_lock: threading.Lock` around put-and-wait in `execute()`; drain
loop replaced by a single `get()` + hard raise on request-id mismatch. Ten
minutes later `b9dfed1e` fixed a companion field-name drift: worker
responses use `column_schema`, `catalog/service.py` read `data["schema"]`
and silently fell back to a local read via broad `except Exception`.
**Rule: shared mp queues need a per-handle lock (pattern in
`flowfile_worker/flowfile_worker/viz_sessions.py`); grep both sides of
worker↔core JSON field names when renaming.**

**5. Parquet corruption race between host and kernel container (#302).**
Symptom: `"File must end with PAR1"` reading kernel input/output. Root
cause: `write_parquet()` can leave data in OS buffers; sharing files
between host and Docker container over a mounted volume lets the reader
see an incomplete file. Fix: explicit `fsync` after writing parquet in both
`flowfile_core/flowfile_core/flowfile/flow_graph.py` and
`kernel_runtime/kernel_runtime/flowfile_client.py`. Evidence: `84b8e416`,
2026-02-03. **Rule: any new host↔kernel file handoff must `fsync` before
signalling readiness — Docker volume mounts don't guarantee write
visibility ordering.**

**6. Kernel double-start race.** Start button stayed clickable while a
kernel was `starting`; a double-click fired two starts that collided on
the container name. Fixed both layers: UI disables the button mid-flight;
`flowfile_core/kernel/manager.py::start_kernel` treats an already-STARTING
kernel as a no-op (`f43874bb`, 2026-05-26). **Rule: guard the state
machine server-side; never trust the client to disable the button
correctly.**

**7. Tauri sidecar lifecycle races.** `161682ba` (2026-05-29, 57
insertions, terse message) hardens spawn/shutdown ordering in
`src-tauri/src/sidecar/{mod.rs,shutdown.rs}` and `state.rs` right after the
Electron→Tauri migration (#11). Pairs with the shutdown ladder (HTTP
`/shutdown` → SIGTERM → SIGKILL) and the macOS quit-path fix (Cmd+Q fires
`RunEvent::Exit` not `ExitRequested`; sidecars leaked until `lib.rs`
matched both plus a parent-death watcher). **Status: still the most
fragile part of the desktop shell** — quit-path desktop E2E is a TODO per
the header of `flowfile_frontend/tests/web-flow.spec.ts`.

**8. Windows Electron tests were green while testing nothing (#184).** Four
compounding anti-patterns made Windows E2E always pass: safety timeout
used OR instead of AND across two startup signals; `test.skip()` instead
of throwing on app-launch failure; `.catch(() => false)` swallowing;
an explicit "mark test as passed anyway" branch on unexpected window
close. Fix: all four inverted. `09ffc9df`, 2026-01-10; follow-up
`67e64190` (#187). **Status:** the suite this hardened was later deleted
in the Tauri migration (#11) — fix outlived its own tests. **Rule: in
Playwright helpers, failures must throw; never `test.skip()` a broken
launch; never force-pass.**

**9. macOS codesign failure on Python.framework (#266).** Electron v36 +
electron-builder v26 broke codesign on the PyInstaller-bundled
`Python.framework` ("bundle format is ambiguous") because
`hardenedRuntime` forces `--timestamp --options runtime`. Fix: `signIgnore`
for `Python.framework`; `CSC_IDENTITY_AUTO_DISCOVERY=false` in the CI test
workflow. Evidence: `97808864`, 2026-01-27.

**10. electron-builder v26 DMG breakage + 1GB asar (#269, #271).**
electron-builder `^26.5.0` → `^25.1.8` (`b424fdbf`, 2026-01-28): v26.5.0
shipped DMGs missing the Electron Framework binary; also dropped
`node_modules` from the `files` array (~1GB of dev deps was landing in the
asar) and compression "maximum" → "normal" (maximum made asar reads slow).
Companion `99c92db1` (#271): `vite.config.js` → `.mjs` (ESM-only
`@vitejs/plugin-react` v5 caused `ERR_REQUIRE_ESM`); pypi-release workflow
moved to Node 20. **Status:** whole Electron toolchain later deleted
(#11); lesson generalizes — bundler major bumps repeatedly broke packaging
in non-obvious ways.

**11. Electron → Tauri migration (#462).** `3777c661`, 2026-05-30: 100
files, +10,653/−6,621. Deleted the Electron main process
(`flowfile_frontend/src/main/*.ts`) **and its whole E2E suite**
(`tests/app.spec.ts`, `tests/complex-flow.spec.ts`,
`tests/helpers/electronTestHelper.ts`). Added `src-tauri/`,
`tools/rename_sidecar.py`, `tools/sign_macos_sidecars.sh`. **Status:
desktop-shell E2E was never re-landed** — root CLAUDE.md still calls
Tauri-shell E2E via `tauri-driver` a follow-up, so #8/#9's hardening was
discarded with the platform. Some docs pages still say "Electron" — treat
any such mention as possibly stale. Aftermath: `8aa1e5c6` (ship `.deb`
only), `a812a326` (release matrix), `161682ba` (#7), `a1e397d1` (#494,
overlay), `da2a5f2c` (#529, see 13).

**12. AG Grid icons vanished in Tauri — the CSP trap (#514, #515).** Two
same-day PRs (2026-06-13) share the literal subject "Fix missing icons in
aggrid table viewer" but are unrelated. `3ffa00d0` (#514) is the real fix:
`src-tauri/tauri.conf.json`'s CSP lacked `font-src 'self' data:`, silently
blocking AG Grid's icon font **only in the desktop webview** (fine in
`npm run dev:web`) — current CSP includes it, verify with `grep -n csp
flowfile_frontend/src-tauri/tauri.conf.json`. `4913e27a` (#515) reused the
title but is actually perf: `include_output: bool = True` added to
`FlowNode.get_node_data` / `GET /node` so the settings panel opens
instantly instead of computing output-schema prediction. **Rule: a
desktop-only rendering bug → check `tauri.conf.json` CSP first; don't
trust PR titles here — read the diff.**

**13. Writing output next to the app binary killed the worker (#529).**
`da2a5f2c`, 2026-06-19, issue #526: a `.` default output path resolved to
the app install location; writing there crashed the worker with an opaque
error. Fix: default output location is now the user's home directory.
Windows/desktop path handling is a repeat offender — see also `082a0136`
(#496).

**14. Sort codegen silently flipped Descending → ascending (#544).** The
generator computed `descending = (how == "desc")`, but visual sort nodes
store `how` as `"Descending"`/`"Ascending"` — generated Polars code sorted
**ascending** for a Descending sort, inverting downstream logic **with no
error raised** (the executed graph was correct; only exported code was
wrong). Fix: centralized parsing in
`flowfile_core/flowfile_core/schemas/transform_schema.py:952 def
is_descending(how)` (+ `SortByInput.descending` property at line 970),
accepting both spellings, used by both codegen (sort + window `order_by`)
and the execution engine, which previously each carried their own copy.
Evidence: `eac45a01`, 2026-06-22. **Status: fixed** — part of a larger
codegen-bug family with the same root pattern (`71851cd5` #223,
`dd521299` #224 join codegen, `65153e0b` #512 alias bug, `d723e0de` #549,
`6cd67c4a` #553 group_by, `a96ab2bf` #503). **Rule: the visual graph and
generated code are two separate executors of the same settings — every
codegen change needs an equivalence test against real execution; enums
with two spellings need one shared parser, not per-consumer copies.**

**15. Flow-naming saga — attempted fix, self-revert, current live branch.**
2026-04-22, branch `claude/fix-flow-tab-display-kM4qD`: `f060a2e8` "fix:
use catalog registration name for flow tab titles", **self-reverted the
same session** (`e339ffcb`), never merged. As of 2026-07-03, branch
`improvement/improve-naming-unnamed-flows` (**unmerged, 1 ahead**:
`fa23a297`) would change `handler.py`: `create_flow_name()` returns a
friendly `"Unnamed flow YYYY-MM-DD HH:MM:SS"` (main today still returns
the old `"%Y%m%d_%H_%M_%S_flow.yaml"`, verified at `handler.py:25-27`); a
new `create_unnamed_flow_filename(flow_id)` produces
`Unnamed_flow_<ts>_<flow_id>.yaml` for the on-disk path ("registration
dedupes on path, not name"). On main today, unnamed flows already live
under `storage.unnamed_flows_directory` — display name and path are
already loosely coupled; the branch tightens it further. **Rule: check
this branch before re-deriving a naming design; it isn't merged, don't
treat its shape as final.**

**16. Save/open overwrite saga (still in flight).** `1133cc77` (#522,
2026-06-16): Save-As handling, `SaveDialog`, `CatalogFlowPicker`,
+508/−37. `8681308e` "Fix open flow overwrite" on branch
`fix/overwrite-open-flow` (2026-07-02, 1 ahead locally): guards in
`handler.py` + new `test_flow_dirty_state.py`. **Origin's copy of that
branch has since walked part of it back**: `b85abc66` "Revert changes to
only targeting the catalog" (deleted 81 lines of the dirty-state test,
stripped handler/routes changes) and `dc9ff9bb` "Revert changes in
routes.py", both 2026-07-03 — an ambitious dirty-state-tracking approach
scoped back to a catalog-only fix. **Rule: diff against both the local and
origin copy of `fix/overwrite-open-flow` before writing new save/open
logic — it overlaps `handler.py` with #15, so landing either branch first
changes the merge conflict the other hits.**

**17. FastAPI upgrade attempted and reverted — main never moved.**
`eff7287b` "Reverting upgrade Fastapi" (2026-05-11) exists only on branch
`feature/LLM-security-patches`. `fastapi = "~0.115.2"` (`pyproject.toml`
line 29) has been unchanged since the file's initial commit. The upgrade
was tried during the LLM-safeguards work (which landed separately as
`f55aa92c` #457) and abandoned before merge; **the revert message does not
record why** — treat the cause as genuinely unknown, not solved. **Rule:
treat the pin as deliberate; budget real investigation time if you attempt
this again.**

**18. Database migrations: born from a production bug, now 28 revisions
(root CLAUDE.md is stale).** Pre-history: `e2977f5c` (#422, ~2026-04-08) —
a run-type mismatch between local and docker DBs needed a downgrade
mechanism, migration `006_normalize_run_type.py`, and two tries to align
local/worker DBs. Alembic itself arrived in `0ded1ebf` (#403, 2026-04-08):
`alembic.ini`, `env.py`, `001_initial_schema.py`, `database/migration.py`
— its own PR body notes "Fix package building with alembic" (bundling it
into PyInstaller needed a follow-up fix too). **Current count, verified
today:** `flowfile_core/flowfile_core/alembic/versions/` runs
`001_initial_schema.py` through `028_catalog_namespace_storage.py` — **28
migrations**. Root CLAUDE.md still says "currently 001–021" — **stale by 7
as of 2026-07-03 (v0.12.7)**. Import-time side effect
(`database/init_db.py:24-27`): importing `flowfile_core` runs
`run_startup_migration()` unless `FLOWFILE_SKIP_STARTUP_MIGRATION` is set —
always set that before importing `flowfile_core` outside the running app.
**Rule: don't trust CLAUDE.md's migration count without re-running `ls`.**

**19. CI timing & flakiness arc.** `f8dbb470` "reverting the test order for
now" (2025-04-12) — earliest CI-ordering churn, rolled back. Flaky-test
firefighting: `66b14004`/`e3a2bc80` (2025-04-13, select.vue timing),
`49e15434` (2025-09-05, deleted 3 lines from `test_endpoints.py` rather
than fix root cause). `90b8dbb2` (#247) — first parallelization. The big
one: `093ead72` "ci: split coverage into its own job with sysmon tracer,
drop redundant frontend builds, cancel superseded (#539)", 2026-06-20 —
coverage now runs in its own job using Python 3.12's `sys.monitoring`
tracer (`COVERAGE_CORE=sysmon`, `test.yaml` ~lines 217-264), redundant
`build:web` calls dropped from backend jobs, a `concurrency:` block
(line 21) cancels superseded runs. The coverage job was ~56min before this
change; **pytest-xdist was explicitly deferred as high-risk** — tests
share one SQLite DB. See Verified-and-rejected below before re-proposing
xdist.

**20. Security-fix lineage.** `3c7c42fa` (#136, 2026-01-06): path-traversal
fix in file endpoints — `SecureFileExplorer`, `sandbox_root`, filename
sanitization, 403 outside user data dir. `64bca383` (#280, 2026-01-31):
that sandbox then **broke desktop UX** — backend silently returned
home-dir contents when navigating above home while the frontend's path
state desynced, breaking all subsequent navigation; fix: desktop/Electron
mode drops the sandbox entirely, `SecureFileExplorer` raises
`PermissionError` instead of silently falling back. `b4e723f2` (#355,
2026-03-16): hardened `flow_data_engine/polars_code_parser.py` — a
sandbox-escape surface, since user-supplied Polars code runs through it.
`f55aa92c` (#457, 2026-05-11): LLM safeguards + litellm bump; branch
`feature/LLM-security-patches` still holds **unmerged follow-ups stalled
since 2026-05-11** plus the FastAPI revert (#17). **Rule: security fix →
silent-fallback regression → explicit-error redesign already repeated once
(#136→#280); prefer raising over silently falling back to a safer-looking
default.**

**21. Worker↔core transport evolution (context for #1).** `7f9d4768`
(#233): HTTP polling with double-base64 → raw-bytes bodies + metadata
headers, ~33% bandwidth cut. → `44543f89` (#267): WebSocket streaming for
`ExternalDfFetcher`/`ExternalSampler`, killing poll latency. → `e8b91fa8`
(#268): parallel execution via stage-based topological sort
(`ThreadPoolExecutor` within stages). The #1 deadlock is the latest
chapter of this same seam — read all four commits before touching worker
transport.

**22. GA4 / deferred connection resolution (#490).** Symptom: a flow whose
secrets/connections had gone invalid could not even be **opened**; GA4
also failed specifically when run from a scheduled workflow. Fix
(`09f9a2a0` #490 + follow-ups): secrets validate at **run** time, not open
time. Follow-ups fixed three latent races: undo-restore from an empty
graph re-stamped connection-backed nodes with `user_id=None` (fixed by
FlowGraph remembering the session owner); memoized credential/Kafka
getters ran on a background schema-callback thread concurrently with
execution (locks added); stale echoed Kafka fields served stale columns
after programmatic updates. **Rule: never resolve secrets at flow open,
only at run; memoized getters touched by the schema callback must be
thread-safe.**

**23. Removed-feature record (deleted = don't reintroduce).** Airbyte
connector removed wholesale in `aac7177e` (#93, 2025-07-29), replaced by
native cloud-storage read. In-house fuzzy matching
(`flowfile_worker/polars_fuzzy_match/*`) extracted to the external
`pl-fuzzy-frame-match` package in `c1d1d1b2` (#108, 2025-08-22). Landing
website `website/` added `118132ba` (#253) then removed `0086b216` (#260).
Electron main process + its E2E suite removed in #462 (see 11).
DraggablePanel + 3 panel Pinia stores in WASM removed by `1c549242` (#561);
replaced by DraggableItem (see 24). `AppHeader.vue` deleted #524;
`aiCompletions.ts` deleted #540 (LLM transparency rework).

**24. The draggable-panel/overlay saga (longest-running UI pain).** Almost
a dozen PRs across six months: `c224c866` #141 (resize alignment) →
`2d31ff35` #198 (z-index bring-to-front) → `47d3f587` #216 (remember
position/size) → `5bfaee7b` #221 (canvas/panel resize) → `33bc7ef7` #305
(layout not resetting on viewport change) → `e3703152` #314 (**unbounded
z-index growth**) → `42094e9d` #439 → `a1e397d1` #494 (Tauri overlay) →
`e4b72ff4` #557 (improve dynamic screens) → `ed6a12c6` #559 (hotfix
sizing) → `1c549242` #561 (2026-07-02) ports the rebuilt **DraggableItem**
system (per-axis resize scale/fill/fixed, tab-strip headers) into
`flowfile_wasm`, deleting `DraggablePanel` and its three Pinia stores.
Branch `claude/draggable-item-sizing-kpu3cn` (2026-07-01, 1 ahead)
continues sizing work on top. **Rule: build new overlay work on
`DraggableItem` + `drawerRegistry`, never resurrect `DraggablePanel`
patterns; z-index growth and viewport-resize regressions are the two
failure modes that recurred repeatedly.**

**25. Run-history fix cluster.** `62774106` (#505, 2026-06-11) →
`636975c5` (#507, 2026-06-11) → `b776bf2c` (#501, 2026-06-09) — three PRs
in one week on the same subsystem, plus unmerged branches
`fix/to-flow-version-from-run-history` **and a typo twin**
`fix/to-flow-version-from-run-histroy` (both 2026-06-11, 1 ahead each) and
`fix/flowfile-lite-run-history-on-error`. **Rule: run-history/flow-version
linkage is fragile — read all three merged commits and check the two
near-duplicate branches before modifying this area.**

---

## Tag and release gotchas

**A tag literally named `main` exists.** `git show-ref main` prints three
refs: `refs/heads/main` and `refs/remotes/origin/main` at current HEAD,
plus `refs/tags/main` at `f6cb5f27` — a January-2026 commit ("Fix Windows
Electron build by installing pyinstaller (#185)"). Any command resolving
the bare name `main` (e.g. `git log main..branch`) warns `refname 'main'
is ambiguous` and may silently resolve to the **tag** instead of the
branch, giving a wildly wrong range. Always write `refs/heads/main` or
`origin/main` explicitly.

**Release tags do NOT stop at v0.9.4 — that's a sort-order illusion.** A
naive `git tag | tail` or `git ls-remote --tags origin | tail` appears to
end at `v0.9.4` (2026-05-07). This is plain-string alphabetical sort:
`git tag` without `-V` sorts `"v0.10.0"` before `"v0.9.4"`
character-by-character (`'1' < '9'`), so every `v0.10.x`–`v0.12.x` tag
sorts *earlier* and gets truncated off a naive `tail`. Verified today:
tags exist up to **`v0.12.7`** (`a3ae528e`, 2026-07-01) both locally and on
`origin`, tracking the in-repo version closely. Correct check:
`git tag | sort -V | tail -5` (or `git ls-remote --tags origin | awk -F/
'{print $3}' | sort -V | tail -5` for the remote). **Rule: if an unsorted
`tail` on tags implies "releases stopped," re-check with `sort -V` before
writing that into any doc or decision** — this exact mistake is easy to
make.

---

## Live / stalled branch inventory

Squash-merging means ~50 local `feature/*`/`fix/*` branches remain "ahead"
of main even after landing — ahead-count alone does **not** mean stalled
work. Confirmed-live as of 2026-07-03:

| Branch | Ahead | Status |
|---|---|---|
| `improvement/improve-naming-unnamed-flows` | 1 (`fa23a297`) | Live, unmerged. See 15. |
| `fix/overwrite-open-flow` | 1 local; origin has extra scope-back reverts | Live, churning. See 16. Overlaps `handler.py` with the naming branch. |
| `claude/core-abstraction-flowgraph-001hrn` | 7, active 2026-07-02 | Unmerged: `WorkerTransport`, `ExecutionBackend` seam, `NodeSpec` registry, declarative node builders. If merged, reshapes how nodes/execution are added — **pending direction**, not current truth. |
| `feature/LLM-security-patches` | several | Stalled since 2026-05-11: softened LLM restrictions + the FastAPI revert (17, 20). |
| `fix/ga-4-when-run-from-workflow` | 3 | Leftover — content landed via the #490 squash merge; ignore. |

**Rule: unmerged `claude/*` branches are an idea graveyard, not authority.**
Dozens exist from one-shot Claude sessions and several were deliberately
abandoned (e.g. `claude/fix-flow-tab-display-kM4qD`'s same-session
self-revert in 15, `claude/plan-future-features-*`,
`claude/research-revenue-opportunities-*`). A branch that "already does X"
is not evidence X is the right design — check *why* it wasn't merged
before reusing it.

---

## Verified and rejected — do not re-investigate these

- **Lowering the worker's 0.5s poll interval.** The hot path is the
  WebSocket push transport (`streaming.py`, incident 21), not HTTP
  polling — polling is a fallback, not the latency-critical path.
- **Removing the catalog's ~1.05s sleep in tests**
  (`flowfile_core/tests/test_catalog_dashboards.py:194`,
  `time.sleep(1.05)`). Load-bearing: SQLite's timestamp columns here have
  **1-second granularity**, so two writes in the same second are
  indistinguishable without it. The ~2s cancel-flow sleeps elsewhere wait
  out a real polling/settle interval for the same reason.
- **Turning on coverage's `parallel=true` without pytest-xdist and
  `coverage combine`.** Tried, actively harmful: `parallel=true` writes
  per-process `.coverage.<host>.<pid>` fragments expecting a combine step;
  without both you get an **empty `coverage.xml`** — worse than not
  setting the flag. xdist itself was deliberately deferred (shared SQLite
  test DB needs per-worker isolation first) as part of incident 19.
- **Upgrading FastAPI off `~0.115.2`.** Already tried on
  `feature/LLM-security-patches` and reverted (`eff7287b`, incident 17).
  Reason not recorded — budget real investigation time, don't assume it's
  a trivial bump.

---

## Provenance and maintenance

Re-run these before trusting a fact above that looks stale (verified
2026-07-03 at `f6963c77`, v0.12.7):

```bash
git rev-list --count HEAD                       # commit count (638)
git show-ref main                                # tag-named-main trap
git tag | sort -V | tail -5                      # correct latest-tag check (not `tail` alone)
git ls-remote --tags origin | awk -F/ '{print $3}' | sort -V | tail -5
git log -1 --format='%ad %s' --date=short <sha>  # spot-check any cited commit
grep -n "def drain_result_queue" flowfile_worker/flowfile_worker/spawner.py    # incident 1
grep -n "def is_descending" flowfile_core/flowfile_core/schemas/transform_schema.py  # 14
cat shared/_version.py                           # incident 3
ls flowfile_core/flowfile_core/alembic/versions/ | grep -c '^[0-9]'  # incident 18 (28)
grep -n "currently 0" CLAUDE.md                  # is the migration-count staleness still there?
git log refs/heads/main..improvement/improve-naming-unnamed-flows --oneline  # incident 15
git log refs/heads/main..fix/overwrite-open-flow --oneline  # incident 16
grep -n "time.sleep(1.05)" flowfile_core/tests/test_catalog_dashboards.py  # verified-rejected
grep -n '^fastapi' pyproject.toml                # incident 17
```

When you land a fix for something chronicled here, or a branch above
finally merges or gets abandoned, update its **Status** in place rather
than adding a new incident — one entry per root cause keeps this a lookup
table, not a duplicate-riddled log.
