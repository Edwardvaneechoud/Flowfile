# Git Archaeology — Failure History of the Flowfile Monorepo

Discovery dimension: `git-archaeology`. All claims below verified via read-only git commands
(`git log`, `git show`, `git branch -a`, `git tag`, `git ls-remote --tags origin`) and file reads
against the working tree at `/Users/edwardvaneechoud/flowfile_backup/Flowfile`, 2026-07-03.
Anything not directly verified is marked **inferred**.

## Repo shape facts

- History starts 2024-11-09 (`0cc7ae32 Initial commit`); **638 commits** on main (`git rev-list --count HEAD`).
- HEAD at time of investigation: `f6963c77` on `feature/claude-skills` (== `refs/heads/main`).
  The prompt's snapshot branch `improvement/improve-naming-unnamed-flows` exists locally, exactly
  **1 commit ahead of main**: `fa23a297 Create different naming for flows`.
- Development style: solo maintainer (edwardvaneechoud) + heavy Claude co-authoring
  (`claude/*` branches, `Co-Authored-By: Claude ...` trailers). PRs are **squash-merged**, so
  ~50 local `feature/*`/`fix/*` branches remain "ahead" of main even after their PR landed —
  ahead-count alone does NOT mean stalled work.

### GOTCHA: a tag literally named `main` exists

```
$ git show-ref main
f6963c77... refs/heads/main
f6963c77... refs/remotes/origin/main
f6cb5f27... refs/tags/main          <-- tag "main" -> "Fix Windows Electron build by installing pyinstaller (#185)" (2026-01-10)
```

Any command using bare `main` (e.g. `git log main..branch`) prints
`warning: refname 'main' is ambiguous` and may resolve to the **tag** (a Jan-2026 commit), giving
wildly wrong ranges. Always use `refs/heads/main` (or `origin/main`) in scripts.

### GOTCHA: release tags stopped at v0.9.4

`git tag | tail` and `git ls-remote --tags origin` both end at `v0.9.4` (= `01566b51`,
2026-05-07), yet in-repo versions marched on to 0.12.7 (`a3ae528e bump versions to 0.12.7 (#560)`,
2026-07-01). Since `pypi-release.yml` and `release.yaml` trigger on `v*` tags, no tag-driven
PyPI/desktop release has fired since v0.9.4. **Inferred:** post-Tauri-migration releases are being
done manually / via workflow_dispatch, or tags simply haven't been pushed. A skill about releasing
must not assume "current version == latest tag".

---

## The 25 significant incidents (symptom → root cause → fix → status)

### 1. Worker result-queue deadlock (#564, `3857aced`, 2026-07-02) — the flagship

- **Symptom:** worker tasks hung forever with no timeout — realistic triggers: wide-table
  `calculate_schema` and deeply-chained query plans.
- **Root cause (from the commit message, exceptionally well documented):** both worker transports
  (`spawner.handle_task` and `streaming.ws_submit`) **joined the subprocess before reading its
  result queue**. A child that `put()` a payload larger than the OS pipe buffer (~64KB) blocks in
  its queue feeder thread until the parent reads; joining first — plus the `while p.is_alive()`
  monitor spin — deadlocked parent and child against each other.
- **Fix:** added `spawner.drain_result_queue` (poll the queue, bail the moment the child exits
  without a result, independent of put/signal ordering); monitor loops break on the completion
  signal (progress == 100) so the drain runs; `apply_model_task` reordered to `put()` **before**
  signalling 100 (matching store/fuzzy/train) so `progress == 100` always implies the result is
  already queued. Regression tests push a >64KB payload through both paths, self-guarded against
  hangs because **pytest-timeout is not available** in this repo.
- **Status:** merged & current. Verified in tree: `flowfile_worker/flowfile_worker/spawner.py:20`
  (`def drain_result_queue`), `spawner.py:92-96` ("Drain the queue BEFORE joining"),
  `streaming.py:316` (`await asyncio.to_thread(drain_result_queue, queue, p)`), tests at
  `flowfile_worker/tests/test_result_queue_deadlock.py`.
- **Skill rule:** in worker code, *never* `p.join()` before draining `q`; `progress == 100` must
  mean "result already queued"; any new task func must `put()` before signalling completion.

### 2. Kafka pushed straight to main, reverted same day, re-landed as PRs (2026-04-02..04-04)

- Timeline: `572012e1` (#380) Kafka/Redpanda connection management lands via PR (Apr 2). Next day
  two commits go **directly to main without PR**: `fa86d940 Adding kafka to flowgraph` and
  `b7e0be80 Adding kafka sync solutions to the scheduler`. `fa86d940` accidentally committed
  scratch files to main: `diff.txt` (**4144 lines**) and `current-review.md` (119 lines) — verified
  in `git show fa86d940 --stat`.
- Same day both were reverted (`84392a6c`, `3ab65033`), which also removed the scratch files.
- The work re-landed properly the next day as `434980b3 Adding kafka frontend integration (#400)`
  and later `7be19166 Adding catalog and kafka to the Flowfile Frame (#404)`.
- **Status:** Kafka is a shipped feature (own CI workflow `test-kafka-integration.yml`, `kafka`
  pytest marker). Lesson: direct-to-main pushes happened and got messy; the revert/re-land cycle is
  the recovery pattern. Also: watch for stray plan/diff files (`775fda49 Delete plan.md`,
  `f670c126 remove plan files` show it happened repeatedly).

### 3. Windows `app_version` hotfix chain → root-cause fix via version centralization

Three escalating hotfixes to `flowfile_core/flowfile_core/database/init_db.py:update_db_info`:

1. `ff2644d8` (2026-06-23): `PackageNotFoundError` fallback changed `"unknown"` → hardcoded `"0.12.3"`.
2. `ebeca9a4` (same day): add bare `except Exception` fallback too — the failure on Windows wasn't
   (only) `PackageNotFoundError`.
3. `95f60158` (next morning): `importlib.metadata.version()` was returning **`None`** (no exception
   at all) in the PyInstaller-built Windows binary; added explicit `if app_version is None` guard.

Root-cause fix landed the following day: `b21f518c Centralize version management across all
manifests (#547)` — introduced `shared/_version.py` as single source of truth plus
`tools/bump_version.py` and `tools/check_version_sync.py` (68 lines each) and
`shared/tests/test_version.py`.
- **Skill rule:** never read the app version via `importlib.metadata` in frozen builds; use
  `shared/_version.py`. Version bumps go through `tools/bump_version.py`; drift is CI-checked by
  `tools/check_version_sync.py`.

### 4. VizSessionRegistry response-queue race → 504s under SQL viz (2026-04-26)

- `969dd74d Fix VizSessionRegistry response-queue race` (co-authored "Claude Opus 4.7 (1M context)"):
  two parents sharing a `session_key` both blocked on the same `response_q` and **stole each
  other's responses**. The "stale, draining" branch discarded the wrong-rid response — which was
  the correct response for the *other* parent, which then waited out the 115s timeout and 504'd.
  Surfaced reliably as 504s under SQL viz (wider await window).
- Fix: per-`SessionHandle` `parent_lock: threading.Lock` around the put-and-wait pair in
  `execute()`; drain loop replaced with a single `get` and a **hard raise** on request_id mismatch
  (now a genuine protocol violation). Tests in `test_catalog_visualize.py` (same-key concurrent
  calls return own results; cross-key still parallelise).
- Follow-up 10 minutes later, `b9dfed1e Implementing parent lock to resolve race condition`, also
  fixed a **field-name contract drift**: worker metadata responses use `column_schema`, but
  `catalog/service.py` read `data["schema"]` (silently fell back to local read via broad
  `except Exception`). Also bumped `_THUMBNAIL_MAX_BYTES` 200_000 → 500_000.
- **Skill rules:** shared mp queues need a per-handle parent lock (pattern exists in
  `flowfile_worker/flowfile_worker/viz_sessions.py`); worker↔core JSON field names are a real drift
  hazard hidden by fallback `except Exception` blocks — grep both sides when renaming.

### 5. Parquet corruption race between host and kernel container (#302, `84b8e416`, 2026-02-03)

- **Symptom:** `"File must end with PAR1"` errors when the kernel read input files or the host read
  output files.
- **Root cause:** `write_parquet()` can leave data in OS buffers; when sharing files between host
  and Docker container via mounted volumes, the reader sees an incomplete file (footer not flushed).
- **Fix:** explicit `fsync` after writing parquet on both sides —
  `flowfile_core/flowfile_core/flowfile/flow_graph.py` and
  `kernel_runtime/kernel_runtime/flowfile_client.py` (+4 lines each).
- **Skill rule:** any new host↔kernel file handoff must fsync before signalling readiness; Docker
  volume mounts do not guarantee write visibility ordering.

### 6. Kernel double-start race (`f43874bb`, 2026-05-26)

- Start button stayed clickable while a kernel was in state `starting`; a double-click fired two
  starts that **collided on the container name**. Fixed in both layers: UI disables the button
  while a start is in flight; `flowfile_core/kernel/manager.py::start_kernel` treats an
  already-STARTING kernel as a no-op. Pattern: guard state machine server-side, never trust the
  button.

### 7. Tauri sidecar lifecycle races (`161682ba`, 2026-05-29)

- "Fix bugs regarding race conditions" touching `src-tauri/src/sidecar/mod.rs`,
  `sidecar/shutdown.rs`, `state.rs` (57 insertions) — right after the Electron→Tauri migration.
  Message is terse; the diff hardens spawn/shutdown ordering. Together with the shutdown ladder
  (HTTP /shutdown → SIGTERM → SIGKILL, per CLAUDE.md) this is the fragile area of the desktop shell.

### 8. Windows Electron tests were green while testing nothing (#184, `09ffc9df`, 2026-01)

- Four compounding anti-patterns made Windows E2E always pass: (1) safety timeout used **OR instead
  of AND** across two startup signals; (2) `test.skip()` instead of throwing on app-launch failure;
  (3) `.catch(() => false)` error swallowing; (4) an explicit "marking test as passed anyway" on
  unexpected window close. Fixed by inverting all four. Follow-up `67e64190 Debug Electron tests on
  Windows and macOS (#187)`.
- **Skill rule:** in Playwright helpers, failures must throw; never `test.skip()` a broken launch;
  never conditionally pass.

### 9. macOS codesign failure on Python.framework (#266, `97808864`)

- Electron v36 + electron-builder v26 upgrade broke codesign on the PyInstaller-bundled
  Python.framework: "bundle format is ambiguous (could be app or framework)" because
  hardenedRuntime forces `--timestamp --options runtime`. Fix: `signIgnore` for Python.framework;
  `CSC_IDENTITY_AUTO_DISCOVERY=false` in the test workflow (CI test builds don't need signing).

### 10. electron-builder v26 DMG breakage + 1GB asar (#269, `b424fdbf`)

- Content (verified via `git log -1 --format=%B`): electron-builder ^26.5.0 → ^25.1.8 because
  v26.5.0 shipped DMGs **missing the Electron Framework binary**; removed `node_modules` from the
  electron-builder `files` array (was bundling ~1GB of dev deps into the asar); compression
  "maximum" → "normal" (slow reads from asar).
- Companion: `99c92db1` (#271) — `vite.config.js` → `.mjs` (ESM-only `@vitejs/plugin-react` v5
  caused `ERR_REQUIRE_ESM`); pypi-release workflow moved to Node 20.
- **Status:** the entire Electron toolchain was later deleted (see 11), but the lesson generalizes:
  bundler major-version bumps have repeatedly broken packaging in non-obvious ways.

### 11. Electron → Tauri migration (#462, `3777c661`, 2026-05-30)

- 100 files, +10,653/−6,621. Deleted the whole Electron main process
  (`flowfile_frontend/src/main/*.ts`, `electron.d.ts`) **and the Electron E2E suite**
  (`tests/app.spec.ts`, `tests/complex-flow.spec.ts`, `tests/helpers/electronTestHelper.ts`,
  `tests/output-field-config.spec.ts`). Added `src-tauri/`, `tools/rename_sidecar.py`,
  `tools/sign_macos_sidecars.sh`.
- Desktop-shell E2E was **not** re-landed: CLAUDE.md still says "Tauri-shell E2E tests via
  tauri-driver are a follow-up". So all the #184/#187 Electron-test hardening was ultimately
  discarded with the platform — a "reverted by obsolescence" arc.
- Immediate aftermath fixes: `8aa1e5c6` (ship .deb only, drop AppImage), `a812a326` (release
  workflow matrix), `161682ba` (sidecar races), `a1e397d1 Fix/tauri overlay (#494)`,
  `da2a5f2c` (#529, see 13).

### 12. AG Grid icons vanished in Tauri — the CSP trap (#514 + #515, 2026-06-13)

- Two same-day PRs share the literal subject "Fix missing icons in aggrid table viewer".
- `3ffa00d0` (#514) is the real icon fix: the Tauri CSP in
  `flowfile_frontend/src-tauri/tauri.conf.json` lacked `font-src 'self' data:`, so AG Grid's icon
  font was silently blocked **only in the desktop webview** (works fine in `npm run dev:web`).
- `4913e27a` (#515) reused the title but is actually a perf change: added
  `include_output: bool = True` to `FlowNode.get_node_data` and `GET /node`, so the settings panel
  opens instantly instead of computing output-schema prediction (a pivot must materialize data to
  determine output columns).
- **Skill rules:** (a) desktop-only rendering bugs → check `tauri.conf.json` CSP first;
  (b) don't trust PR titles in this repo's history — read the diff.

### 13. Writing output next to the app binary killed the worker (#529, `da2a5f2c`, issue #526)

- "Fix handling of . in tauri app": a `.` default path resolved to the application install
  location; writing there "would cause the worker to collapse throwing an unknown error".
  Fix: default output location is now the user's home directory. Windows/desktop path handling is
  a repeat offender (see also `082a0136 Fix/windows python editor (#496)`).

### 14. Sort code-generation silently flipped Descending → ascending (#544, `eac45a01`, 2026-06-23)

- The code generator computed `descending = (how == "desc")`, but visual-editor sort nodes store
  `how` as `"Descending"/"Ascending"` → generated Polars code sorted **ascending** for a Descending
  sort, inverting order-dependent downstream logic **with no error raised** (executed graph was
  correct; only exported code wrong).
- Fix: centralized direction parsing in `transform_schema.is_descending` (+
  `SortByInput.descending` property) accepting both conventions, used by the code generator (sort +
  window order_by) **and** the execution engine (`do_sort` + window order_by), which had each
  carried their own copies.
- This is one of a long **code-generator bug family**: `71851cd5` (#223), `dd521299` (#224 join
  codegen), `65153e0b` (#512 alias bug), `4c4ab017`/`01893447` (review-found bugs + chain-fusion
  tests), `d723e0de` (#549), `6cd67c4a` (#553 group_by expression-only aggregations),
  `a96ab2bf` (#503). **Skill rule:** the visual graph and generated code are two executors of the
  same settings — every codegen change needs an equivalence test against graph execution, and enum
  conventions ("desc" vs "Descending") must go through one shared parser.

### 15. Flow-naming saga — attempted fix, self-revert, and the current live branch

- 2026-04-22, branch `claude/fix-flow-tab-display-kM4qD`: `f060a2e8 fix: use catalog registration
  name for flow tab titles` then **self-reverted** the same session (`e339ffcb`). Branch never
  merged.
- 2026-07-03, branch `improvement/improve-naming-unnamed-flows` (**unmerged, 1 commit**):
  `fa23a297 Create different naming for flows` changes
  `flowfile_core/flowfile_core/flowfile/handler.py`:
  - `create_flow_name()` now returns human-facing `"Unnamed flow YYYY-MM-DD HH:MM:SS"` (was
    `"%Y%m%d_%H_%M_%S_flow.yaml"` — verified the old form is still what's on main at
    `handler.py:25-27`).
  - New `create_unnamed_flow_filename(flow_id)` → `Unnamed_flow_<ts>_<flow_id>.yaml`, keeping
    spaces/colons out of the path and using flow_id for same-second uniqueness ("registration
    dedupes on path, not name" per its docstring).
  - `handler` register path: unnamed flows get the friendly display name + a separate
    filesystem-safe path. Adds tests in `flowfile_core/tests/test_catalog.py`.
- **Status: live, unmerged.** Skill implication: display name vs on-disk path are deliberately
  decoupled; unnamed flows live in `storage.unnamed_flows_directory` (`handler.py:22`).

### 16. Save/open overwrite saga (June–July 2026, still in flight)

- `1133cc77 Fix/save as overwrite issue (#522)` (2026-06-16): Save-As handling, SaveDialog,
  CatalogFlowPicker, +508/−37 incl. big test additions.
- `8681308e Fix open flow overwrite` on `fix/overwrite-open-flow` (2026-07-02, unmerged): guards in
  `handler.py` + new `tests/flowfile/test_flow_dirty_state.py`.
- Then two **scope-back reverts on that branch** (origin copy, 2026-07-03 morning):
  `b85abc66 Revert changes to only targeting the catalog` (deleted 81 lines of
  `test_flow_dirty_state.py`, stripped handler/routes changes) and
  `dc9ff9bb Revert changes in routes.py`.
- **Interpretation (inferred):** an ambitious dirty-state tracking approach was walked back to a
  catalog-only fix. Anyone touching flow open/save/overwrite must check these branches first —
  the area is actively churning and overlaps the naming branch (same file, `handler.py`).

### 17. FastAPI upgrade attempted and reverted — main never moved

- `eff7287b Reverting upgrade Fastapi` (2026-05-11) exists only on `feature/LLM-security-patches`.
  `git log -L` on the pin shows `fastapi = "~0.115.2"` has been in `pyproject.toml` (now line 29)
  **unchanged since the initial file add** (`7fa7424c`). An upgrade was tried on-branch during the
  LLM-safeguards work and abandoned before merge (#457 `f55aa92c` landed the safeguards).
- **Skill rule:** treat the FastAPI `~0.115.2` pin as deliberate; an upgrade already failed once
  (reason not recorded in the message — mark cause as unknown).

### 18. Database migrations: born from a production bug, now 28 revisions (CLAUDE.md is stale)

- Pre-history: `e2977f5c Bugfix/db connection schedule in docker model (#422)` (2026-04-08 window)
  — a run-type mismatch between local and docker DBs required "implement mechanism to downgrade the
  db", migration `006_normalize_run_type.py`, and "align local db and worker db" (twice in the
  bullet list — it took two tries).
- Alembic itself arrived in `0ded1ebf Adding versioning to the database (#403)` (2026-04-08):
  `alembic.ini`, `env.py`, `001_initial_schema.py`, `database/migration.py` (232 lines),
  `test_migration.py` (479 lines). Note its bullet "Fix package building with alembic" — bundling
  alembic into PyInstaller needed its own fix.
- Current tree has **28 migrations** (`ls flowfile_core/flowfile_core/alembic/versions/` →
  `001_...` through `028_catalog_namespace_storage.py`). Root CLAUDE.md still says "currently
  001–021" — **stale by 7 revisions**; recent adds: 022 catalog_notebooks, 023 workspace_projects,
  024 visualization_dashboard_uuids, 025 project_track_data_artifacts,
  026 project_uniqueness_and_uuid_not_null, 027 notebook_files, 028 catalog_namespace_storage.
- **Import-time side effect (verified `flowfile_core/flowfile_core/database/init_db.py:24-27`):**
  importing `flowfile_core` runs `run_startup_migration()` at module import unless
  `FLOWFILE_SKIP_STARTUP_MIGRATION` is set ("so the alembic CLI can import our metadata without
  recursively re-entering migration machinery"). This has already burned diagnostics sessions
  (user memory note). `migration.py` also carries `_ensure_known_revision` and
  `migrate_data_from_legacy_db` (pre-Alembic DB adoption path).
- `b484a117 fix migrations` on `claude/nifty-cray-5PBpi` shows migration conflicts also bite
  long-running feature branches (numeric prefixes collide on merge).

### 19. CI timing & flakiness arc

- `f8dbb470 reverting the test order for now` (2025-04-12): an attempted test-order change in
  `.github/workflows/test.yaml` was rolled back — earliest CI-ordering churn.
- Flaky-test firefighting: `66b14004 Fix for flaky test` + `e3a2bc80 fixing flaky playwright`
  (2025-04-13, select.vue timing), `49e15434 removing flaky test` (2025-09-05, deleted 3 lines from
  `flowfile_core/tests/test_endpoints.py` rather than fix).
- `90b8dbb2 Run electron e2e tests in parallel with backend tests (#247)` — first parallelization.
- `093ead72` (#539, 2026-06-20): the big one — split coverage into its own job using Python 3.12's
  **sys.monitoring tracer** (`sysmon`), dropped redundant frontend builds from backend test jobs,
  added concurrency-cancel for superseded runs. 121 lines changed in `test.yaml` only. (Context
  from user memory: the 3.12 job was ~56 min due to coverage C-tracer + serial tests; xdist was
  deferred as high-risk needing per-worker DB isolation.)
- **Skill rule:** don't re-attempt pytest-xdist casually (shared SQLite test DB); coverage runs are
  isolated to a dedicated job on purpose.

### 20. Security-fix lineage

- `3c7c42fa` (#136, 2026-01-06): path traversal in file endpoints — added sandbox enforcement
  (`SecureFileExplorer`, sandbox_root, filename sanitization; 403 outside user data dir) + tests.
- `64bca383` (#280, 2026-02): the sandbox then **broke desktop UX and caused frontend/backend
  desync** — backend silently returned home-dir contents when navigating above home while the
  frontend updated its path state → all subsequent navigation broke. Fix: Electron mode removes the
  sandbox restriction (desktop users may browse the whole filesystem), `SecureFileExplorer` raises
  `PermissionError` instead of silently falling back, frontend reverts to previous dir on failure.
  Also fixed a missing `raise` on an HTTPException. Classic arc: security fix → silent-fallback
  regression → explicit-error redesign.
- `b4e723f2 Hotfix/fix polars code parser vulnerability (#355)` (2026-03-16): hardened
  `flow_data_engine/polars_code_parser.py` (+32 lines) with 193 lines of new tests — user-supplied
  polars code is a sandbox-escape surface; the parser is the guard.
- `f55aa92c Adding safeguards for LLM ... (#457)` (2026-05-11): LLM safety + litellm bump; the
  branch `feature/LLM-security-patches` still holds unmerged follow-ups
  (`5847e6cb Ensure not too restrictive for normal users`, `4126fa4e Fixing test for not allowed
  nodes`, `2e3f936a Fine tuning documentation`) — **stalled** since 2026-05-11.

### 21. Worker↔core transport evolution (context for incident #1)

`7f9d4768` (#233): HTTP polling with double-base64 → raw-bytes bodies + metadata headers
(X-Task-Id etc.), ~33% bandwidth cut. → `44543f89` (#267): WebSocket streaming for
ExternalDfFetcher/ExternalSampler, killing poll latency. → `e8b91fa8` (#268): parallel execution
via stage-based topological sort (ThreadPoolExecutor within stages). The queue-drain deadlock
(#564) is the latest chapter of the same seam. Anyone touching worker transport should read all
four commits.

### 22. GA4 / deferred connection resolution (#490, `09f9a2a0` + follow-ups)

- Symptom: a flow whose secrets/connections were invalid could not even be **opened**; also GA4
  failing when run from a scheduled workflow (`fix/ga-4-when-run-from-workflow`).
- Fix: secrets are validated at **run** time, not open time. Review follow-ups (in the squash body)
  document three latent races/bugs: (a) undo restoring nodes from an empty graph re-stamped
  connection-backed nodes with `user_id=None` — fixed by remembering the session owner on
  FlowGraph; (b) memoized credential/Kafka-settings getters ran on a background schema-callback
  thread concurrently with the execution thread — locks added; (c) stale echoed Kafka fields could
  serve stale columns after programmatic updates.
- **Skill rule:** connection resolution is deferred and owner-stamped; never resolve secrets during
  flow open; memoized getters that the schema callback touches must be thread-safe.

### 23. Removed-feature record (deleted files = abandoned directions)

- **Airbyte connector** removed wholesale in `aac7177e` (#93) — replaced by native cloud-storage
  read (the message: "remove Airbyte dependency"). Don't reintroduce.
- **In-house fuzzy matching** (`flowfile_worker/polars_fuzzy_match/*`) extracted to the external
  `pl-fuzzy-frame-match` package in `c1d1d1b2` (#108) — fuzzy logic lives outside this repo now.
- **Landing website** (`website/`) added `118132ba` (#253) then removed `0086b216` (#260).
- **Electron main process + Electron E2E suite** removed in #462 (see 11).
- **DraggablePanel + 3 panel stores** in WASM removed by `1c549242` (#561) — replaced by
  DraggableItem (see 24).
- **AppHeader.vue** deleted in #524; `aiCompletions.ts` deleted in #540 (LLM transparency rework).

### 24. The draggable-panel/overlay saga (longest-running UI pain)

Chronology: `c224c866` (#141 alignment on resize) → `2d31ff35` (#198 z-index bring-to-front) →
`47d3f587` (#216 remember position/size) → `5bfaee7b` (#221 canvas/panel resize) → `33bc7ef7`
(#305 layout not resetting on viewport change) → `e3703152` (#314 **unbounded z-index growth**) →
`42094e9d` (#439) → `a1e397d1` (#494 Tauri overlay) → `e4b72ff4` (#557 improve dynamic screens) →
`ab765baa`/`ed6a12c6` (#558/#559 hotfix sizing, drawerRegistry/TabbedDrawer) → `1c549242` (#561)
ports the rebuilt **DraggableItem** system (per-axis resize scale/fill/fixed, tab-strip headers)
into WASM, deleting DraggablePanel and its three Pinia stores. A local branch
`claude/draggable-item-sizing-kpu3cn` (2026-07-01, 1 ahead) continues sizing work.
**Skill rule:** overlay/panel work must build on DraggableItem + drawerRegistry, not resurrect
DraggablePanel patterns; z-index and viewport-resize regressions are the historical failure modes.

### 25. Run-history fix cluster (June 2026)

`62774106` (#505 run history tracking) → `636975c5` (#507 opening flow from run history) →
`b776bf2c` (#501 filter runs on flow page) plus unmerged/leftover branches
`fix/to-flow-version-from-run-history` **and typo twin** `fix/to-flow-version-from-run-histroy`
(both 2026-06-11, 1 ahead each) and `fix/flowfile-lite-run-history-on-error`. Three PRs in one
week on the same subsystem = run-history/flow-version linkage is fragile; check all three commits
before modifying it.

---

## Feature timeline (from merged PR numbers, all dates verified)

| Date | PR / sha | Milestone |
|---|---|---|
| 2024-11-09 | `0cc7ae32` | Initial commit |
| 2026-01-06 | #136/#147 | Path-traversal fixes; docker login/auth |
| 2026-01-11 | #194 | WASM (Pyodide) project started; rapid WASM buildout #192–#230 |
| 2026-02-02 | #285 | Flow Catalog (registration, runs, favorites) |
| 2026-02-23 | #284 | Kernel implementation (Docker sandboxed Python) + artifacts (#283, #291, #294, #296) |
| 2026-03-25/29 | #365/#376 | Scheduler service; catalog → Delta Lake |
| 2026-03-30 | #375 | JWT refresh tokens (docker mode) |
| 2026-04-02/04 | #380/#400/#404 | Kafka (with the revert detour, incident 2) |
| 2026-04-07/08 | #378/#379/#403 | MySQL; ADLS+GCS; **Alembic DB versioning** |
| 2026-04-24/26 | #416/#434/#433 | GA4 reader; ML capabilities; catalog visuals |
| 2026-05-07 | `01566b51` | **v0.9.4 — last release tag** |
| 2026-05-11/13 | #452/#409 | LLM integration (litellm); Graphic Walker |
| 2026-05-26/27/30 | #464/#463/#466/#474 | Group nodes; cron schedules; flow-as-API-endpoint; local llama.cpp ("light llm") |
| 2026-05-30 | **#462** | **Electron → Tauri migration** |
| 2026-06-12 | #502 | RBAC / group-based sharing (migration 020) |
| 2026-06-20/22 | #538/#524 | Notebook environment; git project management ("workspace" phases 1–2) |
| 2026-06-24 | #547 | Centralized version management (after hotfix chain, incident 3) |
| 2026-06-26/30 | #550/#555 | LSP; catalog object storage (S3 backend) |
| 2026-07-02 | #568/#564 | Flow-in-flow; worker deadlock fix |
| 2026-07-03 | #571/#570 | SQL-editor namespace fix; landing designer — current main `f6963c77` |

## Live / stalled branch inventory (beyond squash-merge leftovers)

- **`improvement/improve-naming-unnamed-flows`** — 1 ahead (`fa23a297`), live: unnamed-flow display
  name vs filesystem name decoupling (incident 15).
- **`fix/overwrite-open-flow`** — 1 ahead locally (`8681308e`), origin copy carries two scope-back
  reverts (incident 16). Overlaps `handler.py` with the naming branch — merge-order hazard.
- **`claude/core-abstraction-flowgraph-001hrn`** — 7 ahead, active 2026-07-02, unmerged
  architecture work: `WorkerTransport` (single owner of worker URLs + typed errors),
  `ExecutionBackend` seam (local vs worker compute), `NodeSpec` registry ("single source of truth
  for node types"), declarative `_add_from_spec` path migrating five node builders, core CLAUDE.md
  docs. If merged, this reshapes how nodes/execution are added — skill authors should flag it as
  pending direction, not current truth.
- **`feature/LLM-security-patches`** — stalled since 2026-05-11 with unmerged softening of LLM
  restrictions + the FastAPI-upgrade revert (incident 17).
- **`fix/ga-4-when-run-from-workflow`** — 3 ahead; its content appears merged via #490 squash
  (body matches `ed3a4373`); leftover.
- Dozens of `claude/*` remote branches are one-shot Claude sessions; several were never merged
  (e.g. `claude/fix-flow-tab-display-kM4qD` with its self-revert, `claude/plan-future-features-*`,
  `claude/research-revenue-opportunities-*`). Treat unmerged `claude/*` branches as idea graveyard,
  not authority.

## Verified commands (copy-pasteable) and what they show

```bash
git log --oneline -300                          # recent history; PR-squash subjects carry #NNN
git log --all --oneline -i --grep=revert        # 30+ hits; the load-bearing ones analyzed above
git log --all --oneline -i --grep="deadlock\|race\|workaround\|regression\|rollback\|disable\|flaky\|hotfix"
git show 3857aced --stat                        # deadlock fix: funcs/spawner/streaming + regression test
git show fa86d940 --stat | grep diff.txt        # 4144-line scratch diff committed to main
git show-ref main                               # exposes the tag-named-main ambiguity
git ls-remote --tags origin | tail -8           # remote tags also stop at v0.9.4
git log refs/heads/main..improvement/improve-naming-unnamed-flows --oneline   # exactly fa23a297
git rev-list --count HEAD                       # 638
ls flowfile_core/flowfile_core/alembic/versions/  # 001..028 (CLAUDE.md says 021 — stale)
grep -rn "FLOWFILE_SKIP_STARTUP_MIGRATION" --include="*.py" .   # init_db.py:26 import-time gate
grep -n "drain_result_queue" flowfile_worker/flowfile_worker/{spawner,streaming}.py
```

All shas in this document were copied verbatim from command output; `b424fdbf` re-verified via
`git log --all --oneline --grep "downgrade electron-builder"`.
