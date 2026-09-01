# Telemetry implementation review — four independent passes

2026-09-01, branch `feature/add-telemetry`. Four independent read-only review
agents (integration seams, frontend consent lifecycle, privacy/abuse posture,
fresh-eyes design review), reports verbatim below. Synthesis and triage live in
the session discussion; corrections these forced onto telemetry-findings.md are
marked there. Every claim below carries its own file:line citations.

---

# Report 1: integration seams (engine ↔ telemetry wiring)

## Overall verdict

The observer wiring is structurally sound and the isolation contract holds: `events.publish` is the only seam product code touches, no product module imports telemetry, subscriber exceptions are caught (`flowfile_core/flowfile_core/events.py:32-36`), `run_snapshot` and `emit_run_events` are both blanket-try'd (`telemetry.py:147/166`, `201`), the ASGI middleware is genuinely pure-ASGI with its emit wrapped so it cannot corrupt a response (`telemetry.py:294-303`), all 9 `ROUTE_EVENTS` keys match real routes with the exact method and full prefixed path (verified against `routes/routes.py:987-1100`, `ai/diff_routes.py:218/302`, `routes/catalog.py:1349`; there are no `app.mount` sub-apps, so no relative-path scope shadowing, and the axios-307 case is doubly filtered by `status >= 300` and the fact that Starlette's redirect branch never stamps `scope["route"]`), and `flow_run_started`/`flow_run_finished` are correctly balanced for every ordinary Exception path because the `started` publish sits *inside* the try that the crash handler guards (`flow_graph.py:6713-6763`). What is not sound is the **semantics** of two of the events: `export_code_used` fires when a user merely looks at the Code tab (the real export button makes no HTTP request at all), and `error_class` collapses to `OtherError` on the desktop app's default execution path, so the one prop that was supposed to make failures actionable will be mostly noise. There is also one class of failure — a polars/pyo3 panic — that escapes both the crash handler and the node error recorder, silently deleting exactly the worst runs from the failure metric.

---

### 1. `export_code_used` counts *viewing* the code panel, not exporting — and the real export emits nothing. **should-fix-before-merge**

`ROUTE_EVENTS` maps `GET /editor/code_to_polars`, `code_to_flowframe` and `code_to_project` to `export_code_used` (`flowfile_core/flowfile_core/telemetry.py:100-102`). All three are fetched by the panel purely to *render* code:

- `CodeGenerator.vue:106` `fetchCode()` is called from a watcher with `{ immediate: true }` (`CodeGenerator.vue:137-145`), from `setMode` on every tab switch (`:128-135`), and from a manual `refreshCode` button (`:157`, wired at `:27`).
- `ProjectExport.vue:211` calls `fetchManifest()` from `onMounted`, plus again on every `flow_id` change (`:202-209`).

Meanwhile the actual "Export" button (`CodeGenerator.vue:44` → `exportCode` at `:163-174`) builds a client-side `Blob` and never touches the backend.

**Failure scenario:** a user opens the Code tab to glance at the generated Polars, switches to the FlowFrame tab, switches back, then closes it without exporting. That produces three `export_code_used` events (`polars`, `flowframe`, `polars` again). A user who opens the tab once and clicks Export produces one — the same as the browser. The dashboard reads "codegen used N times"; the true number is unknowable from this data, and `target: "polars"`/`"flowframe"` will structurally dominate `project_zip`/`project_save` (which *are* genuine actions, `ProjectExport.vue:155/177`) purely because the first two are drive-by renders.

---

### 2. A polars panic (or any `BaseException`) orphans `flow_run_started` and is never classified. **should-fix-before-merge**

`run_graph`'s crash guard is `except Exception` (`flow_graph.py:6761-6763`), and the node-level recorder is also `except Exception` (`flow_node/executor.py:152`). `flowfile_core/flowfile_core/flowfile/flow_graph.py:6171-6175` wraps `node.execute_node` in `try/finally` with no `except`, so anything the executor doesn't catch propagates straight up.

Verified in this environment:

```
>>> polars.exceptions.PanicException.__mro__
(<class 'pyo3_runtime.PanicException'>, <class 'BaseException'>, <class 'object'>)
```

**Failure scenario:** a Rust-side panic in a join or a group-by (the class of bug that most needs telemetry) propagates out of `_execute_stages` → `except Exception` at `flow_graph.py:6761` does not match → `flow_run_crashed` never publishes, `flow_run_finished` never publishes. The wire shows a bare `flow_run_started`. The failure rate computed as `flow_run_failed / flow_run_started` is biased *downward* by precisely the worst outcomes. The same holds for `KeyboardInterrupt` on a CLI run. Note `"PanicException"` is on `ERROR_CLASS_ALLOWLIST` (`telemetry.py:60`) but is unreachable by construction through either path — the allowlist entry documents an intent the code does not implement.

Secondary effect: the snapshot stored at `telemetry.py:230` is never popped for such a run. It is a `WeakKeyDictionary` so it will not leak unboundedly, but a designer graph held live in `flow_file_handler` keeps a stale snapshot until its next run overwrites it.

---

### 3. `error_class` is `OtherError` for most desktop failures, and can report a *stale* class. **should-fix-before-merge**

`_failed_error_class` reads `FlowNode._last_exception_class` (`telemetry.py:210-222`), stamped only at `flow_node/executor.py:426` as `type(error).__name__`. Which exception object reaches that line depends on the strategy chosen at `executor.py:248-275`:

- `LOCAL_WITH_SAMPLING` (narrow transforms) re-raises the original error (`flow_node/flow_node.py:1465`) — real class preserved.
- `FULL_LOCAL` (CLI / scheduled runs, `execution_location="local"`) propagates the polars error — real class preserved.
- **`REMOTE`** — the default for any non-narrow node when `OFFLOAD_TO_WORKER` is on — converts every worker-side failure into a **bare `Exception`**: `raise Exception(external_df_fetcher.error_description) from e` (`flow_node.py:1632`), `raise Exception(guidance) from e` (`:1612`), `raise Exception("get_resulting_data returned None")` (`:1551`). `type(e).__name__ == "Exception"`, which is not on the allowlist → `OtherError` (`telemetry.py:128`).

The `read` node — the single most common failure site — is declared `transform_type="other"` (`configs/node_store/nodes.py:45`), so it takes the REMOTE branch.

**Failure scenario:** a user in the desktop app points a Read node at a CSV with a type conflict. The worker fails, core wraps it in `Exception`, and telemetry reports `flow_run_failed{error_class: "OtherError"}`. Every `ComputeError`, `SchemaError`, `NoDataError`, `ColumnNotFoundError` and `MemoryError` on the allowlist is unreachable for offloaded nodes; the prop degenerates to a constant for the app's primary execution mode while looking correct in headless CI (which forces `local`).

Two additional paths mark a node failed without ever stamping the attribute, and nothing clears it between runs (the only reset is on a *successful* execution, `executor.py:147`):

- parameter-resolution failure: `flow_graph.py:6160-6167` sets `node_result.success = False` and returns before `node.execute_node` is called.
- gate-formula failure: `flow_graph.py:6374-6380` sets `node_result.success = False` directly.

**Failure scenario:** node 3 fails run #1 with `FileNotFoundError` (stamped). Run #2 fails at parameter resolution on the same node. `_failed_error_class` finds `result.success is False`, reads the un-cleared attribute, and reports `FileNotFoundError` for a run that never touched the filesystem. `tests/test_telemetry_events.py:256-267` pins that a class on a node which *succeeded* this run is ignored, but not this case — the node did fail this run, just not for that reason.

---

### 4. Every Flow-as-API request emits a full `flow_run_started` + `flow_run_succeeded` pair. **should-fix-before-merge**

`_skip` ignores only nested subflows and `_system_run` (`telemetry.py:205-207`). The Flow-as-API runner opens a fresh graph and calls `run_graph()` per HTTP request (`flowfile/api_runner.py:229`, `:255`) and sets neither marker. That router is API-key authenticated (`flow_api.data_router`), i.e. machine traffic.

**Failure scenario:** a user publishes a 5-node flow as an API and a client polls it once a second. That install emits ~86 400 `flow_run_started` + `flow_run_succeeded` pairs per day. Beyond destroying the run-count metric, the client queue is `QUEUE_MAXSIZE = 256` drained on a 5s tick (`shared/telemetry.py:42-44`) and a full queue drops silently (`shared/telemetry.py:451-454`), so during a burst the low-frequency signals you actually want (`flow_created`, `schedule_created`, `ai_diff_accepted`) are the ones discarded. The same applies to scheduled runs, which spawn `flowfile run flow` children that call `install_headless()` (`flowfile/flowfile/__main__.py:96`) with no marker — and because `emit_once` is per-*process* (`shared/telemetry.py:469-479`), each scheduled run is a fresh process and re-emits `activation` / `catalog_used` / `kernel_used`.

---

### 5. `release_run()` happens before the `flow_run_finished` publish, opening a snapshot/cancel interleave. **minor**

`run_graph` releases the single-run slot at `flow_graph.py:6755`, then reads run info at `:6758`, then publishes at `:6759`. Graph identity is stable for one run (plain class, default identity hash, `_snapshots` keyed by object at `telemetry.py:230`), and `try_claim_run` (`flow_graph.py:5939-5945`) normally serialises runs of the same object — but the claim is already released when `flow_run_finished` publishes. `POST /flow/run/` queues `_run_and_track` as a background task (`routes/routes.py:505`) whose pre-run DB work (`:397-421`) precedes `run_graph`, so its route-level `is_running` guard (`:503`) can admit a second task.

**Failure scenario:** a user double-clicks Run. Task A reaches `release_run()` at `:6755`; task B, already queued, enters `run_graph`, sets `is_canceled = False` (`:6714`), claims, and publishes `flow_run_started`, storing *its* snapshot under the same key. Task A then publishes `flow_run_finished`; `_on_flow_run_finished` pops task B's snapshot (`telemetry.py:239`) and reports run B's node shape and node count as run A's result. Task B's own `finished` then finds nothing and — because `emit_run_events` returns early when `outcome == "succeeded"` and `not snapshot` (`telemetry.py:186`) — emits nothing at all. The same window lets run B's `is_canceled = False` reset defeat run A's cancel guard (`telemetry.py:240`), emitting a `flow_run_succeeded`/`failed` for a run the user cancelled.

---

### 6. The middleware emits when the response *starts*, not when it succeeds. **minor**

The docstring claims "one event per successful request" (`telemetry.py:280`), but `_emit_for_route` runs on `http.response.start` (`telemetry.py:295-300`), which ASGI sends before any body bytes.

**Failure scenario:** `GET /editor/code_to_project/zip` streams a large archive; the connection drops or the generator raises after headers are sent. Status 200 was already reported, `export_code_used{target: "project_zip"}` is already queued, and the user got no file. (Correctly, an exception *before* headers never reaches `_send` at all — `ServerErrorMiddleware` sits outside this middleware — which is what `tests/test_telemetry_events.py:493-495` pins.)

---

### 7. `flow_run_started` can be terminal-less by design in two more cases. **minor**

A cancelled run publishes `started` and then nothing (`telemetry.py:240`), which is documented and deliberate. Less deliberate: if `run_snapshot` returns `None` — any exception in it, including a first-call failure of the lazy `get_all_standard_nodes` import (`telemetry.py:134-137`, `147/166`) — nothing is stored (`:229-230`), and a subsequent *successful* run returns silently from `emit_run_events` (`:186`). So `started - (succeeded + failed) > 0` is expected but has three distinct causes (cancel, snapshot failure, `BaseException` per finding 2) that the collector cannot separate.

---

### 8. `events.publish` isolates `Exception`, not `BaseException`. **nit**

`flowfile_core/flowfile_core/events.py:33-36` catches `Exception`. A subscriber raising `KeyboardInterrupt`, `SystemExit`, or a `MemoryError`-adjacent `BaseException` propagates into the publishing thread — i.e. into `run_graph`, where it would be misattributed as a flow failure. Today's only subscribers are the four `_on_*` handlers, all of which are internally guarded, so this is latent rather than live. Related nit: failures are logged at `DEBUG` with no counter, so a permanently broken observer is invisible in production logs.

---

### 9. `duration_bucket` excludes pre-execution work. **nit**

`flow_run_started` publishes at `flow_graph.py:6718`, but `start_time` is stamped at `:6739` by `create_initial_run_information` (`:5913-5924`) — after `_refresh_catalog_reader_freshness` (`:6720`, which probes live Delta versions, cloud-aware and network-bound), `_refresh_read_source_freshness` (`:6721`), gate evaluation (`:6727`) and plan computation (`:6729`). A run that spends 8 s probing a cloud Delta table and 0.5 s executing buckets as `<1s`.

---

### 10. `kernel_exec` fires before the cell runs and has no run-origin guard. **nit**

`publish("kernel_exec")` at `flowfile_core/flowfile_core/kernel/manager.py:1937` is inside the exec lock but before `_execute_locked` (`:1938`), so an immediately-failing cell still marks the install as `kernel_used`. It also has no `_skip`-equivalent, so a dry run or an internal/system execution counts. Because `_on_kernel_exec` is `emit_once` (`telemetry.py:258-259`), the blast radius is one boolean per process, which is why this is a nit rather than a finding.

---

**Files read for this review:** `flowfile_core/flowfile_core/events.py`, `flowfile_core/flowfile_core/telemetry.py`, `flowfile_core/flowfile_core/flowfile/flow_graph.py` (5896-6800), `flowfile_core/flowfile_core/flowfile/flow_node/executor.py`, `flowfile_core/flowfile_core/flowfile/flow_node/flow_node.py` (1390-1650), `flowfile_core/flowfile_core/flowfile/subflow.py` (370-460), `flowfile_core/flowfile_core/flowfile/api_runner.py`, `flowfile_core/flowfile_core/kernel/manager.py` (1900-1945), `flowfile_core/flowfile_core/main.py`, `flowfile_core/flowfile_core/routes/routes.py` (340-510, 987-1100), `flowfile_core/flowfile_core/configs/node_store/nodes.py`, `flowfile_core/flowfile_core/schemas/output_model.py` (10-45), `shared/telemetry.py`, `flowfile/flowfile/__main__.py`, `flowfile_core/tests/test_telemetry_events.py`, `flowfile_frontend/src/renderer/app/views/DesignerView/CodeGenerator/{CodeGenerator,ProjectExport}.vue`.
---

# Report 2: frontend consent lifecycle

## (1) Overall verdict

The consent lifecycle is well-shaped and biased in the right direction — the ask is gated behind five independent conditions, consent is never optimistic (the store adopts only the server's word, `/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/stores/telemetry-store.ts:43-48`), the backend refuses to report a write that did not stick (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_core/flowfile_core/routes/telemetry.py:76-83`), the settings switch is fully controlled so it snaps back on failure, and the modal copy plus `EXAMPLE_EVENT` are an exact match for what `shared/telemetry.py` actually emits (7 envelope keys, `flow_run_succeeded`'s four allowlisted props, custom node types collapsed to `"custom"` in `flowfile_core/telemetry.py:156`) with a docs target that really exists and is in the mkdocs nav. I could not find any path where the UI shows consent as *granted* when the file write failed — that direction is airtight. What is not robust is the other three axes: the load-once store is never invalidated on login/logout, so in docker mode the modal's `canManage` gate can be answered with the *previous* user's authority; Element Plus's `el-dialog` emits `update:model-value(false)` from its `afterLeave` hook on **every** close, including a close caused by the visibility gate flipping, so navigating away from the designer while the dialog is open silently POSTs a permanent decline the user never made; and when `/telemetry/status` fails the Privacy card renders a confident "Off" for a state it does not know.

## (2) Findings

---

### 1. Any non-user close of the consent dialog silently POSTs a permanent decline — `should-fix-before-merge`

`el-dialog`'s `afterLeave` hook emits `update:modelValue false` unconditionally (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/node_modules/element-plus/es/components/dialog/src/use-dialog.mjs:118-125`), and the `props.modelValue` watcher routes a prop-driven `true → false` into that same close/transition path (`use-dialog.mjs:201-225`, with the transition hooks wired at `dialog2.mjs`'s `<Transition v-bind="transitionConfig" persisted>` + `vShow`). The modal maps that emit straight to a permanent decline:

```
@update:model-value="(v: boolean) => !v && decline()"
```
`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/components/settings/TelemetryConsentModal.vue:11`, `:126-128`

But `visible` is a *computed over gates*, not a user-owned ref (`TelemetryConsentModal.vue:86-97`), so it also flips false when the route leaves `designer`, when the tutorial activates, or when a refetched status changes `available`/`canManage` (`telemetryConsent.ts:60-69`). The component is mounted in `AppLayout` **outside** `<router-view>` (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/layouts/AppLayout.vue:8` vs `:27`), so a route change does not unmount it — the leave transition runs and the emit fires.

**Failure scenario:** web/docker mode, hash router. User is on `#/main/catalog`, navigates to `#/main/designer`; the consent dialog appears. They press the browser Back button (the dialog overlay at z-index 2000+ blocks the sidebar, but not browser chrome). `route.name` becomes `catalog` → `visible` → false → ~300ms later `afterLeave` fires → `decline()` → `answer(false)` writes the `flowfile-telemetry-consent-answered` localStorage tombstone (`TelemetryConsentModal.vue:56`, `:66-72`, `:114`) and POSTs `consent: false`. The user has now permanently opted out without answering, and the tombstone plus the `consent !== null` gate mean the ask never returns. Same result for logout-while-open (`Sidebar.vue:187-190` does `router.push({name:'login'})`) or any programmatic navigation.

The `answered` guard at `TelemetryConsentModal.vue:112` correctly prevents *double*-posting after a real answer, but does nothing here because no answer was given. A `userClosed` flag set by the footer buttons / `show-close` / escape — with the `@update:model-value` handler ignoring closes it didn't cause — is the fix.

---

### 2. Load-once status is never invalidated on auth change, so the `canManage` gate can be evaluated with the wrong user's authority — `should-fix-before-merge`

`loadStatus()` short-circuits on `this.loaded` unless forced (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/stores/telemetry-store.ts:25-28`), and `AppLayout` calls the *unforced* form (`AppLayout.vue:51`). Nothing resets the telemetry store on logout — `authStore.logout()` clears only its own fields (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/stores/auth-store.ts:89-93`) and the only `$reset` in the renderer is in `project-store.ts:236`. Logout is a client-side `router.push` (`Sidebar.vue:187-190`), not a reload, so Pinia survives the user switch while `AppLayout` unmounts and remounts.

Meanwhile `can_manage` is per-user and mode-dependent server-side (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_core/flowfile_core/routes/telemetry.py:46-51`).

**Failure scenario A (modal shown to someone who cannot consent):** docker deployment. Admin logs in but stays on Catalog (or the tutorial is running), so consent stays `null` and `status.canManage === true` is cached. Admin logs out; a regular user logs in and lands on the designer. `AppLayout.onMounted` → `loadStatus()` returns the cached admin status → `shouldShowConsentModal` passes on `canManage: true` (`telemetryConsent.ts:66`) → the non-admin sees the one-time ask. Whatever they click, the POST 403s (`routes/telemetry.py:70-74`) and the modal swallows it (`TelemetryConsentModal.vue:117`), but the tombstone is already written — so that browser never asks again, and nothing was recorded.

**Failure scenario B (the person who *can* consent is never asked):** same deployment, reverse order. A non-admin logs in first → `canManage: false` cached. They log out, the admin logs in → cached `canManage: false` → `shouldShowConsentModal` returns false → the admin is never asked. Only a hard reload (or visiting Compute → Privacy, which force-reloads at `TelemetryCard.vue:76-80`) repairs the store.

---

### 3. The Privacy card renders a definite "Off" for a state it does not know — `should-fix-before-merge`

`loadStatus` swallows every failure and leaves `status` at its previous value — `null` on a cold failure, **stale** on a failed forced refresh — with no error flag exposed (`telemetry-store.ts:29-35`). The card then derives everything from that:

- `consentOn = status.value?.consent === true` → `false` when status is `null` (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/components/settings/TelemetryCard.vue:71`), so the word next to the switch reads **"Off"** (`:13`) and the switch renders unchecked.
- Both explanatory blocks are gated on a non-null status — `v-if="status && !status.canManage"` (`:31`) and `v-else-if="status"` (`:40`) — so on a failed load the user sees a disabled toggle labelled "Off" with **no error, no lock explainer, no warning banner at all**.
- The panel header above it asserts "nothing is ever sent without consent" (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/views/ComputeView/PrivacyPanel.vue:6-8`), reinforcing the false reading.

**Failure scenario:** `telemetry.yaml` holds `consent: true` and core is happily sending events. The user opens Compute → Privacy; `GET /telemetry/status` fails (proxy hiccup in docker, a 500, or a JWT refresh that fails after the interceptor's single retry at `services/axios.config.ts:44-52`). The privacy panel tells them telemetry is Off while it is On, and gives them no way to tell that the read failed. This is the one screen where being wrong in that direction matters most. A third state (`error`/`unknown`) rendered instead of `Off` is the fix; the store already has the information, it just discards it.

---

### 4. A failed *accept* from the modal is dropped silently and tombstoned — `minor`

`answer()` writes the tombstone and closes before the POST resolves, then discards the outcome: `void telemetryStore.setConsent(enabled).catch(() => undefined)` (`TelemetryConsentModal.vue:111-118`). Store state is left unchanged on rejection (`telemetry-store.ts:43-48`), which is honest, but nothing is surfaced.

**Failure scenario:** read-only storage (`~/.flowfile` on a locked-down image). User clicks "Share anonymous usage data". `set_consent` returns `persisted=False` (`shared/telemetry.py:289-291`) and the route answers 503 `TELEMETRY_PERSIST_FAILED` (`routes/telemetry.py:76-83`). The dialog is already gone, no toast appears, the tombstone is set. The user believes they opted in; nothing was recorded, and because of the tombstone they will never be asked again on that machine even after storage becomes writable. The direction is safe, but the deliberate opt-in the user gave is lost with zero feedback — unlike the settings card, which does toast the same failure (`TelemetryCard.vue:89-93`).

---

### 5. The modal never tells a docker admin the choice is made on everyone's behalf — `minor`

`_can_manage` returns admin-only in docker (`routes/telemetry.py:46-51`), and consent is a single install-wide `telemetry.yaml` (`shared/telemetry.py:166-167`). The docs state this explicitly — `/Users/edwardvaneechoud/flowfile_backup/Flowfile/docs/users/telemetry.md:22`: consent is "a single deployment-wide setting an administrator grants or revokes on behalf of everyone using that server" — and `TelemetryCard` says it too, but **only in the branch non-admins see** (`TelemetryCard.vue:31-38`). `CONSENT_COPY.body` says only "Flowfile can send anonymous usage events" (`/Users/edwardvaneechoud/flowfile_backup/Flowfile/flowfile_frontend/src/renderer/app/components/settings/telemetryConsent.ts:28-33`) — first person singular, desktop-flavoured.

**Failure scenario:** a docker admin sees the modal, reads copy that reads like a personal preference, and consents on behalf of every user of that deployment without being told. The one audience that needs the server-wide sentence is the only audience that never gets it.

---

### 6. The modal is a permanent decision but points nowhere for recovery — `minor`

Dismissal is permanent by design (`TelemetryConsentModal.vue:124-128`, docs `telemetry.md:19`: "silent and permanent — you are never asked again"), yet `CONSENT_COPY` contains no "you can change this later under Compute → Privacy" line (`telemetryConsent.ts:26-44`) — the only strings are the body, the env-var line, the example toggle, the docs link, and the two button labels. Combined with `close-on-click-modal: true` (`TelemetryConsentModal.vue:8`), a stray click on the backdrop permanently declines with no on-screen hint that the setting is recoverable. Every other permanence signal in this feature is documented; the modal itself is the one surface that isn't.

---

### 7. The consequential half of the consent logic is the untested half — `minor`

`telemetryConsent.ts:1-3` states its rationale: "@vue/test-utils is not a dependency, so everything meaningful lives here … and the .vue stays a thin binding." That is not what shipped. `telemetryConsent.test.ts` covers only `shouldShowConsentModal` and the copy/example constants. The dismiss→decline mapping, the localStorage tombstone (write-before-POST), the one-POST `answered` guard, and the fire-and-forget error swallow all live in the `.vue` (`TelemetryConsentModal.vue:56-72`, `:111-128`) and have no test at all — which is precisely why findings 1 and 4 are unpinned. Extracting an `interpretConsentClose(...)` / `nextAnswerState(...)` pure helper into `telemetryConsent.ts` would make the `.vue` genuinely thin and put the risky logic under the existing vitest file.

---

### 8. Small stuff — `nit`

- The example disclosure button carries no `aria-expanded` / `aria-controls` and the `<pre>` it toggles has no id (`TelemetryConsentModal.vue:17-21`); screen-reader users get an unlabelled state toggle.
- `width="520px"` is a fixed dialog width (`TelemetryConsentModal.vue:5`); `AppLayout` explicitly supports viewports down to 1000px and below (`AppLayout.vue:82`, `:94`), where the dialog will overflow.
- `EXAMPLE_EVENT` shows a bare envelope, while the wire actually carries a batch wrapper `{"events": [...]}` (`shared/telemetry.py:391-393`), and its `app_version: "0.12.7"` (`telemetryConsent.ts:14`) predates the shipping `0.16.0` (root `pyproject.toml`). Both are harmless as illustration and both match `docs/users/telemetry.md:29-40` exactly, so changing one means changing both.
- No in-flight dedupe on `loadStatus`: `AppLayout`'s unforced call and `TelemetryCard`'s forced one can overlap, and a `loadStatus(true)` that resolves after a concurrent `setConsent` will overwrite the fresher POST response (`telemetry-store.ts:25-48`, `TelemetryCard.vue:76-80`). Requires unusual timing; last-write-wins on identical data most of the time.
---

# Report 3: privacy & abuse posture

## 1. Overall verdict

The client is genuinely well-built for this threat model: the leak vectors I was asked to attack (`node_types`, `error_class`, anything user-derived) are closed at two independent layers, `install_id` never crosses the API boundary or a log line, and I could not construct a path — custom node names, user-defined exception classes, route path params, flow/node names, settings blobs — that puts a user-authored string on the wire. The problem is not what leaves the machine, it is **where it goes and whether the user was told**: `DEFAULT_ENDPOINT` now ships a live project collector, five documentation locations still assert that no default ships and that unset means disabled, and the consent modal that links to that page never names a destination at all — so consent is currently being solicited against a false statement, which I rate the blocker here. On the collector side, `POST /events` is an unauthenticated, unrate-limited, unrotated append to a single file whose disk-fill rate is ~1:1 with attacker ingress and whose funnel I collapsed from 5 installs to 1 with a single spoofed timestamp; that is tolerable for a single-maintainer funnel only with two or three one-line mitigations that are not currently present anywhere in the repo. Finally, `tools/telemetry_collector/data/events.jsonl` is committed with a real install UUID and the compose file bind-mounts that same directory, so live events accrete into a git-tracked path.

---

## Findings

### 1. BLOCKER — Consent is obtained against documentation that misstates where data goes

`shared/telemetry.py:38` ships a live default collector:

```python
DEFAULT_ENDPOINT = "https://events.flowfile.app/events"  # project collector; "" = none ships, telemetry off unless env-set
```

and `shared/telemetry.py:242` resolves `env override → DEFAULT_ENDPOINT → None`, so gate 3 (`is_available`, `shared/telemetry.py:247`) passes on every install out of the box. Five places still say the opposite:

- `docs/users/telemetry.md:16` — "no default ships today, so with the variable unset telemetry is disabled"
- `docs/users/telemetry.md:130` — "No default ships today, so unset means disabled."
- `docs/users/deployment/docker.md:142` — "(none ships today, so unset ⇒ disabled)"
- `.env.example:161` — "(none ships today, so unset = disabled)"
- `CLAUDE.md:389` — "Unset ⇒ telemetry fully disabled"

(`shared/CLAUDE.md:35` is the only correct one.)

**Failure scenario:** a privacy-conscious user opens the consent modal. Its copy (`flowfile_frontend/src/renderer/app/components/settings/telemetryConsent.ts:26-43`) names no destination — it says only "Flowfile can send anonymous usage events" and offers `docsLinkLabel: "Read exactly what is sent"` pointing at `TELEMETRY_DOCS_URL` (`:7` → `users/telemetry.html`). They follow that link, read line 16, conclude gate 3 is closed on their machine and nothing can be sent, click **"Share anonymous usage data"** believing it is inert, and their events begin flowing to a third-party host they were never told about. Nothing in the UI or the docs names `events.flowfile.app`. The four-gate story is the load-bearing trust artifact for this whole feature and one of its four gates is documented backwards.

**Cheapest fix:** correct the five lines, and add the destination host to `CONSENT_COPY.body` (one clause: "…to `events.flowfile.app`") plus the docs page. The code needs no change.

---

### 2. SHOULD-FIX-BEFORE-MERGE — Collector: unauthenticated append with no rate limit, no rotation, no size cap, no source attribution

`tools/telemetry_collector/app.py:173-203` accepts any `POST /events` and appends to one ever-growing file (`:160-165`). There is no auth, no rate limiting, no per-IP accounting, no file-size ceiling, and nothing in `Dockerfile`, `docker-compose.yml`, or `README.md` adds any (the README's Cloudflare Tunnel recommendation at `README.md:40-52` supplies TLS and hides the origin — it is not a rate limiter, and no WAF/rate-limit rule is configured in-repo).

Quantified abuse surface, measured against the real validator:

- **Disk fill ≈ ingress rate.** `MAX_BODY_BYTES = 256*1024` (`:26`). A 256 KiB body holds 61 maximal `flow_run_succeeded` events (60 × 64-char `node_types`); each is re-serialized with `received_at` at 4338 bytes, so one request writes 264,618 bytes — **amplification 1.009×**. A 10 Mbit/s trickle sustains ~108 GB/day; a 100 Mbit/s link ~1 TB/day. There is no rotation and no cap, so this terminates only when the volume is full.
- **Disk-full is unhandled.** `_append_lines` (`:163`) does a bare `open(..., "a")`/`write`; an `OSError` on ENOSPC propagates out of `ingest` (no try/except at `:202`) → 500, and a partially-written line stays in the file, becoming a permanent `malformed` count in `funnel.py`.
- **Funnel poisoning is free.** The only identity check is `uuid.UUID(install_id)` (`app.py:104-110`) — any random UUID validates. `installs`, `launched`, `run_attempted`, `activated` are distinct-`install_id` cardinalities (`funnel.py:86-99`), so one attacker inflates all of them arbitrarily. `week2_return` is equally forgeable: two spoofed events for one UUID at `T` and `T+8d` satisfy `funnel.py:97-99`.
- **Nothing is recorded to attribute or clean up after an abuse.** The stored envelope is a fixed 7 keys plus `received_at` (`app.py:130-138, 199`); no source IP, no user agent. After a poisoning run there is no field to filter on — the only remedy is discarding the whole file.
- **Browser drive-by is only partly blocked.** There is no `CORSMiddleware` (confirmed absent from `app.py`), so an `application/json` XHR is stopped by the failed preflight — but `json.loads(body)` (`:179`) never inspects `Content-Type`, so a `navigator.sendBeacon` / `fetch(..., {mode:"no-cors"})` with a `text/plain` blob is a simple request that lands. Any page a victim visits can drive traffic at the collector.
- **Blocking I/O on the event loop.** `ingest` is `async def` but calls the synchronous `_append_lines` under a `threading.Lock` (`:163, 202`) directly on the loop, so concurrent floods serialize the whole service including `/health`.

**Verdict:** *acceptable-with-mitigations* for a single-maintainer funnel, not acceptable as-is — the failure mode is not data theft, it is a full disk on the maintainer's own machine or VPS and an analytics file that can never be trusted again. Cheapest one-line-ish mitigations, in order of value: (a) a Cloudflare rate-limit rule on `/events` (zero code, matches the recommended deployment); (b) a shared-secret header check in `ingest` baked into the client's `_post` — turns anonymous abuse into abuse-by-anyone-who-read-the-binary, which still stops drive-by and scanners; (c) a size check on `events.jsonl` before `_append_lines` that 503s past N GB; (d) wrap `_append_lines` in try/except and offload it via `run_in_threadpool`.

---

### 3. SHOULD-FIX-BEFORE-MERGE — Real telemetry data is committed, and the compose file keeps writing into the tracked path

`tools/telemetry_collector/data/events.jsonl` is git-tracked (`git ls-files` confirms; `git check-ignore` exits 1 — not ignored) and was added in `c49cbdb8`. It holds one real event with a real install UUID `1b5440fa-e391-4c3f-8923-2fc51a9d825b`, `platform: darwin`, `mode: electron`, `app_version: 0.16.0` — i.e. the maintainer's own machine.

The one UUID is close to harmless. The structural problem is `tools/telemetry_collector/docker-compose.yml`:

```yaml
volumes:
  - ./data:/data
environment:
  TELEMETRY_DATA_DIR: /data
```

**Failure scenario:** the maintainer runs `docker compose up -d` per `README.md:10-14` to test against a local install, real events from whoever is pointed at it land in `tools/telemetry_collector/data/events.jsonl`, and the next `git add -A` / `git commit -am` publishes third-party telemetry — install ids, platforms, node-type profiles — to a public repo. There is no `.gitignore` anywhere under `tools/telemetry_collector/`, and the repo-root `.gitignore` covers only `/flowfile_core/tests/data/*` (`.gitignore:27`).

**Cheapest fix:** add `tools/telemetry_collector/data/` to `.gitignore` and `git rm --cached tools/telemetry_collector/data/events.jsonl`. Keep a `data/.gitkeep` if the mount target must exist. (Related nit: the `Dockerfile` declares no `USER`, so uvicorn runs as root and the bind mount leaves root-owned files inside the working tree.)

---

### 4. MINOR — `FLOWFILE_TELEMETRY_ENDPOINT=""` silently selects the project collector, and the shipped compose sets exactly that

`shared/telemetry.py:242` is `os.environ.get(ENV_ENDPOINT) or DEFAULT_ENDPOINT or None` — an **empty** value is falsy and falls through to the baked default. The root `docker-compose.yml:48` ships `FLOWFILE_TELEMETRY_ENDPOINT=${FLOWFILE_TELEMETRY_ENDPOINT:-}`, which sets the variable to the empty string in every container.

**Failure scenario:** an operator reads `docs/users/deployment/docker.md:142` ("unset ⇒ disabled"), decides they want telemetry on but only to their own collector, sets `FLOWFILE_TELEMETRY=1` in `.env` and leaves `FLOWFILE_TELEMETRY_ENDPOINT` blank intending to configure it later. Their deployment now reports to `events.flowfile.app` instead of nowhere. Symmetrically, an operator who blanks the variable to *turn telemetry off* has done nothing. Today the compose default `FLOWFILE_TELEMETRY=0` (`:47`) masks this, so it fires only for the operator who deliberately lifts the kill switch — which is precisely the operator who thinks they are in control of the destination.

**Cheapest fix:** treat an explicitly-set-but-empty endpoint as "disabled" — `raw = os.environ.get(ENV_ENDPOINT); return DEFAULT_ENDPOINT or None if raw is None else (raw or None)`.

---

### 5. MINOR — The funnel trusts the client-controlled `ts` when a server-stamped `received_at` is already stored

`app.py:188, 199` stamps every accepted event with a server-side `received_at`, but `funnel.py:62` reads `ts` (attacker-controlled, validated only as ISO-parseable and ≤64 chars at `app.py:120-122`), and `funnel.py:83` derives the whole `--days` window from it:

```python
cutoff = max(ts for _, _, ts in parsed) - timedelta(days=days)
```

**Failure scenario (reproduced):** I built a clean 5-install file and ran `compute_funnel(path, days=30)` → `installs=5, launched=5`. Appending one event with `ts: "9999-01-01T00:00:00Z"` (which `_validate_event` accepts — verified) gave `installs=1, launched=1`. **A single well-formed request silently zeroes every `--days` report**, with no error and no `malformed` count to hint at it. Naive (timezone-less) timestamps are also accepted and coerced to UTC (`funnel.py:47-49`), so a client in any timezone can shift its own `week2_return` classification.

**Cheapest fix:** one line — have `funnel._parse_line` prefer `raw.get("received_at")` and fall back to `ts`.

---

### 6. NIT — A failed `os.replace` on a consent grant leaves `telemetry.yaml.tmp` holding the install id

`shared/telemetry.py:194-202`:

```python
tmp = target.with_suffix(".yaml.tmp")
tmp.write_text(content, encoding="utf-8")
os.replace(tmp, target)
```

If `write_text` succeeds and `os.replace` raises (`OSError` caught at `:200`), the temp file — containing `install_id: <uuid>` on the grant path (`:192-193`) — survives with no cleanup anywhere (grep for `.tmp` in `shared/telemetry.py` finds only line 196).

**Failure scenario:** a user grants consent on storage where the rename fails (a bind-mounted or network `~/.flowfile`, a Windows AV holding a handle on the target). The route correctly reports 503 `TELEMETRY_PERSIST_FAILED` (`flowfile_core/flowfile_core/routes/telemetry.py:76-83`) and the user believes nothing was written — but a file bearing their install id sits in `~/.flowfile` forever. The privacy impact is small (it never leaves the machine, `load_state` at `:175` reads only `telemetry.yaml`, and a successful grant would have written the same id to a sibling file). The real defect is the contradiction between "the write did not stick" and a durable on-disk artifact of it. Note `shared/tests/telemetry/test_telemetry_state.py:49-51` asserts no `.tmp` leftovers only on the success path; the failure path is untested.

**Cheapest fix:** `finally: tmp.unlink(missing_ok=True)` around the replace, one line, plus extending that existing test to the failing-replace case.

---

### 7. NIT — The `error_class` allowlist is enforced in exactly one module; neither the client nor the collector re-checks it

`flowfile_core/flowfile_core/telemetry.py:126-128` (`classify_error`) is the only place the 45-name `ERROR_CLASS_ALLOWLIST` (`:41-94`) is applied. Both downstream layers accept any Python identifier ≤64 chars: `shared/telemetry.py:340` and `tools/telemetry_collector/app.py:87`.

**Failure scenario:** today this is airtight — I traced every producer. `flow_node/executor.py:426` sets `_last_exception_class = type(error).__name__`, and both consumers pass it through `classify_error` (`telemetry.py:219` for the finished path, `:255` for the crash path), so a custom node raising `class CustomerEmailNotFoundError(Exception)` reports `OtherError`; non-string values fail the frozenset membership test and also collapse. `flowfile_core/tests/test_telemetry_events.py:415` pins this with `_last_exception_class = "SELECT * FROM t"`. The exposure is purely structural: the day a second producer emits `flow_run_failed` (the worker, a future subscriber, a headless path) it will ship raw exception class names, and neither the client's sanitizer nor the collector's validator will catch it — exception names in user-authored custom nodes are frequently domain-shaped (`AcmePayrollAuthError`) and would land verbatim in the collector file.

**Cheapest fix:** move the allowlist into `shared/telemetry.py` and have `_is_valid_prop` check membership rather than `.isidentifier()`, so the sanitizer — not one call site — is the enforcement point.

---

## Verified clean (no finding)

- **`node_types` cannot carry a custom node name.** `run_snapshot` (`flowfile_core/flowfile_core/telemetry.py:156`) collapses anything outside `_builtin_types()` to `"custom"`. `_builtin_types()` (`:131-138`) calls `get_all_standard_nodes()`, which rebuilds `node_dict` from literals on every call (`configs/node_store/nodes.py:812`) — it is **not** the mutable `node_store.node_dict` that `register_custom_node` / `register_missing_node_template` / `add_to_custom_node_store` write into (`configs/node_store/__init__.py:103, 135, 139`), so registry pollution cannot widen the builtin set. Pinned by `tests/test_telemetry_events.py:118-123`. The raw `node_type` collected into `source_types` (`telemetry.py:159`) escapes only as the boolean `used_sample_data` (`:163`). Client-side, `_clean_node_types` (`shared/telemetry.py:321-327`) rejects the entire list on any bad entry — fail-closed.
- **`install_id` reaches no renderer, log, or response.** The only non-test occurrences are the consent file, the envelope, and the collector (`grep` across `*.py`/`*.ts`/`*.vue`). `TelemetryStatus.as_dict` (`shared/telemetry.py:149-155`) and `TelemetryStatusOut` (`routes/telemetry.py:36-43`) both omit it, and `shared/tests/telemetry/test_telemetry_gates.py:186` asserts that. No logging statement interpolates it — `persist_state` logs the path (`shared/telemetry.py:201`), `_send` logs an httpx exception (`:395`), neither the payload.
- **The route middleware cannot leak path params or bodies.** `_emit_for_route` (`flowfile_core/flowfile_core/telemetry.py:266-276`) reads `scope["route"].path` — the *template* (`/ai/diff/{diff_id}/accept`) — and emits only the constant props from the `ROUTE_EVENTS` table (`:98-108`), never anything from the request.
- **Collector envelope hardening is sound.** Unknown top-level keys are dropped by rebuilding a fixed 7-key dict (`app.py:130-138`); unknown prop keys reject the whole event (`:126-129`); `_read_capped_body` (`:141-157`) checks the declared `Content-Length` *and* streams with a running total, so a lying header cannot get past it; per-event exceptions cannot abort a batch (`:193-197`); `APP_VERSION_RE` is used with `fullmatch` so the unanchored pattern is safe.
---

# Report 4: fresh-eyes design review (client + collector)

## Overall verdict

`shared/telemetry.py` is a well-built module and the review should say so plainly: the four-gate design, the allowlist-only payload construction, the lazy `httpx`/`yaml` imports for worker import-purity, the RLock audit (nothing slow is held under it — `persist_state`'s write is deliberately outside), and the bounded-queue/daemon-thread delivery are all correct, and the test suite around it is unusually good for a telemetry client. I could not find a double-drain, a deadlock, or any path where user content reaches the wire. The defects cluster at two edges the design never quite closed: **process exit**, where `flush()` provably fails to deliver the very events it exists to deliver because it does not wait for the batch the daemon thread already took (verified empirically — the terminal event of a CLI run is lost even on a graceful exit, which contradicts the "only SIGKILL loses the queue" conclusion recorded in `docs/notes/telemetry-findings.md:69`); and **the collector plus its docs**, where a constant flipped in the branch's second commit (`DEFAULT_ENDPOINT`) left five documentation locations — including the user-facing privacy page the consent modal links to — asserting the opposite of what the code now does, and where the standalone service has no auth, rate limit, or file rotation behind an endpoint baked into every shipped client. Several items below (`_send` discarding the response, the `_ensure_worker`-after-put edge, the doc drift) are already recorded in `docs/notes/telemetry-findings.md`; they are unfixed in code, so I report them, flagged as known.

---

### 1. `flush()` does not wait for the batch the background thread already took — CLI runs silently lose their terminal event — **should-fix-before-merge**

`flush` drains only what is still *in the queue* (`shared/telemetry.py:489-499`). It never joins `_thread` and has no notion of an in-flight send. But `_enqueue` calls `_wake.set()` (`shared/telemetry.py:456-457`), so the daemon thread wakes immediately, `_take_batch`'s `get_nowait` removes the envelope (`shared/telemetry.py:398-405`), and it enters `_send`. `flush` then finds an empty queue and returns. The daemon is a daemon (`shared/telemetry.py:426`), so `Py_FinalizeEx` kills it mid-request; the `atexit`-registered `flush` (`shared/telemetry.py:429`) finds the same empty queue and also returns.

Verified empirically against the real module (scratch harness, no network — `_post` patched to a 300 ms stand-in, storage redirected out of `~/.flowfile`): with a gap of only **0.5 ms** between `emit()` and `flush(2.0)`, the sender is always `telemetry-flush`, `flush` returns in ~85 ms with `qsize()==0`, and the delivery-completion marker does not exist after the process exits. Sweep at 0.5/1/2/5/10/50 ms: event lost in every run. With a zero gap the main thread wins the race and `flush` delivers synchronously — so the outcome is purely who wins, and the real callers give the daemon a large head start.

Failure scenario: `flowfile run flow` completes. `run_graph` publishes `flow_run_finished`; the observer emits `flow_run_succeeded` (`flowfile_core/flowfile_core/telemetry.py:190-198`). The main thread then prints the summary and iterates node results (`flowfile/flowfile/__main__.py:130-140`), writes run completion, and imports `flowfile_core.telemetry` before calling `flush(2.0)` at `flowfile/flowfile/__main__.py:146` — tens of milliseconds, far more than the 0.5 ms the daemon needs. The daemon holds the batch; `flush` returns instantly; the process exits; the POST to a real HTTPS collector (≥100 ms RTT) is killed in flight. The `flow_run_succeeded`/`flow_run_failed` event — the one that carries the run outcome, duration and node shape — never arrives. The same applies at `flowfile_core/flowfile_core/main.py:438`.

This defeats the function's own docstring ("Best-effort synchronous drain for short-lived processes", `shared/telemetry.py:483`) and both of its call sites. The blast radius is data completeness only, never the user's run — but the loss is systematic and biased toward exactly the headless/scheduled runs the funnel is meant to measure. A one-line-ish fix (have `flush` join `_thread` for the remaining budget after the queue empties, or track an in-flight flag) closes it.

### 2. `DEFAULT_ENDPOINT` contradicts five documentation locations, including the user-facing privacy page — **should-fix-before-merge**

`shared/telemetry.py:38` ships `DEFAULT_ENDPOINT = "https://events.flowfile.app/events"`, and `_endpoint()` resolves `env or DEFAULT_ENDPOINT or None` (`shared/telemetry.py:242`). Verified: with `FLOWFILE_TELEMETRY_ENDPOINT` unset **and** with it set to `""`, `_endpoint()` returns the production collector.

These still say the opposite:
- `docs/users/telemetry.md:16` — "no default ships today, so with the variable unset telemetry is disabled"
- `docs/users/telemetry.md:130` — "No default ships today, so unset means disabled."
- `docs/users/deployment/docker.md:142` — "(none ships today, so unset ⇒ disabled)"
- `.env.example:161` — "Overrides the built-in default endpoint (none ships today, so unset = disabled)."
- `CLAUDE.md:389` — "Unset ⇒ telemetry fully disabled."

This is not pre-existing drift relative to this branch: the docs landed in `1b1484fc` and `DEFAULT_ENDPOINT` was introduced one commit later in `c49cbdb8` (`git log -S'events.flowfile.app'`), so the PR is internally inconsistent at merge. `docs/users/telemetry.md` is the page the consent modal links to (`shared/telemetry.py:75`), which makes it a false statement in a privacy document rather than ordinary doc rot.

Concrete failure scenario: an operator following `.env.example:160-162` uncomments `FLOWFILE_TELEMETRY=1` and leaves `FLOWFILE_TELEMETRY_ENDPOINT=` blank — which is also literally what `docker-compose.yml:48` interpolates to (`${FLOWFILE_TELEMETRY_ENDPOINT:-}`) — believing from the adjacent comment that blank means "no collector". They get the project's hosted collector. Consent (gate 4) still holds, so no data actually leaves without a user opt-in — that is what keeps this out of blocker territory — but the operator's mental model of where their users' events go is wrong. Either set `DEFAULT_ENDPOINT = ""` or fix all five locations; a blank env value arguably ought to mean "off" rather than falling through.

### 3. Collector has no auth, no rate limit, and no file rotation behind a baked-in public endpoint — **should-fix-before-merge (ops)**

`tools/telemetry_collector/app.py` exposes `POST /events` (`:173`) with no authentication dependency, no rate limiting, and `_append_lines` (`:160-165`) appending unboundedly to a single `events.jsonl` with no size or age rotation. The endpoint is compiled into every shipped client (`shared/telemetry.py:38`) and `README.md` recommends a public deployment.

Two concrete scenarios, both from one unauthenticated caller:
- **Disk fill / data poisoning.** 100 well-formed events per request pass `_validate_event` if `install_id` is any UUID (`:104-110`); each costs ~300 B on disk. A modest loop fills the host, after which `_append_lines` raises, `ingest` 500s, and — per the README's own warning — every real event is silently lost. Distinct-install counts in the funnel are trivially inflated by minting UUIDs.
- **One request breaks the funnel report.** `_validate_event` accepts any `ts` that `datetime.fromisoformat` parses and is ≤64 chars (`:120-122`, `_parse_ts` at `:63-70`) — no clock-skew or range check. `funnel.py:82` computes `cutoff = max(ts) - timedelta(days=days)`. A single posted event with `ts` in year 9999 pushes the cutoff past every genuine event, so `python -m tools.telemetry_collector.funnel data/events.jsonl --days 30` reports all zeros with no error.

A shared bearer token in the client envelope, or at minimum a documented Cloudflare rate-limit rule plus a `ts` sanity window and log rotation, would close both.

### 4. `_send` discards the response; the parity test's own comment overstates what it proves — **minor** *(known: `docs/notes/telemetry-findings.md` §2, §6)*

`_send` (`shared/telemetry.py:385-395`) throws away the `httpx.Response` from `_post`, so 202, 413, 422 and 500 are indistinguishable. Today nothing acts on the difference, but it makes any future spool with delete-on-success semantics unimplementable without changing the seam.

The concrete hazard it hides is a genuine cap mismatch. `shared/tests/telemetry/test_telemetry_schema_parity.py:78` asserts `BATCH_SIZE <= MAX_BATCH_SIZE` with the comment *"a full client batch must fit in one collector request"* — that comment is false as a byte statement. Measured: a maximal envelope permitted by `_clean_node_types` (60 names × 64 chars, `shared/telemetry.py:54-55`, `:327`) serializes to **4,293 B**; 100 of them are **429,412 B** against `MAX_BODY_BYTES = 262,144` (`tools/telemetry_collector/app.py:26`). The collector 413s the whole request before parsing (`:175-177`) and the client silently drops all 100.

I checked reachability honestly and it is **not** reachable from any shipped emitter: the only producer is `flowfile_core/flowfile_core/telemetry.py:156`, which collapses everything to built-in node type keys or `"custom"` — 48 distinct keys, longest 23 chars — so a realistic envelope is ~364 B and a 100-batch is ~36 KB. So this is a latent contract gap plus a misleading test comment, not a live bug. Worth fixing the comment either way, since it is the line a future reader will trust when raising `BATCH_SIZE` or adding an envelope field.

### 5. `_ensure_worker` runs only after a successful `put_nowait` — **minor** *(known: `docs/notes/telemetry-findings.md` §2)*

`shared/telemetry.py:449-458`: `queue.Full` returns at `:454`, before `_ensure_worker()` at `:455`. If the daemon thread ever died with a full queue, no replacement is started and every subsequent `emit` drops forever until something calls `flush()`. I traced the death paths and agree the rate is very low — `_loop` (`:408-417`) can only raise via `wake.wait`/`clear` or `_take_batch`, and `_send` swallows every `Exception` (`:394`) — so this is defensive, not a live bug. Moving `_ensure_worker()` above the `try` (or adding it to the `except` branch) costs nothing.

### 6. Collector does blocking file I/O on the event loop; `_write_lock` cannot be contended in that design — **minor**

`ingest` is `async def` (`tools/telemetry_collector/app.py:174`) and calls `_append_lines` directly (`:202`), which does `mkdir` + `open` + write synchronously on the loop thread (`:160-165`). Two consequences: a slow disk stalls the whole service rather than one request; and because there is no `await` inside the critical section and a single loop thread, `_write_lock` (`:56`) can never actually be contended — it is protecting against a caller shape that does not exist. Making `ingest` a plain `def` (FastAPI would run it in the threadpool) fixes both at once, and would give the lock a real job.

Related, at nit level: under `uvicorn --workers N` the lock is useless cross-process and the buffered per-line writes (`:164-165`) can flush at a non-line boundary, tearing JSONL records. The shipped `Dockerfile` CMD uses no `--workers`, so this is only a trap for whoever scales it.

### 7. `received_at` granularity does not match `ts`, and the README documents a value the code never produces — **minor**

`shared/telemetry.py:368` emits `ts` at whole-second granularity (`strftime("%Y-%m-%dT%H:%M:%SZ")` → `2026-09-01T12:30:00Z`), while `tools/telemetry_collector/app.py:188` stamps `received_at` with microseconds (`isoformat().replace("+00:00","Z")` → `2026-09-01T12:30:00.589209Z`). Both were confirmed by running them. `tools/telemetry_collector/README.md:60` shows the stored example as `"received_at":"2026-08-29T12:00:01Z"` — a shape the collector never writes. Failure scenario: an analyst parses both fields of a stored line with one `strptime("%Y-%m-%dT%H:%M:%SZ")` derived from the README and gets a `ValueError` on every row. Either truncate `received_at` to seconds or fix the README sample.

### 8. An empty or whitespace `FLOWFILE_TELEMETRY` does not engage the kill switch — **nit**

`_kill_switch_engaged` (`shared/telemetry.py:219-227`) tests `raw.strip().lower() in FALSY` where `FALSY = ("false","0","no","off")` (`:39`). Verified: `""` and `" "` both yield `False`. The `FALSY` tuple itself is consistent with the repo's `FLOWFILE_KERNEL_GC` convention (`0/false/no/off`), so that part is fine. But `export FLOWFILE_TELEMETRY=` — a common shorthand for "unset this" and the exact shape `docker-compose.yml:48` uses for the sibling variable — reads as "not falsy, so do not kill". For a privacy kill switch specifically, ambiguity should resolve toward off; treating empty-after-strip as falsy is a one-token change.

### 9. `_reset_for_tests` does not join the thread, leaving a sub-millisecond window for a real POST to the production endpoint from CI — **nit**

`_reset_for_tests` (`shared/telemetry.py:505-519`) sets `_stop`/`_wake` and nulls the globals but never joins `_thread`. A stale thread already past its `stop.is_set()` check with a batch in hand will still call `_send`, which resolves `_post` and `_endpoint()` as module globals at call time (`:386`, `:391-393`) — after `monkeypatch` teardown that is the *real* `httpx` post to `DEFAULT_ENDPOINT`.

I checked reachability in the current suite and it is effectively nil: every test that leaves events queued uses the `no_background` fixture (`shared/tests/telemetry/test_telemetry_client.py:42-45`), which stubs `_ensure_worker` so no thread exists, and the one real-thread test (`:211-221`) waits for delivery before teardown. The session-end `atexit` flush is also safe because `_queue is None` after the last reset makes `flush` return at `:490`. So this is a latent CI-egress hazard on a privacy feature, not a present one — but `_thread.join(0.5)` in the reset removes it, and is the same primitive finding #1 needs.

### 10. `_clean_node_types` sorts before capping and does not dedupe — **nit**

`shared/telemetry.py:327` returns `sorted(value)[:MAX_NODE_TYPES]`. Verified: given 5 `z_late_*` plus 60 `a_early_*` names, zero `z_*` survive — the cap systematically discards the alphabetically-last types, which for real node names means `union`, `unpivot`, `write_output` would drop before `add_*`/`cross_join`. It also does not dedupe (`_clean_node_types(["read"]*5)` returns five copies), so duplicates could spend the budget. Neither is reachable today: the only caller passes `sorted(set(...))` of built-in keys (`flowfile_core/flowfile_core/telemetry.py:156`, `:160-162`) and there are 48 of them, so the 60-cap never binds. Worth a comment noting the cap is defensive, or `sorted(set(value))[:MAX_NODE_TYPES]` with a truncation that is not alphabetically biased, if the cap is ever expected to engage.

---

### Verified as correct — no action

- **No double-drain, no deadlock.** `_take_batch` uses `Queue.get_nowait` (`shared/telemetry.py:402`), which is atomic, so an envelope reaches exactly one drainer even when `flush` and the daemon race — the client is at-most-once end to end. `_lock` is an RLock acquired only in `_cached_state`/`_invalidate_state_cache`/`_enqueue`/`emit_once`/`_reset_for_tests`, all same-thread nesting; the daemon thread and `flush` never take it, so no cycle exists. A stale generation's thread drains only its own now-unreachable queue.
- **`emit_once` holding the RLock across `_enqueue`** (`:473-477`) is fine — the only slow work under it is the one-time lazy `yaml` import and consent read inside `_cached_state`, bounded and cached.
- **`_read_capped_body`** (`tools/telemetry_collector/app.py:141-157`) is correct: it rejects an oversized declared `Content-Length` and a non-integer one, then still caps the streamed total, so a chunked body without `Content-Length` is handled properly.
- **No fsync in `_append_lines` is the right call** for telemetry — the loss window on a host crash is bounded by the page cache and is cheaper than the alternative.
- **Error responses leak nothing** (`:177`, `:181`, `:184`, `:186` are static strings), and `FastAPI(title=...)` leaves `debug=False`, so an `_append_lines` `OSError` surfaces as a bare 500 with no traceback. `/health` (`:168-170`) exposes nothing.
- **`get_status()` reading the consent file under the kill switch** (`shared/telemetry.py:267-273`) is correct behavior, not a missing short-circuit: the UI needs the stored consent value to render the toggle as granted-but-overridden. The only cost is a lazy `yaml` import on a route that is not hot.
- **`set_consent`'s `persisted and consent() is enabled`** (`:291`) correctly reports a read-only-storage failure as not-taken while returning the actually-stored status.