# Discovery dossier: frontend-tauri-wasm

> **FROZEN EVIDENCE** — snapshot at commit `f6963c77` (2026-07-03, v0.12.7); deliberately unmaintained and expected to drift.
> Authority order: **live repo → `.claude/skills/` → this file (leads only — re-verify before citing).** See [`README.md`](./README.md).

Scope: `flowfile_frontend/` (Vue 3 renderer + Tauri 2 shell) and `flowfile_wasm/` (Pyodide browser build).
All paths absolute under `Flowfile/`. Every claim below was verified
by reading the cited file unless marked **inferred**.

---

## 1. Renderer layout (`flowfile_frontend/src/renderer`)

```
src/renderer/
  main.ts            # Vue bootstrap: createApp, stores plugin, router, i18n, ElementPlus(size:"large",zIndex:2000),
                     # theme init, setupService.getSetupStatus() -> authService.setModeFromBackend -> auth init,
                     # then app.mount("#app"); non-desktop unauth -> router.push login; desktop update check
  index.html         # Vite entry (vite root IS src/renderer, see vite.config.mjs:22)
  config/constants.ts    # axios baseURL + GA OAuth callback URL resolution
  config/environment.ts  # ENV flags from NODE_ENV (isProduction gates prod_ready palette filter)
  lib/desktop.ts     # THE ONLY renderer<->Tauri bridge
  typings/desktop.ts # ServicesStatus type
  app/               # '@' alias target
    App.vue, api/, components/, composables/, directives/, features/, i18n/ (only locale: locales/gb.json),
    layouts/, pages/ (NodeDesigner standalone page), router/, services/, stores/, types/, utils/, views/
```

- Router: `app/router/index.ts` uses `createWebHashHistory` (line 1); routes lazy-imported; `/main` parent
  layout carries `meta.requiresAuth: true` (line 27); children: home, designer, nodeData, documentation,
  connections, project, databaseManager, cloudConnectionManager, kafkaConnectionManager, secretManager,
  aiProviders, kernelManager, templates, catalog, dashboards (new//:id/edit//:id), nodeDesigner, fileManager,
  admin, groups. `/setup` and `/login` have `requiresAuth: false`.

### 1.1 axios / API conventions

- `app/services/axios.config.ts`: sets `axios.defaults.baseURL = flowfileCorebaseURL` and
  `withCredentials = true` (lines 6-7). Request interceptor injects `Authorization: Bearer <token>`
  from `authService.getToken()` unless header `X-Skip-Auth-Header` present. Response interceptor:
  on 401 (once, `_retry` flag) refreshes token and replays; on refresh failure calls
  `authService.logout()`.
- `config/constants.ts:41-43`:
  ```ts
  export const flowfileCorebaseURL = isDesktop
    ? `http://127.0.0.1:${resolveCorePort()}/`
    : `${window.location.origin}/api/`;
  ```
  Desktop port comes from `window.__FLOWFILE_PORTS__` injected by the shell before any renderer script
  runs (fallback 63578). Base must be ABSOLUTE because `aiStreamClient`/`aiDiffClient` do
  `new URL(path, base)` which rejects a relative base (comment at constants.ts:33-35).
  There is an unresolved `TODO(H)` at constants.ts:36-40 about dev-mode CORS under `tauri dev`
  (page origin localhost:8080 vs baseURL 127.0.0.1:<port> — cross-origin, believed fine, unverified).
- API wrappers: `app/api/*.api.ts` (ai, apiConsumers, catalog, expressions, file, fileManager, flow,
  flowApi, kernel, lsp, node, notebook, project, secrets, shares, templates, userGroups) are static-method
  classes importing the configured axios from `../services/axios.config`. `app/services/` additionally holds
  auth.service.ts, setup.service.ts, user.service.ts and the SSE clients aiStreamClient.ts / aiDiffClient.ts.

### 1.2 Trailing-slash 307 trap — VERIFIED

- FastAPI normalizes trailing slashes with an **absolute** 307 redirect. Frontend paths must match the
  core route **exactly**, slash included. Verified pairs:
  - `node.api.ts:127` posts `/update_settings/` ↔ `routes.py:1146 @router.post("/update_settings/")`
  - `node.api.ts:74` posts `/node/description/` ↔ `routes.py:1361 @router.post("/node/description/")`
  - `node.api.ts:99` posts `/node/reference/` ↔ `routes.py:1396 @router.post("/node/reference/")`
- Two proxy layers replicate FastAPI's absolute-307 rewrite so mismatches are *masked in dev*:
  - `vite.config.mjs:37-63`: `/api` proxy → `http://localhost:63578`, `rewrite` strips `/api`, and a
    `configure(proxy)` hook rewrites `Location` headers from the backend origin back to `/api/...`
    ("Replicate nginx's built-in proxy_redirect default").
  - `nginx.conf:9-32` (Docker): `location /api/ { proxy_pass http://flowfile-core:63578/; }` and the
    comment explains it deliberately does NOT override Host so nginx's default `proxy_redirect`
    rewrites the redirect Location back to the external URL.
- User memory (feedback_match_axios_slash_to_route.md): the mismatch "→ 307 that silently fails in
  Docker; Vite + pytest mask it. Fix frontend-side; verify in core logs." So: always author the axios
  path with the same trailing slash the FastAPI decorator uses.

### 1.3 Pinia stores (`app/stores/`, `index.ts` is the plugin)

| Store (id) | File | Role |
|---|---|---|
| `flow` | flow-store.ts (159 LOC) | flowId (persisted in sessionStorage key `last_flow_id`), `vueFlowInstance`, undo/redo `historyState`, artifactData, flow `parameters`, and two monotonic signal counters: `pendingReloadCounter` (`requestReload()` = "backend mutated flow, re-fetch"; Canvas.vue watches) and `pendingLayoutResetCounter` (`requestLayoutReset()` = re-run auto-layout). `updateHistoryState` is the hook every canvas mutation routes through; it bumps editor `graphVersion` (dirty state). |
| `node` | node-store.ts (730 LOC) | current nodeId, `nodeData` cache (`getNodeData(nodeId, useCache, includeOutput)`), per-node validation callbacks (`setNodeValidateFunc`/`validateNode`), node descriptions/references caches, `updateSettings`/`updateSettingsDirectly` (POST `/update_settings/` with node_type from `node.data.nodeTemplate.item`, then bump graphVersion + validate downstream ids + refresh auto description). Carries many `@deprecated` proxy getters to flow/editor/results stores (legacy snake_case names). |
| `editor` | editor-store.ts (251 LOC) | drawer state (isDrawerOpen, activeDrawerComponent (shallowRef), drawerProps, drawCloseFunction), inputCode (shared code-editor buffer used by e.g. Filter advanced mode), log-viewer state, showCodeGenerator, showEdgeLabels, isRunning/showFlowResult, `isAiOpen` (independent panel, deliberately NOT via activeDrawerComponent), `graphVersion` counter, plus "request" token signals: flowSettingsOpenRequest, nodeSettingsOpenRequest/nodeDataOpenRequest ({nodeId, token}), openFlowRequest ({flowPath,name,token}). |
| `results` | results-store.ts | run results per flow (`runResults`), per-flow-per-node `runNodeResults` and `runNodeValidations`, `currentRunResult`, `resultVersion`. |
| `auth` | auth-store.ts | user/isAuthenticated/isLoading/error; login/logout/initialize via `services/auth.service`; getters isAdmin, mustChangePassword. |
| `theme` | theme-store.ts | light/dark/system, localStorage key `flowfile-theme-preference`, matchMedia system pref. |
| `column` | column-store.ts | DEPRECATED shim: doc comment records the store split (flow/node/results/editor); re-exports useNodeStore for backward compat; keeps `localColumns`. Canvas.vue still imports `useNodeStore` from `"../../stores/column-store"`. |
| `drawer` | drawer-store.ts | drawer-related state used by Canvas (per-panel). |
| `editor`-adjacent | fileBrowserStore.ts, global-store.ts, project-store.ts, catalog-store.ts (829 LOC), dashboards-store.ts, sharing-store.ts, tutorial-store.ts, notebook-store.ts | feature stores: file browser, global misc, git project tracking, catalog view, dashboards, group-sharing UI, tutorial, notebooks. |
| AI stores | ai-store.ts (1666 LOC), ai-agent-store.ts, ai-diff-store.ts, ai-command-palette-store.ts, ai-ghost-node-store.ts, ai-autocomplete-store.ts, ai-code-generator-store.ts (+ *-persistence.ts) | assistant chat, agent runs, staged-diff accept/reject, command palette, ghost-node suggestions. These have colocated `*.test.ts` (Vitest picks up only `src/**/*.test.ts`). |

Naming: files kebab-case `xxx-store.ts` (one legacy camelCase: `fileBrowserStore.ts`).

### 1.4 Node UI system — registry & end-to-end trace

**There is no static registry.** Node settings components are resolved by a **naming convention +
`import.meta.glob` + string-interpolated path**:

- Templates come from the backend: `GET /node_list` → `flowfile_core/flowfile_core/routes/routes.py:1263`
  returns `nodes_list` built in `flowfile_core/flowfile_core/configs/node_store/nodes.py`
  (`get_all_standard_nodes()` — a hardcoded `list[NodeTemplate]` with `name`, `item` (snake_case id),
  `input`/`output` counts, `image` (svg filename), `node_group`, `prod_ready`, `drawer_title`,
  `drawer_intro`, `laziness`, `tags`).
- Frontend `NodeTemplate` type: `app/types/flow.types.ts:99-117` (adds `multi`, `custom_node`,
  `output_names?`, `dynamic_inputs?`).
- Fetch+cache: `app/composables/useNodes.ts:44-68` `fetchNodeTemplates()` GETs `/node_list` once per
  session (module-level cache). Palette (`views/DesignerView/NodeList.vue` → local `useNodes.ts` shim →
  `composables/useNodes.ts:176-208 useNodes()`) filters `prod_ready` when `ENV.isProduction`
  (environment.ts: NODE_ENV-derived).
- Component resolution convention (TWO independent copies, keep in sync):
  1. `app/composables/useDragAndDrop.ts:88` — `import.meta.glob("../components/nodes/node-types/elements/**/*.vue")`;
     `getComponent()` at :158 builds `elements/${camelCase(item)}/${TitleCase(item)}.vue`
     (custom nodes → `elements/customNode/CustomNode.vue`); validates names `/^[a-zA-Z][a-zA-Z0-9]*$/`.
     This resolves the component rendered in the VueFlow node (passed into `data.component`).
  2. `app/components/nodes/GenericNode.vue:25` — `import.meta.glob("./node-types/elements/**/*.vue")`;
     `loadDrawerComponent()` at :47 same convention → the settings-drawer component
     (`defineAsyncComponent`, timeout 3000, retry ≤3, `console.error` on missing path — NO build error).
- 46 element dirs / 87 .vue files under `app/components/nodes/node-types/elements/`
  (apiResponse applyModel catalogReader catalogWriter cloudStorageReader cloudStorageWriter crossJoin
  customNode databaseReader databaseWriter dynamicRename evaluateModel exploreData externalSource filter
  flowInput flowOutput formula fuzzyMatch googleAnalyticsReader graphSolver groupBy join kafkaSource
  manualInput output pivot polarsCode pythonScript randomSplit read recordCount recordId restApiReader
  runFlow sample select sort sqlQuery textToRows trainModel union unique unpivot waitFor windowFunctions).
- **Trap:** `composables/useNodes.ts:8` also exports a `getComponent` whose glob points at
  `../features/designer/nodes/elements/**/*.vue` — that directory DOES NOT EXIST (verified: no
  `features/designer/nodes` dir). It has no live callers (only re-exported via composables/index.ts and
  the deprecated views/DesignerView/useNodes.ts shim) but will throw "Component not found" if anyone
  imports it. Use useDragAndDrop's loader / the GenericNode path instead.
- `app/components/nodes/getComponents.ts` is unrelated: it lazy-loads only
  `./elements/manualInput/${name}.vue` editor cells.
- Node icons: template `image` (e.g. `filter.svg`) resolves via
  `features/designer/utils.ts:70 getImageUrl` → `features/designer/assets/icons/<name>` for builtin
  icons; unknown names hit `${flowfileCorebaseURL}user_defined_components/icon/<name>` (custom nodes).

**End-to-end trace (filter node):**
1. Backend defines template: `configs/node_store/nodes.py:158-170` (`item="filter"`, input=1, output=1,
   `image="filter.svg"`, drawer_title "Filter Data"…).
2. Palette lists it (NodeList.vue via `useNodes()` — GET `/node_list`).
3. Drag: `useDragAndDrop.onDragStart` puts the JSON template into
   `dataTransfer("application/vueflow")`; `setPaletteDragImage` clones the palette item because
   **WebKit renders no drag preview for user-select:none elements** (useDragAndDrop.ts:289-311).
4. Drop: `onDrop` (useDragAndDrop.ts:555) validates via `isValidNodeTemplate`, `getComponent(nodeData)`
   loads `elements/filter/Filter.vue`, builds VueFlow `Node{type:"custom-node", data:{id,label,component,
   inputs,outputs,nodeTemplate}}`, calls `FlowApi.insertNode(flowId,nodeId,"filter",x,y)`, then
   `addNodes`. If dropped on a hovered edge and node is effectively 1-in/≥1-out and not dynamic-input,
   it splices via `insertNodeOnEdge` (delete old connection → connect upstream → connect downstream,
   staged rollback on failure; `suppressedEdgeRemovals` set prevents Canvas's `@edges-change` from
   double-deleting the backend connection).
5. Canvas registers node types: `views/DesignerView/Canvas.vue:127-130`
   `nodeTypes = { "custom-node": NodeWrapper, group: GroupNode }`; edges use DeletableEdge /
   GroupProxyEdge. `Canvas.vue` (~1170 LOC; has a TODO(refactor) plan at top) owns loadFlow (token-based
   stale-run discard), clipboard/copy-paste, context menu, keyboard shortcuts, draggable panels.
6. Open settings: `components/nodes/baseNode/nodeButton.vue:157` →
   `nodeStore.openDrawer(props.drawerComponent, nodeTitleInfo)` (deprecated proxy →
   `editorStore.openDrawer`). GenericNode.vue supplied `drawerComponent` via its own glob.
7. Drawer lifecycle: `views/DesignerView/NodeSettingsDrawer.vue` requires the settings component to
   expose `loadNodeData(nodeId)` and `pushNodeData()` (lines 26-27, called at 69-71; the close function
   registered via `nodeStore.setCloseFunction`). "Universal Apply: every drawer-entry node component
   exposes pushNodeData" (line 34 comment).
8. Settings component pattern (`elements/filter/Filter.vue`):
   - wraps content in `<generic-node-settings v-model="nodeFilter" @update:model-value="handleGenericSettingsUpdate" @request-save="saveSettings">`
   - `const { saveSettings, pushNodeData, handleGenericSettingsUpdate } = useNodeSettings({ nodeRef, onBeforeSave })`
     (`app/composables/useNodeSettings.ts` — auto-sets `is_setup=true`, calls `nodeStore.updateSettings`,
     shows ElMessage on failure, supports onAfterSave/getValidationFunc)
   - `loadNodeData` fetches `nodeStore.getNodeData(nodeId, false)` and hydrates `setting_input`
   - ends with `defineExpose({ loadNodeData, pushNodeData, saveSettings })`
9. Save: `nodeStore.updateSettings` (node-store.ts:485-513) stamps live canvas pos_x/pos_y, POSTs
   `/update_settings/?node_type=filter`, bumps graphVersion, fetches `/node/downstream_node_ids` and
   re-validates each downstream node, refreshes auto description.
10. Backend: `routes.py:1146 POST /update_settings/` dispatches on node_type into the Pydantic schema
   (`input_schema.NodeFilter`) and the FlowGraph setter.

**Add-a-node-UI checklist (main app):**
1. Backend `configs/node_store/nodes.py`: add `NodeTemplate(...)` (item snake_case, input/output counts,
   image svg, node_group, drawer_title/intro, prod_ready, laziness, tags). Also settings schema in
   `schemas/input_schema.py` + FlowGraph add-method + `/update_settings` dispatch (other agents' dimension).
2. Icon: add `<image>.svg` to `src/renderer/app/features/designer/assets/icons/`.
3. Settings component at EXACTLY
   `src/renderer/app/components/nodes/node-types/elements/<camelCase(item)>/<TitleCase(item)>.vue`
   (TitleCase = each `_`-separated word capitalized then joined, e.g. `text_to_rows` → `textToRows/TextToRows.vue`).
   Use the Filter.vue pattern: `useNodeSettings` + `<generic-node-settings>` + `defineExpose({loadNodeData, pushNodeData})`.
4. No registration file — resolution is by path convention. Renaming dir/file breaks at runtime only
   (console.error, drawer never loads). Both globs (GenericNode.vue + useDragAndDrop.ts) point at the
   same tree, so one component file serves both.
5. `prod_ready: false` keeps it out of the production palette but still loadable in saved flows
   (useNodes.ts caches the full list; the filter is per-consumer).

### 1.5 VueFlow canvas integration

- `@vue-flow/core` ^1.42.1 + `@vue-flow/minimap`. Canvas.vue holds `useVueFlow()` instance and stores
  it in flow-store (`setVueFlowInstance`) so stores/components can `findNode`/mutate handles (e.g.
  Filter.vue `updateNodeOutputHandles` rewrites `vfNode.data.outputs` for split pass/fail mode).
- Node ids are the numeric backend node ids as strings. `data` carries
  `{id, label, component (markRaw), inputs/outputs (NodeHandle[]), nodeTemplate, nodeReference?}`.
- Handles derived in `utils/nodeHandles.ts` `deriveHandles` from template counts + `dynamic_inputs`
  /`output_names`; `multi:true` nodes render ONE input handle accepting many edges (union, polars_code
  input=10 but splice-wise 1) — see onDrop comment useDragAndDrop.ts:626-630.
- Groups: `composables/useNodeGroups.ts`; group nodes are VueFlow `type:"group"`, children re-parented
  with parent-relative positions, collapse → proxy edges (`GROUP_PROXY_EDGE_PREFIX`).
- Edge labels: from source handle label, else `nodeReference`, else `df_<nodeId>` (importFlow,
  useDragAndDrop.ts:519-536), gated by `editorStore.showEdgeLabels`.
- VueFlow has no `@pane-dblclick`; Canvas listens natively (Canvas.vue:334 comment).
- Right drawer is a declarative tab registry: `views/DesignerView/drawerRegistry.ts` — "Single source of
  truth for the designer's tabbed drawers. Adding/moving a view is a one-entry edit here"; tabs
  settings/results/code with `visibleWhen`/`focusWhen`/`onMinimize` closures; Code tab defers CodeMirror
  creation until visible ("it breaks if built while hidden").

---

## 2. Tauri bridge — `src/renderer/lib/desktop.ts` (sole boundary)

`isDesktop = typeof window !== "undefined" && !!window.__TAURI_INTERNALS__` (line 43). Uses the
`withGlobalTauri` global (`window.__TAURI__.core.invoke` / `.event.listen`) rather than importing
`@tauri-apps/api` (except one dynamic import for clipboard). Full capability surface (each with web-mode
fallback):

| Method | Desktop behavior | Web fallback |
|---|---|---|
| `getAppVersion()` | `window.__TAURI__.app.getVersion()` else invoke `get_app_version` | `""` |
| `getServicesStatus()` | invoke `get_services_status` | `{status:"not_started",error:null}` |
| `getServicePorts()` | invoke `get_service_ports` (debug/health only — axios uses sync `__FLOWFILE_PORTS__`) | `null` |
| `quitApp()` | invoke `quit_app` (Rust: shutdown_all then app.exit(0)) | no-op |
| `refreshApp()` | invoke `app_refresh` (Rust evals `window.location.reload()`) | `window.location.reload()` |
| `openOauth(url)` | invoke `open_oauth` → modal Tauri webview (oauth.rs), resolves captured `code` or null | `window.location.assign(url)`, returns null |
| `openExternal(url)` | `invoke("plugin:opener|open_url")` — system browser (Google blocks embedded webviews: disallowed_useragent) | `window.open(url,"_blank","noopener")` |
| `readClipboardText()` | dynamic `import("@tauri-apps/plugin-clipboard-manager").readText()` (NSPasteboard — avoids macOS "Paste" pill) | `navigator.clipboard.readText()` |
| `onServicesStatus(h)` | listen `services-status` | returns no-op unsubscriber |
| `onStartupSuccess(h)` | listen `startup-success` | no-op |
| `onViewZoom(h)` | listen `view:zoom` ("in"/"out"/"reset" from native View menu, menu.rs::emit_zoom) | no-op |

Rules (frontend CLAUDE.md:28, verified): view code must never import `@tauri-apps/*` directly; new Rust
commands must be added to `generate_handler![]` in lib.rs (current set exactly: get_services_status,
get_service_ports, get_app_version, quit_app, app_refresh, open_oauth — lib.rs:42-49) AND wrapped in
desktop.ts. Plugin commands (opener, clipboard-manager) are invoked directly — no Rust command needed —
but require permissions in `src-tauri/capabilities/main.json` (currently includes `opener:default`,
`clipboard-manager:allow-read-text`, `updater:default`, `process:allow-exit/restart`, `window-state:default`).

**Clipboard pill gotcha (verified in code comment desktop.ts:124-131 + memory):** in WKWebView,
`navigator.clipboard.readText()` pops macOS's native "Paste" confirmation pill on every programmatic
read; always go through `desktop.readClipboardText()`. Used by canvas paste
(useDragAndDrop.ts:873-891 `createManualInputFromClipboard`).

---

## 3. Tauri shell (`src-tauri/src/`)

Modules: lib.rs (entry), commands.rs, env.rs, menu.rs, oauth.rs, sidecar/{mod.rs,readiness.rs,shutdown.rs},
state.rs, window.rs, main.rs.

### 3.1 Boot sequence (lib.rs)

1. Plugins: log (stdout + LogDir file "flowfile", Info), opener, process, os, clipboard_manager,
   window_state; updater only `#[cfg(desktop)]` (lib.rs:19-39).
2. `setup`: async task emits `services-status {status:"starting"}` → `sidecar::start_services(handle)`.
3. On success: `create_main_window(&handle, ports)` — built **programmatically** (not tauri.conf windows)
   so `initialization_script` can inject
   `window.__FLOWFILE_PORTS__ = Object.freeze({core, worker})` BEFORE any renderer script (lib.rs:164-186).
   Window: 1600x1000 min 1024x700, `visible(false)`, `.disable_drag_drop_handler()` (Tauri's native
   drag-drop capture would swallow HTML5 drag events VueFlow/AG Grid need — comment lines 176-181),
   `.accept_first_mouse(true)` (macOS first-click focus). Then emit `startup-success`, `window::show_main`.
   If window creation fails: services are healthy → graceful `shutdown_all`.
4. On start_services error: `sidecar::shutdown::kill_spawned(&handle)` — kill by PID, **no HTTP**,
   because ports may be unresponsive (often why readiness failed) and in the NoFreePortPair case
   `state.ports` still holds defaults that may belong to ANOTHER Flowfile instance — POSTing /shutdown
   there could kill an unrelated app (lib.rs:88-99, shutdown.rs:49-77). Sets is_shutting_down first so
   the supervisor won't respawn what it kills (prevents "self-restarting orphans behind the error window").
5. Exit paths: `main` window CloseRequested → block_on shutdown_all. Run loop matches BOTH
   `RunEvent::ExitRequested` and `RunEvent::Exit` (lib.rs:133-148) — macOS Cmd+Q / app-menu Quit /
   dock-quit go through AppKit `terminate:` → tao LoopDestroyed → `RunEvent::Exit` (NOT ExitRequested);
   without the Exit arm those paths orphan the sidecars. `is_shutting_down` guard makes the second call a no-op.
6. Known open race, documented `TODO(D)` lib.rs:115-121: quitting during startup can shutdown before
   sidecar PIDs are stored → the spawn that lands after shutdown is never reaped.

### 3.2 Sidecar spawn + port scan (sidecar/mod.rs)

- Port pair scan: `find_free_port_pair()` (mod.rs:47-56): for k in 0..100 (`PORT_SCAN_PAIRS`,
  state.rs:11), `core = 63578 + k*2`, `worker = core+1`; free = can bind 127.0.0.1:<port>.
- Binary resolution: dev (`debug_assertions`) → `src-tauri/binaries/`; release →
  `<resource_dir>/binaries/`. Name = `{flowfile_core|flowfile_worker}-{env!("FLOWFILE_TARGET_TRIPLE")}[.exe]`
  (mod.rs:142-169). Missing file → `SidecarError::BinaryNotFound`. (Staging is `tools/rename_sidecar.py`
  via `make services`.)
- `ensure_executable`: Tauri's `bundle.resources` copy may strip the exec bit → chmod 755 (mod.rs:171-187).
- Spawn (mod.rs:192-272): `tokio::process::Command`, args
  core: `--host 127.0.0.1 --port <core> --worker-port <worker>`;
  worker: `--host 127.0.0.1 --port <worker> --core-host 127.0.0.1 --core-port <core>`.
  cwd = home dir (packaged cwd may be read-only `/`). `kill_on_drop(false)`.
  Unix: `cmd.process_group(0)` — own pgid == pid so shutdown can `killpg` the whole subtree (reaps the
  worker's multiprocessing children); core's *scheduled-flow* runs use start_new_session=True and
  deliberately escape the group to survive app exit. Windows: `CREATE_NO_WINDOW` (0x08000000).
  stdout/stderr piped to `pump_stream` → tauri log (target "sidecar").
- Env injection (env.rs `build_child_env`): inherits parent env, creates `~/.flowfile` +
  cache/temp/logs/system_logs/flows/database dirs, then sets HOME, TMPDIR, DOCKER_CONFIG,
  `FLOWFILE_STORAGE_DIR=~/.flowfile`, **`FLOWFILE_MODE=electron`** (canonical value — backend has
  hard-coded `== "electron"` checks; renderer treats electron|tauri|desktop as synonyms),
  **`FLOWFILE_SUPERVISOR_PID=<shell pid>`** (presence enables `shared/parent_watcher.py` self-reap when
  the shell is SIGKILLed; never set for CLI/Docker so the watcher never fires there),
  `FLOWFILE_WORKER_PORT`, `CORE_PORT`, `CORE_HOST=127.0.0.1`, `WORKER_HOST=127.0.0.1` (else core's
  settings builds worker URL with 0.0.0.0 — fragile connect target). Windows: DOCKER_HOST npipe;
  Unix: DOCKER_HOST unix socket + PATH prefixed with /usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin.
- Supervisor (`handle_termination` mod.rs:306-408): on child exit, if not shutting down → clear pid,
  `RestartCounter.next_backoff()` (state.rs: MAX_RESTARTS=5 within RESTART_WINDOW_SECS=60, backoffs
  [500,1000,2000,4000,8000]ms; counter resets after 60s continuous uptime) → sleep → **re-check
  is_shutting_down under the same lock as respawn** (closes restart↔shutdown race; guard must NOT span
  the async readiness wait) → respawn + readiness. Exhausted budget → emit services-status error
  "crashed 5 times within 60s — giving up".

### 3.3 Readiness probe (sidecar/readiness.rs)

- `probe_once(port)`: single `GET http://127.0.0.1:<port>/docs`, 1s timeout (HEALTH_CHECK_TIMEOUT_MS);
  success = HTTP 2xx.
- `wait_until_ready`: polls every 1s until success or **120s** deadline (SERVICE_START_TIMEOUT_MS;
  state.rs:14-19 comment: onedir bundles boot ~2s warm but first-launch-after-reboot / AV scanning can
  take 30-60s; loading window shows per-attempt "services-status" events). Timeout →
  `SidecarError::ReadinessTimeout`. Core and worker are awaited concurrently (`tokio::join!`).

### 3.4 Shutdown ladder (sidecar/shutdown.rs)

`shutdown_all` (best-effort, idempotent via is_shutting_down):
1. POST `http://127.0.0.1:<port>/shutdown` to core AND worker in parallel (3s timeout each,
   SHUTDOWN_TIMEOUT_MS).
2. Sleep 2s (FORCE_KILL_TIMEOUT_MS) for natural exit.
3. Take PIDs; Unix `killpg(SIGTERM)` (whole group — a bare kill(pid) would orphan the worker's
   multiprocessing viz-session grandchildren); Windows `taskkill /T /PID` (WM_CLOSE first).
4. Poll `process_alive` (kill(pid,0)) every 100ms up to 5s (SIGTERM_GRACE_MS — sized for the worker's
   viz-session drain, SHUTDOWN_GRACE_SECONDS=10 internal); still alive → `killpg(SIGKILL)`
   (/ `taskkill /F /T`).
`kill_spawned`: startup-failure path — sets is_shutting_down, takes PIDs, SIGTERM→SIGKILL ladder,
**no network I/O** (see 3.1.4 rationale).
Crash backstop: sidecars self-reap via FLOWFILE_SUPERVISOR_PID + shared/parent_watcher.py.
Known TODO(C) mod.rs:317-318: pid not cleared when termination happens during shutdown → stale
`.take()` could killpg a recycled PID.

### 3.5 tauri.conf.json (version 0.12.7, identifier com.flowfile.app)

- build: `frontendDist ../build/renderer`, `devUrl http://localhost:8080`,
  `beforeDevCommand npm run dev:web`, `beforeBuildCommand npm run build:renderer-only`.
- `withGlobalTauri: true` (that's why desktop.ts can use `window.__TAURI__`).
- Windows array defines ONLY the `loading` window (700x400 undecorated transparent, `loading.html`);
  `main` is created in Rust (see 3.1).
- CSP: `default-src 'self' https://apis.google.com; connect-src 'self' http://127.0.0.1:* http://localhost:* https://accounts.google.com https://apis.google.com ipc: http://ipc.localhost; script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: https://apis.google.com; style-src 'self' 'unsafe-inline'; font-src 'self' data:; img-src 'self' data: blob: https:; worker-src 'self' blob:; frame-src 'self' https://accounts.google.com`
  — connect-src wildcards localhost ports (any allocated core port); Google hosts for GA OAuth;
  external CDNs otherwise blocked (that's why Material Icons are bundled from npm — main.ts:10-14 comment).
- `macOSPrivateApi: true`; assetProtocol enabled, empty scope.
- bundle: targets ["app","dmg","nsis","deb"], `createUpdaterArtifacts: true`, resources `binaries/**/*`
  (the PyInstaller sidecars), macOS min 11.0 + hardenedRuntime + entitlements.mac.plist; Windows NSIS
  perMachine + embedBootstrapper webview install.
- updater plugin: endpoint `https://github.com/Edwardvaneechoud/Flowfile/releases/latest/download/latest.json`,
  minisign pubkey pinned, windows installMode passive. Renderer-side startup check:
  `composables/useDesktopUpdater.ts` called from main.ts (`checkForUpdatesOnStartup()` — desktop-only no-op otherwise).
- oauth.rs: modal `oauth` webview window (600x700, parented to main), `on_navigation` intercepts first
  URL with `code=` query param, cancels navigation and resolves it; window Destroyed → resolves None.

### 3.6 Menu (menu.rs)

`build()` constructs native menu; `on_menu_event` maps ids: view-zoom-in/out/reset → `emit_zoom` →
event `view:zoom` payload "in"/"out"/"reset" (renderer drives VueFlow zoom via `desktop.onViewZoom`);
plus open_external items. (Shell can't zoom the canvas itself.)

---

## 4. Frontend configs & tooling

### 4.1 vite.config.mjs (verified, key lines)

- `root: src/renderer` (:22); `define.__APP_VERSION__` from package.json version (web fallback; desktop
  uses get_app_version) (:24-26); cacheDir pinned to `node_modules/.vite` (parallel Vite instances:
  dev:web vs tauri-dev beforeDevCommand) (:30).
- server: host 0.0.0.0, **port 8080, strictPort: true** (fail fast — Tauri devUrl hard-coded) (:31-36).
- `/api` proxy → localhost:63578 with rewrite strip + the 307 Location rewriter (see 1.2).
- build: outDir `flowfile_frontend/build/renderer`, `minify: false` (!), emptyOutDir.
- `optimizeDeps.force: true` — rebuilds dep cache every start (~5-15s cold) to avoid webview
  "504 Outdated Optimize Dep" (:72-79).
- aliases: `@` → `src/renderer/app` plus `@/api`, `@/types`, `@/stores`, `@/composables`.
  **Aliases exist in three files and must stay in sync**: vite.config.mjs (:83-91), tsconfig.json
  (`@/*` glob + the four named ones, each with `/*` variants), vitest.config.ts (only bare `@`).
- No `@vitejs/plugin-react` on purpose: React only via dynamic `import("react")` inside the
  Graphic Walker Vue wrappers (:6-9 comment); tsconfig excludes `*.tsx`/`*.jsx` (**inferred from
  CLAUDE.md, tsconfig excludes not re-read**).

### 4.2 package.json (v0.12.7)

Scripts: `dev` = tauri dev; `dev:web` = vite; `build` = lint + vue-tsc --noEmit + tauri build;
`build:web` = lint + vue-tsc + vite build; `build:renderer-only` = vite build only;
`preview:web` = vite preview (:4173); `lint` = `eslint --fix ./src/**/*.{ts,vue}`;
`test:web` = playwright tests/web-flow.spec.ts; `test:all` = playwright (all specs);
`test:unit` = vitest run.
Key deps: vue ^3.5.13, pinia ^2.0.16, element-plus ^2.13.0, @vue-flow/core ^1.42.1, axios ^1.16.0,
vue-router ^4.3.2, vue-i18n ^9.13.1, @tauri-apps/api ^2.0.0, @kanaries/graphic-walker ^0.5.0,
**react/react-dom EXACT 19.2.0 (also in `overrides`)** — see incident in §7.

### 4.3 Lint/format

- `.eslintrc.js` (legacy eslintrc, eslint 8): root:true, vue-eslint-parser + @typescript-eslint,
  extends eslint:recommended / @typescript-eslint/recommended / plugin:vue/vue3-recommended /
  @vue/typescript/recommended / @vue/prettier. Rules: prettier/prettier=warn (usePrettierrc),
  no-non-null-assertion=off, no-explicit-any=off, vue/multi-word-component-names=off,
  `linebreak-style: ["error","unix"]`, no-console/no-debugger warn only in production NODE_ENV.
  Declares global `__APP_VERSION__: readonly`.
- `.prettierrc.json`: semi true, tabWidth 2, singleQuote false (double quotes), printWidth 100,
  trailingComma "all", endOfLine lf.

### 4.4 Tests

- Vitest: `vitest.config.ts`, node env; picks up only `src/**/*.test.ts` — currently the ai-* store
  tests, features/ai/markdown.test.ts, views/CatalogView/cron-builder.test.ts, types tests. Co-locate new
  unit tests next to the module.
- Playwright: `playwright.config.ts` — testDir ./tests, timeout 120s, workers 1, retries 2 on CI,
  html reporter, **no webServer block** (core :63578 + a Vite server must already be running).
  Specs: `tests/web-flow.spec.ts` (BASE_URL env TEST_URL default http://localhost:8080; API_URL default
  :63578) and `tests/canvas-overlays.spec.ts`. `make test_e2e` orchestrates (core + preview :4173).
- **Desktop E2E gap (verified TODO(F) at top of web-flow.spec.ts):** the Electron suites
  (app.spec.ts, complex-flow.spec.ts — startup, window lifecycle) were deleted in the Tauri migration and
  not replaced; sidecar startup/port allocation/shutdown is untested; wanted: tauri-driver smoke test that
  quits both ways and asserts no core/worker/viz processes survive.

---

## 5. flowfile_wasm (browser-only Pyodide build)

npm package `flowfile-editor` v0.1.0. Vue 3 + VueFlow SPA/library where Polars runs in WebAssembly.
**No backend at all** — no axios; execution is client-side Pyodide.

### 5.1 Architecture (verified against src)

- `src/stores/pyodide-store.ts`: injects CDN script
  `https://cdn.jsdelivr.net/pyodide/v0.27.7/full/pyodide.js` (:42-44, indexURL same v0.27.7) —
  **pinned: last Pyodide release with Polars support** (Polars 1.18.0 in-browser). Loads packages
  `['polars','pydantic']`. Exposes `runPythonWithResult` (:114, deep-converts Maps→objects),
  `runPythonGetBytes` (:214, for staged output binaries via `PyProxy.getBuffer('u8')`),
  `setGlobal` (:238). Writes the engine package into Pyodide's virtual FS
  (`import.meta.glob` of `src/pyodide/engine/`) and **dumps every submodule's non-dunder names into
  Pyodide globals** (flat-namespace contract — the JS bridge references engine internals like
  `_lazyframes` and imported modules like `gc`, so `from engine import *` is not enough).
- `src/pyodide/engine/`: real Python package — state.py, dtypes.py, errors.py, log.py, preview.py,
  validation.py, schema_propagation.py, nodes_io.py, nodes_transform.py, nodes_combine.py,
  nodes_aggregate.py, nodes_formula.py, nodes_explore.py, nodes_polars_code.py, `__init__.py`
  re-exports. Same files run in browser, under pytest (`tests/python/`, CPython pinned to Polars
  1.18.0), and in the real-Pyodide smoke test.
- `src/stores/flow-store.ts` (~2700+ lines): DAG state, `getExecutionOrder()` topological sort,
  `executeNode`/`executeNodeWithUpstream`/`executeFlow` dispatching `execute_<type>(...)` bridge strings,
  `toPythonJson()` (:141) — settings cross as `json.loads(${toPythonJson(node.settings)})`; binary crosses
  via `setGlobal('_temp_bytes', Uint8Array)` + `.to_py()` in the bridge string (e.g. :2019
  `execute_read_excel(${nodeId}, _temp_bytes.to_py(), ...)`); IndexedDB persistence orchestration;
  legacy `preview` node type migrated to `explore_data` on load.
- Other stores: `file-storage.ts` (sessionStorage <5MB / IndexedDB ≥5MB hybrid),
  `schema-inference.ts` (pure-TS schema prediction; returns null for polars_code/formula/pivot/
  join-without-right-schema → triggers lazy execution).
- `src/components/Canvas.vue`: palette + `getSettingsComponent` map (:1108) + VueFlow canvas.
  Node settings panels live flat in `src/components/nodes/*Settings.vue` (unlike the main app's
  per-dir convention) and are mapped **explicitly** in getSettingsComponent.
- Library surface: `src/lib/index.ts` (exports FlowfileEditor + FlowfileEditorPlugin),
  `FlowfileEditor.vue` (template-ref API: executeFlow, executeNode, exportFlow, importFlow,
  setInputData, getNodeResult, getNodeResultArrow, clearFlow, initializePyodide; events
  ready/output/execution-complete/error), `types.ts` (props: initialFlow, inputData (string |
  {content, format 'csv'|'json'|'arrow-ipc'|'parquet', delimiter, hasHeaders}), pyodide config
  (autoInit, pyodideUrl), theme, toolbar toggles, nodeCategories filter, readonly, height/width).

### 5.2 Node support — the actual list

Root CLAUDE.md's "lightweight, 16 nodes" is **STALE**. Verified current state:

- `NODE_TYPES` (`src/types/index.ts:606-639`) has 22 keys: read, manual_input, external_data,
  read_from_catalog | filter, select, sort, group_by, unique, formula, record_id, dynamic_rename,
  sample | pivot, unpivot | join, cross_join, union | explore_data, output, external_output,
  write_to_catalog. (Palette/executor use `head` for Take Sample; `sample` remains in NODE_TYPES;
  `preview` is legacy → migrated to explore_data on load.)
- Runnable palette (`Canvas.vue:573+ nodeCategories`, 6 categories) = **23 runnable types**:
  - Input Sources: read, manual_input, external_data, read_from_catalog
  - Transformations: filter, select, formula, sort, polars_code, unique, dynamic_rename, record_id, head
  - Combine: join, cross_join, union
  - Aggregations: group_by, pivot, unpivot
  - Output: explore_data, output, write_to_catalog, external_output
  - (Machine Learning category exists but ALL its nodes are locked)
- Plus **locked placeholders** (`available: false`, greyed out, link to docs; hidden from default browse
  unless searched or toggled): database_reader, cloud_storage_reader, rest_api_reader, kafka_source,
  google_analytics_reader, window_functions, sql_query, python_script, fuzzy_match, graph_solver,
  train_model, apply_model, evaluate_model, database_writer, cloud_storage_writer.
- `getSettingsComponent` map (Canvas.vue:1108-1135) has exactly the 23 runnable types.
- `src/config/nodeDescriptions.ts` — 23 keys (per wasm CLAUDE.md; not re-counted).

### 5.3 Add-a-node checklist (WASM) — from wasm CLAUDE.md:31, cross-checked against source

Touch: `nodeCategories` + `getSettingsComponent` in Canvas.vue; `nodeDescriptions.ts`; a
`src/components/nodes/<X>Settings.vue`; `NODE_TYPES`/types in `src/types/index.ts`; an
`execute_<type>` fn in the right `src/pyodide/engine/nodes_*.py` (re-exported via `__init__.py
__all__`); a `case` in `executeNode` (flow-store.ts); usually `useCodeGeneration.ts` +
`schema-inference.ts`.

### 5.4 Hard rules (wasm CLAUDE.md, load-bearing)

- **Execution is explicit-only**: only Run flow (toolbar/Ctrl+E/lib `run`), Run Now (context menu),
  Apply (settings), Fetch data (table button) may execute. Select/open/click/drop/paste must never run
  data. `fetchNodePreview` is preview-only (gated on `result.success`, never reaches `execute_*`).
  Regression guard: `tests/unit/no-auto-run.test.ts`.
- **No `.collect()` unless required** — only output, pivot, explore_data, polars_code materialize.
- Pyodide needs SharedArrayBuffer → page must send COOP/COEP headers; dev server sets them in
  vite.config.ts:20-23 (`Cross-Origin-Opener-Policy: same-origin`,
  `Cross-Origin-Embedder-Policy: require-corp`); embedders must set them or Pyodide fails to load.
- Parquet never reaches Python as parquet (wasm polars wheel has parquet compiled out, IPC kept):
  JS `parquet-wasm@0.7.1` converts Parquet⇄Arrow IPC via a **bundler-opaque** dynamic import
  (`src/utils/parquet-bridge.ts`; literal https `import()` breaks webpack5/esbuild embedders —
  guarded by tests/unit/parquet-bridge.test.ts).
- Excel via micropip openpyxl==3.1.5 / XlsxWriter==3.2.0 lazily (pins must match
  tests/python/requirements.txt); polars 1.18 read_excel has no row-offset kwarg (start_row is a
  .slice()).
- `optimizeDeps` in vite.config.ts:48-51: `exclude: ['pyodide']`,
  `include: ['react','react-dom/client','@kanaries/graphic-walker','grid-layout-plus']` — load-bearing.
  `pyodide` is NOT an npm dep (CDN only).
- Lib build (vite.config.ts:25-44): BUILD_MODE=lib, entry src/lib/index.ts, format es only, rollup
  `external: ['vue','pinia']` (peer deps, pinia optional per package.json peerDeps `pinia ^2.0.0`,
  `vue ^3.3.0`), `assetsInlineLimit: 100000` (icons as data URIs), cssCodeSplit false →
  dist/flowfile-editor.js + style.css.

### 5.5 Divergences from the main frontend

| Aspect | Main frontend | WASM |
|---|---|---|
| Framework | Vue 3 + Pinia + VueFlow + Element Plus | Vue 3 + Pinia + VueFlow, NO Element Plus (own components) |
| Backend | flowfile_core HTTP (axios) | None — Pyodide in-browser |
| Node templates | Backend GET /node_list | Hardcoded `nodeCategories` ref in Canvas.vue |
| Settings components | Convention-glob `elements/<dir>/<Title>.vue` | Explicit map `getSettingsComponent` |
| Node settings save | POST /update_settings/ per node type | Local store state, executed via Python bridge |
| Persistence | Backend .flowfile files | sessionStorage/IndexedDB + save-file structurally aligned with core schemas (compat only, no runtime coupling) |
| Ports | 8080 dev / 4173 preview | 5174 dev |
| Tests | Vitest (node env) + Playwright | Vitest (happy-dom, fake-indexeddb) + CPython pytest for engine + real-Pyodide smoke (tests/pyodide-smoke/smoke.cjs) |
| React usage | dynamic import for Graphic Walker only | same (GW), react/react-dom pinned 19.2.0 in deps |

### 5.6 Build/publish flow

- Dev/CI: `npm run dev` (:5174), `build` (vue-tsc + vite build), `build:lib` (BUILD_MODE=lib),
  `build:all`, `test`/`test:run`/`test:coverage`.
- CI `flowfile-wasm-build.yml` jobs: `build` (build + build:lib + "Guard the published bundle against
  literal CDN imports" grep + tests), `python-engine-tests` (pinned deps, pytest), `pyodide-smoke`
  (installs Pyodide + parquet-wasm on demand, replays the JS→Python bridge against real Pyodide —
  the only guard for the flat-namespace contract; CPython tests can't catch a broken browser namespace).
- Publish: `npm-publish-wasm.yml` on tag `wasm-v*` (or dispatch): test job (npm ci, vue-tsc, test:run)
  → publish job (environment `npm`): verifies tag version == package.json version, `npm run build:lib`,
  `npm publish --provenance --access public` with NPM_TOKEN.

---

## 6. Web vs desktop mode differences (renderer)

| Concern | Web (vite dev / Docker nginx) | Desktop (Tauri) |
|---|---|---|
| API base | `<origin>/api/` (proxy strips /api, replicates 307 rewrite) | `http://127.0.0.1:<injected core port>/` direct |
| Detection | `isDesktop=false` (`window.__TAURI_INTERNALS__` absent) | true |
| Auth | full login flow; unauthenticated → /login | FLOWFILE_MODE=electron backend auto-issues tokens; no login redirect (main.ts checks `authService.isInDesktopMode()`) |
| Ports | fixed 63578 backend | scanned pair (63578+2k, +1), multiple app instances coexist |
| Clipboard read | navigator.clipboard (browser permission) | clipboard-manager plugin (no macOS pill) |
| External links / OAuth | window.open / location.assign | opener plugin (system browser) / modal oauth window |
| Updater | n/a | tauri-plugin-updater against GitHub latest.json |
| App version | `__APP_VERSION__` Vite define | `get_app_version` command |
| Zoom menu | n/a | native menu emits view:zoom events |
| /project routes, sharing routers | Docker mode gates (backend-side: 404 in electron for /user-groups, /shares) | electron mode: projects always on |

---

## 7. Historical incidents / stories (verified via git log & code comments)

1. **Graphic Walker 0.5.0 React pin (commit 21c3274d, 2026-04-25)**: GW 0.5.0 requires React 19 +
   styled-components 6. First attempt used caret ^19.2.0; npm resolved react 19.2.5 while GW 0.5.0
   inlines react-dom 19.2.0 and hard-throws `if (React.version !== "19.2.0")` → runtime
   "Incompatible React versions". Fix: pin react/react-dom to EXACT 19.2.0 in dependencies AND
   overrides. Applies to both flowfile_frontend and flowfile_wasm (both verified at 19.2.0 exact).
   WASM needed `--legacy-peer-deps` + explicit codemirror/styled-components adds (user memory).
2. **macOS Cmd+Q orphaned sidecars**: Cmd+Q/dock-quit surface as `RunEvent::Exit`, not `ExitRequested`;
   the shell must match both or sidecars leak (lib.rs:133-147 comment). Backstop added:
   FLOWFILE_SUPERVISOR_PID + shared/parent_watcher.py self-reap for hard-killed shells.
3. **Startup-failure self-restarting orphans**: readiness timeout used to leave live sidecars whose
   wait-tasks respawned them behind the error window (up to MAX_RESTARTS). Fix: `kill_spawned` (PID
   kill, no HTTP — ports may be unresponsive or owned by another instance) sets is_shutting_down first
   (lib.rs:88-99, shutdown.rs:49-77).
4. **Vite "504 Outdated Optimize Dep"**: webview cached import URLs with old hashes across dev
   restarts → `optimizeDeps.force: true` accepted ~5-15s cold-start cost (vite.config.mjs:72-79).
5. **Material Icons CDN blocked by CSP**: fonts.googleapis.com fetch blocked in Tauri (and broken
   offline) → bundle `material-icons` npm package locally (main.ts:10-14).
6. **WebKit drag preview invisible**: WKWebView/Safari render no default drag image for
   user-select:none elements → palette drags showed nothing in the desktop app; fixed by cloning the
   palette item as an explicit drag image (useDragAndDrop.ts:289-311).
7. **Tauri native drag-drop swallowed HTML5 drag events** (VueFlow palette drag, AG Grid column
   reorder) → `.disable_drag_drop_handler()` on the main window (lib.rs:176-181).
8. **macOS clipboard "Paste" pill**: WKWebView `navigator.clipboard.readText()` pops the native pill
   per read → route through clipboard-manager plugin (desktop.ts:124-131; user memory).
9. **307 silent failures in Docker**: axios path/route trailing-slash mismatch produced FastAPI 307s
   that failed only in Docker (Vite proxy + tests masked it); nginx.conf keeps default $proxy_host so
   proxy_redirect rewrites Location, and vite.config.mjs replicates that; the durable rule is
   "match the slash frontend-side" (user memory + nginx.conf/vite.config.mjs comments).
10. **Sidecar exec-bit stripped by bundler**: Tauri `bundle.resources` copy could drop the executable
    bit → EACCES on spawn; shell now chmods 755 at spawn (mod.rs:171-187).
11. **Electron→Tauri migration deleted desktop E2E**: tests/app.spec.ts & complex-flow.spec.ts gone;
    the regressed area (sidecar startup/shutdown) is untested — open TODO(F) in web-flow.spec.ts.
12. **Pyodide pin**: v0.27.7 is the last Pyodide with a Polars package; bumping breaks
    `loadPackage(['polars','pydantic'])` (pyodide-store.ts:42 comment).
13. **wasm "capacity overflow" panic**: exporting String/Binary columns whose buffers came through IPC
    import or the excel reader panics the wasm polars build; `_clean_strings_for_export` in nodes_io.py
    rebuilds those columns before IPC export (wasm CLAUDE.md:37; not re-read in source).

---

## 8. Open problems / TODO-shaped debt (all verified in-source)

- `constants.ts:36-40` TODO(H): dev-mode CORS under tauri dev unverified (localhost:8080 origin vs
  127.0.0.1 baseURL).
- `lib.rs:115-121` TODO(D): startup-phase exit race — quit during start_services can leave one
  unreaped spawn; needs is_shutting_down check before each spawn.
- `sidecar/mod.rs:317-318` TODO(C): pid not cleared when a sidecar terminates during shutdown →
  stale killpg on recycled PID possible.
- `tests/web-flow.spec.ts:1-9` TODO(F): no desktop-shell E2E (tauri-driver smoke test wanted).
- `useDragAndDrop.ts:3-9` TODO(refactor): 867-LOC composable doing 5 jobs; split plan documented.
- `Canvas.vue:2-6` TODO(refactor): ~1170 LOC, 7+ concerns; extraction plan documented.
- Dead export: `composables/useNodes.ts getComponent` globs a nonexistent dir
  (`features/designer/nodes/elements/`) — latent trap, no live callers.
- Root CLAUDE.md says wasm has "16 nodes" — stale; actual 23 runnable palette types (+15 locked
  placeholders). wasm's own CLAUDE.md says "23 types across 5 categories" — also slightly stale on
  category count: Canvas.vue now has 6 categories (Machine Learning added, all locked).
- Legacy shims kept for migration: `views/DesignerView/useNodes.ts`, `useDnD.ts` ("DEPRECATED: Import
  from '@/composables'"), `stores/column-store.ts`, node-store's deprecated proxy getters.
- flowfile_frontend version (0.12.7 in package.json + tauri.conf.json) differs from root project
  version 0.11.0 (root CLAUDE.md) — desktop shell versions independently.

---

## 9. Verified commands

```bash
# from flowfile_frontend/ (npm install first)
npm run dev:web          # Vite dev server :8080 strictPort; needs `poetry run flowfile_core` for /api
npm run dev              # tauri dev (runs dev:web as beforeDevCommand; needs staged sidecar binaries:
                         #   make build_python_services && make rename_sidecars   from repo root)
npm run lint             # eslint --fix ./src/**/*.{ts,vue}
npm run build:web        # lint + vue-tsc --noEmit + vite build -> build/renderer/
npm run test:unit        # vitest run (src/**/*.test.ts only)
npm run test:web         # playwright tests/web-flow.spec.ts (core + web server must already run)
npm run preview:web      # vite preview :4173

# from flowfile_wasm/
npm run dev              # :5174 with COOP/COEP headers
npm run build            # vue-tsc --noEmit && vite build
npm run build:lib        # BUILD_MODE=lib publishable flowfile-editor ES lib
npm run test:run         # vitest one-shot (happy-dom)

# repo root
make test_e2e            # build:web, start core + preview :4173, run web-flow.spec.ts
```

(Static verification only in this session — commands' definitions read from package.json/Makefile;
dev servers not booted.)

## 10. What the skill library must capture (summary for authors)

1. Node-UI-by-convention: `elements/<camelCase(item)>/<TitleCase(item)>.vue`, dual globs
   (GenericNode.vue + useDragAndDrop.ts), no build-time error on mismatch — full add-a-node checklist
   including backend node_store template + icon + useNodeSettings/defineExpose contract.
2. Store map and the signal-counter pattern (pendingReloadCounter vs graphVersion semantics —
   watching graphVersion for re-fetch loops).
3. desktop.ts as the sole Tauri boundary + command registration recipe (Rust generate_handler! +
   capabilities/main.json + desktop.ts wrapper + web fallback).
4. Axios: absolute baseURL requirement, trailing-slash === FastAPI decorator, auth interceptors,
   `X-Skip-Auth-Header`.
5. Sidecar lifecycle: port-pair scan math, /docs readiness (120s), shutdown ladder
   (POST /shutdown 3s → wait 2s → killpg SIGTERM, 5s poll → SIGKILL), restart budget 5/60s,
   FLOWFILE_MODE=electron + FLOWFILE_SUPERVISOR_PID env contract.
6. WKWebView gotchas: clipboard pill, drag image, disable_drag_drop_handler, CSP (no external CDNs).
7. WASM: explicit-run-only rule, flat-namespace bridge + pyodide-smoke guard, Pyodide 0.27.7 pin,
   COOP/COEP, parquet-as-IPC, exact node list (23 runnable), lib externals vue/pinia, wasm-v* publish flow.
8. React is pinned exactly 19.2.0 for Graphic Walker 0.5.0 in BOTH frontends — never relax to caret.
