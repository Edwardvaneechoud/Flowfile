---
name: flowfile-frontend-conventions
description: Vue 3 renderer + Tauri 2 shell conventions for flowfile_frontend and flowfile_wasm — path aliases, Pinia store map, the axios trailing-slash 307 trap, the node-settings-by-glob-convention resolution system, VueFlow canvas wiring, desktop.ts as the sole Tauri boundary, the sidecar boot/readiness/shutdown ladder, the 19-file god-component TODO(refactor) policy, and WASM's explicit-run-only rule. Use when adding or editing a Vue component/view/store/route in flowfile_frontend, building a new node's settings UI, touching axios API wrappers or seeing unexplained 307s, changing anything under src-tauri/ (sidecar, lifecycle, capabilities), calling a Tauri/native API from renderer code, working in flowfile_wasm, or wiring the ShareDialog sharing UI onto a new connection view.
---

# Flowfile frontend conventions

This skill covers `flowfile_frontend/` (Vue 3 renderer + Tauri 2 desktop shell)
and `flowfile_wasm/` (Pyodide browser-only build). It is about *how the
frontend is put together and where its landmines are* — not about the backend
it talks to.

## When NOT to use this skill

- Backend/core/worker/kernel contracts, secrets format, worker offload wire
  protocol → `flowfile-architecture-contract`.
- Adding a new node *type* end-to-end (backend template, settings schema,
  `add_<type>` method) → `flowfile-node-development` (this skill only covers
  the **frontend half**: where the settings `.vue` file goes and how it's
  loaded).
- `flowfile_frame` Python API / codegen → `flowfile-frame-and-codegen`.
- AI subsystem internals (agents, providers, BYOK) → `flowfile-ai-subsystem`.
- Running/building the whole stack, Docker Compose, ports → `flowfile-build-and-env` / `flowfile-run-and-operate`.
- Env vars and feature flags in detail → `flowfile-config-and-flags`.
- Writing/running Vitest or Playwright tests → `flowfile-testing-and-validation`.
- Diagnosing a live symptom step-by-step → `flowfile-debugging-playbook`.
- Known historical bugs as a searchable archive → `flowfile-failure-archaeology`.
- Change-control process (drift gates, review, release tags) → `flowfile-change-control` — nothing here overrides it.

---

## 1. Renderer layout & path aliases

Vite's `root` is `flowfile_frontend/src/renderer/` (not the package root),
entry `index.html` (`vite.config.mjs:22`). Inside that root:

```
src/renderer/
  main.ts             # bootstrap: createApp, Pinia, router, i18n, Element Plus,
                       # theme init, then setupService -> authService -> auth init -> app.mount("#app")
  config/constants.ts    # axios baseURL + GA OAuth callback URL resolution
  config/environment.ts  # ENV flags derived from NODE_ENV
  lib/desktop.ts       # THE ONLY renderer<->Tauri bridge (see §6)
  app/                 # '@' alias target — all feature code lives here
    App.vue, api/, components/, composables/, features/, layouts/,
    pages/, router/, services/, stores/, types/, utils/, views/
```

**Path aliases exist in three files and must stay in sync** — adding or
renaming one in only one file silently breaks the dev server, the
TypeScript checker, or Vitest (whichever file you forgot):

| File | Aliases defined |
|---|---|
| `vite.config.mjs` (~:83-91) | `@` → `src/renderer/app`, plus `@/api`, `@/types`, `@/stores`, `@/composables` |
| `tsconfig.json` | `@/*` glob, plus the same four named ones with `/*` variants |
| `vitest.config.ts` | only the bare `@` |

Router: `app/router/index.ts` uses `createWebHashHistory` (URLs look like
`#/main/designer`). Routes are lazy `import()`ed; most live under the
`/main` `AppLayout` parent, which carries `meta.requiresAuth: true` by
default. `/setup` and `/login` opt out.

---

## 2. Pinia stores — inventory and the signal-counter pattern

Files live in `app/stores/`, kebab-case `xxx-store.ts` (one legacy exception:
`fileBrowserStore.ts`). `index.ts` is the Pinia plugin.

| Store (id) | Role |
|---|---|
| `flow` | flowId (persisted in `sessionStorage['last_flow_id']`), the live `vueFlowInstance`, undo/redo `historyState`, and two **monotonic signal counters** (see below). |
| `node` | current nodeId, `nodeData` cache, `updateSettings` (the POST `/update_settings/` path — see §4), many `@deprecated` proxy getters to flow/editor/results for legacy callers. |
| `editor` | drawer open/active-component state, shared code-editor buffer, `graphVersion` (dirty-state counter), plus "request token" signals like `nodeSettingsOpenRequest {nodeId, token}`. |
| `results` | run results per flow/node, `resultVersion`. |
| `auth`, `theme` | user/session; light/dark/system (localStorage `flowfile-theme-preference`). |
| `column` | **deprecated shim** — re-exports `useNodeStore`; `Canvas.vue` still imports it under this name for back-compat. Don't add new state here. |
| Feature stores | `fileBrowserStore`, `global-store`, `project-store`, `catalog-store` (829 LOC), `dashboards-store`, `sharing-store`, `tutorial-store`, `notebook-store`. |
| AI stores | `ai-store` (1666 LOC), `ai-agent-store`, `ai-diff-store`, `ai-command-palette-store`, `ai-ghost-node-store`, `ai-autocomplete-store`, `ai-code-generator-store` (+ `*-persistence.ts` siblings). These have colocated `*.test.ts` — Vitest picks up `src/**/*.test.ts` only. |

**The signal-counter pattern** (flow-store): `pendingReloadCounter` +
`requestReload()` means "the backend mutated the flow behind your back,
re-fetch it" — `Canvas.vue` watches the counter, not a boolean, so that *two
reload requests in the same tick* still both fire (a boolean flag toggled
twice collapses to a no-op watch). `pendingLayoutResetCounter` /
`requestLayoutReset()` is the same idea for re-running auto-layout. Use this
pattern for any new "something changed, someone downstream should react"
signal — don't reach for a boolean.

`editorStore.graphVersion` is different: bumped by `nodeStore.updateSettings`
after every successful save, it represents dirty state ("something on the
canvas changed"), not "reload from the network."

---

## 3. Axios conventions — and the trailing-slash 307 trap

`app/services/axios.config.ts` sets `axios.defaults.baseURL` and
`withCredentials = true`. A request interceptor injects `Authorization:
Bearer <token>` unless the request carries header `X-Skip-Auth-Header`; a
response interceptor retries once on 401 after a token refresh, else calls
`authService.logout()`.

BaseURL resolution (`config/constants.ts`):

```ts
export const flowfileCorebaseURL = isDesktop
  ? `http://127.0.0.1:${resolveCorePort()}/`
  : `${window.location.origin}/api/`;
```

Desktop's port comes from `window.__FLOWFILE_PORTS__`, injected by the Rust
shell before any renderer script runs (see §7). The base **must be
absolute** — the AI streaming clients do `new URL(path, base)`, which throws
on a relative base.

### The rule, stated once because it costs real debugging time

**A frontend axios call path must match the FastAPI route string exactly,
trailing slash included.** FastAPI issues an **absolute** 307 redirect when
the slash doesn't match. Verified pairs in this codebase:

- `node.api.ts` posts `/update_settings/` ↔ `routes.py:1146 @router.post("/update_settings/")`
- `node.api.ts` posts `/node/description/` ↔ `routes.py:1361 @router.post("/node/description/")`
- `node.api.ts` posts `/node/reference/` ↔ `routes.py:1396 @router.post("/node/reference/")`

**Why this bites in production and nowhere else:** two proxy layers happen
to paper over a mismatch during development, so the bug reaches Docker
before anyone notices. `vite.config.mjs`'s `/api` proxy strips the `/api`
prefix and its `configure(proxy)` hook rewrites the backend's `Location`
header back to `/api/...` (replicating nginx's default `proxy_redirect`);
`nginx.conf` (Docker) proxies `/api/` → core **without** overriding `Host`,
so nginx's own default `proxy_redirect` also rewrites the Location back to
the external URL; and pytest's `TestClient` follows redirects transparently
too. The upshot: a slash mismatch works in `npm run dev:web`, works under
pytest, and **only breaks in a real Docker deployment**. **Fix it
frontend-side** — make the axios path match the decorator, not the other way
around. **Verify** via `docker compose logs flowfile-core | grep " 307 "`
after exercising the endpoint — a 307 there on a route you expected to be a
200/201/422 is this bug.

API wrapper files: `app/api/*.api.ts` (one per resource: `node`, `flow`,
`catalog`, `secrets`, `shares`, `userGroups`, …) are static-method classes
importing the configured axios instance from `../services/axios.config`.
`app/services/` additionally holds `auth.service.ts`, `setup.service.ts`,
`user.service.ts`, and the SSE clients `aiStreamClient.ts` / `aiDiffClient.ts`.

---

## 4. Node UI system — resolved by naming convention, not a registry

**There is no static node-component registry.** Settings components are
found by string-interpolating a path and globbing it with
`import.meta.glob` — fast to extend, zero build-time safety net if you get
a name wrong.

Node templates come from the backend (`GET /node_list`); the frontend's
`NodeTemplate` type is `app/types/flow.types.ts`. `app/composables/useNodes.ts`
fetches and caches them once per session; the palette filters `prod_ready`
nodes when `environment.ts` says production.

**Two independent globs point at the same component tree and must stay in
sync** if you ever restructure the directory:

1. `app/composables/useDragAndDrop.ts` — `import.meta.glob("../components/nodes/node-types/elements/**/*.vue")`. `getComponent()` builds the path
   `elements/${camelCase(item)}/${TitleCase(item)}.vue` and resolves the
   component rendered **inside the VueFlow node itself**.
2. `app/components/nodes/GenericNode.vue` — same glob pattern, own relative
   path, resolves the component for the **settings drawer**
   (`defineAsyncComponent`, 3000ms timeout, retries ≤3, logs `console.error`
   on a missing path — this is a runtime failure, not a build error).

There are 46 element directories / 87 `.vue` files under
`app/components/nodes/node-types/elements/` today (count drifts as nodes are
added — re-run the command in Provenance to check). `getComponents.ts`
(singular `get`, no `s` on `Nodes`) is unrelated — it only lazy-loads
`elements/manualInput/*.vue` editor cells for that one node's table editor.

**Trap:** `composables/useNodes.ts` also exports a `getComponent` that globs
`../features/designer/nodes/elements/**/*.vue` — **that directory does not
exist**. It has no live callers today but will throw "Component not found"
if anyone imports it. Use the `useDragAndDrop` loader or the `GenericNode`
path, never this one.

### Add-a-node checklist (frontend half only)

1. Backend side (template, settings schema, `add_<type>`) is
   `flowfile-node-development`'s job — do that first.
2. Drop the icon `<item>.svg` into `app/features/designer/assets/icons/`.
3. Create the settings component at **exactly**
   `app/components/nodes/node-types/elements/<camelCase(item)>/<TitleCase(item)>.vue`
   — TitleCase capitalizes each `_`-separated word and joins, e.g.
   `text_to_rows` → `textToRows/TextToRows.vue`. A wrong directory/filename
   fails **silently at runtime**, not at build time.
4. Follow the `Filter.vue` pattern: wrap content in
   `<generic-node-settings v-model="..." @request-save="saveSettings">`,
   destructure `useNodeSettings({ nodeRef, onBeforeSave })` for
   `saveSettings`/`pushNodeData`/`handleGenericSettingsUpdate`, implement
   `loadNodeData(nodeId)` hydrating from `nodeStore.getNodeData`, and end
   with `defineExpose({ loadNodeData, pushNodeData, saveSettings })` — the
   drawer host (`NodeSettingsDrawer.vue`) calls these two names by contract.
5. No registration file to edit — both globs above pick it up automatically
   since they point at the same tree.
6. `prod_ready: false` on the backend template hides it from the
   production palette but keeps it loadable in already-saved flows (the
   full list is cached; the filter is applied per-consumer, not server-side).

---

## 5. VueFlow canvas integration

`@vue-flow/core` (^1.42) + `@vue-flow/minimap`. `Canvas.vue` (see §8 — it's
one of the god components) owns the `useVueFlow()` instance and stores it in
`flow-store` so other components/stores can `findNode`/mutate handles
directly (e.g. a node component rewriting its own output handles for a
split-output mode).

- Node ids on the canvas are the backend's numeric node ids, as strings.
  `data` carries `{id, label, component (markRaw), inputs/outputs
  (NodeHandle[]), nodeTemplate}`.
- Handle counts come from `utils/nodeHandles.ts deriveHandles`, driven by
  template input/output counts plus `dynamic_inputs`/`output_names`.
  `multi: true` templates (union, `polars_code`) render **one** input handle
  that accepts many edges, not N handles.
- Groups are VueFlow `type: "group"` nodes (`composables/useNodeGroups.ts`);
  collapsing swaps real edges for synthetic "proxy" edges. VueFlow has no
  `@pane-dblclick` event — `Canvas.vue` listens for the native DOM event
  directly instead.
- The right-hand drawer (Settings/Results/Code tabs) is a **declarative
  registry**, `views/DesignerView/drawerRegistry.ts` — its own comment
  calls it "single source of truth... adding/moving a view is a one-entry
  edit here." Add new tabs there, not by hand-wiring a drawer component.
  The Code tab defers CodeMirror instantiation until visible — it breaks if
  constructed while hidden.

---

## 6. `lib/desktop.ts` — the sole Tauri boundary

**Rule (enforced by convention, not lint): view/component code must never
import `@tauri-apps/*` directly.** Every native capability is wrapped in
`src/renderer/lib/desktop.ts` with an explicit web-mode fallback, gated on
`isDesktop = typeof window !== "undefined" && !!window.__TAURI_INTERNALS__`.

| Method | Desktop | Web fallback |
|---|---|---|
| `getAppVersion()` | Tauri `app.getVersion()` / invoke `get_app_version` | `""` |
| `quitApp()` | invoke `quit_app` (Rust: graceful shutdown then exit) | no-op |
| `openOauth(url)` | invoke `open_oauth` → modal Tauri webview, resolves captured `code` | `window.location.assign(url)` |
| `openExternal(url)` | `invoke("plugin:opener\|open_url")` — system browser | `window.open(url, "_blank", "noopener")` |
| `readClipboardText()` | dynamic `import("@tauri-apps/plugin-clipboard-manager").readText()` | `navigator.clipboard.readText()` |
| `onViewZoom(h)` | listen `view:zoom` event from the native menu | no-op |

**Never call `navigator.clipboard.readText()` directly in renderer code.**
In WKWebView (Tauri's macOS webview), that call pops the native macOS
"Paste" confirmation pill **on every programmatic read**. Always go through
`desktop.readClipboardText()`, which uses the clipboard-manager plugin
(NSPasteboard access) and never triggers it. This is why canvas paste
(`useDragAndDrop.ts`'s `createManualInputFromClipboard`) routes through
`desktop.ts` instead of calling the browser API. The general principle
behind this whole module: **privileged operations belong on the native
(Rust) side of the boundary, invoked through one narrow, auditable seam** —
not sprinkled through view code as direct browser/Tauri API calls.

**To add a new native capability:** add the Rust command to `commands.rs`
and list it in `lib.rs`'s `tauri::generate_handler![...]` (current exact
set: `get_services_status`, `get_service_ports`, `get_app_version`,
`quit_app`, `app_refresh`, `open_oauth`) — or, for a *plugin* command
(opener, clipboard-manager) instead of a custom Rust command, add its
permission string to `src-tauri/capabilities/main.json`'s `"permissions"`
array (no Rust code needed, but the invoke silently fails without the
grant). Either way, wrap it in `desktop.ts` with a web-mode fallback; never
call `window.__TAURI__.*` or `@tauri-apps/*` from anywhere else.

---

## 7. Tauri shell summary — and why it's the riskiest surface in this package

Modules under `src-tauri/src/`: `lib.rs` (entry/lifecycle),
`sidecar/{mod.rs, readiness.rs, shutdown.rs}`, `commands.rs`, `menu.rs`,
`oauth.rs`, `state.rs`, `env.rs`, `window.rs`.

**Boot, in order:** plugins register (log, opener, process, os,
clipboard-manager, window-state, updater) → `setup` emits
`services-status {status:"starting"}` and `sidecar::start_services` spawns
core+worker on a **scanned free port pair** (`core = 63578 + k*2`,
`worker = core + 1`, `k` in `0..100`; binaries resolve from
`src-tauri/binaries/` in dev or `<resource_dir>/binaries/` in release,
staged there by `make services`; each process gets
`FLOWFILE_MODE=electron` + `FLOWFILE_SUPERVISOR_PID=<shell pid>` injected,
the latter letting `shared/parent_watcher.py` self-reap if the shell itself
is SIGKILLed) → readiness polls `GET http://127.0.0.1:<port>/docs` every 1s
up to a **120s** deadline (core and worker awaited concurrently; cold
onedir bundles + first-launch AV scanning can be slow) → on success the
**main window is created programmatically in Rust** (not via
`tauri.conf.json`'s static window list) so an `initialization_script` can
inject `window.__FLOWFILE_PORTS__ = Object.freeze({core, worker})`
**before any renderer script runs** — the only place that value comes
from. On readiness failure, `kill_spawned` does a **PID-only kill, no
HTTP** — a failed readiness often means the port itself is unresponsive,
and in the `NoFreePortPair` edge case the recorded ports might belong to a
*different* running Flowfile instance, so POSTing `/shutdown` there could
kill someone else's process.

**Shutdown ladder** (`sidecar/shutdown.rs`, best-effort + idempotent): POST
`http://127.0.0.1:<port>/shutdown` to core AND worker in parallel (3s
timeout each) → sleep 2s for natural exit → Unix `killpg(SIGTERM)` on the
whole process group (a bare `kill` would orphan the worker's
multiprocessing children) / Windows `taskkill /T /PID` → poll liveness
every 100ms up to 5s → still alive → `killpg(SIGKILL)` / `taskkill /F /T`.

**macOS Cmd+Q / dock-quit is a distinct code path from closing the
window** — it surfaces as tao's `RunEvent::Exit`, not
`RunEvent::ExitRequested`. The shell's run loop matches **both** arms; if a
future edit only handles one, the other quit path leaks sidecars silently
(this has happened before).

### This is among the riskiest edit surfaces in the whole repo

`src-tauri/src/sidecar/*` has **zero desktop end-to-end test coverage** —
the old Electron-era `app.spec.ts`/`complex-flow.spec.ts` suites were
deleted in the Tauri migration and never replaced (open TODO at the top of
`tests/web-flow.spec.ts`). Two known races are documented in-source as
`TODO` comments, not fixed: (1) quit-during-startup — the app can quit
while `start_services` is still spawning, shutdown running before a
sidecar's PID is recorded, so that spawn is never reaped; (2)
pid-not-cleared-during-shutdown — a sidecar that terminates *during*
shutdown can leave a stale PID a later `killpg` could target after the OS
recycles it onto an unrelated process.

**Consequence:** editing anything under `sidecar/`, `lib.rs`'s lifecycle
handlers, or `env.rs` has no automated test to catch a regression.
**Manually verify both quit paths** after any change — launch, quit via
Cmd+Q (or app menu/dock); relaunch, quit via the window close button; after
**each**, confirm nothing survived:
```bash
ps aux | grep -i flowfile | grep -v grep   # expect: no flowfile_core-*/flowfile_worker-* rows
```

---

## 8. God-component policy — 19 files, pre-written extraction plans

19 files in this package (as of 2026-07-03) carry a `TODO(refactor)` header
comment with a **pre-written extraction plan** (what to pull out, into
what, at roughly which lines) — among them `views/DesignerView/Canvas.vue`,
`composables/useDragAndDrop.ts`, `views/CatalogView/CatalogView.vue`
(~1700 LOC), `views/AdminView/AdminView.vue`,
`components/common/DraggableItem/DraggableItem.vue`, and four
`node-types/elements/*` settings components (`databaseReader`,
`databaseWriter`, `googleAnalyticsReader`, `pythonScript`). Get the full
current list with the grep in Provenance.

Example — `Canvas.vue`'s header:
```ts
// TODO(refactor): ~1170 LOC; bundles 7+ concerns. Plan to extract:
//   - 6 draggable panel wrappers (~lines 927-1021) → individual *Panel components
//   - clipboard/copy-paste logic (~lines 533-700) → useFlowClipboard composable
//   - context menu handling (~lines 702-794) → useContextMenu composable
//   - keyboard shortcuts (~lines 729-778) → useFlowHotkeys composable
```

**The rule when you touch one of these files:**
- Doing a substantial edit near a documented seam? **Follow the plan
  already at the top of the file** — it was written by someone who read the
  whole thing; extract into the named composable/component it specifies.
- **Never invent your own decomposition** that diverges from the written
  plan without discussing it — two different extraction shapes for the same
  file compound the mess instead of fixing it.
- **Never leave behind a deprecated shim path** once you do extract — this
  codebase already carries intentional back-compat shims
  (`views/DesignerView/useNodes.ts`, `useDnD.ts` marked "DEPRECATED: Import
  from '@/composables'", `stores/column-store.ts`) from past refactors;
  don't add another one you don't need to.
- A small, surgical fix that doesn't touch the plan's seams does **not**
  require doing the whole refactor — the header is a map for when someone
  eventually does the extraction, not a blocking requirement on every edit.

---

## 9. Web vs. desktop mode differences

| Concern | Web (vite dev / Docker nginx) | Desktop (Tauri) |
|---|---|---|
| API base | `<origin>/api/` (proxied, replicates 307 rewrite) | `http://127.0.0.1:<injected port>/` direct |
| Detection | `isDesktop` false | `window.__TAURI_INTERNALS__` present |
| Auth | full login flow, unauth → `/login` | `FLOWFILE_MODE=electron` auto-issues tokens, no login redirect |
| Ports | fixed 63578 | scanned pair; multiple app instances can coexist |
| Clipboard read | `navigator.clipboard` (browser permission prompt) | clipboard-manager plugin (no macOS pill) |
| External links / OAuth | `window.open` / `location.assign` | opener plugin (system browser) / modal oauth window |
| App version | Vite `__APP_VERSION__` define | `get_app_version` command |
| `/project`, `/user-groups`, `/shares` routers | gated by `FLOWFILE_MODE`/`FLOWFILE_ENABLE_PROJECTS` server-side (404 in electron for the sharing routers) | projects always on in electron; sharing routers still 404 |

---

## 10. `flowfile_wasm` hard rules

Separate npm package `flowfile-editor`, pure Vue 3 + VueFlow + Pyodide-in-
browser — **no backend, no axios; nothing in §1-§9 above applies here.**

- **Execution is explicit-only.** Only four actions may run data: Run flow
  (toolbar/Ctrl+E/lib `run()`), Run Now (node context menu), Apply
  (settings drawer), Fetch data (table preview button). Selecting, opening
  a panel, clicking, dragging, dropping, or pasting a node must **never**
  trigger an `execute_*` Python bridge call — regression-tested by
  `tests/unit/no-auto-run.test.ts`. Adding a new node-touching affordance?
  Ask "does this run data?" before wiring its handler.
- **Node count:** root `CLAUDE.md`'s "lightweight, 16 nodes" is **stale** —
  as of 2026-07-03 (v0.12.7) there are 23 runnable node types across 6
  palette categories (Machine Learning exists but every node in it is
  locked/greyed-out), plus 15 locked placeholder types visible only via
  search. `flowfile_wasm`'s own `CLAUDE.md` still says "5 categories" —
  also stale; re-verify both before quoting a number.
- **Pyodide is pinned to v0.27.7** (CDN script, not an npm dependency) —
  the last release shipping a Polars wheel. Bumping it breaks
  `loadPackage(['polars', 'pydantic'])`.
- Pyodide needs `SharedArrayBuffer`, gated behind COOP/COEP response
  headers (`Cross-Origin-Opener-Policy: same-origin`,
  `Cross-Origin-Embedder-Policy: require-corp`); the dev server sets these
  and **any embedder hosting this library must set them too** or Pyodide
  fails to load.
- Parquet never reaches Python as Parquet — the wasm Polars build has
  Parquet compiled out (Arrow IPC only). `parquet-wasm` converts
  Parquet↔IPC via a deliberately bundler-opaque dynamic import so
  webpack5/esbuild don't try to statically resolve a literal `https://` URL.
- The engine Python package (`src/pyodide/engine/`) runs identically under
  CPython pytest, real Pyodide, and the browser — new node logic must
  work under both. Add the `execute_<type>` case to `flow-store.ts`'s
  `executeNode` dispatcher plus an entry in `Canvas.vue`'s
  `getSettingsComponent` map (WASM uses an **explicit** map, unlike the
  main app's glob convention in §4).

---

## 11. Three traps worth knowing by name before you hit them

### StatsPanel duplicates RunOverviewPanel/ScheduleOverviewPanel — forward emits through all three hops

`CatalogView.vue`'s default "overview" tab renders `StatsPanel.vue`, which
mounts its **own, separate copies** of `RunOverviewPanel` and
`ScheduleOverviewPanel` — pixel-identical to the dedicated Runs/Schedules
tabs `CatalogView.vue` also mounts directly. An event from the inner panel
(`@view-run`, `@open-snapshot`, `@toggle-schedule`, …) only reaches
`CatalogView`'s handler if forwarded at **every hop**: panel → `StatsPanel`
(`@x="$emit('x', $event)"` + a matching `defineEmits` entry) →
`CatalogView`'s binding on the `<StatsPanel>` tag. Miss one hop and the
click **dies silently on the stats-tab mount only** — the identical click on
the dedicated Runs/Schedules tab keeps working, which is what makes this
bug confusing to reproduce. Adding a new emit to `RunOverviewPanel` or
`ScheduleOverviewPanel`? Grep both `StatsPanel.vue` and `CatalogView.vue`
and wire all three places, not just the one you're staring at.

### el-dialog's `@open` does not fire for a dialog mounted already-open

If a parent does `v-if="show"` and sets `show = true` in the same code path
(dialog is created *already open*, not opened after mount), Element Plus's
`el-dialog` `@open` handler **never fires** for that first appearance — `@open`
only fires on a *transition* from closed to open on an already-mounted
dialog. `components/sharing/ShareDialog.vue` hit this and fixed it with a
`modelValue` watcher instead:
```ts
// Parents mount this dialog with v-if AND open it in the same tick, so
// el-dialog's @open never fires on that initial already-open mount. Load
// on modelValue instead (immediate covers the mount-while-open case,
// reloads on each subsequent open).
watch(() => props.modelValue, (open) => { if (open) void onOpen(); }, { immediate: true });
```
Use this pattern (`watch` + `immediate: true` on the `v-model` prop, not
`@open`) for any dialog whose load-on-open logic must run reliably regardless
of how the parent mounts it.

### Sharing UI rollout is unfinished — three connection views still need it

Group-based resource sharing (`ShareDialog.vue` + `SharedBadge.vue` +
`useResourceSharing()` composable — `isOwned`/`isShared`/`canManage`/
`canShare`/`canManageGrants`) is wired into `SecretsView`, `DatabaseView`,
and several catalog detail panels — but **not yet** into
`CloudConnectionView`, `KafkaConnectionView`, or
`GoogleAnalyticsConnectionView` (verified as of 2026-07-03: none of the three
import `ShareDialog`/`SharedBadge`/`useResourceSharing`). If asked to finish
this rollout, copy the exact pattern from `DatabaseView.vue` — a
`SharedBadge :access="connection.access"` on each list item, a `ShareDialog
v-model="showShareDialog"` — and remember **two places must add the field**,
not one:
1. The connection's TS type needs `id: number` and `access?: AccessInfo |
   null` added (see `types/secrets.types.ts` for the reference shape).
2. Its **hand-written `api.ts` mapper** needs the same fields added
   explicitly. These mappers enumerate fields one by one when converting
   backend JSON to the frontend type — an unlisted field is silently
   dropped, so adding it only to the TS type produces a field that
   type-checks but is always `undefined` at runtime. (The catalog store/api
   layer is a verbatim pass-through, which is why catalog types only needed
   the type-level change — connection views are not that shape.)

---

## 12. Lint, format, and test commands

```bash
cd flowfile_frontend
npm run lint            # eslint --fix ./src/**/*.{ts,vue} — legacy eslintrc (eslint 8), not the root flat config
npm run build:web       # lint + vue-tsc --noEmit + vite build -> build/renderer/
npm run test:unit       # vitest run, node env, picks up only src/**/*.test.ts — co-locate new unit tests
npm run test:web        # playwright tests/web-flow.spec.ts (needs core :63578 + a web server already running)
```

ESLint config is `.eslintrc.js` (legacy format), not the repo's root flat
config — `vue/multi-word-component-names` and `no-explicit-any` are off,
`prettier/prettier` runs at warn. Prettier: double quotes, 2-space tabs,
100-char width, trailing commas everywhere, LF line endings
(`.prettierrc.json`).

`flowfile_wasm` has its own toolchain (`npm run test` / `test:run` under
`flowfile_wasm/`, Vitest with `happy-dom` + `fake-indexeddb`, plus a CPython
pytest suite for the shared engine package and a real-Pyodide smoke test) —
see `flowfile_wasm/CLAUDE.md` for the full command set; it is not duplicated
here.

---

## Provenance and maintenance

All facts above were verified by reading source at commit `f6963c77`
(branch `feature/claude-skills`), app/frontend version **0.12.7**, dated
**2026-07-03**. Line numbers and counts drift fastest — re-run these before
trusting a specific number:

```bash
# --- path aliases stay in sync across the three config files ---
grep -n "['\"]@['\"]" flowfile_frontend/vite.config.mjs flowfile_frontend/vitest.config.ts
grep -n '"@/\*"' flowfile_frontend/tsconfig.json

# --- trailing-slash route pairs (frontend path == backend decorator) ---
grep -n "update_settings\|node/description\|node/reference" flowfile_frontend/src/renderer/app/api/node.api.ts
grep -n '@router.post("/update_settings/")\|@router.post("/node/description/")\|@router.post("/node/reference/")' \
  flowfile_core/flowfile_core/routes/routes.py

# --- node-settings element tree size + dead useNodes.ts glob target ---
find flowfile_frontend/src/renderer/app/components/nodes/node-types/elements/ -name "*.vue" | wc -l
find flowfile_frontend/src/renderer/app/features/designer/nodes -maxdepth 0 2>/dev/null; echo "exit=$?"  # nonzero/empty = still missing

# --- Tauri generate_handler! set + capabilities ---
grep -n "generate_handler" flowfile_frontend/src-tauri/src/lib.rs
cat flowfile_frontend/src-tauri/capabilities/main.json

# --- god-component TODO(refactor) file count + full current list (currently 19) ---
grep -rl "TODO(refactor)" flowfile_frontend/src/ flowfile_frontend/src-tauri/ | grep -v node_modules

# --- sharing UI rollout: which connection views still lack ShareDialog? ---
for f in CloudConnectionView KafkaConnectionView GoogleAnalyticsConnectionView DatabaseView; do
  echo "=== $f ==="; grep -rn "ShareDialog\|SharedBadge\|useResourceSharing" \
    flowfile_frontend/src/renderer/app/views/$f*/*.vue 2>/dev/null
done

# --- ShareDialog @open workaround + StatsPanel forwarding still present ---
grep -n "already-open\|modelValue.*immediate" flowfile_frontend/src/renderer/app/components/sharing/ShareDialog.vue
grep -n "defineEmits" flowfile_frontend/src/renderer/app/views/CatalogView/StatsPanel.vue

# --- wasm node/category count (root AND wasm CLAUDE.md are known-stale) + pyodide pin ---
grep -n "16 nodes" CLAUDE.md; grep -n "categories" flowfile_wasm/CLAUDE.md
grep -n "pyodide.js\|indexURL" flowfile_wasm/src/stores/pyodide-store.ts

# --- versions ---
grep -n '"version"' flowfile_frontend/package.json flowfile_frontend/src-tauri/tauri.conf.json flowfile_wasm/package.json
```
