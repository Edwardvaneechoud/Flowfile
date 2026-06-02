# CLAUDE.md - flowfile_frontend

Tauri 2 desktop shell + Vue 3 renderer: the VueFlow-based visual flow designer and all of Flowfile's UI (designer, AI assistant, catalog, connections, admin, dashboards). Package-specific notes; see the root `/CLAUDE.md` for monorepo-wide setup, build, lint, ports, and cross-package contracts.

## Role
Pure client. Talks to `flowfile_core` over HTTP; never to `flowfile_worker` directly. Two run modes:
- **web** (`vite`, dev port 8080): renderer only. API calls go to `<origin>/api/*`; the Vite dev proxy (or nginx in Docker, see `nginx.conf`) rewrites `/api/` → core:63578, including replicating FastAPI's absolute-307 trailing-slash redirects (`vite.config.mjs` `proxy.configure`).
- **desktop** (`tauri dev`): Rust shell in `src-tauri/` spawns the two Python services as **sidecars** on a discovered free `(core, worker)` port pair (scan starts at 63578, `core = 63578 + k*2`, `worker = core+1`), injects them into `window.__FLOWFILE_PORTS__` before any renderer script runs; the renderer then hits `http://127.0.0.1:<core>/` directly (no `/api` prefix).
- Axios baseURL is resolved once in `src/renderer/config/constants.ts` (`flowfileCorebaseURL`). Desktop runtime detected via `window.__TAURI_INTERNALS__` (`isDesktop` in `lib/desktop.ts`).

## Layout
- `src/renderer/main.ts` — Vue bootstrap (createApp, router, i18n, Element Plus, theme + auth init). Vite `root` is `src/renderer/`, entry `index.html`.
- `src/renderer/app/` — all renderer code; the `@` alias points here.
- `src/renderer/app/views/<Name>View/` — one dir per routed page (DesignerView, CatalogView, ConnectionsView, AdminView, LoginView, SetupView, KernelManagerView, AiProvidersView, …). `app/pages/NodeDesigner.vue` is a standalone routed page outside `views/`.
- `src/renderer/app/features/designer/` — flow editor internals (`drawflowExtensions.ts`, `editor/` with FunctionEditor / ColumnSelector / pythonEditor).
- `src/renderer/app/features/ai/` — AI assistant, command palette, ghost-node suggestions, diff panel.
- `src/renderer/app/components/nodes/` — VueFlow node wrappers; `node-types/elements/` has 43 per-node-type subdirs (76 `.vue` settings components). `GenericNode.vue` and `composables/useDragAndDrop.ts` resolve them via `import.meta.glob` + a string-interpolated path. `getComponents.ts` is unrelated — it lazy-loads only `elements/manualInput/*.vue` editor cells.
- `src/renderer/app/stores/` — Pinia stores (`flow-store`, `node-store`, `results-store`, `auth-store`, `theme-store`, the `ai-*` stores, …); `index.ts` is the plugin.
- `src/renderer/app/api/` & `app/services/` — Axios API wrappers; `services/` adds auth/setup/user services, `axios.config.ts` (interceptors, `withCredentials`), and SSE clients (`aiStreamClient.ts`, `aiDiffClient.ts`).
- `src/renderer/app/composables/`, `app/router/index.ts`, `app/i18n/` (only locale `locales/gb.json`), `app/layouts/`.
- `src/renderer/lib/desktop.ts` — the only renderer↔Tauri bridge (invoke/listen wrappers + web-mode fallbacks).
- `src/renderer/config/` — `constants.ts` (baseURL/port resolution), `environment.ts` (NODE_ENV-derived `ENV` flags).
- `src-tauri/src/` — Rust shell: `lib.rs` (entry, `generate_handler!`, lifecycle), `commands.rs`, `sidecar/` (`mod.rs` spawn + port scan, `readiness.rs`, `shutdown.rs`), `menu.rs`, `oauth.rs`, `window.rs`, `state.rs`, `env.rs`.

## Key patterns & conventions
- Path aliases declared in **three** places, keep in sync: `vite.config.mjs` and `tsconfig.json` define `@` (`@/*` glob form in tsconfig) plus `@/api`, `@/types`, `@/stores`, `@/composables`; `vitest.config.ts` defines only bare `@`.
- Router uses **hash history** (`createWebHashHistory`). Routes are lazy `import()`ed; most live under the `/main` `AppLayout` parent with `meta.requiresAuth` (default true).
- All desktop-native calls MUST go through `lib/desktop.ts`, which no-ops / falls back in web mode (`isDesktop` guard). Don't import `@tauri-apps/*` directly in view code. New Tauri commands must be registered in `src-tauri/src/lib.rs` `generate_handler!` (current set: `get_services_status`, `get_service_ports`, `get_app_version`, `quit_app`, `app_refresh`, `open_oauth`) and wrapped in `desktop.ts`. `desktop.ts` also calls plugin commands directly (`plugin:opener|open_url`, clipboard-manager) — no Rust command needed for those.
- No JSX/TSX: React (`react`/`react-dom` v19, pinned via `overrides`) is only a dynamic `import("react")` inside the `@kanaries/graphic-walker` Vue wrappers. `vite.config.mjs` deliberately omits `@vitejs/plugin-react`; `tsconfig.json` excludes `*.tsx`/`*.jsx`.
- AG Grid is the **modular** `@ag-grid-community/*` v31 (`ModuleRegistry.registerModules`; not the monolithic `ag-grid-community`).
- OAuth: the GA connection opens the **system browser** (`desktop.openExternal`) because Google blocks embedded webviews; the generic `open_oauth` command opens a modal Tauri webview window (`oauth.rs`) for providers that allow it.

## Running / entry points
From this dir (`npm install` first):
- `npm run dev:web` — renderer at :8080 (needs `poetry run flowfile_core` running for `/api`).
- `npm run dev` — full Tauri desktop; runs `npm run dev:web` as `beforeDevCommand`, then the shell spawns sidecars (requires staged sidecar binaries via `make build_python_services && make rename_sidecars` from repo root — see root doc).
- `npm run build:web` — `npm run lint` + `vue-tsc --noEmit` + Vite build → `build/renderer/`.
- `npm run build` — same checks + `tauri build`. `tauri.conf.json`: `devUrl http://localhost:8080`, `frontendDist ../build/renderer`, `beforeDevCommand npm run dev:web`, `beforeBuildCommand npm run build:renderer-only`.

## Testing
- **Unit (Vitest):** `npm run test:unit` (`vitest.config.ts`, `node` env, `globals:false`). Picks up only `src/**/*.test.ts` — currently the `ai-*` stores, `features/ai/markdown.test.ts`, and `views/CatalogView/cron-builder.test.ts`. Co-locate new unit tests next to the module.
- **E2E (Playwright):** `tests/*.spec.ts` (`web-flow.spec.ts`, `canvas-overlays.spec.ts`). `npm run test:web` runs the web-flow spec; `npm run test:all` runs the dir. Needs core (`:63578`) + the web server running first. `playwright.config.ts`: single worker, 120s timeout. `make test_e2e` from repo root orchestrates it.

## Gotchas
- `vite.config.mjs` sets `optimizeDeps.force: true` and `server.strictPort: true` — cold starts re-bundle deps (~5–15s) and the dev server fails (not falls back) if 8080 is taken, on purpose so Tauri's hard-coded `devUrl` never points at the wrong port.
- ESLint here is the **legacy** `.eslintrc.js` (eslint 8 + `vue-eslint-parser`), not the root flat config. `npm run lint` only touches `src/**/*.{ts,vue}` (with `--fix`).
- Sidecar shutdown is a graceful ladder (HTTP `/shutdown` → SIGTERM → SIGKILL) in `src-tauri/src/sidecar/shutdown.rs`; sidecars also self-reap via `FLOWFILE_SUPERVISOR_PID` (set in `env.rs`) + `shared/parent_watcher.py` if the shell is hard-killed. macOS Cmd+Q/dock quit fires `RunEvent::Exit` (not `ExitRequested`), so `lib.rs` matches both. Desktop E2E for this path is a known gap (TODO at top of `tests/web-flow.spec.ts`).
- The per-node settings loader is a **string-interpolated dynamic import** keyed by node-type folder (`GenericNode.vue`, `useDragAndDrop.ts`): renaming a node's settings dir/component breaks loading at runtime (logs `console.error`, no build error).

## Key files
- `src/renderer/main.ts` — Vue app bootstrap and startup auth/theme/update flow.
- `src/renderer/lib/desktop.ts` — renderer↔Tauri bridge (sole boundary).
- `src/renderer/config/constants.ts` — axios baseURL + desktop port resolution.
- `src/renderer/app/services/axios.config.ts` — auth/refresh interceptors, `withCredentials`.
- `src/renderer/app/router/index.ts` — hash router, lazy routes, auth meta.
- `src/renderer/app/stores/flow-store.ts` — central flow/graph state store.
- `src/renderer/app/components/nodes/GenericNode.vue` — per-node settings drawer loader (`import.meta.glob`).
- `vite.config.mjs` — root, aliases, `/api` proxy, build out, port strictness.
- `tsconfig.json` / `vitest.config.ts` — path aliases (keep aligned with Vite).
- `src-tauri/tauri.conf.json` — windows, CSP, bundle, dev/build commands, updater.
- `src-tauri/src/sidecar/mod.rs` — free-port-pair scan + Python sidecar spawn.
- `src-tauri/src/lib.rs` — shell entry, `generate_handler!`, lifecycle/cleanup.
- `tests/web-flow.spec.ts` — web-mode E2E entry (and the desktop-coverage TODO).
