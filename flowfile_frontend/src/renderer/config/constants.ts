import { isDesktop } from "../lib/desktop";

interface InjectedPorts {
  core: number;
  worker: number;
}

const DEFAULT_CORE_PORT = 63578;

/**
 * In desktop mode the Tauri shell allocates a free port pair at startup and
 * injects them via `window.__FLOWFILE_PORTS__` BEFORE the renderer script
 * runs (see `src-tauri/src/lib.rs::create_main_window`). This lets multiple
 * Flowfile instances coexist — each gets its own backend pair.
 *
 * Fallback to the standard 63578 if the shell did not inject ports (e.g. a
 * future `flowfile run ui` mode that still exposes the desktop bridge).
 */
function resolveCorePort(): number {
  const injected = (window as Window & { __FLOWFILE_PORTS__?: InjectedPorts })
    .__FLOWFILE_PORTS__;
  if (injected && typeof injected.core === "number") {
    return injected.core;
  }
  return DEFAULT_CORE_PORT;
}

// In desktop mode (Tauri shell), connect directly to the local backend on the
// port the shell allocated for this instance. In web/Docker mode, send to
// <origin>/api/ so requests stay same-origin and nginx (Docker) or the Vite
// dev proxy (dev) can forward /api/* to flowfile-core.
//
// The base must be absolute because callers that use `new URL(path, base)`
// (aiStreamClient / aiDiffClient) reject relative bases — `new URL(path, "/api/")`
// throws "Invalid base URL".
// TODO(H): verify dev-mode CORS. Under `tauri dev` the page origin is
// http://localhost:8080 while this baseURL targets http://127.0.0.1:<port>/ —
// a cross-origin request. Confirm flowfile_core's CORS allows the Tauri origin
// (and tauri://localhost in a packaged build). Likely fine since the app runs,
// but it's unverified; dev-only breakage would surface as blocked API calls.
export const flowfileCorebaseURL = isDesktop
  ? `http://127.0.0.1:${resolveCorePort()}/`
  : `${window.location.origin}/api/`;

// GA OAuth callback Google redirects to after consent. Must be browser-reachable
// and registered verbatim in the Google Cloud console. Desktop: the system
// browser hits the local core directly. Web/Docker: <origin>/api/ (nginx forwards
// to core), so it tracks whatever host serves the app — e.g. a Cloudflare domain.
export const gaOAuthCallbackUrl = isDesktop
  ? `http://localhost:${DEFAULT_CORE_PORT}/ga_connections/oauth/callback`
  : `${window.location.origin}/api/ga_connections/oauth/callback`;
