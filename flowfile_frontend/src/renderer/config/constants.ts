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
export const flowfileCorebaseURL = isDesktop
  ? `http://127.0.0.1:${resolveCorePort()}/`
  : `${window.location.origin}/api/`;
