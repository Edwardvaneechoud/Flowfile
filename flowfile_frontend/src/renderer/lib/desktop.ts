// Desktop-shell bridge. Abstracts Tauri 2 over what the renderer needs.
//
// The renderer also runs in pure web mode (Docker, `flowfile run ui`), in which
// case none of these calls have a desktop runtime backing them and they fall
// through to safe defaults / no-ops.

import type { ServicesStatus } from "../typings/desktop";

type TauriInternals = unknown;

interface TauriCore {
  invoke<T = unknown>(cmd: string, args?: Record<string, unknown>): Promise<T>;
}

interface TauriEvent {
  listen<T = unknown>(event: string, handler: (event: { payload: T }) => void): Promise<() => void>;
}

interface TauriApp {
  getVersion(): Promise<string>;
}

interface TauriRuntime {
  core?: TauriCore;
  event?: TauriEvent;
  app?: TauriApp;
}

declare global {
  interface Window {
    __TAURI_INTERNALS__?: TauriInternals;
    __TAURI__?: TauriRuntime;
    /**
     * Service ports allocated by the Tauri shell at startup. Injected before
     * any renderer script runs (see src-tauri/src/lib.rs::create_main_window).
     * Read by config/constants.ts to build the axios baseURL.
     */
    __FLOWFILE_PORTS__?: { core: number; worker: number };
  }
}

/** True when the renderer is running inside the Tauri desktop shell. */
export const isDesktop: boolean = typeof window !== "undefined" && !!window.__TAURI_INTERNALS__;

function runtime(): TauriRuntime | null {
  if (typeof window === "undefined") return null;
  return window.__TAURI__ ?? null;
}

async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  const rt = runtime();
  if (!rt?.core?.invoke) {
    throw new Error(`Tauri runtime not available: cannot invoke '${cmd}'`);
  }
  return rt.core.invoke<T>(cmd, args);
}

async function listen<T>(event: string, handler: (payload: T) => void): Promise<() => void> {
  const rt = runtime();
  if (!rt?.event?.listen) return () => undefined;
  return rt.event.listen<T>(event, (e) => handler(e.payload));
}

export const desktop = {
  async getAppVersion(): Promise<string> {
    if (!isDesktop) return "";
    const rt = runtime();
    if (rt?.app?.getVersion) return rt.app.getVersion();
    return invoke<string>("get_app_version");
  },

  async getServicesStatus(): Promise<ServicesStatus> {
    if (!isDesktop) return { status: "not_started", error: null };
    return invoke<ServicesStatus>("get_services_status");
  },

  /**
   * Returns the ports the Tauri shell allocated for this instance. Prefer the
   * synchronous `window.__FLOWFILE_PORTS__` for axios baseURL; this async
   * variant is only useful for debugging / health UI.
   */
  async getServicePorts(): Promise<{ core: number; worker: number } | null> {
    if (!isDesktop) return null;
    return invoke<{ core: number; worker: number }>("get_service_ports");
  },

  async quitApp(): Promise<void> {
    if (!isDesktop) return;
    await invoke<void>("quit_app");
  },

  async refreshApp(): Promise<void> {
    if (!isDesktop) {
      window.location.reload();
      return;
    }
    await invoke<void>("app_refresh");
  },

  async openOauth(url: string): Promise<string | null> {
    if (!isDesktop) {
      // In web mode the consumer should fall back to a popup or top-level redirect.
      window.location.assign(url);
      return null;
    }
    return invoke<string | null>("open_oauth", { url });
  },

  /**
   * Open a URL in the user's default system browser. Used for OAuth on desktop:
   * Google blocks its sign-in flow inside embedded webviews (disallowed_useragent),
   * so the consent screen must run in a real browser. Routes through the `opener`
   * plugin (granted via `opener:default` in capabilities/main.json). In web mode
   * this opens a new tab instead.
   */
  async openExternal(url: string): Promise<void> {
    if (!isDesktop) {
      window.open(url, "_blank", "noopener");
      return;
    }
    await invoke<void>("plugin:opener|open_url", { url });
  },

  /**
   * Read the OS clipboard as text. On desktop this goes through the native
   * clipboard-manager plugin (NSPasteboard on macOS, granted via
   * `clipboard-manager:allow-read-text` in capabilities/main.json) rather than
   * the WebKit async Clipboard API — the latter pops macOS's native "Paste"
   * confirmation pill on every programmatic read. In web mode we fall back to
   * navigator.clipboard, where the browser's own permission model applies.
   */
  async readClipboardText(): Promise<string> {
    if (!isDesktop) return navigator.clipboard.readText();
    const { readText } = await import("@tauri-apps/plugin-clipboard-manager");
    return (await readText()) ?? "";
  },

  onServicesStatus(handler: (status: ServicesStatus) => void): Promise<() => void> {
    return listen<ServicesStatus>("services-status", handler);
  },

  onStartupSuccess(handler: () => void): Promise<() => void> {
    return listen<unknown>("startup-success", handler);
  },

  /**
   * Native View-menu zoom commands (Zoom In / Out / Reset, incl. the
   * Cmd+`+`/`-`/`0` accelerators) emitted by the Tauri shell. The shell can't
   * zoom the VueFlow canvas itself, so it forwards the intent here; the renderer
   * drives the actual zoom. No-op in web mode. See src-tauri/src/menu.rs::emit_zoom.
   */
  onViewZoom(handler: (direction: "in" | "out" | "reset") => void): Promise<() => void> {
    return listen<"in" | "out" | "reset">("view:zoom", handler);
  },
};
