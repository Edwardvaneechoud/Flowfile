// Desktop-shell bridge. Abstracts Tauri 2 over what the renderer needs.
//
// The renderer also runs in pure web mode (Docker, `flowfile run ui`), in which
// case none of these calls have a desktop runtime backing them and they fall
// through to safe defaults / no-ops.

import type { Update } from "@tauri-apps/plugin-updater";

import type { ServicesStatus } from "../typings/desktop";
import {
  isPopoutKind,
  POPOUT_TITLES,
  popoutWindowName,
  popoutWindowUrl,
  type PopoutKind,
} from "./popoutWindow";

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

interface TauriWebviewWindow {
  label: string;
  listen: TauriEvent["listen"];
  close(): Promise<void>;
  setTitle(title: string): Promise<void>;
}

interface TauriRuntime {
  core?: TauriCore;
  event?: TauriEvent;
  app?: TauriApp;
  webviewWindow?: { getCurrentWebviewWindow(): TauriWebviewWindow };
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

/**
 * True on the macOS desktop shell — the only platform where dropped files can be
 * linked in place (via the drag pasteboard, see readDragPaths). Other webviews
 * never expose dropped-file paths, so those platforms import a copy instead.
 */
export const isMacDesktop: boolean =
  isDesktop && typeof navigator !== "undefined" && navigator.platform.toUpperCase().includes("MAC");

function resolveDesktopPlatform(): "mac" | "windows" | "linux" | null {
  if (!isDesktop || typeof navigator === "undefined") return null;
  const id = `${navigator.platform} ${navigator.userAgent}`.toUpperCase();
  if (id.includes("MAC")) return "mac";
  if (id.includes("WIN")) return "windows";
  return "linux";
}

/** Which desktop OS the shell runs on; null in web mode. Drives per-platform update copy. */
export const desktopPlatform: "mac" | "windows" | "linux" | null = resolveDesktopPlatform();

/** A release the updater feed offers that is newer than the running build. */
export interface UpdateInfo {
  version: string;
  currentVersion: string;
  date?: string;
}

interface WebView2Bridge {
  postMessageWithAdditionalObjects?: (message: string, objects: readonly unknown[]) => void;
}

function webview2(): WebView2Bridge | null {
  if (typeof window === "undefined") return null;
  const chrome = (window as { chrome?: { webview?: WebView2Bridge } }).chrome;
  return chrome?.webview ?? null;
}

/**
 * The desktop platforms where a dropped file's real path is recoverable: macOS via
 * the drag pasteboard, Windows via WebView2's additional-objects message. Elsewhere
 * the webview only ever hands over the file's bytes, so callers import a copy.
 */
export const canLinkDroppedFiles: boolean =
  isMacDesktop || (isDesktop && typeof webview2()?.postMessageWithAdditionalObjects === "function");

let dropTokenSeq = 0;
let pendingUpdate: Update | null = null;

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

/**
 * Listen as this window: a listener registered through the current webview window receives the
 * shell's global emits plus the `emit_to` aimed at this window only, so a menu action the shell
 * routes to the focused window (zoom) never lands in the other windows as well.
 */
async function listen<T>(event: string, handler: (payload: T) => void): Promise<() => void> {
  const rt = runtime();
  const current = rt?.webviewWindow?.getCurrentWebviewWindow?.();
  if (current?.listen) return current.listen<T>(event, (e) => handler(e.payload));
  if (!rt?.event?.listen) return () => undefined;
  return rt.event.listen<T>(event, (e) => handler(e.payload));
}

/** A flow's pop-out window of one kind, as the shell and the web handle map identify it. */
export interface PopoutRef {
  kind: PopoutKind;
  flowId: number;
}

/** A pop-out window's flow moved to another id (a Save As). */
export interface PopoutMove {
  kind: PopoutKind;
  from: number;
  to: number;
}

const isPopoutRef = (value: unknown): value is PopoutRef => {
  const ref = value as Partial<PopoutRef> | null;
  return isPopoutKind(ref?.kind) && typeof ref?.flowId === "number";
};

const isPopoutMove = (value: unknown): value is PopoutMove => {
  const move = value as Partial<PopoutMove> | null;
  return isPopoutKind(move?.kind) && typeof move?.from === "number" && typeof move?.to === "number";
};

// Web mode's pop-out handles by `<kind>:<flowId>`, and who wants to know when one closes.
const webPopouts = new Map<string, { ref: PopoutRef; handle: Window }>();
const popoutKey = (kind: PopoutKind, flowId: number): string => `${kind}:${flowId}`;
// Web mode's messages from a pop-out to its opener: "Return to designer" and a Save As.
const POPOUT_MESSAGE_TYPE = "flowfile:popout";
type PopoutWire = PopoutRef & { type: typeof POPOUT_MESSAGE_TYPE } & (
    | { event: "returned" }
    | { event: "rekeyed"; to: number }
  );
const popoutClosedHandlers = new Set<(popout: PopoutRef) => void>();
const popoutReturnedHandlers = new Set<(popout: PopoutRef) => void>();
const popoutRekeyedHandlers = new Set<(move: PopoutMove) => void>();
let popoutPoll: ReturnType<typeof setInterval> | null = null;
let wireListening = false;

const isBlankWindow = (handle: Window): boolean => {
  try {
    return handle.location.href === "about:blank";
  } catch {
    return false;
  }
};

function watchWebPopouts(): void {
  if (popoutPoll) return;
  popoutPoll = setInterval(() => {
    for (const [key, { ref, handle }] of webPopouts) {
      if (!handle.closed) continue;
      webPopouts.delete(key);
      for (const handler of popoutClosedHandlers) handler(ref);
    }
    if (!webPopouts.size && popoutPoll) {
      clearInterval(popoutPoll);
      popoutPoll = null;
    }
  }, 1000);
}

const readPopoutWire = (event: MessageEvent): PopoutWire | null => {
  if (event.origin !== window.location.origin) return null;
  const data = event.data as {
    type?: unknown;
    event?: unknown;
    kind?: unknown;
    flowId?: unknown;
    to?: unknown;
  } | null;
  if (data?.type !== POPOUT_MESSAGE_TYPE) return null;
  if (!isPopoutKind(data.kind) || typeof data.flowId !== "number") return null;
  const ref = { type: POPOUT_MESSAGE_TYPE, kind: data.kind, flowId: data.flowId } as const;
  if (data.event === "returned") return { ...ref, event: "returned" };
  if (data.event === "rekeyed" && typeof data.to === "number") {
    return { ...ref, event: "rekeyed", to: data.to };
  }
  return null;
};

function rekeyWebPopout({ kind, from, to }: PopoutMove): void {
  const popout = webPopouts.get(popoutKey(kind, from));
  if (!popout) return;
  webPopouts.delete(popoutKey(kind, from));
  webPopouts.set(popoutKey(kind, to), { ref: { kind, flowId: to }, handle: popout.handle });
}

// One listener for every pop-out's message; the handle map moves before anyone hears of a rekey.
function listenWire(): void {
  if (wireListening) return;
  wireListening = true;
  window.addEventListener("message", (event: MessageEvent) => {
    const wire = readPopoutWire(event);
    if (!wire) return;
    if (wire.event === "returned") {
      const ref: PopoutRef = { kind: wire.kind, flowId: wire.flowId };
      for (const handler of popoutReturnedHandlers) handler(ref);
      return;
    }
    const move: PopoutMove = { kind: wire.kind, from: wire.flowId, to: wire.to };
    rekeyWebPopout(move);
    for (const handler of popoutRekeyedHandlers) handler(move);
  });
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

  /**
   * Write text to the OS clipboard. Desktop goes through the clipboard-manager
   * plugin (granted via `clipboard-manager:allow-write-text`); web mode falls
   * back to navigator.clipboard (callers needing an insecure-context fallback
   * should use clipboardUtils.copyToClipboard instead).
   */
  async writeClipboardText(text: string): Promise<void> {
    if (!isDesktop) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const { writeText } = await import("@tauri-apps/plugin-clipboard-manager");
    await writeText(text);
  },

  /**
   * Save `bytes` via the native Save dialog; returns the chosen path, or null if cancelled/web.
   * The dialog adds that path to the fs scope, so main.json grants only save + write-file.
   */
  async saveFile(
    defaultName: string,
    bytes: Uint8Array,
    filter: { name: string; extensions: string[] } = { name: "CSV", extensions: ["csv"] },
  ): Promise<string | null> {
    if (!isDesktop) return null;
    const { save } = await import("@tauri-apps/plugin-dialog");
    const path = await save({ defaultPath: defaultName, filters: [filter] });
    if (!path) return null;
    const { writeFile } = await import("@tauri-apps/plugin-fs");
    await writeFile(path, bytes);
    return path;
  },

  /**
   * Filesystem paths of the drag currently over the window. WebKit blanks file://
   * URLs out of DataTransfer, so during a drop the renderer asks the shell to read
   * the macOS drag pasteboard instead. Empty in web mode and on platforms with no
   * native path source (Windows/Linux webviews), where callers upload instead.
   */
  async readDragPaths(): Promise<string[]> {
    if (!isDesktop) return [];
    try {
      return await invoke<string[]>("read_drag_paths");
    } catch (error) {
      console.warn("[file-drop] read_drag_paths failed:", error);
      return [];
    }
  },

  /**
   * Filesystem paths of files just dropped into a WebView2 webview. Handing the DOM
   * File objects to the host is the only documented way to recover them (WebView2
   * strips the paths from DataTransfer); the shell reads each one and answers on the
   * `file-drop-paths` event. Empty everywhere else, and empty if the shell stays
   * silent — callers then upload a copy instead of linking.
   */
  async requestDroppedFilePaths(files: readonly File[], timeoutMs = 800): Promise<string[]> {
    const bridge = webview2();
    if (typeof bridge?.postMessageWithAdditionalObjects !== "function") return [];
    dropTokenSeq += 1;
    const token = `ff-drop-${dropTokenSeq}`;

    return new Promise<string[]>((resolve) => {
      let settled = false;
      let unlisten: (() => void) | null = null;
      let timer: ReturnType<typeof setTimeout> | null = null;

      const settle = (paths: string[]) => {
        if (settled) return;
        settled = true;
        if (timer) clearTimeout(timer);
        unlisten?.();
        resolve(paths);
      };

      void listen<{ token?: string; paths?: string[] }>("file-drop-paths", (payload) => {
        if (payload?.token === token) settle(payload.paths ?? []);
      })
        .then((off) => {
          unlisten = off;
          if (settled) off();
          else bridge.postMessageWithAdditionalObjects?.(token, files);
        })
        .catch((error) => {
          console.warn("[file-drop] webview2 path request failed:", error);
          settle([]);
        });

      timer = setTimeout(() => settle([]), timeoutMs);
    });
  },

  /**
   * Ask the updater feed for a newer release. Each `check()` mints a Rust-side
   * resource, so the previous one is closed before it is replaced. Returns null
   * in web mode and when the running build is current.
   */
  async checkForUpdate(): Promise<UpdateInfo | null> {
    if (!isDesktop) return null;
    const { check } = await import("@tauri-apps/plugin-updater");
    const update = await check();
    await pendingUpdate?.close().catch(() => undefined);
    pendingUpdate = update;
    if (!update) return null;
    return { version: update.version, currentVersion: update.currentVersion, date: update.date };
  },

  /**
   * Download the update checked for above. `total` is null when the feed serves
   * the bundle without a content length, in which case callers show an
   * indeterminate progress bar. No-op when nothing is pending (web mode).
   */
  async downloadUpdate(
    onProgress: (downloaded: number, total: number | null) => void,
  ): Promise<void> {
    if (!pendingUpdate) return;
    let downloaded = 0;
    let total: number | null = null;
    await pendingUpdate.download((event) => {
      if (event.event === "Started") {
        downloaded = 0;
        total = event.data.contentLength ?? null;
      } else if (event.event === "Progress") {
        downloaded += event.data.chunkLength;
        onProgress(downloaded, total);
      } else {
        onProgress(total ?? downloaded, total);
      }
    });
  },

  /**
   * Install the downloaded update and come back up. Never use the plugin's
   * `downloadAndInstall()`: installing replaces the running app bundle the
   * PyInstaller sidecars execute from, so the shell's shutdown ladder has to run
   * first (`prepare_for_update`, up to ~10s). On Windows `install()` hands off to
   * the NSIS installer and exits the process, so it never resolves there and the
   * relaunch below is unreachable — Windows restarts via the installer instead.
   * The update resource is deliberately left open: the process is on its way out.
   */
  async installUpdate(): Promise<void> {
    if (!pendingUpdate) return;
    await invoke<void>("prepare_for_update");
    await pendingUpdate.install();
    const { relaunch } = await import("@tauri-apps/plugin-process");
    await relaunch();
  },

  /** Relaunch the app — the recovery exit after a failed install, sidecars already down. */
  async restartApp(): Promise<void> {
    if (!isDesktop) return;
    const { relaunch } = await import("@tauri-apps/plugin-process");
    await relaunch();
  },

  /**
   * Show a file in the OS file manager. Routes through the `opener` plugin like
   * openExternal (granted by `opener:default`). No-op in web mode, where the
   * renderer has no access to the host filesystem.
   */
  async revealInFolder(path: string): Promise<void> {
    if (!isDesktop) return;
    await invoke<void>("plugin:opener|reveal_item_in_dir", { paths: [path] });
  },

  /**
   * Open (or focus) a flow's pop-out window of one kind. Desktop: a native window the shell builds
   * with the same injected ports as the main window, on the route `hash` the renderer passes (the
   * shell knows no kinds or routes). Web: `window.open` on that route with a per-flow target name,
   * so a second click focuses the window that is already there instead of opening another.
   */
  async openPopoutWindow(
    kind: PopoutKind,
    flowId: number,
    target: { hash: string; name: string },
  ): Promise<void> {
    if (isDesktop) {
      await invoke<void>("open_popout_window", {
        kind,
        flowId,
        hash: target.hash,
        title: POPOUT_TITLES[kind],
      });
      return;
    }
    const key = popoutKey(kind, flowId);
    const existing = webPopouts.get(key);
    if (existing && !existing.handle.closed) {
      existing.handle.focus();
      return;
    }
    // An empty URL hands back the named window as is (one forgotten over a reload), else a blank one.
    const handle = window.open("", target.name, "popup=yes,width=1100,height=800");
    if (!handle) throw new Error(`The browser blocked the ${POPOUT_TITLES[kind]} window`);
    if (isBlankWindow(handle)) handle.location.assign(popoutWindowUrl(target.hash, window.location));
    else handle.focus();
    webPopouts.set(key, { ref: { kind, flowId }, handle });
    watchWebPopouts();
  },

  async focusPopoutWindow(kind: PopoutKind, flowId: number): Promise<void> {
    if (isDesktop) {
      await invoke<void>("focus_popout_window", { kind, flowId });
      return;
    }
    webPopouts.get(popoutKey(kind, flowId))?.handle.focus();
  },

  async closePopoutWindow(kind: PopoutKind, flowId: number): Promise<void> {
    if (isDesktop) {
      await invoke<void>("close_popout_window", { kind, flowId });
      return;
    }
    const key = popoutKey(kind, flowId);
    const popout = webPopouts.get(key);
    webPopouts.delete(key);
    if (popout && !popout.handle.closed) popout.handle.close();
  },

  /** The pop-out windows open right now (desktop: the shell's registry, kinds this renderer knows). */
  async listPopoutWindows(): Promise<PopoutRef[]> {
    if (isDesktop) {
      const open = await invoke<unknown[]>("list_popout_windows");
      return open.filter(isPopoutRef);
    }
    return [...webPopouts.values()].filter(({ handle }) => !handle.closed).map(({ ref }) => ref);
  },

  /** A pop-out window went away: the shell's `popout-window-closed`, or the web handle's `closed`. */
  onPopoutWindowClosed(handler: (popout: PopoutRef) => void): Promise<() => void> {
    if (isDesktop) {
      return listen<unknown>("popout-window-closed", (popout) => {
        if (isPopoutRef(popout)) handler(popout);
      });
    }
    popoutClosedHandlers.add(handler);
    return Promise.resolve(() => {
      popoutClosedHandlers.delete(handler);
    });
  },

  /**
   * A pop-out window's "Return to designer": the shell's `popout-window-returned` on `main`, or the
   * pop-out's message to its opener. The designer reopens the panel on that flow; the window closes
   * after.
   */
  onPopoutWindowReturned(handler: (popout: PopoutRef) => void): Promise<() => void> {
    if (isDesktop) {
      return listen<unknown>("popout-window-returned", (popout) => {
        if (isPopoutRef(popout)) handler(popout);
      });
    }
    listenWire();
    popoutReturnedHandlers.add(handler);
    return Promise.resolve(() => {
      popoutReturnedHandlers.delete(handler);
    });
  },

  /**
   * A pop-out window's flow moved to another id (a Save As it followed): the shell's
   * `popout-window-rekeyed` on `main`, or the pop-out's message to its opener, whose handle for the
   * window is re-keyed before the handler runs.
   */
  onPopoutWindowRekeyed(handler: (move: PopoutMove) => void): Promise<() => void> {
    if (isDesktop) {
      return listen<unknown>("popout-window-rekeyed", (move) => {
        if (isPopoutMove(move)) handler(move);
      });
    }
    listenWire();
    popoutRekeyedHandlers.add(handler);
    return Promise.resolve(() => {
      popoutRekeyedHandlers.delete(handler);
    });
  },

  /**
   * Hand this pop-out window's flow back to the designer and close the window. Desktop: the shell
   * tells `main`, focuses it and closes this window. Web: a message to the opener, then `close()`.
   */
  async returnPopoutToDesigner(kind: PopoutKind, flowId: number): Promise<void> {
    if (isDesktop) {
      await invoke<void>("return_popout_window", { kind, flowId });
      return;
    }
    const wire: PopoutWire = { type: POPOUT_MESSAGE_TYPE, event: "returned", kind, flowId };
    window.opener?.postMessage(wire, window.location.origin);
    window.close();
  },

  /**
   * This pop-out window's flow moved to another id (a Save As). Desktop: the shell's registry
   * follows (a window's label cannot change) and tells `main`. Web: the window takes the new id's
   * name, so the designer finds it by name again after a reload, and tells its opener, which
   * re-keys its handle.
   */
  async rekeyPopoutWindow(kind: PopoutKind, from: number, to: number): Promise<void> {
    if (isDesktop) {
      await invoke<void>("rekey_popout_window", { kind, from, to });
      return;
    }
    window.name = popoutWindowName(kind, to);
    const wire: PopoutWire = { type: POPOUT_MESSAGE_TYPE, event: "rekeyed", kind, flowId: from, to };
    window.opener?.postMessage(wire, window.location.origin);
  },

  /** Title this window: `document.title`, and on desktop the native window title with it. */
  async setWindowTitle(title: string): Promise<void> {
    document.title = title;
    if (!isDesktop) return;
    const current = runtime()?.webviewWindow?.getCurrentWebviewWindow?.();
    await current?.setTitle(title).catch(() => undefined);
  },

  /** Close the window this renderer runs in (the pop-out of a flow closed elsewhere). */
  async closeCurrentWindow(): Promise<void> {
    if (!isDesktop) {
      window.close();
      return;
    }
    const current = runtime()?.webviewWindow?.getCurrentWebviewWindow?.();
    if (current) await current.close();
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

  /**
   * Native Help-menu "Request a Node" emitted by the Tauri shell; the renderer
   * opens its in-app dialog. No-op in web mode. See src-tauri/src/menu.rs.
   */
  onHelpRequestNode(handler: () => void): Promise<() => void> {
    return listen<unknown>("help:request-node", handler);
  },
};
