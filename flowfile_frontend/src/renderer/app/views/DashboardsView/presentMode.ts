import type { LocationQuery, LocationQueryRaw } from "vue-router";

/** The viewer's query key that opens a dashboard in present mode (`?present=1`). */
export const PRESENT_QUERY_KEY = "present";

/** True when the viewer's URL asks for present mode. */
export function isPresentQuery(query: LocationQuery): boolean {
  const value = query[PRESENT_QUERY_KEY];
  const first = Array.isArray(value) ? value[0] : value;
  return first === "1" || first === "true";
}

/** The same query with present mode on or off; every other key (`f`) is kept. */
export function withPresentQuery(query: LocationQuery, on: boolean): LocationQueryRaw {
  const next: LocationQueryRaw = { ...query };
  if (on) next[PRESENT_QUERY_KEY] = "1";
  else delete next[PRESENT_QUERY_KEY];
  return next;
}

interface FullscreenDocument {
  fullscreenEnabled?: boolean;
  fullscreenElement?: Element | null;
  exitFullscreen?: () => Promise<void>;
}

/**
 * Asks the browser to show `el` fullscreen; resolves whether it did. A missing API, a disabled
 * one (a webview without element fullscreen) or a refusal resolves false and throws nothing.
 */
export async function requestNativeFullscreen(
  el: Element | null | undefined,
  doc: FullscreenDocument = typeof document !== "undefined" ? document : {},
): Promise<boolean> {
  if (!el || !doc.fullscreenEnabled || typeof el.requestFullscreen !== "function") return false;
  try {
    await el.requestFullscreen();
    return true;
  } catch {
    return false;
  }
}

/** Leaves native fullscreen when the page is in it; never throws. */
export async function exitNativeFullscreen(
  doc: FullscreenDocument = typeof document !== "undefined" ? document : {},
): Promise<void> {
  if (!doc.fullscreenElement || typeof doc.exitFullscreen !== "function") return;
  try {
    await doc.exitFullscreen();
  } catch {
    // Already left, e.g. by the browser's own Esc.
  }
}
