import type { DashboardFilter } from "../../types";

/**
 * A viewer's filter choices as a URL parameter.
 *
 * Only the states that differ from the saved dashboard are written, keyed by filter id, so a link
 * to an untouched dashboard stays clean and a filter the author later removes is simply ignored.
 */

const sameState = (a: unknown, b: unknown): boolean => JSON.stringify(a) === JSON.stringify(b);

const toBase64Url = (text: string): string => {
  const bytes = new TextEncoder().encode(text);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
};

const fromBase64Url = (encoded: string): string => {
  const base64 = encoded.replace(/-/g, "+").replace(/_/g, "/");
  const binary = atob(base64 + "=".repeat((4 - (base64.length % 4)) % 4));
  return new TextDecoder().decode(Uint8Array.from(binary, (ch) => ch.charCodeAt(0)));
};

/** Whether the viewer changed any filter's values. */
export const filtersDiffer = (live: DashboardFilter[], saved: DashboardFilter[]): boolean =>
  live.some((f) => {
    const original = saved.find((s) => s.id === f.id);
    return !original || !sameState(f.state, original.state);
  });

/** The changed states as a URL-safe string; "" when nothing differs from the saved dashboard. */
export const encodeFilterState = (live: DashboardFilter[], saved: DashboardFilter[]): string => {
  const changed: Record<string, Record<string, unknown>> = {};
  for (const f of live) {
    const original = saved.find((s) => s.id === f.id);
    if (original && !sameState(f.state, original.state)) changed[f.id] = f.state;
  }
  return Object.keys(changed).length ? toBase64Url(JSON.stringify(changed)) : "";
};

/** The saved filters with the states a URL parameter holds; anything unreadable is ignored. */
export const decodeFilterState = (param: unknown, saved: DashboardFilter[]): DashboardFilter[] => {
  if (typeof param !== "string" || !param) return saved.map((f) => ({ ...f }));
  let changed: Record<string, unknown> = {};
  try {
    const parsed = JSON.parse(fromBase64Url(param));
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) changed = parsed;
  } catch {
    changed = {};
  }
  return saved.map((f) => {
    const state = changed[f.id];
    return state && typeof state === "object" && !Array.isArray(state)
      ? { ...f, state: state as Record<string, unknown> }
      : { ...f };
  });
};
