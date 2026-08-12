// Remembers the last catalog sub-page so the sidebar's catalog icon resumes it.
// Session-scoped, following the split labelingStorage.ts documents: real user effort goes
// to localStorage, per-tab view state to sessionStorage.

import { ref } from "vue";

import { CATALOG_TAB_KEYS } from "./catalogTabs";

export const LAST_LOCATION_KEY = "flowfile.catalog.lastLocation.v1";

export type CatalogLocation = Record<string, string>;

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const ID_KEYS = ["flowId", "runId", "artifactId", "tableId", "scheduleId", "namespaceId"] as const;

// Ids are opaque and server-authorized; the length cap only stops a hand-edited entry
// reaching the API as 1e20.
const ID_PATTERN = /^\d{1,9}$/;

const ENUM_KEYS: Record<string, readonly string[]> = {
  kind: ["charts", "dashboards"],
  apiView: ["endpoints", "consumers"],
};

const resolveStorage = (storage?: StorageLike | null): StorageLike | null => {
  if (storage) return storage;
  try {
    return (globalThis as { sessionStorage?: StorageLike }).sessionStorage ?? null;
  } catch {
    return null;
  }
};

/**
 * Keep only the query params CatalogView's applyRouteToStore understands.
 *
 * Returns null when there is no usable tab, so a bare `{ name: "catalog" }` push
 * (WelcomeScreen's "Explore catalog") can't overwrite a good memory with nothing.
 * `scheduleFlowId` is deliberately excluded: it is a one-shot trigger that opens the
 * create-schedule modal and is stripped again, so remembering it would re-pop that modal
 * on every catalog open.
 */
export const sanitizeCatalogQuery = (raw: unknown): CatalogLocation | null => {
  if (typeof raw !== "object" || raw === null || Array.isArray(raw)) return null;

  const query = raw as Record<string, unknown>;
  const tab = query.tab;
  if (typeof tab !== "string" || !(CATALOG_TAB_KEYS as readonly string[]).includes(tab)) {
    return null;
  }

  const location: CatalogLocation = { tab };
  for (const key of ID_KEYS) {
    const value = query[key];
    if (typeof value === "string" && ID_PATTERN.test(value)) location[key] = value;
  }
  for (const [key, allowed] of Object.entries(ENUM_KEYS)) {
    const value = query[key];
    if (typeof value === "string" && allowed.includes(value)) location[key] = value;
  }
  return location;
};

export const loadLastCatalogLocation = (storage?: StorageLike | null): CatalogLocation | null => {
  const store = resolveStorage(storage);
  if (!store) return null;

  let raw: string | null;
  try {
    raw = store.getItem(LAST_LOCATION_KEY);
  } catch {
    return null;
  }
  if (raw === null) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    try {
      store.removeItem(LAST_LOCATION_KEY);
    } catch {
      // Best effort — storage rejected the removal.
    }
    return null;
  }

  // Re-validate on the way in: the stored value is as untrusted as a route query.
  return sanitizeCatalogQuery(parsed);
};

export const persistLastCatalogLocation = (
  location: CatalogLocation,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage);
  if (!store) return;

  try {
    store.setItem(LAST_LOCATION_KEY, JSON.stringify(location));
  } catch {
    // QuotaExceededError or storage disabled — the in-memory ref still works this session.
  }
};

export const isSameLocation = (a: CatalogLocation | null, b: CatalogLocation): boolean => {
  if (!a) return false;
  const keys = Object.keys(a);
  return keys.length === Object.keys(b).length && keys.every((key) => a[key] === b[key]);
};

// Module singleton so the sidebar's `items` computed re-resolves when this changes.
export const lastCatalogQuery = ref<CatalogLocation | null>(loadLastCatalogLocation());

export const rememberCatalogLocation = (query: unknown): void => {
  const location = sanitizeCatalogQuery(query);
  if (!location || isSameLocation(lastCatalogQuery.value, location)) return;
  lastCatalogQuery.value = location;
  persistLastCatalogLocation(location);
};
