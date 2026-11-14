// Remembers the last catalog sub-page so the sidebar's catalog icon resumes it.
// Session-scoped, following the split labelingStorage.ts documents: real user effort goes
// to localStorage, per-tab view state to sessionStorage.

import { ref } from "vue";

import { resolveStorage, type StorageLike } from "./browserStorage";
import { CATALOG_TAB_KEYS } from "./catalogTabs";

export const LAST_LOCATION_KEY = "flowfile.catalog.lastLocation.v1";
export const LAST_BY_TAB_KEY = "flowfile.catalog.lastLocationByTab.v1";

export type CatalogLocation = Record<string, string>;

const ID_KEYS = ["flowId", "runId", "artifactId", "tableId", "scheduleId", "namespaceId"] as const;
export type CatalogSelectionKey = (typeof ID_KEYS)[number];

// Ids are opaque and server-authorized; the length cap only stops a hand-edited entry
// reaching the API as 1e20.
const ID_PATTERN = /^\d{1,9}$/;

const ENUM_KEYS: Record<string, readonly string[]> = {
  kind: ["charts", "dashboards"],
  apiView: ["endpoints", "consumers"],
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
  const store = resolveStorage(storage, "sessionStorage");
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
  const store = resolveStorage(storage, "sessionStorage");
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

export type CatalogLocationsByTab = Record<string, CatalogLocation>;

/** Where each tab was left, re-validated like the single location; an entry filed under another tab is dropped. */
export const loadCatalogLocationsByTab = (storage?: StorageLike | null): CatalogLocationsByTab => {
  const store = resolveStorage(storage, "sessionStorage");
  if (!store) return {};

  let parsed: unknown;
  try {
    const raw = store.getItem(LAST_BY_TAB_KEY);
    if (raw === null) return {};
    parsed = JSON.parse(raw);
  } catch {
    return {};
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return {};

  const byTab: CatalogLocationsByTab = {};
  for (const [tab, raw] of Object.entries(parsed as Record<string, unknown>)) {
    const location = sanitizeCatalogQuery(raw);
    if (location && location.tab === tab) byTab[tab] = location;
  }
  return byTab;
};

export const persistCatalogLocationsByTab = (
  byTab: CatalogLocationsByTab,
  storage?: StorageLike | null,
): void => {
  const store = resolveStorage(storage, "sessionStorage");
  if (!store) return;

  try {
    store.setItem(LAST_BY_TAB_KEY, JSON.stringify(byTab));
  } catch {
    // QuotaExceededError or storage disabled — the in-memory ref still works this session.
  }
};

/**
 * Where a switch to `tab` lands: where that tab was left (its open table, flow, run…), or its
 * root when nothing is remembered or `tab` is the tab already shown, so clicking the active tab
 * still closes what is open on it.
 */
export const catalogTabLocation = (
  tab: string,
  currentTab: string | null | undefined,
  byTab: CatalogLocationsByTab,
): CatalogLocation => (tab === currentTab ? { tab } : (byTab[tab] ?? { tab }));

// Module singletons so the sidebar's `items` computed re-resolves when these change.
export const lastCatalogQuery = ref<CatalogLocation | null>(loadLastCatalogLocation());
export const lastCatalogQueryByTab = ref<CatalogLocationsByTab>(loadCatalogLocationsByTab());

/** A location that names the given item, back at its tab's root; any other location unchanged. */
export const withoutSelection = (
  location: CatalogLocation,
  key: CatalogSelectionKey,
  id: number,
): CatalogLocation => (location[key] === String(id) ? { tab: location.tab } : location);

/** Forgets a deleted item, so no tab or the rail's catalog icon reopens it. */
export const forgetCatalogSelection = (key: CatalogSelectionKey, id: number): void => {
  const byTab = lastCatalogQueryByTab.value;
  const next = Object.fromEntries(
    Object.entries(byTab).map(([tab, location]) => [tab, withoutSelection(location, key, id)]),
  );
  if (Object.keys(byTab).some((tab) => next[tab] !== byTab[tab])) {
    lastCatalogQueryByTab.value = next;
    persistCatalogLocationsByTab(next);
  }
  const last = lastCatalogQuery.value;
  if (last && withoutSelection(last, key, id) !== last) {
    lastCatalogQuery.value = { tab: last.tab };
    persistLastCatalogLocation(lastCatalogQuery.value);
  }
};

export const rememberCatalogLocation = (query: unknown): void => {
  const location = sanitizeCatalogQuery(query);
  if (!location) return;
  if (!isSameLocation(lastCatalogQueryByTab.value[location.tab] ?? null, location)) {
    lastCatalogQueryByTab.value = { ...lastCatalogQueryByTab.value, [location.tab]: location };
    persistCatalogLocationsByTab(lastCatalogQueryByTab.value);
  }
  if (isSameLocation(lastCatalogQuery.value, location)) return;
  lastCatalogQuery.value = location;
  persistLastCatalogLocation(location);
};
