import { ref } from "vue";

export interface RecentFlow {
  path: string;
  name: string;
  lastOpened: number;
  // Dotted catalog location ("General.default.my_flow") for flows opened or
  // created via the catalog; shown on the welcome screen instead of the path.
  catalogRef?: string;
  // Catalog registration id (when the flow is registered) — enables the
  // "View in catalog" context-menu action. Resolved by refreshCatalogRefs.
  catalogId?: number;
}

const STORAGE_KEY = "flowfile.recentFlows";
export const MAX_RECENT = 8;

export function upsertRecent(list: RecentFlow[], entry: RecentFlow, max = MAX_RECENT): RecentFlow[] {
  return [entry, ...list.filter((f) => f.path !== entry.path)].slice(0, max);
}

export function removeRecent(list: RecentFlow[], path: string): RecentFlow[] {
  return list.filter((f) => f.path !== path);
}

export function parseStored(raw: string | null): RecentFlow[] {
  if (!raw) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (f): f is RecentFlow =>
        !!f &&
        typeof f === "object" &&
        typeof (f as RecentFlow).path === "string" &&
        (f as RecentFlow).path.length > 0 &&
        typeof (f as RecentFlow).name === "string" &&
        typeof (f as RecentFlow).lastOpened === "number" &&
        ((f as RecentFlow).catalogRef === undefined ||
          typeof (f as RecentFlow).catalogRef === "string") &&
        ((f as RecentFlow).catalogId === undefined ||
          typeof (f as RecentFlow).catalogId === "number"),
    );
  } catch {
    return [];
  }
}

export function basenameNoExt(path: string): string {
  const base = path.split(/[\\/]/).pop() ?? path;
  const dot = base.lastIndexOf(".");
  return dot > 0 ? base.slice(0, dot) : base;
}

// Recents are best-effort: localStorage can be unavailable (node test env) or
// throw (private mode / quota), so reads fall back to [] and writes are silent.
function readStored(): RecentFlow[] {
  try {
    return parseStored(localStorage.getItem(STORAGE_KEY));
  } catch {
    return [];
  }
}

function persist(list: RecentFlow[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(list));
  } catch {
    /* best-effort */
  }
}

// Module-level singleton: DesignerView (renders the list) and HeaderButtons
// (records creates) must share one reactive source.
const recentFlows = ref<RecentFlow[]>(readStored());

export function useRecentFlows() {
  function recordFlow(entry: { path: string; name?: string; catalogRef?: string }): void {
    if (!entry.path) return;
    // Re-records without explicit metadata (e.g. reopening from the recents
    // list) keep the name/catalogRef/catalogId captured on the original open.
    const existing = recentFlows.value.find((f) => f.path === entry.path);
    recentFlows.value = upsertRecent(recentFlows.value, {
      path: entry.path,
      name: entry.name || existing?.name || basenameNoExt(entry.path),
      lastOpened: Date.now(),
      catalogRef: entry.catalogRef ?? existing?.catalogRef,
      catalogId: existing?.catalogId,
    });
    persist(recentFlows.value);
  }

  function removeFlow(path: string): void {
    recentFlows.value = removeRecent(recentFlows.value, path);
    persist(recentFlows.value);
  }

  function loadRecentFlows(): void {
    recentFlows.value = readStored();
  }

  // Best-effort: recompute every entry's catalogRef from the live catalog, so
  // registered flows show their catalog location even when the entry was
  // recorded without one (quick create, file-system opens, pre-existing
  // entries) — and so refs follow re-registrations / drop on unregistration.
  // APIs are imported lazily to keep this module dependency-free for unit tests.
  async function refreshCatalogRefs(): Promise<void> {
    if (!recentFlows.value.length) return;
    try {
      const [{ CatalogApi }, { findNamespacePath }] = await Promise.all([
        import("../api"),
        import("../types"),
      ]);
      const [flows, tree] = await Promise.all([
        CatalogApi.getFlows(),
        CatalogApi.getNamespaceTree(),
      ]);
      // id resolves whenever the flow is registered; ref only when it also
      // sits under a namespace.
      const byPath = new Map<string, { id: number; ref?: string }>();
      for (const reg of flows) {
        const nsPath = reg.namespace_id !== null ? findNamespacePath(tree, reg.namespace_id) : [];
        byPath.set(reg.flow_path, {
          id: reg.id,
          ref: nsPath.length ? `${nsPath.join(".")}.${reg.name}` : undefined,
        });
      }
      recentFlows.value = recentFlows.value.map((f) => {
        const match = byPath.get(f.path);
        if (match?.ref === f.catalogRef && match?.id === f.catalogId) return f;
        return { ...f, catalogRef: match?.ref, catalogId: match?.id };
      });
      persist(recentFlows.value);
    } catch {
      /* best-effort */
    }
  }

  // Record from a flow-settings object (the shape getFlowSettings returns and
  // HeaderButtons' loadFlowSettings populates); no-op when there's no path.
  // Centralizes the path-guard + entry shape so the create-flow recorders in
  // HomeView and HeaderButtons can't drift apart.
  function recordFlowFromSettings(
    settings: { path?: string | null; name?: string } | null | undefined,
    catalogRef?: string,
  ): void {
    if (settings?.path) recordFlow({ path: settings.path, name: settings.name, catalogRef });
  }

  return {
    recentFlows,
    recordFlow,
    recordFlowFromSettings,
    removeFlow,
    loadRecentFlows,
    refreshCatalogRefs,
  };
}
