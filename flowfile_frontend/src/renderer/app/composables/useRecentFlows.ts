import { ref } from "vue";

export interface RecentFlow {
  path: string;
  name: string;
  lastOpened: number;
  // Dotted catalog location ("General.default.my_flow") for flows opened or
  // created via the catalog; shown on the welcome screen instead of the path.
  catalogRef?: string;
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
          typeof (f as RecentFlow).catalogRef === "string"),
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
    // list) keep the name/catalogRef captured on the original open.
    const existing = recentFlows.value.find((f) => f.path === entry.path);
    recentFlows.value = upsertRecent(recentFlows.value, {
      path: entry.path,
      name: entry.name || existing?.name || basenameNoExt(entry.path),
      lastOpened: Date.now(),
      catalogRef: entry.catalogRef ?? existing?.catalogRef,
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
      const refByPath = new Map<string, string>();
      for (const reg of flows) {
        if (reg.namespace_id === null) continue;
        const nsPath = findNamespacePath(tree, reg.namespace_id);
        if (nsPath.length) refByPath.set(reg.flow_path, `${nsPath.join(".")}.${reg.name}`);
      }
      recentFlows.value = recentFlows.value.map((f) => {
        const ref = refByPath.get(f.path);
        return ref === f.catalogRef ? f : { ...f, catalogRef: ref };
      });
      persist(recentFlows.value);
    } catch {
      /* best-effort */
    }
  }

  return { recentFlows, recordFlow, removeFlow, loadRecentFlows, refreshCatalogRefs };
}
