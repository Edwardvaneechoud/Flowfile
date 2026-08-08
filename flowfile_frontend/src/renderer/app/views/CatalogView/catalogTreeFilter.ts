// Catalog tree search + leaf filtering. Kept Vue-free so the matching rules stay
// unit-testable, mirroring usePaletteGroups.ts.
import type { NotebookSummary } from "../../api/notebook.api";
import type {
  CatalogTable,
  CatalogVisualization,
  FlowRegistration,
  GlobalArtifact,
  NamespaceTree,
} from "../../types";

export type SectionKind = "flows" | "models" | "tables" | "visualizations" | "notebooks";

export interface TreeFilter {
  // Already normalized: lowercased and trimmed.
  query: string;
  showUnavailable: boolean;
}

export interface ArtifactGroup {
  name: string;
  latest: GlobalArtifact;
  versionCount: number;
}

export function normalizeQuery(raw?: string): string {
  return (raw ?? "").trim().toLowerCase();
}

// Expansion-state storage keys. These are persisted in localStorage, so changing
// the format orphans every user's saved tree layout.
export const nsKey = (namespaceId: number): string => `ns:${namespaceId}`;
export const secKey = (namespaceId: number, kind: SectionKind): string =>
  `sec:${namespaceId}:${kind}`;

const matches = (name: string, query: string): boolean => name.toLowerCase().includes(query);

export function visibleFlows(node: NamespaceTree, filter: TreeFilter): FlowRegistration[] {
  let flows = node.flows;
  if (!filter.showUnavailable) flows = flows.filter((f) => f.file_exists);
  if (filter.query) flows = flows.filter((f) => matches(f.name, filter.query));
  return flows;
}

export function groupArtifacts(artifacts: GlobalArtifact[]): ArtifactGroup[] {
  const byName = new Map<string, GlobalArtifact[]>();
  for (const a of artifacts) {
    const list = byName.get(a.name) ?? [];
    list.push(a);
    byName.set(a.name, list);
  }
  return [...byName.entries()].map(([name, versions]) => {
    const sorted = [...versions].sort((a, b) => b.version - a.version);
    // The tree ships one row per artifact with a server-side version_count; the
    // row count is only a fallback for payloads that predate it.
    return {
      name,
      latest: sorted[0],
      versionCount: sorted[0].version_count ?? versions.length,
    };
  });
}

export function visibleArtifacts(node: NamespaceTree, filter: TreeFilter): ArtifactGroup[] {
  let groups = groupArtifacts(node.artifacts ?? []);
  if (!filter.showUnavailable) groups = groups.filter((g) => g.latest.blob_exists !== false);
  if (filter.query) groups = groups.filter((g) => matches(g.name, filter.query));
  return groups;
}

export function visibleTables(node: NamespaceTree, filter: TreeFilter): CatalogTable[] {
  let tables = node.tables ?? [];
  if (!filter.showUnavailable) tables = tables.filter((t) => t.file_exists !== false);
  if (filter.query) tables = tables.filter((t) => matches(t.name, filter.query));
  return tables;
}

// Visualizations and notebooks have no on-disk artifact, so "show unavailable"
// doesn't apply to them.
export function visibleVisualizations(
  node: NamespaceTree,
  filter: TreeFilter,
): CatalogVisualization[] {
  const viz = node.visualizations ?? [];
  return filter.query ? viz.filter((v) => matches(v.name, filter.query)) : viz;
}

export function visibleNotebooks(node: NamespaceTree, filter: TreeFilter): NotebookSummary[] {
  const notebooks = node.notebooks ?? [];
  return filter.query ? notebooks.filter((n) => matches(n.name, filter.query)) : notebooks;
}

/**
 * Leaf sections render only on level-1 (schema) nodes. The backend also attaches
 * flows and tables to level-0 catalogs, which the tree never shows — matching on
 * those would expand a catalog row with nothing under it.
 */
export const rendersLeaves = (node: NamespaceTree): boolean => node.level === 1;

export const namespaceNameMatches = (node: NamespaceTree, query: string): boolean =>
  matches(node.name, query);

export function visibleLeafCount(node: NamespaceTree, filter: TreeFilter): number {
  if (!rendersLeaves(node)) return 0;
  return (
    visibleFlows(node, filter).length +
    visibleArtifacts(node, filter).length +
    visibleTables(node, filter).length +
    visibleVisualizations(node, filter).length +
    visibleNotebooks(node, filter).length
  );
}

export const hasVisibleLeaves = (node: NamespaceTree, filter: TreeFilter): boolean =>
  visibleLeafCount(node, filter) > 0;

/** Matching leaves in this namespace and everything below it — the count chip. */
export function subtreeLeafCount(node: NamespaceTree, filter: TreeFilter): number {
  return node.children.reduce(
    (sum, child) => sum + subtreeLeafCount(child, filter),
    visibleLeafCount(node, filter),
  );
}

/** Whether this namespace or anything under it matches the active search. */
export function subtreeMatches(node: NamespaceTree, filter: TreeFilter): boolean {
  if (!filter.query) return true;
  // A namespace matched by its own name shows its full contents.
  if (namespaceNameMatches(node, filter.query)) return true;
  if (hasVisibleLeaves(node, filter)) return true;
  return node.children.some((child) => subtreeMatches(child, filter));
}

/** How many top-level namespaces have a match — drives the "no results" hint. */
export function countMatches(nodes: NamespaceTree[], filter: TreeFilter): number {
  return nodes.filter((node) => subtreeMatches(node, filter)).length;
}
