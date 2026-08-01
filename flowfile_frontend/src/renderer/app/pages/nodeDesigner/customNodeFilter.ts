// Search + category grouping for the custom-node lists (designer browser and
// the catalog's Custom Nodes tab). Vue-free so the matching and bucketing rules
// stay unit-testable, mirroring usePaletteGroups.ts. Generic over the row type
// because the two surfaces carry different extra fields.
export const UNCATEGORIZED = "Uncategorized";

/** The fields both custom-node row shapes share, and all that search reads. */
export interface SearchableNode {
  file_name: string;
  node_name?: string;
  node_category?: string;
  title?: string;
  intro?: string;
  tags?: string[];
}

export interface NodeGroup<T extends SearchableNode = SearchableNode> {
  key: string;
  label: string;
  nodes: T[];
}

export function tokenize(raw: string): string[] {
  return raw.toLowerCase().split(/\s+/).filter(Boolean);
}

function haystack(node: SearchableNode): string {
  return [node.node_name, node.title, node.intro, node.node_category, node.file_name]
    .concat(node.tags ?? [])
    .filter(Boolean)
    .join(" ")
    .toLowerCase();
}

/** Every term must hit some field, so "auto class" finds "Auto Classifier". */
export function matchesTokens(node: SearchableNode, tokens: string[]): boolean {
  if (!tokens.length) return true;
  const text = haystack(node);
  return tokens.every((token) => text.includes(token));
}

export function filterNodes<T extends SearchableNode>(nodes: T[], query: string): T[] {
  const tokens = tokenize(query);
  return tokens.length ? nodes.filter((node) => matchesTokens(node, tokens)) : nodes;
}

/** Bucket by category: alphabetical, uncategorized last, backend order kept inside. */
export function groupByCategory<T extends SearchableNode>(nodes: T[]): NodeGroup<T>[] {
  const byKey = new Map<string, T[]>();
  for (const node of nodes) {
    const key = (node.node_category || "").trim() || UNCATEGORIZED;
    const bucket = byKey.get(key);
    if (bucket) bucket.push(node);
    else byKey.set(key, [node]);
  }
  return [...byKey.entries()]
    .map(([key, groupNodes]) => ({ key, label: key, nodes: groupNodes }))
    .sort((a, b) => {
      if (a.key === UNCATEGORIZED) return 1;
      if (b.key === UNCATEGORIZED) return -1;
      return a.label.localeCompare(b.label);
    });
}
