// Documentation links for the node-info popup ("Learn more about this node").
//
// Each node group maps to its category page in the published MkDocs site. Groups
// without a dedicated docs page (e.g. user-defined "custom" nodes) return an empty
// string so the card simply omits the link. Mirrors the per-category docs URLs the
// in-browser (wasm) build uses.

const DOCS_BASE = "https://edwardvaneechoud.github.io/Flowfile/users/visual-editor/nodes";

// node_group values that have a dedicated docs page (the slug equals the group name).
const DOCUMENTED_GROUPS = new Set(["input", "transform", "combine", "aggregate", "ml", "output"]);

/** Docs page URL for a node group, or "" when the group has no documentation page. */
export function nodeDocsUrl(nodeGroup: string): string {
  return DOCUMENTED_GROUPS.has(nodeGroup) ? `${DOCS_BASE}/${nodeGroup}` : "";
}
