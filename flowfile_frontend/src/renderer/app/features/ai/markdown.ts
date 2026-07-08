// Re-export shim — the markdown helper moved to app/lib/markdown.ts so
// non-AI surfaces (node designer, catalog) can use it without importing ai/.
export * from "../../lib/markdown";
