// Guards the catalog tree's search rules: which namespaces count as a match
// (and therefore auto-expand), how "show unavailable" gates them, and the
// storage-key format, which is a persisted-localStorage compatibility contract.

import { describe, it, expect } from "vitest";
import {
  countMatches,
  groupArtifacts,
  normalizeQuery,
  nsKey,
  secKey,
  subtreeLeafCount,
  subtreeMatches,
  visibleFlows,
  visibleNotebooks,
  visibleTables,
  visibleVisualizations,
  type TreeFilter,
} from "./catalogTreeFilter";
import type { NamespaceTree } from "../../types";

const node = (over: Partial<NamespaceTree> & { id: number; name: string; level: number }) =>
  ({
    parent_id: null,
    children: [],
    flows: [],
    artifacts: [],
    tables: [],
    visualizations: [],
    notebooks: [],
    ...over,
  }) as unknown as NamespaceTree;

const flow = (name: string, file_exists = true) => ({ id: 1, name, file_exists }) as any;
const table = (name: string, file_exists = true) => ({ id: 1, name, file_exists }) as any;
const artifact = (name: string, version: number, blob_exists = true) =>
  ({ id: version, name, version, blob_exists }) as any;

const filter = (query: string, showUnavailable = false): TreeFilter => ({
  query,
  showUnavailable,
});

describe("normalizeQuery", () => {
  it("trims and lowercases", () => {
    expect(normalizeQuery("  MyFlow \t")).toBe("myflow");
  });

  it("treats a whitespace-only query as empty", () => {
    expect(normalizeQuery("   ")).toBe("");
    expect(normalizeQuery(undefined)).toBe("");
  });
});

describe("storage keys", () => {
  // Persisted in localStorage — changing the format orphans saved tree layouts.
  it("uses the established format", () => {
    expect(nsKey(4)).toBe("ns:4");
    expect(secKey(4, "flows")).toBe("sec:4:flows");
    expect(secKey(4, "visualizations")).toBe("sec:4:visualizations");
  });
});

describe("subtreeMatches", () => {
  it("matches everything when there is no query", () => {
    expect(subtreeMatches(node({ id: 1, name: "Marketing", level: 0 }), filter(""))).toBe(true);
  });

  it("matches a schema holding a matching flow", () => {
    const schema = node({ id: 2, name: "default", level: 1, flows: [flow("Q3 Sales")] });
    expect(subtreeMatches(schema, filter("sales"))).toBe(true);
  });

  it("matches a catalog through its child schema", () => {
    const schema = node({ id: 2, name: "default", level: 1, flows: [flow("Q3 Sales")] });
    const catalog = node({ id: 1, name: "General", level: 0, children: [schema] });
    expect(subtreeMatches(catalog, filter("sales"))).toBe(true);
  });

  it("ignores leaves attached to a level-0 catalog", () => {
    // The backend attaches them but the tree only renders leaves on schemas, so
    // matching here would expand a catalog row with nothing underneath it.
    const catalog = node({ id: 1, name: "General", level: 0, flows: [flow("Q3 Sales")] });
    expect(subtreeMatches(catalog, filter("sales"))).toBe(false);
  });

  it("matches on the namespace's own name", () => {
    expect(subtreeMatches(node({ id: 3, name: "Marketing", level: 1 }), filter("market"))).toBe(
      true,
    );
  });

  it("does not match when nothing inside matches", () => {
    const schema = node({ id: 2, name: "default", level: 1, flows: [flow("Orders")] });
    const catalog = node({ id: 1, name: "General", level: 0, children: [schema] });
    expect(subtreeMatches(catalog, filter("sales"))).toBe(false);
  });

  it("only matches an unavailable flow when unavailable items are shown", () => {
    const schema = node({ id: 2, name: "default", level: 1, flows: [flow("Q3 Sales", false)] });
    expect(subtreeMatches(schema, filter("sales"))).toBe(false);
    expect(subtreeMatches(schema, filter("sales", true))).toBe(true);
  });

  it("matches a table or visualization, not just flows", () => {
    const tables = node({ id: 2, name: "default", level: 1, tables: [table("sales_fact")] });
    expect(subtreeMatches(tables, filter("sales"))).toBe(true);
    const viz = node({
      id: 3,
      name: "default",
      level: 1,
      visualizations: [{ id: 1, name: "Sales chart" } as any],
    });
    expect(subtreeMatches(viz, filter("sales"))).toBe(true);
  });
});

describe("leaf filters", () => {
  it("hides missing files unless unavailable items are shown", () => {
    const schema = node({
      id: 2,
      name: "default",
      level: 1,
      flows: [flow("here"), flow("gone", false)],
      tables: [table("t_here"), table("t_gone", false)],
    });
    expect(visibleFlows(schema, filter("")).map((f) => f.name)).toEqual(["here"]);
    expect(visibleFlows(schema, filter("", true)).map((f) => f.name)).toEqual(["here", "gone"]);
    expect(visibleTables(schema, filter("")).map((t) => t.name)).toEqual(["t_here"]);
  });

  it("leaves visualizations and notebooks unaffected by the unavailable toggle", () => {
    const schema = node({
      id: 2,
      name: "default",
      level: 1,
      visualizations: [{ id: 1, name: "chart" } as any],
      notebooks: [{ id: 1, name: "book" } as any],
    });
    expect(visibleVisualizations(schema, filter("")).length).toBe(1);
    expect(visibleNotebooks(schema, filter("")).length).toBe(1);
  });

  it("matches case-insensitively", () => {
    const schema = node({ id: 2, name: "default", level: 1, flows: [flow("Q3 Sales")] });
    expect(visibleFlows(schema, filter("q3 sal")).length).toBe(1);
  });
});

describe("groupArtifacts", () => {
  it("groups by name, picking the highest version as latest", () => {
    const groups = groupArtifacts([artifact("model", 1), artifact("model", 3), artifact("other", 1)]);
    const model = groups.find((g) => g.name === "model")!;
    expect(model.latest.version).toBe(3);
    expect(model.versionCount).toBe(2);
    expect(groups).toHaveLength(2);
  });
});

describe("subtreeLeafCount", () => {
  it("counts matching leaves across the subtree, skipping level-0 leaves", () => {
    const schema = node({
      id: 2,
      name: "default",
      level: 1,
      flows: [flow("Q3 Sales"), flow("Orders")],
      tables: [table("sales_fact")],
    });
    const catalog = node({
      id: 1,
      name: "General",
      level: 0,
      children: [schema],
      flows: [flow("Sales ghost")],
    });
    expect(subtreeLeafCount(catalog, filter("sales"))).toBe(2);
  });
});

describe("countMatches", () => {
  it("counts only the roots with a match", () => {
    const hit = node({
      id: 1,
      name: "General",
      level: 0,
      children: [node({ id: 2, name: "default", level: 1, flows: [flow("Q3 Sales")] })],
    });
    const miss = node({ id: 3, name: "Archive", level: 0 });
    expect(countMatches([hit, miss], filter("sales"))).toBe(1);
  });
});
