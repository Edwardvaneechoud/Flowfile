import { describe, expect, it } from "vitest";
import {
  UNCATEGORIZED,
  filterNodes,
  groupByCategory,
  matchesTokens,
  tokenize,
} from "./customNodeFilter";
import type { CustomNodeInfo } from "./types";

function makeNode(overrides: Partial<CustomNodeInfo>): CustomNodeInfo {
  return {
    file_name: "node.py",
    node_name: "Node",
    node_category: "Tools",
    title: "Node",
    intro: "A node",
    node_icon: "user-defined-icon.png",
    ...overrides,
  };
}

describe("tokenize", () => {
  it("lowercases, splits on whitespace and drops empties", () => {
    expect(tokenize("  Auto   ML ")).toEqual(["auto", "ml"]);
    expect(tokenize("")).toEqual([]);
    expect(tokenize("   ")).toEqual([]);
  });
});

describe("matchesTokens", () => {
  it("returns true for every node when there are no tokens", () => {
    expect(matchesTokens(makeNode({}), [])).toBe(true);
  });

  it("ANDs terms across different fields", () => {
    const node = makeNode({ node_name: "Classifier", node_category: "AutoML" });
    expect(matchesTokens(node, tokenize("automl classifier"))).toBe(true);
    expect(matchesTokens(node, tokenize("automl regressor"))).toBe(false);
  });

  it("matches partial terms so 'auto class' finds 'Auto Classifier'", () => {
    const node = makeNode({ node_name: "Auto Classifier" });
    expect(matchesTokens(node, tokenize("auto class"))).toBe(true);
  });

  it("matches on file_name", () => {
    const node = makeNode({ node_name: "Widget", file_name: "mood_emoji.py" });
    expect(matchesTokens(node, tokenize("mood_emoji"))).toBe(true);
  });

  it("matches on tags", () => {
    const node = makeNode({ node_name: "Widget", tags: ["sklearn", "ml"] });
    expect(matchesTokens(node, tokenize("sklearn"))).toBe(true);
  });

  it("tolerates a node with no tags", () => {
    const node = makeNode({ node_name: "Widget", tags: undefined });
    expect(matchesTokens(node, tokenize("widget"))).toBe(true);
    expect(matchesTokens(node, tokenize("sklearn"))).toBe(false);
  });
});

describe("filterNodes", () => {
  const nodes = [
    makeNode({ file_name: "a.py", node_name: "Auto Classifier", node_category: "AutoML" }),
    makeNode({ file_name: "b.py", node_name: "Date Parser", node_category: "Date & Time" }),
  ];

  it("returns everything for an empty or whitespace-only query", () => {
    expect(filterNodes(nodes, "")).toEqual(nodes);
    expect(filterNodes(nodes, "   ")).toEqual(nodes);
  });

  it("narrows to the matching nodes", () => {
    expect(filterNodes(nodes, "automl").map((n) => n.file_name)).toEqual(["a.py"]);
  });

  it("returns an empty list when nothing matches", () => {
    expect(filterNodes(nodes, "zzz")).toEqual([]);
  });
});

describe("groupByCategory", () => {
  it("sorts group labels alphabetically", () => {
    const groups = groupByCategory([
      makeNode({ file_name: "z.py", node_category: "Zoology" }),
      makeNode({ file_name: "a.py", node_category: "AutoML" }),
      makeNode({ file_name: "d.py", node_category: "Date & Time" }),
    ]);
    expect(groups.map((g) => g.label)).toEqual(["AutoML", "Date & Time", "Zoology"]);
  });

  it("keeps backend order inside a bucket", () => {
    const groups = groupByCategory([
      makeNode({ file_name: "second.py", node_category: "Tools" }),
      makeNode({ file_name: "first.py", node_category: "Tools" }),
    ]);
    expect(groups).toHaveLength(1);
    expect(groups[0].nodes.map((n) => n.file_name)).toEqual(["second.py", "first.py"]);
  });

  it("buckets blank, whitespace and missing categories into Uncategorized, sorted last", () => {
    const groups = groupByCategory([
      makeNode({ file_name: "blank.py", node_category: "" }),
      makeNode({ file_name: "space.py", node_category: "   " }),
      makeNode({ file_name: "missing.py", node_category: undefined as unknown as string }),
      makeNode({ file_name: "zoo.py", node_category: "Zoology" }),
      makeNode({ file_name: "auto.py", node_category: "AutoML" }),
    ]);
    expect(groups.map((g) => g.key)).toEqual(["AutoML", "Zoology", UNCATEGORIZED]);
    expect(groups[2].nodes.map((n) => n.file_name)).toEqual([
      "blank.py",
      "space.py",
      "missing.py",
    ]);
  });

  it("returns no groups for an empty node list", () => {
    expect(groupByCategory([])).toEqual([]);
  });
});
