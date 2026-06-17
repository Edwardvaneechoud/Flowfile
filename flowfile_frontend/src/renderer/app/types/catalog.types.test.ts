// Guards the namespace ancestry lookup used to build catalog references
// ("General.default.my_flow") for the welcome screen's recent flows.

import { describe, it, expect } from "vitest";
import { findNamespacePath, type NamespaceTree } from "./catalog.types";

const ns = (id: number, name: string, children: NamespaceTree[] = []): NamespaceTree =>
  ({
    id,
    name,
    parent_id: null,
    level: 0,
    description: null,
    owner_id: 1,
    created_at: "",
    updated_at: "",
    children,
    flows: [],
    artifacts: [],
    tables: [],
    visualizations: [],
  }) as NamespaceTree;

const tree = [
  ns(1, "General", [ns(2, "default"), ns(3, "Local Flows")]),
  ns(4, "Marketing", [ns(5, "campaigns")]),
];

describe("findNamespacePath", () => {
  it("returns the root name for a top-level namespace", () => {
    expect(findNamespacePath(tree, 1)).toEqual(["General"]);
  });

  it("returns root-to-leaf ancestry for nested namespaces", () => {
    expect(findNamespacePath(tree, 2)).toEqual(["General", "default"]);
    expect(findNamespacePath(tree, 5)).toEqual(["Marketing", "campaigns"]);
  });

  it("returns [] for an unknown id", () => {
    expect(findNamespacePath(tree, 99)).toEqual([]);
  });
});
