// Guards the namespace ancestry lookup used to build catalog references
// ("General.default.my_flow") for the welcome screen's recent flows, and the
// default save-target selection shared by the save/create namespace pickers.

import { describe, it, expect } from "vitest";
import {
  filterSelectableNamespaces,
  findDefaultSaveNamespace,
  findNamespacePath,
  type NamespaceTree,
} from "./catalog.types";

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
    notebooks: [],
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

describe("findDefaultSaveNamespace", () => {
  it("prefers General > Local Flows when present", () => {
    expect(findDefaultSaveNamespace(tree)?.id).toBe(3);
  });

  it("falls back to General > default when Local Flows is absent or unselectable", () => {
    const withoutLocal = [ns(1, "General", [ns(2, "default")]), ns(4, "Marketing")];
    expect(findDefaultSaveNamespace(withoutLocal)?.id).toBe(2);
    expect(findDefaultSaveNamespace(tree, (n) => n.name !== "Local Flows")?.id).toBe(2);
  });

  it("returns null when nothing under General is selectable", () => {
    expect(findDefaultSaveNamespace(tree, () => false)).toBeNull();
    expect(findDefaultSaveNamespace([ns(4, "Marketing", [ns(5, "campaigns")])])).toBeNull();
  });
});

const fromHub = (node: NamespaceTree): NamespaceTree => ({
  ...node,
  source: "hub",
  hub_id: -node.id,
  children: node.children.map(fromHub),
});

// A connected hub lists its catalogs after the local ones, with negative ids.
const withHub = [
  ...tree,
  fromHub(ns(-10, "General", [ns(-11, "default"), ns(-12, "Local Flows")])),
];

describe("filterSelectableNamespaces", () => {
  it("drops a hub catalog and keeps the local ones", () => {
    const hubSales = fromHub(ns(-20, "sales", [ns(-21, "raw")]));
    const filtered = filterSelectableNamespaces([...tree, hubSales]);
    expect(filtered.map((n) => n.id)).toEqual([1, 4]);
    expect(filtered[0].children.map((c) => c.name)).toEqual(["default"]);
    expect(filtered[1].children.map((c) => c.name)).toEqual(["campaigns"]);
  });

  it("drops a hub General too, so only the local one is offered", () => {
    expect(filterSelectableNamespaces(withHub).map((n) => n.id)).toEqual([1, 4]);
  });
});

describe("findDefaultSaveNamespace with a hub connected", () => {
  it("still answers the local General > Local Flows", () => {
    expect(findDefaultSaveNamespace(withHub)?.id).toBe(3);
  });
});
