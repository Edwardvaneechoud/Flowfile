import { describe, it, expect, beforeEach, vi } from "vitest";
import { ref } from "vue";
import { buildPaletteGroups, usePaletteGroups, BUILTIN_GROUPS } from "./usePaletteGroups";
import type { NodeTemplate } from "../../types";

function makeNode(overrides: Partial<NodeTemplate>): NodeTemplate {
  return {
    name: "Node",
    color: "#fff",
    item: "node",
    input: 1,
    output: 1,
    image: "x.svg",
    multi: false,
    node_group: "custom",
    prod_ready: true,
    drawer_title: "t",
    drawer_intro: "i",
    custom_node: false,
    ...overrides,
  } as NodeTemplate;
}

// jsdom-free localStorage stub (vitest node env has no window).
function stubLocalStorage(): void {
  const store = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: (k: string) => (store.has(k) ? store.get(k)! : null),
    setItem: (k: string, v: string) => store.set(k, v),
    removeItem: (k: string) => store.delete(k),
    clear: () => store.clear(),
  });
}

describe("buildPaletteGroups", () => {
  it("orders built-in groups in the fixed order and skips empty ones", () => {
    const nodes = [
      makeNode({ item: "out", name: "Out", node_group: "output" }),
      makeNode({ item: "inp", name: "In", node_group: "input" }),
      makeNode({ item: "trn", name: "Trn", node_group: "transform" }),
    ];
    const groups = buildPaletteGroups(nodes);
    expect(groups.map((g) => g.key)).toEqual(["input", "transform", "output"]);
    expect(groups.every((g) => !g.isDynamic)).toBe(true);
    expect(groups[0].label).toBe("Input Sources");
  });

  it("keeps the User Defined Operations label for the custom group", () => {
    const nodes = [makeNode({ item: "c", name: "C", node_group: "custom", custom_node: true })];
    const groups = buildPaletteGroups(nodes);
    expect(groups).toHaveLength(1);
    expect(groups[0].key).toBe("custom");
    expect(groups[0].label).toBe("User Defined Operations");
    expect(groups[0].isDynamic).toBe(false);
  });

  it("places dynamic custom-category groups after built-ins, sorted alphabetically", () => {
    const nodes = [
      makeNode({ item: "i", name: "I", node_group: "input" }),
      makeNode({
        item: "z",
        name: "Z",
        node_group: "zoology",
        node_group_label: "Zoology",
        custom_node: true,
      }),
      makeNode({
        item: "g",
        name: "G",
        node_group: "geo",
        node_group_label: "Geo Tools",
        custom_node: true,
      }),
    ];
    const groups = buildPaletteGroups(nodes);
    expect(groups.map((g) => g.key)).toEqual(["input", "geo", "zoology"]);
    expect(groups[1]).toMatchObject({ label: "Geo Tools", isDynamic: true });
    expect(groups[2]).toMatchObject({ label: "Zoology", isDynamic: true });
  });

  it("title-cases the slug when node_group_label is missing", () => {
    const nodes = [
      makeNode({
        item: "s",
        name: "S",
        node_group: "my_cool-tools",
        node_group_label: null,
        custom_node: true,
      }),
    ];
    const groups = buildPaletteGroups(nodes);
    expect(groups[0].label).toBe("My Cool Tools");
    expect(groups[0].isDynamic).toBe(true);
  });

  it("lets a custom node land in a built-in group when its slug matches", () => {
    const nodes = [
      makeNode({ item: "b", name: "Builtin Transform", node_group: "transform" }),
      makeNode({
        item: "u",
        name: "User Transform",
        node_group: "transform",
        custom_node: true,
        node_group_label: "Transformations",
      }),
    ];
    const groups = buildPaletteGroups(nodes);
    expect(groups).toHaveLength(1);
    expect(groups[0].key).toBe("transform");
    expect(groups[0].isDynamic).toBe(false);
    expect(groups[0].nodes).toHaveLength(2);
  });

  it("defaults a node with empty node_group into the custom group", () => {
    const nodes = [makeNode({ item: "x", name: "X", node_group: "", custom_node: true })];
    const groups = buildPaletteGroups(nodes);
    expect(groups[0].key).toBe("custom");
  });

  it("exposes all seven built-in groups in canonical order", () => {
    expect(BUILTIN_GROUPS.map((g) => g.key)).toEqual([
      "input",
      "transform",
      "combine",
      "aggregate",
      "ml",
      "output",
      "custom",
    ]);
  });
});

describe("usePaletteGroups", () => {
  beforeEach(() => {
    stubLocalStorage();
  });

  it("filters nodes by name and tag, dropping empty groups", () => {
    const nodes = ref([
      makeNode({ item: "a", name: "Alpha", node_group: "input" }),
      makeNode({ item: "b", name: "Beta", node_group: "transform", tags: ["special"] }),
    ]);
    const { searchQuery, filteredGroups } = usePaletteGroups(nodes);

    searchQuery.value = "alph";
    expect(filteredGroups.value.map((g) => g.key)).toEqual(["input"]);

    searchQuery.value = "special";
    expect(filteredGroups.value.map((g) => g.key)).toEqual(["transform"]);

    searchQuery.value = "";
    expect(filteredGroups.value.map((g) => g.key)).toEqual(["input", "transform"]);
  });

  it("persists open/closed state to localStorage keyed by group key", () => {
    const nodes = ref([makeNode({ item: "a", name: "Alpha", node_group: "input" })]);
    const { isGroupOpen, toggleGroup } = usePaletteGroups(nodes);

    expect(isGroupOpen("input")).toBe(true); // default open
    toggleGroup("input");
    expect(isGroupOpen("input")).toBe(false);
    expect(localStorage.getItem("nodeList.groupOpenState")).toContain("input");

    // A fresh composable reads the persisted state.
    const second = usePaletteGroups(nodes);
    expect(second.isGroupOpen("input")).toBe(false);
  });

  it("force-expands all groups while searching", () => {
    const nodes = ref([makeNode({ item: "a", name: "Alpha", node_group: "input" })]);
    const { searchQuery, isGroupOpen, toggleGroup } = usePaletteGroups(nodes);
    toggleGroup("input");
    expect(isGroupOpen("input")).toBe(false);
    searchQuery.value = "alph";
    expect(isGroupOpen("input")).toBe(true);
  });

  it("defaults newly appearing groups to open", () => {
    const nodes = ref([makeNode({ item: "a", name: "Alpha", node_group: "input" })]);
    const { isGroupOpen } = usePaletteGroups(nodes);
    // A group never toggled has no stored entry yet — defaults open.
    expect(isGroupOpen("geo")).toBe(true);
  });
});
