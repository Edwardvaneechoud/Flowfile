import { beforeEach, describe, expect, it, vi } from "vitest";
import type { HubNamespace } from "../../api/hubFlows.api";

const mocks = vi.hoisted(() => ({ listNamespaces: vi.fn(), listFlows: vi.fn() }));

vi.mock("../../api/hubFlows.api", () => ({
  listHubNamespaces: mocks.listNamespaces,
  listHubFlows: mocks.listFlows,
}));

import { listHubFlows } from "../../api/hubFlows.api";
import {
  chevronLabel,
  rootRows,
  toggled,
  useHubNamespaceTree,
  visibleRows,
  withChildren,
  type TreeRow,
} from "./hubNamespaceTree";

const general: HubNamespace = {
  id: 1,
  name: "General",
  parentId: null,
  level: 0,
  description: null,
};
const demo: HubNamespace = { id: 3, name: "Demo", parentId: null, level: 0, description: null };
const sales: HubNamespace = { id: 2, name: "sales", parentId: 1, level: 1, description: null };
const hr: HubNamespace = { id: 4, name: "hr", parentId: 1, level: 1, description: null };
const west: HubNamespace = { id: 5, name: "west", parentId: 2, level: 2, description: null };

const names = (rows: TreeRow[]) => rows.map((row) => row.name);

describe("the tree's steps", () => {
  it("lists the roots at depth 0, collapsed, unloaded, with children unknown", () => {
    expect(rootRows([general, demo])).toEqual([
      { ...general, depth: 0, expanded: false, loaded: false, hasChildren: null },
      { ...demo, depth: 0, expanded: false, loaded: false, hasChildren: null },
    ]);
  });

  it("puts fetched children right under their parent, one deeper, and marks the parent loaded and expanded", () => {
    const rows = withChildren(rootRows([general, demo]), 1, [sales, hr]);

    expect(names(rows)).toEqual(["General", "sales", "hr", "Demo"]);
    expect(rows[0]).toMatchObject({ loaded: true, expanded: true, hasChildren: true });
    expect(rows[1]).toMatchObject({ depth: 1, expanded: false, loaded: false, hasChildren: null });
    expect(rows[3]).toMatchObject({ depth: 0, loaded: false });
  });

  it("a fetch that returned no rows settles that the parent has no children", () => {
    const rows = withChildren(rootRows([general, demo]), 3, []);

    expect(names(rows)).toEqual(["General", "Demo"]);
    expect(rows[1]).toMatchObject({ loaded: true, expanded: true, hasChildren: false });
  });

  it("leaves the rows alone for an unknown parent", () => {
    const rows = rootRows([general]);
    expect(withChildren(rows, 99, [sales])).toBe(rows);
  });

  it("toggling collapses a row and hides its descendants, toggling again shows them", () => {
    let rows = withChildren(withChildren(rootRows([general, demo]), 1, [sales, hr]), 2, [west]);
    expect(names(visibleRows(rows))).toEqual(["General", "sales", "west", "hr", "Demo"]);

    rows = toggled(rows, 1);
    expect(names(visibleRows(rows))).toEqual(["General", "Demo"]);
    expect(names(rows)).toEqual(["General", "sales", "west", "hr", "Demo"]);

    rows = toggled(rows, 1);
    expect(names(visibleRows(rows))).toEqual(["General", "sales", "west", "hr", "Demo"]);

    rows = toggled(rows, 2);
    expect(names(visibleRows(rows))).toEqual(["General", "sales", "hr", "Demo"]);
  });

  it("names the chevron after what a click does", () => {
    const [row] = rootRows([general]);
    expect(chevronLabel(row)).toBe("Show namespaces in General");
    expect(chevronLabel({ ...row, expanded: true })).toBe("Hide namespaces in General");
  });
});

describe("useHubNamespaceTree", () => {
  beforeEach(() => {
    mocks.listNamespaces.mockReset();
    mocks.listFlows.mockReset();
  });

  it("load lists the hub's roots and nothing below them", async () => {
    mocks.listNamespaces.mockResolvedValue([general, demo]);
    const tree = useHubNamespaceTree();

    await tree.load();

    expect(mocks.listNamespaces).toHaveBeenCalledTimes(1);
    expect(mocks.listNamespaces).toHaveBeenCalledWith();
    expect(names(tree.visible.value)).toEqual(["General", "Demo"]);
    expect(tree.loading.value).toBe(false);
    expect(tree.error.value).toBe("");
  });

  it("expanding a root fetches its children once and shows them under it at depth 1", async () => {
    mocks.listNamespaces.mockResolvedValueOnce([general, demo]).mockResolvedValueOnce([sales, hr]);
    const tree = useHubNamespaceTree();
    await tree.load();

    await tree.expand(1);

    expect(mocks.listNamespaces).toHaveBeenLastCalledWith(1);
    expect(names(tree.visible.value)).toEqual(["General", "sales", "hr", "Demo"]);
    expect(tree.visible.value[1].depth).toBe(1);
    expect(tree.visible.value[0]).toMatchObject({ expanded: true, hasChildren: true });

    await tree.expand(1);
    await tree.expand(1);

    expect(mocks.listNamespaces).toHaveBeenCalledTimes(2);
    expect(names(tree.visible.value)).toEqual(["General", "sales", "hr", "Demo"]);
  });

  it("a namespace whose fetch returned no rows loses its chevron", async () => {
    mocks.listNamespaces.mockResolvedValueOnce([general, demo]).mockResolvedValueOnce([]);
    const tree = useHubNamespaceTree();
    await tree.load();

    await tree.expand(3);

    expect(mocks.listNamespaces).toHaveBeenLastCalledWith(3);
    expect(tree.visible.value[1]).toMatchObject({ name: "Demo", hasChildren: false });
    expect(names(tree.visible.value)).toEqual(["General", "Demo"]);
  });

  it("collapsing hides the children and keeps them for the next expand without a fetch", async () => {
    mocks.listNamespaces.mockResolvedValueOnce([general]).mockResolvedValueOnce([sales]);
    const tree = useHubNamespaceTree();
    await tree.load();
    await tree.expand(1);

    await tree.expand(1);
    expect(names(tree.visible.value)).toEqual(["General"]);
    expect(names(tree.rows.value)).toEqual(["General", "sales"]);

    await tree.expand(1);
    expect(names(tree.visible.value)).toEqual(["General", "sales"]);
    expect(mocks.listNamespaces).toHaveBeenCalledTimes(2);
  });

  it("selecting a child's id lists its flows through listHubFlows with that id", async () => {
    mocks.listNamespaces.mockResolvedValueOnce([general]).mockResolvedValueOnce([sales]);
    mocks.listFlows.mockResolvedValue([]);
    const tree = useHubNamespaceTree();
    await tree.load();
    await tree.expand(1);

    expect(tree.selectedId.value).toBeNull();
    tree.select(sales.id);
    await listHubFlows(tree.selectedId.value);

    expect(tree.selectedId.value).toBe(2);
    expect(mocks.listFlows).toHaveBeenCalledWith(2);
  });

  it("the publish picker: nothing chosen is null, a chosen child is its id, none again is null", async () => {
    mocks.listNamespaces.mockResolvedValueOnce([general]).mockResolvedValueOnce([sales]);
    const tree = useHubNamespaceTree();
    await tree.load();
    await tree.expand(1);

    expect(tree.selectedId.value).toBeNull();
    tree.select(2);
    expect(tree.selectedId.value).toBe(2);
    tree.select(null);
    expect(tree.selectedId.value).toBeNull();
  });

  it("a hub that cannot be reached leaves the rows empty and says so", async () => {
    mocks.listNamespaces.mockRejectedValue(new Error("boom"));
    const tree = useHubNamespaceTree();

    await tree.load();

    expect(tree.visible.value).toEqual([]);
    expect(tree.error.value).toBe("Couldn't reach the hub.");
    expect(tree.loading.value).toBe(false);
  });

  it("a failed expand keeps the row collapsed with its chevron and says so", async () => {
    mocks.listNamespaces.mockResolvedValueOnce([general]).mockRejectedValueOnce(new Error("boom"));
    const tree = useHubNamespaceTree();
    await tree.load();

    await tree.expand(1);

    expect(tree.visible.value[0]).toMatchObject({
      expanded: false,
      loaded: false,
      hasChildren: null,
    });
    expect(tree.error.value).toBe("Couldn't reach the hub.");
  });
});
