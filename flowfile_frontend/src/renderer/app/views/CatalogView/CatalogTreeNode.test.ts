// @vitest-environment happy-dom
// A catalog row in the tree beside a hub: no Hub badge where everything is the hub's, one on a hub catalog beside
// this machine's, the storage kind off a hub row, counts only on collapsed rows with hub rows in the tree, and the
// row a desktop without a hub has always had.
import { createApp, h, nextTick, type App, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CatalogTable, NamespaceTree } from "../../types";
import type { HubPresence } from "./hubRows";

vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    isMultiUser: false,
    isOwned: () => false,
    canManage: () => false,
    isShared: (r: { access?: { is_owner: boolean } | null }) => r.access?.is_owner === false,
    sharedLabel: () => "Shared · use",
    sharedTitle: (r: { access?: { shared_by?: string | null } | null }) =>
      r.access?.shared_by ? `Shared by ${r.access.shared_by}` : "Shared with you",
  }),
}));

import CatalogTreeNode from "./CatalogTreeNode.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-tooltip": (_, { slots }) => h("span", slots.default?.()),
};

const table = (id: number, name: string, source: "local" | "hub" = "local"): CatalogTable =>
  ({
    id,
    name,
    namespace_id: 0,
    table_type: "physical",
    file_exists: true,
    source,
  }) as CatalogTable;

const schema = (
  id: number,
  name: string,
  parentId: number,
  tables: CatalogTable[],
  source: "local" | "hub" = "local",
): NamespaceTree =>
  ({
    id,
    name,
    level: 1,
    parent_id: parentId,
    source,
    children: [],
    tables,
    flows: [],
    artifacts: [],
    visualizations: [],
    notebooks: [],
  }) as unknown as NamespaceTree;

const catalog = (
  id: number,
  name: string,
  children: NamespaceTree[],
  source: "local" | "hub" = "local",
): NamespaceTree =>
  ({
    id,
    name,
    level: 0,
    parent_id: null,
    source,
    storage_uri: "s3://acme/data",
    children,
    tables: [],
    flows: [],
    artifacts: [],
    visualizations: [],
    notebooks: [],
  }) as unknown as NamespaceTree;

const local = catalog(1, "Sales", [schema(2, "default", 1, [table(3, "orders")])]);
const hub = catalog(
  -10,
  "main",
  [schema(-11, "curated", -10, [table(-12, "sales", "hub")], "hub")],
  "hub",
);

const fakeLocalStorage = () => {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
};

let app: App | null = null;
let host: HTMLElement;

const mount = async (node: NamespaceTree, hubPresence: HubPresence = "none") => {
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(CatalogTreeNode, {
    node,
    selectedFlowId: null,
    selectedArtifactId: null,
    selectedTableId: null,
    hubPresence,
  });
  app.use(createPinia());
  for (const [name, stub] of Object.entries(stubs)) app.component(name, stub);
  app.mount(host);
  await nextTick();
};

const badges = () => host.querySelectorAll('[data-testid="hub-badge"]').length;
const storageBadges = () => host.querySelectorAll(".storage-badge").length;
const rowCounts = () => Array.from(host.querySelectorAll(".ns-count")).map((el) => el.textContent);
const sectionCounts = () => host.querySelectorAll(".section-count").length;
const collapse = async () => {
  (host.querySelector(".chevron") as HTMLElement).click();
  await nextTick();
};

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
});

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
  vi.unstubAllGlobals();
});

describe("CatalogTreeNode", () => {
  it("shows a desktop without a hub its row as before: storage kind, counts on every row and section", async () => {
    await mount(local);

    expect(badges()).toBe(0);
    expect(storageBadges()).toBe(1);
    expect(rowCounts()).toEqual(["1", "1"]);
    expect(sectionCounts()).toBe(1);
    await collapse();
    expect(rowCounts()).toEqual(["1"]);
  });

  it("marks nothing as the hub's on a server whose every catalog is the hub's", async () => {
    await mount(hub, "only");

    expect(badges()).toBe(0);
    expect(storageBadges()).toBe(0);
  });

  it("badges a hub catalog beside this machine's, never its schemas or tables", async () => {
    await mount(hub, "beside");

    expect(badges()).toBe(1);
    expect(host.querySelector(".tree-row [data-testid='hub-badge']")).not.toBeNull();
    expect(host.querySelector(".tree-table [data-testid='hub-badge']")).toBeNull();
    expect(storageBadges()).toBe(0);
  });

  it("leaves this machine's catalog unbadged beside the hub's", async () => {
    await mount(local, "beside");

    expect(badges()).toBe(0);
    expect(storageBadges()).toBe(1);
  });

  it("counts only a collapsed catalog or schema where the tree shows hub rows", async () => {
    await mount(hub, "only");

    expect(rowCounts()).toEqual([]);
    expect(sectionCounts()).toBe(0);
    await collapse();
    expect(rowCounts()).toEqual(["1"]);
  });

  it("names who shared a hub catalog only when a person did, never a team's raw account", async () => {
    const sharedBy = (by: string) => ({ is_owner: false, access_level: "use", shared_by: by });
    await mount({ ...hub, access: sharedBy("team:sales") } as NamespaceTree, "only");
    expect(host.querySelector(".resource-badge")?.getAttribute("title")).toBe("Shared with you");
    app?.unmount();
    host.remove();

    await mount({ ...hub, access: sharedBy("ada") } as NamespaceTree, "only");
    expect(host.querySelector(".resource-badge")?.getAttribute("title")).toBe("Shared by ada");
  });

  it("keeps a local catalog's shared title as before", async () => {
    const access = { is_owner: false, access_level: "use", shared_by: "team:sales" };
    await mount({ ...local, access } as NamespaceTree);

    expect(host.querySelector(".resource-badge")?.getAttribute("title")).toBe(
      "Shared by team:sales",
    );
  });

  it("lets a row's name keep its width only where the tree shows hub rows", async () => {
    await mount(hub, "only");
    expect(host.querySelector(".tree-row")?.classList.contains("keeps-name")).toBe(true);
    app?.unmount();
    host.remove();

    await mount(local);
    expect(host.querySelector(".tree-row")?.classList.contains("keeps-name")).toBe(false);
  });
});
