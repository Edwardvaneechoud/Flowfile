// @vitest-environment happy-dom
// A catalog row in the tree beside a hub: the storage kind off a hub row, a hub row's Shared title, and the row a
// desktop without a hub has always had.
import { h, nextTick, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flow, ns, table } from "../../__fixtures__/catalogTree";
import { fakeStorage, mountIn } from "../../__fixtures__/mount";
import type { NamespaceTree } from "../../types";

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

const storage_uri = "s3://acme/data";
const local = ns({
  storage_uri,
  children: [ns({ id: 2, name: "default", level: 1, parent_id: 1, tables: [table()] })],
});
const hub = ns({
  id: -10,
  name: "main",
  source: "hub",
  storage_uri,
  children: [
    ns({
      id: -11,
      name: "curated",
      level: 1,
      parent_id: -10,
      source: "hub",
      tables: [table({ id: -12, name: "sales", source: "hub" })],
    }),
  ],
});

let host: HTMLElement;

const mount = async (node: NamespaceTree) => {
  host = await mountIn(CatalogTreeNode, {
    props: {
      node,
      selectedFlowId: null,
      selectedArtifactId: null,
      selectedTableId: null,
    },
    pinia: createPinia(),
    stubs,
  });
};

const storageBadges = () => host.querySelectorAll(".storage-badge").length;
const rowCounts = () => Array.from(host.querySelectorAll(".ns-count")).map((el) => el.textContent);
const sectionCounts = () => host.querySelectorAll(".section-count").length;
const collapse = async () => {
  (host.querySelector(".chevron") as HTMLElement).click();
  await nextTick();
};

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("CatalogTreeNode", () => {
  it("shows a desktop without a hub its row as before: storage kind, counts on every row and section", async () => {
    await mount(local);

    expect(storageBadges()).toBe(1);
    expect(rowCounts()).toEqual(["1", "1"]);
    expect(sectionCounts()).toBe(1);
    await collapse();
    expect(rowCounts()).toEqual(["1"]);
  });

  it("shows a hub catalog without a storage kind, and its counts as any catalog's", async () => {
    await mount(hub);

    expect(storageBadges()).toBe(0);
    expect(rowCounts()).toEqual(["1", "1"]);
    expect(sectionCounts()).toBe(1);
  });

  it("names the team that shared a hub catalog when the hub sends its name", async () => {
    const access = { is_owner: false, access_level: "use", shared_by: "Sales" };
    await mount({ ...hub, access } as NamespaceTree);

    expect(host.querySelector(".resource-badge")?.getAttribute("title")).toBe("Shared by Sales");
  });

  it("keeps a local catalog's shared title as before", async () => {
    const access = { is_owner: false, access_level: "use", shared_by: "team:sales" };
    await mount({ ...local, access } as NamespaceTree);

    expect(host.querySelector(".resource-badge")?.getAttribute("title")).toBe(
      "Shared by team:sales",
    );
  });
});

// Expansion is remembered per namespace id across the tests above, so these rows get ids of their own.
describe("a hub flow's row", () => {
  const published = ns({
    id: -30,
    name: "main",
    source: "hub",
    storage_uri,
    children: [
      ns({
        id: -31,
        name: "default",
        level: 1,
        parent_id: -30,
        source: "hub",
        flows: [
          flow({
            id: -40,
            name: "demo-flow",
            namespace_id: -31,
            flow_path: "hub://flows/u-1",
            source: "hub",
            flow_uuid: "u-1",
            latest_version: 3,
            author_name: "ana",
            last_run_success: true,
          }),
        ],
      }),
    ],
  });

  it("shows the flow with its newest version and who published it, and a local flow's favourite, delete and run dot", async () => {
    await mount(published);

    const row = host.querySelector(".tree-flow");
    expect(row?.querySelector(".flow-name")?.textContent).toBe("demo-flow");
    const chip = row?.querySelector("[data-testid='hub-flow-version']");
    expect(chip?.textContent).toBe("v3");
    expect(chip?.getAttribute("title")).toBe("Version 3 on the hub, published by ana");
    expect(row?.querySelector(".flow-actions .star-btn")).not.toBeNull();
    expect(row?.querySelector(".flow-actions .delete-btn")).not.toBeNull();
    expect(row?.querySelector(".run-indicator")).not.toBeNull();
  });

  it("keeps a local flow's favourite, delete and run dot", async () => {
    const own = ns({
      id: 50,
      storage_uri,
      children: [
        ns({
          id: 51,
          name: "default",
          level: 1,
          parent_id: 50,
          flows: [flow({ id: 52, namespace_id: 51, last_run_success: true })],
        }),
      ],
    });
    await mount(own);

    const row = host.querySelector(".tree-flow");
    expect(row?.querySelector("[data-testid='hub-flow-version']")).toBeNull();
    expect(row?.querySelectorAll(".flow-actions .action-btn").length).toBe(2);
    expect(row?.querySelector(".run-indicator")).not.toBeNull();
  });
});
