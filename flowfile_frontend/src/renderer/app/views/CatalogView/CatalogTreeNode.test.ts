// @vitest-environment happy-dom
// A catalog row in the tree beside a hub: the storage kind off a hub row, a hub row's Shared title, and the row a
// desktop without a hub has always had.
import { h, nextTick, type FunctionalComponent } from "vue";
import { createPinia, setActivePinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { flow, ns, table } from "../../__fixtures__/catalogTree";
import { fakeStorage, mountIn, text } from "../../__fixtures__/mount";
import type { NamespaceTree } from "../../types";

const sharing = vi.hoisted(() => ({ multiUser: false, owned: false }));

vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    isMultiUser: sharing.multiUser,
    isOwned: () => sharing.owned,
    canManage: () => sharing.owned,
    canShare: (r: { access?: { is_owner: boolean; access_level: string } | null }) =>
      !r.access || r.access.is_owner || r.access.access_level === "manage",
    isShared: (r: { access?: { is_owner: boolean } | null }) => r.access?.is_owner === false,
    sharedLabel: () => "Shared · use",
    sharedTitle: (r: { access?: { shared_by?: string | null } | null }) =>
      r.access?.shared_by ? `Shared by ${r.access.shared_by}` : "Shared with you",
  }),
}));

import CatalogTreeNode from "./CatalogTreeNode.vue";
import { useCatalogStore } from "../../stores/catalog-store";

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

    expect(host.querySelector(".shared-badge")?.getAttribute("title")).toBe("Shared by Sales");
  });

  it("keeps a local catalog's shared title as before", async () => {
    const access = { is_owner: false, access_level: "use", shared_by: "team:sales" };
    await mount({ ...local, access } as NamespaceTree);

    expect(host.querySelector(".shared-badge")?.getAttribute("title")).toBe("Shared by team:sales");
  });
});

describe("a schema's Semantic model entry", () => {
  const catalogWith = (id: number, schema: Partial<NamespaceTree> = {}) =>
    ns({ id, children: [ns({ id: id + 1, name: "market", level: 1, parent_id: id, ...schema })] });
  const entries = () => host.querySelectorAll("[data-testid='semantic-model-entry']").length;

  it("shows where core says there is a model to open, and on a tree sent without the flag", async () => {
    await mount(catalogWith(60, { semantic_model_readable: true }));
    expect(entries()).toBe(1);

    await mount(catalogWith(62));
    expect(entries()).toBe(1);
  });

  it("is not shown on a schema the person has no model to open in", async () => {
    await mount(catalogWith(64, { semantic_model_readable: false }));
    expect(entries()).toBe(0);
  });
});

describe("the owner's actions on a namespace", () => {
  beforeEach(() => Object.assign(sharing, { multiUser: true, owned: true }));
  afterEach(() => Object.assign(sharing, { multiUser: false, owned: false }));

  const catalogWith = (id: number, fields: Partial<NamespaceTree> = {}) =>
    ns({
      id,
      ...fields,
      children: [ns({ id: id + 1, name: "market", level: 1, parent_id: id, ...fields })],
    });
  const count = (selector: string) => host.querySelectorAll(selector).length;

  it("offers sharing and deleting where the person manages the namespace", async () => {
    await mount(catalogWith(70));
    expect(count("button[title='Share namespace']")).toBe(2);
    expect(count("[data-testid='schema-delete']")).toBe(1);
  });

  it("offers neither on a public namespace someone else owns, while adding to it stays", async () => {
    await mount(catalogWith(72, { can_manage: false }));
    expect(count("button[title='Share namespace']")).toBe(0);
    expect(count("[data-testid='schema-delete']")).toBe(0);
    expect(count("button[title='Add schema']")).toBe(1);
    expect(count("button[title='Register table']")).toBe(1);
  });

  it("offers no adding or registering where the person may not write, such as a breadcrumb", async () => {
    await mount(catalogWith(74, { can_manage: false, can_write: false }));
    const titles = Array.from(host.querySelectorAll(".tree-actions button")).map((b) =>
      b.getAttribute("title"),
    );
    expect(titles).toEqual([]);
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

describe("a catalog kept elsewhere", () => {
  const listedAt = "2026-11-24T09:05:00";
  const localTime = (iso: string) => {
    const d = new Date(iso + "Z");
    const pad = (n: number) => String(n).padStart(2, "0");
    return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
  };
  const kept = ns({
    id: 60,
    name: "polaris",
    kept_elsewhere: true,
    children: [ns({ id: 61, name: "sales", level: 1, parent_id: 60, kept_elsewhere: true })],
  });

  const mountWithStore = async (node: NamespaceTree) => {
    const pinia = createPinia();
    setActivePinia(pinia);
    const store = useCatalogStore();
    store.externalTables["60:61"] = {
      tables: [{ name: "orders", reference: "polaris.sales.orders" }],
      listedAt,
      loading: false,
      error: null,
    };
    const load = vi.spyOn(store, "loadExternalTables").mockResolvedValue(undefined);
    host = await mountIn(CatalogTreeNode, {
      props: { node, selectedFlowId: null, selectedArtifactId: null, selectedTableId: null },
      pinia,
      stubs,
    });
    return load;
  };

  it("shows the catalog's refresh without hover, and when a schema's tables were listed with its own refresh", async () => {
    const load = await mountWithStore(kept);

    const refresh = host.querySelector("[data-testid='external-refresh']");
    expect(refresh).not.toBeNull();
    expect(refresh?.closest(".tree-actions")).toBeNull();

    expect(text(host.querySelector("[data-testid='external-listed']"))).toBe(
      `listed ${localTime(listedAt)}`,
    );
    load.mockClear();
    (host.querySelector("[data-testid='external-tables-refresh']") as HTMLElement).click();
    await nextTick();
    expect(load).toHaveBeenCalledTimes(1);
    expect(load).toHaveBeenCalledWith(60, 61, true);
  });

  it("opens the badge's pop-over without selecting, expanding or collapsing the catalog's row", async () => {
    const select = vi.fn();
    host = await mountIn(CatalogTreeNode, {
      props: {
        node: ns({
          id: 62,
          name: "polaris",
          kept_elsewhere: true,
          external: { uri: "http://polaris/api/catalog", warehouse: "w" },
        } as Partial<NamespaceTree>),
        selectedFlowId: null,
        selectedArtifactId: null,
        selectedTableId: null,
        onSelectNamespace: select,
      },
      pinia: createPinia(),
      stubs: {
        ...stubs,
        "el-popover": (_, { slots }) => h("span", [slots.reference?.(), slots.default?.()]),
      },
    });
    const row = host.querySelector(".tree-row") as HTMLElement;
    const expandedBefore = row.classList.contains("expanded");

    (host.querySelector("[data-testid='external-badge']") as HTMLElement).click();
    await nextTick();
    expect(select).not.toHaveBeenCalled();
    expect(row.classList.contains("expanded")).toBe(expandedBefore);

    (host.querySelector(".ns-name") as HTMLElement).click();
    expect(select).toHaveBeenCalledWith(62);
  });

  it("shows an ordinary catalog neither", async () => {
    await mountWithStore(
      ns({
        id: 70,
        storage_uri,
        children: [ns({ id: 71, name: "default", level: 1, parent_id: 70, tables: [table()] })],
      }),
    );

    expect(host.querySelector("[data-testid='external-refresh']")).toBeNull();
    expect(host.querySelector("[data-testid='external-listed']")).toBeNull();
    expect(host.querySelector("[data-testid='external-tables-refresh']")).toBeNull();
  });
});
