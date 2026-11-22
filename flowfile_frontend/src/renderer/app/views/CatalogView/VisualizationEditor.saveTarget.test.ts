// @vitest-environment happy-dom
// The chart editor mounted as the app mounts it: where it asks "Save in", what it says for a table of this machine,
// and the request a Save sends for a chart of a hub table.
import { reactive, type FunctionalComponent } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ns, table } from "../../__fixtures__/catalogTree";
import { element, fakeStorage, mountIn, settle, text } from "../../__fixtures__/mount";
import type { CatalogTable, NamespaceTree, VizSourceDescriptor } from "../../types";

const created = vi.hoisted(() => ({ calls: [] as Record<string, unknown>[] }));
const catalog = vi.hoisted(() => ({ tree: [] as unknown[] }));

vi.mock("../../stores/catalog-store", () => ({
  useCatalogStore: () => ({
    get tree() {
      return catalog.tree as NamespaceTree[];
    },
    visualizationsByTable: {},
    findTableInTree(id: number): CatalogTable | null {
      const under = (node: NamespaceTree): CatalogTable[] => [
        ...node.tables,
        ...node.children.flatMap(under),
      ];
      return (catalog.tree as NamespaceTree[]).flatMap(under).find((t) => t.id === id) ?? null;
    },
    loadVisualizationFields: async () => [],
    createVisualization: async (payload: Record<string, unknown>) => {
      created.calls.push(payload);
      return { id: -301, updated_at: "", ...payload };
    },
  }),
}));
vi.mock("../../api/catalog.api", () => ({
  CatalogApi: { computeAdHocVisualization: vi.fn(), getVisualization: vi.fn() },
}));
vi.mock("../../stores/auth-store", () => ({
  useAuthStore: () => ({ currentUser: { id: 1 }, isAdmin: false }),
}));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canManage: (r: { access?: { is_owner: boolean; access_level: string } | null }) =>
      !r.access || r.access.is_owner || r.access.access_level === "manage",
  }),
}));
vi.mock("../../composables/useChartThumbnail", () => ({ captureThumbnail: async () => null }));
vi.mock("../../composables/useGraphicWalkerCompute", async () => {
  const { ref } = await import("vue");
  return {
    useGraphicWalkerCompute: () => ({ computation: async () => [], lastError: ref(null) }),
  };
});
vi.mock("./ChartPromptBar.vue", () => ({ default: { render: () => null } }));
vi.mock(
  "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue",
  async () => {
    const { defineComponent, h } = await import("vue");
    return {
      default: defineComponent({
        setup(_, { expose }) {
          expose({ exportCode: async () => [{ visId: "gw_1", name: "Chart 1" }] });
          return () => h("div", { "data-testid": "builder" });
        },
      }),
    };
  },
);

import VisualizationEditor from "./VisualizationEditor.vue";
import { LOCAL_TABLE_STAYS_LOCAL } from "./chartSaveTarget";

const stubs: Record<string, FunctionalComponent> = {
  "el-alert": element("div"),
  "el-input": element("input"),
  "el-tooltip": element("span"),
  "el-skeleton": element("div"),
  "el-button": element("button"),
  "el-select": element("div"),
  "el-option-group": element("div"),
  "el-option": element("div"),
};

const stock = table({ id: 7, name: "stock", namespace_id: 2 });
const orders = table({ id: -101, name: "orders", namespace_id: -12, source: "hub" });
const own = (offered = true) =>
  ns({
    id: 1,
    name: "General",
    place: "On this computer",
    offered,
    children: [ns({ id: 2, name: "default", parent_id: 1, level: 1, tables: [stock] })],
  });
const hub = () =>
  ns({
    id: -10,
    name: "sales",
    source: "hub",
    place: "Acme Data",
    children: [
      ns({
        id: -12,
        name: "raw",
        parent_id: -10,
        level: 1,
        source: "hub",
        access: { is_owner: false, access_level: "manage" },
        tables: [orders],
      }),
      ns({
        id: -13,
        name: "locked",
        parent_id: -10,
        level: 1,
        source: "hub",
        access: { is_owner: false, access_level: "use" },
      }),
    ],
  });

const open = (source: VizSourceDescriptor) =>
  mountIn(VisualizationEditor, { props: reactive({ source }), stubs });
const target = (host: HTMLElement) => host.querySelector('[data-testid="viz-save-target"]');
const note = (host: HTMLElement) => host.querySelector('[data-testid="viz-target-note"]');
const save = async (host: HTMLElement) => {
  (host.querySelector('[data-testid="viz-save-button"]') as HTMLButtonElement).click();
  await settle();
};

describe("VisualizationEditor, where a new chart is saved", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeStorage());
    created.calls.length = 0;
  });

  it("asks nothing and sends no schema where no hub is shown", async () => {
    catalog.tree = [own()];
    const host = await open({ source_type: "table", table_id: 7 });
    expect(host.querySelector('[data-testid="builder"]')).not.toBeNull();
    expect(target(host)).toBeNull();
    expect(note(host)).toBeNull();
    await save(host);
    expect(created.calls).toHaveLength(1);
    expect(created.calls[0]).toMatchObject({ source_type: "table", catalog_table_id: 7 });
    expect(created.calls[0]).not.toHaveProperty("namespace_id");
  });

  it("offers a chart of a table on this machine no hub schema and says why", async () => {
    catalog.tree = [own(), hub()];
    const host = await open({ source_type: "table", table_id: 7 });
    expect(target(host)).toBeNull();
    expect(text(note(host))).toBe(LOCAL_TABLE_STAYS_LOCAL);
    await save(host);
    expect(created.calls[0]).not.toHaveProperty("namespace_id");
  });

  it("saves a chart of a hub table in the table's hub schema, by the table's id", async () => {
    catalog.tree = [own(false), hub()];
    const host = await open({ source_type: "table", table_id: -101 });
    expect(target(host)?.getAttribute("modelvalue")).toBe("-12");
    expect(text(target(host))).not.toContain("locked");
    await save(host);
    expect(created.calls[0]).toMatchObject({
      source_type: "table",
      catalog_table_id: -101,
      sql_query: null,
      namespace_id: -12,
    });
  });

  it("does not save on a joined server when the person may write no hub schema", async () => {
    const locked = hub();
    locked.children = locked.children.filter((schema) => schema.name === "locked");
    catalog.tree = [own(false), locked];
    const host = await open({ source_type: "sql", sql_query: "SELECT 1" });
    const button = host.querySelector('[data-testid="viz-save-button"]') as HTMLButtonElement;
    expect(button.disabled).toBe(true);
    await save(host);
    expect(created.calls).toEqual([]);
  });
});
