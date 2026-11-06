// @vitest-environment happy-dom
// The table panel for a hub table: no Hub badge, and no "Read by" card, whose flow ids are the hub's and would open a
// local flow of the same id; a local table keeps both as before.
import { createApp, h, nextTick, type App, type FunctionalComponent } from "vue";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CatalogTable } from "../../types";

vi.mock("../../api/catalog.api", () => ({
  CatalogApi: { getTableCdc: vi.fn(), resetCdcCursor: vi.fn(), deleteCdcCursor: vi.fn() },
}));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canShare: () => false,
    canManage: () => false,
    canManageGrants: () => false,
    isShared: () => false,
    sharedLabel: () => "",
    sharedTitle: () => "",
  }),
}));
vi.mock("./VisualizationsTab.vue", () => ({ default: { render: () => null } }));
vi.mock("./TableMaintenance.vue", () => ({ default: { render: () => null } }));
vi.mock("./TableEditorDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/ShareDialog.vue", () => ({ default: { render: () => null } }));

import TableDetailPanel from "./TableDetailPanel.vue";

const element = (tag: string): FunctionalComponent => {
  const stub: FunctionalComponent = (_, { attrs, slots }) => h(tag, attrs, slots.default?.());
  stub.inheritAttrs = false;
  return stub;
};

const stubs: Record<string, FunctionalComponent> = {
  "el-tooltip": (_, { slots }) => h("span", slots.default?.()),
  "el-popover": (_, { slots }) => h("span", slots.reference?.()),
  "el-button": element("button"),
};

const table = (over: Partial<CatalogTable> = {}): CatalogTable =>
  ({
    id: 3,
    name: "orders",
    namespace_id: 2,
    table_type: "physical",
    file_exists: true,
    row_count: 1200,
    column_count: 4,
    size_bytes: 2048,
    created_at: "2026-01-01T00:00:00",
    read_by_flows: [{ id: 5, name: "Daily orders" }],
    source: "local",
    ...over,
  }) as CatalogTable;

const hubTable = table({ id: -3, source: "hub", hub_id: 3, hub_namespace_id: 2 });

let app: App | null = null;
let host: HTMLElement;

const mount = async (row: CatalogTable) => {
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(TableDetailPanel, {
    table: row,
    preview: null,
    loadingPreview: false,
    previewError: null,
    tableHistory: null,
    loadingTableHistory: false,
    tableHistoryStale: false,
    selectedVersion: null,
  });
  for (const [name, stub] of Object.entries(stubs)) app.component(name, stub);
  app.mount(host);
  await nextTick();
};

const metaLabels = () =>
  Array.from(host.querySelectorAll(".meta-label")).map((el) => el.textContent?.trim());

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
});

describe("TableDetailPanel", () => {
  it("shows a local table its Read by card, as before", async () => {
    await mount(table());

    expect(metaLabels()).toContain("Read by");
    expect(host.querySelector('[data-testid="hub-badge"]')).toBeNull();
  });

  it("shows a hub table no Hub badge and no Read by card, whose flows are the hub's", async () => {
    await mount(hubTable);

    expect(metaLabels()).not.toContain("Read by");
    expect(metaLabels()).not.toContain("Change tracking");
    expect(host.querySelector('[data-testid="hub-badge"]')).toBeNull();
  });
});
