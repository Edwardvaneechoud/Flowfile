// @vitest-environment happy-dom
// The table panel for a hub table: no Hub badge, and no "Read by" card, whose flow ids are the hub's and would open a
// local flow of the same id; a local table keeps both as before.
import { h, type FunctionalComponent } from "vue";
import { describe, expect, it, vi } from "vitest";
import { table } from "../../__fixtures__/catalogTree";
import { element, mountIn } from "../../__fixtures__/mount";
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

const stubs: Record<string, FunctionalComponent> = {
  "el-tooltip": (_, { slots }) => h("span", slots.default?.()),
  "el-popover": (_, { slots }) => h("span", slots.reference?.()),
  "el-button": element("button"),
};

const localTable = table({
  row_count: 1200,
  column_count: 4,
  size_bytes: 2048,
  created_at: "2026-01-01T00:00:00",
  read_by_flows: [{ id: 5, name: "Daily orders" }],
} as Partial<CatalogTable>);
const hubTable = table({ ...localTable, id: -3, source: "hub", hub_id: 3, hub_namespace_id: 2 });

let host: HTMLElement;

const mount = async (row: CatalogTable) => {
  host = await mountIn(TableDetailPanel, {
    props: {
      table: row,
      preview: null,
      loadingPreview: false,
      previewError: null,
      tableHistory: null,
      loadingTableHistory: false,
      tableHistoryStale: false,
      selectedVersion: null,
    },
    stubs,
  });
};

const metaLabels = () =>
  Array.from(host.querySelectorAll(".meta-label")).map((el) => el.textContent?.trim());

describe("TableDetailPanel", () => {
  it("shows a local table its Read by card, and a hub table none, whose flows are the hub's, and no Hub badge", async () => {
    await mount(localTable);
    expect(metaLabels()).toContain("Read by");
    expect(host.querySelector('[data-testid="hub-badge"]')).toBeNull();

    await mount(hubTable);
    expect(metaLabels()).not.toContain("Read by");
    expect(metaLabels()).not.toContain("Change tracking");
    expect(host.querySelector('[data-testid="hub-badge"]')).toBeNull();
  });
});
