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
    canShare: () => true,
    canManage: () => true,
    canManageGrants: () => false,
    isShared: () => false,
    sharedLabel: () => "",
    sharedTitle: () => "",
  }),
}));
vi.mock("./VisualizationsTab.vue", () => ({ default: { render: () => null } }));
vi.mock("./TableMaintenance.vue", async () => {
  const { h } = await import("vue");
  const render = (props: { remote?: boolean }) =>
    h("span", { "data-testid": "maintenance", "data-remote": String(props.remote) });
  return { default: Object.assign(render, { props: ["table", "remote"] }) };
});
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
const hubTable = table({
  ...localTable,
  id: -3,
  source: "hub",
  hub_id: 3,
  hub_namespace_id: 2,
  file_exists: false,
});

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
    expect(metaLabels()).toContain("Change tracking");
    expect(host.querySelector('[data-testid="hub-badge"]')).toBeNull();
  });

  it("gives a hub table Favourite, Delete, Share, Edit data and the maintenance a local table has", async () => {
    const favourited = vi.fn();
    const deleted = vi.fn();
    host = await mountIn(TableDetailPanel, {
      props: {
        table: hubTable,
        preview: null,
        loadingPreview: false,
        previewError: null,
        tableHistory: null,
        loadingTableHistory: false,
        tableHistoryStale: false,
        selectedVersion: null,
        onToggleTableFavorite: favourited,
        onDeleteTable: deleted,
      },
      stubs,
    });
    (host.querySelector('[data-testid="table-favorite"]') as HTMLButtonElement).click();
    expect(favourited).toHaveBeenCalledWith(-3);
    (host.querySelector('[data-testid="table-delete"]') as HTMLButtonElement).click();
    expect(deleted).toHaveBeenCalledWith(-3);
    expect(host.querySelector('[title="Share table"]')).not.toBeNull();
    expect(host.querySelector('[data-testid="query-table"]')?.hasAttribute("disabled")).toBe(false);
    expect(host.querySelector('[data-testid="hub-create-viz"]')).not.toBeNull();
    expect(host.querySelector('[data-testid="maintenance"]')?.getAttribute("data-remote")).toBe(
      "true",
    );
    expect(host.querySelector('[data-testid="edit-data"]')).not.toBeNull();
    expect(host.querySelector(".viz-section")).toBeNull();
  });
});
