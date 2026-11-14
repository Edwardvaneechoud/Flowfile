// @vitest-environment happy-dom
// The table panel for a hub table: no Hub badge, and no "Read by" card, whose flow ids are the hub's and would open a
// local flow of the same id; a local table keeps both as before.
import { defineComponent, h, reactive, type FunctionalComponent } from "vue";
import { describe, expect, it, vi } from "vitest";
import { table } from "../../__fixtures__/catalogTree";
import { element, mountIn, settle, text } from "../../__fixtures__/mount";
import type { CatalogTable, CatalogTablePreview, DeltaTableHistory } from "../../types";

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
  const render = () => h("span", { "data-testid": "maintenance" });
  return { default: Object.assign(render, { props: ["table"] }) };
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
  is_remote_storage: true,
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
  it("shows a local table its Read by card, and a hub table none, whose flows are the hub's", async () => {
    await mount(localTable);
    expect(metaLabels()).toContain("Read by");

    await mount(hubTable);
    expect(metaLabels()).not.toContain("Read by");
    expect(metaLabels()).toContain("Change tracking");
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
    expect(host.querySelector('[data-testid="maintenance"]')).not.toBeNull();
    expect(host.querySelector('[data-testid="edit-data"]')).not.toBeNull();
    expect(host.querySelector(".viz-section")).toBeNull();
  });

  describe("layout while data is pending", () => {
    const physical = table({
      ...localTable,
      row_count: 3,
      column_count: 2,
      schema_columns: [
        { name: "id", dtype: "Int64" },
        { name: "amount", dtype: "Float64" },
      ],
    } as Partial<CatalogTable>);
    const preview: CatalogTablePreview = {
      columns: ["id", "amount"],
      dtypes: ["Int64", "Float64"],
      rows: [
        [1, 10.5],
        [2, 20.25],
        [3, 30],
      ],
      total_rows: 3,
    };
    const history: DeltaTableHistory = {
      current_version: 2,
      history: [
        { version: 2, timestamp: null, operation: "WRITE", parameters: null },
        { version: 1, timestamp: null, operation: "WRITE", parameters: null },
      ],
    } as DeltaTableHistory;

    const mountLive = async (row: CatalogTable, initial: Record<string, unknown>) => {
      const state = reactive({
        table: row,
        preview: null as CatalogTablePreview | null,
        loadingPreview: false,
        previewError: null as string | null,
        tableHistory: null as DeltaTableHistory | null,
        loadingTableHistory: false,
        tableHistoryStale: false,
        selectedVersion: null as number | null,
        ...initial,
      });
      const Host = defineComponent({ setup: () => () => h(TableDetailPanel, { ...state }) });
      host = await mountIn(Host, { stubs });
      return state;
    };

    const sectionTitles = () =>
      Array.from(host.querySelectorAll(".section h3")).map((el) => el.textContent?.trim());
    const q = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);

    it("lays out every section from the first paint and fills the slots as responses land", async () => {
      const state = await mountLive(physical, { loadingPreview: true, loadingTableHistory: true });
      const pendingTitles = sectionTitles();
      expect(pendingTitles).toEqual(["Version History", "Schema", "Data Preview"]);
      expect(q("table-version")?.querySelector(".skeleton-inline")).not.toBeNull();
      expect(q("history-skeleton")?.querySelectorAll("tbody tr")).toHaveLength(4);
      const skeleton = q("preview-skeleton");
      expect(
        Array.from(skeleton?.querySelectorAll("thead .col-header-name") ?? []).map(text),
      ).toEqual(["id", "amount"]);
      expect(skeleton?.querySelectorAll("tbody tr")).toHaveLength(3);
      expect(text(q("preview-loading"))).toBe("Loading preview…");

      Object.assign(state, { preview, loadingPreview: false });
      await settle();
      expect(sectionTitles()).toEqual(pendingTitles);
      expect(q("preview-skeleton")).toBeNull();
      expect(q("preview-table")?.querySelectorAll("tbody tr")).toHaveLength(3);
      expect(q("history-skeleton")).not.toBeNull();

      Object.assign(state, { tableHistory: history, loadingTableHistory: false });
      await settle();
      expect(sectionTitles()).toEqual(pendingTitles);
      expect(q("history-skeleton")).toBeNull();
      expect(host.querySelectorAll(".version-row")).toHaveLength(2);
      expect(text(q("table-version")?.querySelector(".meta-value") ?? null)).toBe("2");
    });

    it("keeps the Version History section, as one line, for a table whose history is empty", async () => {
      const state = await mountLive(physical, { loadingTableHistory: true, preview });
      Object.assign(state, { loadingTableHistory: false });
      await settle();
      expect(sectionTitles()).toEqual(["Version History", "Schema", "Data Preview"]);
      expect(text(q("history-empty"))).toBe("No version history available for this table.");
      expect(text(q("table-version")?.querySelector(".meta-value") ?? null)).toBe("--");
    });

    it("keeps the rows on screen, marked as refreshing, while another version loads", async () => {
      const state = await mountLive(physical, { preview, tableHistory: history });
      Object.assign(state, { selectedVersion: 1, loadingPreview: true });
      await settle();
      const shown = q("preview-table");
      expect(shown?.querySelectorAll("tbody tr")).toHaveLength(3);
      expect(shown?.classList.contains("is-refreshing")).toBe(true);
      expect(shown?.getAttribute("aria-busy")).toBe("true");
      expect(q("preview-skeleton")).toBeNull();
      expect(text(q("preview-loading"))).toBe("Refreshing…");
      expect(text(host.querySelector(".version-banner"))).toContain("Viewing version 1");

      Object.assign(state, {
        preview: { ...preview, rows: [[1, 9]], total_rows: 1 },
        loadingPreview: false,
      });
      await settle();
      expect(q("preview-table")?.classList.contains("is-refreshing")).toBe(false);
      expect(q("preview-table")?.querySelectorAll("tbody tr")).toHaveLength(1);
      expect(q("preview-loading")).toBeNull();
    });

    it("shows the error with Back to latest when an older version cannot be read", async () => {
      const state = await mountLive(physical, {
        preview,
        tableHistory: history,
        selectedVersion: 1,
      });
      Object.assign(state, { preview: null, previewError: "files vacuumed" });
      await settle();
      expect(q("preview-table")).toBeNull();
      expect(text(host.querySelector(".preview-error"))).toContain("files vacuumed");
      expect(text(host.querySelector(".preview-error"))).toContain("Back to latest");
    });

    it("gives a virtual table no Version History section and no Version card", async () => {
      await mountLive(table({ ...localTable, table_type: "virtual" } as Partial<CatalogTable>), {});
      expect(q("version-history")).toBeNull();
      expect(metaLabels()).not.toContain("Version");
    });

    it("keeps the Load preview button for an object-storage table", async () => {
      await mountLive(table({ ...physical, is_remote_storage: true } as Partial<CatalogTable>), {
        tableHistory: history,
      });
      expect(Array.from(host.querySelectorAll(".empty-state")).map(text)).toContain("Load preview");
    });
  });
});
