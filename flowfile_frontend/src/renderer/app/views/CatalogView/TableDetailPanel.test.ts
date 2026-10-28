// @vitest-environment happy-dom
import { createApp, h, nextTick, reactive, type App } from "vue";
import { ElButton, ElPopover } from "element-plus";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { CatalogTable, CatalogTablePreview, DeltaTableHistory } from "../../types";

vi.mock("./VisualizationsTab.vue", () => ({ default: { render: () => null } }));
vi.mock("./TableMaintenance.vue", () => ({ default: { render: () => null } }));
vi.mock("./TableEditorDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/ShareDialog.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/sharing/SharedBadge.vue", () => ({ default: { render: () => null } }));
vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));
vi.mock("../../composables/useResourceSharing", () => ({
  useResourceSharing: () => ({
    canShare: () => false,
    canManage: () => false,
    canManageGrants: () => false,
  }),
}));

import TableDetailPanel from "./TableDetailPanel.vue";

const table: CatalogTable = {
  id: 1,
  name: "example",
  namespace_id: null,
  namespace_name: null,
  full_table_name: null,
  qualified_name: null,
  description: null,
  owner_id: 1,
  file_exists: true,
  is_remote_storage: false,
  is_favorite: false,
  schema_columns: [],
  row_count: 10,
  column_count: 1,
  size_bytes: null,
  source_registration_id: null,
  source_registration_name: null,
  source_run_id: null,
  prediction_table_id: null,
  prediction_table_name: null,
  read_by_flows: [],
  table_type: "physical",
  producer_registration_id: null,
  producer_registration_name: null,
  is_optimized: null,
  laziness_blockers: null,
  sql_query: null,
  polars_plan: null,
  source_table_versions: null,
  partition_columns: null,
  scd2: null,
  cdc_enabled: false,
  cdc_enabled_version: null,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};

const preview = (total: number): CatalogTablePreview => ({
  columns: ["a"],
  dtypes: ["int64"],
  rows: total ? [[1]] : [],
  total_rows: total,
});

let app: App | null = null;
let host: HTMLDivElement;
function mount(initialPreview: CatalogTablePreview | null) {
  const props = reactive({
    table: { ...table },
    preview: initialPreview,
    loadingPreview: false,
    previewError: null as string | null,
    tableHistory: null as DeltaTableHistory | null,
    loadingTableHistory: false,
    tableHistoryStale: false,
    selectedVersion: null as number | null,
  });
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp({ render: () => h(TableDetailPanel, { ...props }) });
  app.component("ElPopover", ElPopover);
  app.component("ElButton", ElButton);
  app.mount(host);
  return props;
}

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
});

describe("catalog Rows summary", () => {
  it.each([250, 0])("uses the preview total %i rather than stale catalog metadata", (total) => {
    const props = mount(preview(total));
    expect(host.querySelector(".meta-card .meta-value")?.textContent).toBe(String(total));
    expect(host.querySelector(".preview-info")?.textContent).toContain(`of ${total} rows`);
    expect(props.table.row_count).toBe(10);
  });

  it("keeps catalog metadata as the fallback before a preview is loaded", async () => {
    const props = mount(null);
    expect(host.querySelector(".meta-card .meta-value")?.textContent).toBe("10");
    props.table.row_count = null;
    await nextTick();
    expect(host.querySelector(".meta-card .meta-value")?.textContent).toBe("--");
  });

  it("uses the selected historical preview without overwriting current catalog metadata", async () => {
    const props = mount(preview(250));
    props.selectedVersion = 0;
    props.tableHistory = { current_version: 1, history: [] };
    props.preview = preview(23);
    await nextTick();
    expect(host.querySelector(".meta-card .meta-value")?.textContent).toBe("23");
    expect(host.querySelector(".preview-info")?.textContent).toContain("of 23 rows");
    expect(host.querySelector(".version-banner")?.textContent).toContain("Viewing version 0");
    expect(props.table.row_count).toBe(10);
  });

  it("follows refresh results and returns to metadata after preview failure", async () => {
    const props = mount(preview(250));
    props.preview = preview(300);
    await nextTick();
    expect(host.querySelector(".meta-card .meta-value")?.textContent).toBe("300");
    props.preview = null;
    props.previewError = "Preview unavailable";
    await nextTick();
    expect(host.querySelector(".meta-card .meta-value")?.textContent).toBe("10");
    expect(host.querySelector(".preview-info")).toBeNull();
    expect(props.table.row_count).toBe(10);
  });
});
