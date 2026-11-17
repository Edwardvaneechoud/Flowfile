import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import type { APIRequestContext } from "@playwright/test";
import { API_URL, authHeaders } from "./api";

const dimension = (fid: string) => ({
  fid,
  name: fid,
  basename: fid,
  semanticType: "nominal",
  analyticType: "dimension",
  offset: 0,
});

const measure = (fid: string) => ({
  fid,
  name: fid,
  basename: fid,
  semanticType: "quantitative",
  analyticType: "measure",
  aggName: "sum",
  offset: 0,
});

/** A Graphic Walker bar chart of sum(amount) per region, as the chart builder exports one;
 * `colorByRegion` also colours each bar by its region. */
export const regionBarChart = ({ colorByRegion = false } = {}) => ({
  visId: "e2e-region-bar",
  name: "Chart 1",
  config: { defaultAggregated: true, geoms: ["bar"], coordSystem: "generic", limit: -1 },
  encodings: {
    dimensions: [dimension("region")],
    measures: [measure("amount")],
    rows: [{ ...dimension("region"), sort: "descending" }],
    columns: [measure("amount")],
    color: colorByRegion ? [dimension("region")] : [],
    opacity: [],
    size: [],
    shape: [],
    radius: [],
    theta: [],
    longitude: [],
    latitude: [],
    geoId: [],
    details: [],
    filters: [],
    text: [],
  },
  layout: {
    showActions: false,
    showTableSummary: false,
    stack: "stack",
    interactiveScale: false,
    zeroScale: true,
    size: { mode: "full", width: 320, height: 200 },
    format: {},
    geoKey: "name",
    resolve: { x: false, y: false, color: false, opacity: false, shape: false, size: false },
  },
});

// West 150, East 70, North 30, South 20, Central 0: 270 in all, over 7 rows.
const ORDERS_CSV = [
  "region,amount",
  "West,100",
  "West,50",
  "East,70",
  "North,30",
  "South,20",
  "Central,0",
  "Central,0",
].join("\n");

export interface SeededDashboard {
  tableId: number;
  vizId: number;
  dashboardId: number;
  cleanup: (request: APIRequestContext) => Promise<void>;
}

/**
 * A catalog table, one chart on it and a dashboard with a titled chart tile, a KPI, a KPI still to
 * be set up, a text tile, a table tile that reads the table directly (five rows a page, largest
 * amount first) and a region filter. Core reads the CSV from this machine's temp dir.
 * `colorByRegion` colours the chart's bars by region.
 */
export async function seedDashboard(
  request: APIRequestContext,
  token: string,
  name: string,
  { colorByRegion = false } = {},
): Promise<SeededDashboard> {
  const headers = authHeaders(token);
  const dir = mkdtempSync(join(tmpdir(), "flowfile-e2e-dashboard-"));
  const csvPath = join(dir, "orders.csv");
  writeFileSync(csvPath, ORDERS_CSV);

  const post = async (path: string, data: unknown) => {
    const response = await request.post(`${API_URL}${path}`, { headers, data });
    if (!response.ok())
      throw new Error(`POST ${path}: ${response.status()} ${await response.text()}`);
    return response.json();
  };

  const created: Created = {};
  // Removes what was made so far, newest first, so a failed seed leaves nothing behind.
  const cleanup = async (req: APIRequestContext) => {
    if (created.dashboard != null)
      await req.delete(`${API_URL}/catalog/dashboards/${created.dashboard}`, { headers });
    if (created.viz != null)
      await req.delete(`${API_URL}/catalog/visualizations/${created.viz}`, { headers });
    if (created.table != null)
      await req.delete(`${API_URL}/catalog/tables/${created.table}`, { headers });
    rmSync(dir, { recursive: true, force: true });
  };

  try {
    await seedRows(post, name, csvPath, created, colorByRegion);
  } catch (err) {
    await cleanup(request).catch(() => undefined);
    throw err;
  }
  return {
    tableId: created.table!,
    vizId: created.viz!,
    dashboardId: created.dashboard!,
    cleanup,
  };
}

interface Created {
  table?: number;
  viz?: number;
  dashboard?: number;
}

/** Creates the table, the chart and the dashboard, noting each id in `created` once it exists. */
async function seedRows(
  post: (path: string, data: unknown) => Promise<{ id: number }>,
  name: string,
  csvPath: string,
  created: Created,
  colorByRegion: boolean,
): Promise<void> {
  // No spaces: a chart on a table whose name has one cannot read its files.
  const tableName = `${name.replace(/\W+/g, "_")}_orders`;
  const table = await post("/catalog/tables", { name: tableName, file_path: csvPath });
  created.table = table.id;
  const viz = await post("/catalog/visualizations", {
    name: `${name} chart`,
    spec: [regionBarChart({ colorByRegion })],
    source_type: "table",
    catalog_table_id: table.id,
  });
  created.viz = viz.id;
  const dashboard = await post("/catalog/dashboards", {
    name,
    layout: {
      grid: { cols: 48, row_height: 40, version: 2 },
      tiles: [
        {
          id: "t-chart",
          type: "viz",
          viz_id: viz.id,
          title: "Revenue by region",
          x: 0,
          y: 0,
          w: 24,
          h: 8,
        },
        {
          id: "t-kpi",
          type: "kpi",
          viz_id: viz.id,
          kpi: { field: "amount", agg: "sum", label: "Total amount", compact: false },
          x: 24,
          y: 0,
          w: 12,
          h: 4,
        },
        { id: "t-empty", type: "kpi", viz_id: null, kpi: null, x: 36, y: 0, w: 12, h: 4 },
        { id: "t-text", type: "text", viz_id: null, text_md: "## Notes", x: 24, y: 4, w: 24, h: 3 },
        {
          id: "t-table",
          type: "table",
          viz_id: null,
          table_id: table.id,
          title: "Orders",
          table: {
            columns: [{ field: "region", label: "Region" }, { field: "amount" }],
            sort_by: ["amount"],
            sort_direction: "descending",
            page_size: 5,
          },
          x: 0,
          y: 8,
          w: 24,
          h: 8,
        },
      ],
      filters: [
        {
          id: "f-region",
          field_name: "region",
          label: "Region",
          kind: "categorical",
          state: { selected: [] },
          target: "all",
          target_tile_ids: [],
          datasource_id: table.id,
          scope: "datasource",
        },
      ],
    },
  });

  created.dashboard = dashboard.id;
}

export interface SeededWideFilter {
  dashboardId: number;
  cleanup: (request: APIRequestContext) => Promise<void>;
}

/**
 * A table of 150 distinct codes (C001..C150) and a dashboard with a table tile on it and a code
 * filter, so the filter's first value list (100 values) leaves codes out that only a search finds.
 */
export async function seedWideFilterDashboard(
  request: APIRequestContext,
  token: string,
  name: string,
): Promise<SeededWideFilter> {
  const headers = authHeaders(token);
  const dir = mkdtempSync(join(tmpdir(), "flowfile-e2e-wide-filter-"));
  const csvPath = join(dir, "codes.csv");
  const codes = Array.from({ length: 150 }, (_, i) => `C${String(i + 1).padStart(3, "0")},${i}`);
  writeFileSync(csvPath, ["code,amount", ...codes].join("\n"));

  let tableId: number | undefined;
  let dashboardId: number | undefined;
  const cleanup = async (req: APIRequestContext) => {
    if (dashboardId != null)
      await req.delete(`${API_URL}/catalog/dashboards/${dashboardId}`, { headers });
    if (tableId != null) await req.delete(`${API_URL}/catalog/tables/${tableId}`, { headers });
    rmSync(dir, { recursive: true, force: true });
  };
  const post = async (path: string, data: unknown) => {
    const response = await request.post(`${API_URL}${path}`, { headers, data });
    if (!response.ok())
      throw new Error(`POST ${path}: ${response.status()} ${await response.text()}`);
    return response.json();
  };

  try {
    const table = await post("/catalog/tables", {
      name: `${name.replace(/\W+/g, "_")}_codes`,
      file_path: csvPath,
    });
    tableId = table.id;
    const dashboard = await post("/catalog/dashboards", {
      name,
      layout: {
        grid: { cols: 48, row_height: 40, version: 2 },
        tiles: [
          {
            id: "t-codes",
            type: "table",
            viz_id: null,
            table_id: table.id,
            title: "Codes",
            table: { columns: [], sort_by: ["code"], sort_direction: "ascending", page_size: 5 },
            x: 0,
            y: 0,
            w: 24,
            h: 8,
          },
        ],
        filters: [
          {
            id: "f-code",
            field_name: "code",
            label: "Code",
            kind: "categorical",
            state: { selected: [] },
            target: "all",
            target_tile_ids: [],
            datasource_id: table.id,
            scope: "datasource",
          },
        ],
      },
    });
    dashboardId = dashboard.id;
  } catch (err) {
    await cleanup(request).catch(() => undefined);
    throw err;
  }
  return { dashboardId: dashboardId!, cleanup };
}

export interface SeededTrend {
  dashboardId: number;
  cleanup: (request: APIRequestContext) => Promise<void>;
}

// Per month: Jan 100, Feb 120 (West 50 + East 70), no March, Apr 30, May 20; 270 in all.
const DATED_ORDERS_CSV = [
  "region,amount,order_date",
  "West,100,2024-01-03",
  "West,50,2024-02-10",
  "East,70,2024-02-20",
  "North,30,2024-04-02",
  "South,20,2024-05-15",
  "Central,0,",
].join("\n");

/**
 * A dashboard with one KPI (sum of amount) whose trend is drawn per month, and a region filter.
 * A CSV table keeps its dates as text, so the KPI reads a query virtual table that casts them.
 */
export async function seedTrendDashboard(
  request: APIRequestContext,
  token: string,
  name: string,
): Promise<SeededTrend> {
  const headers = authHeaders(token);
  const dir = mkdtempSync(join(tmpdir(), "flowfile-e2e-trend-"));
  const csvPath = join(dir, "dated_orders.csv");
  writeFileSync(csvPath, DATED_ORDERS_CSV);

  const ids: { table?: number; dated?: number; dashboard?: number } = {};
  const cleanup = async (req: APIRequestContext) => {
    if (ids.dashboard != null)
      await req.delete(`${API_URL}/catalog/dashboards/${ids.dashboard}`, { headers });
    if (ids.dated != null) await req.delete(`${API_URL}/catalog/tables/${ids.dated}`, { headers });
    if (ids.table != null) await req.delete(`${API_URL}/catalog/tables/${ids.table}`, { headers });
    rmSync(dir, { recursive: true, force: true });
  };
  const post = async (path: string, data: unknown) => {
    const response = await request.post(`${API_URL}${path}`, { headers, data });
    if (!response.ok())
      throw new Error(`POST ${path}: ${response.status()} ${await response.text()}`);
    return response.json();
  };

  try {
    const stem = name.replace(/\W+/g, "_");
    const table = await post("/catalog/tables", { name: `${stem}_raw`, file_path: csvPath });
    ids.table = table.id;
    const dated = await post("/catalog/query-virtual-tables", {
      name: `${stem}_dated`,
      sql_query: `SELECT region, amount, CAST(order_date AS DATE) AS order_date FROM ${table.name}`,
    });
    ids.dated = dated.id;
    const dashboard = await post("/catalog/dashboards", {
      name,
      layout: {
        grid: { cols: 48, row_height: 40, version: 2 },
        tiles: [
          {
            id: "t-trend",
            type: "kpi",
            viz_id: null,
            table_id: dated.id,
            kpi: {
              field: "amount",
              agg: "sum",
              label: "Total amount",
              compact: false,
              trend: { field: "order_date", grain: "month" },
            },
            x: 0,
            y: 0,
            w: 16,
            h: 5,
          },
        ],
        filters: [
          {
            id: "f-region",
            field_name: "region",
            label: "Region",
            kind: "categorical",
            state: { selected: [] },
            target: "all",
            target_tile_ids: [],
            datasource_id: dated.id,
            scope: "datasource",
          },
        ],
      },
    });
    ids.dashboard = dashboard.id;
  } catch (err) {
    await cleanup(request).catch(() => undefined);
    throw err;
  }
  return { dashboardId: ids.dashboard!, cleanup };
}
