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

/** A Graphic Walker bar chart of sum(amount) per region, as the chart builder exports one. */
export const regionBarChart = () => ({
  visId: "e2e-region-bar",
  name: "Chart 1",
  config: { defaultAggregated: true, geoms: ["bar"], coordSystem: "generic", limit: -1 },
  encodings: {
    dimensions: [dimension("region")],
    measures: [measure("amount")],
    rows: [{ ...dimension("region"), sort: "descending" }],
    columns: [measure("amount")],
    color: [],
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

// West 150, East 70, North 30, South 20: 270 in all.
const ORDERS_CSV = ["region,amount", "West,100", "West,50", "East,70", "North,30", "South,20"].join(
  "\n",
);

export interface SeededDashboard {
  tableId: number;
  vizId: number;
  dashboardId: number;
  cleanup: (request: APIRequestContext) => Promise<void>;
}

/**
 * A catalog table, one chart on it and a dashboard with a titled chart tile, a KPI, a KPI still to
 * be set up, a text tile and a region filter. Core reads the CSV from this machine's temp dir.
 */
export async function seedDashboard(
  request: APIRequestContext,
  token: string,
  name: string,
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

  // No spaces: a chart on a table whose name has one cannot read its files.
  const tableName = `${name.replace(/\W+/g, "_")}_orders`;
  const table = await post("/catalog/tables", { name: tableName, file_path: csvPath });
  const viz = await post("/catalog/visualizations", {
    name: `${name} chart`,
    spec: [regionBarChart()],
    source_type: "table",
    catalog_table_id: table.id,
  });
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

  return {
    tableId: table.id,
    vizId: viz.id,
    dashboardId: dashboard.id,
    cleanup: async (req) => {
      await req.delete(`${API_URL}/catalog/dashboards/${dashboard.id}`, { headers });
      await req.delete(`${API_URL}/catalog/visualizations/${viz.id}`, { headers });
      await req.delete(`${API_URL}/catalog/tables/${table.id}`, { headers });
      rmSync(dir, { recursive: true, force: true });
    },
  };
}
