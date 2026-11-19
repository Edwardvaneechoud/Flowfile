import { mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import type { APIRequestContext } from "@playwright/test";
import { API_URL, authHeaders } from "./api";
import { regionBarChart } from "./dashboards";

// West 150, East 70, North 30, South 20: 270 in all.
const SALES_CSV = [
  "region,category,amount",
  "West,Tools,100",
  "West,Toys,50",
  "East,Tools,70",
  "North,Toys,30",
  "South,Tools,20",
].join("\n");

export interface SeededCharts {
  tableId: number;
  tableName: string;
  /** A bar chart of sum(amount) per region on the table. */
  tableVizId: number;
  /** A chart on a free SQL query over the table. */
  sqlVizId: number;
  /** The bar chart with region also on Details, a part a chart request cannot express. */
  detailVizId: number;
  cleanup: (request: APIRequestContext) => Promise<void>;
}

/**
 * A catalog table with two charts on it (one the AI can describe, one it cannot) and a chart on a
 * SQL query over it. Core reads the CSV from this machine's temp dir.
 */
export async function seedCharts(
  request: APIRequestContext,
  token: string,
  name: string,
): Promise<SeededCharts> {
  const headers = authHeaders(token);
  const dir = mkdtempSync(join(tmpdir(), "flowfile-e2e-chart-ai-"));
  const csvPath = join(dir, "sales.csv");
  writeFileSync(csvPath, SALES_CSV);

  const ids: { table?: number; vizzes: number[] } = { vizzes: [] };
  const cleanup = async (req: APIRequestContext) => {
    // Charts the spec saved itself are found by table, so they go too.
    if (ids.table != null) {
      const listed = await req.get(`${API_URL}/catalog/tables/${ids.table}/visualizations`, {
        headers,
      });
      if (listed.ok()) {
        for (const viz of (await listed.json()) as { id: number }[]) {
          if (!ids.vizzes.includes(viz.id)) ids.vizzes.push(viz.id);
        }
      }
    }
    for (const id of ids.vizzes) {
      await req.delete(`${API_URL}/catalog/visualizations/${id}`, { headers });
    }
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
    // No spaces: a chart on a table whose name has one cannot read its files.
    const tableName = `${name.replace(/\W+/g, "_")}_sales`;
    // In a namespace, so the catalog tree lists it and the table page opens from the URL.
    const nsResponse = await request.get(`${API_URL}/catalog/default-namespace-id`, { headers });
    const namespaceId = nsResponse.ok() ? await nsResponse.json() : null;
    const table = await post("/catalog/tables", {
      name: tableName,
      file_path: csvPath,
      namespace_id: namespaceId,
    });
    ids.table = table.id;
    const viz = await post("/catalog/visualizations", {
      name: `${name} chart`,
      chart_type: "bar",
      spec: [regionBarChart()],
      source_type: "table",
      catalog_table_id: table.id,
    });
    ids.vizzes.push(viz.id);
    const sqlViz = await post("/catalog/visualizations", {
      name: `${name} query chart`,
      spec: [regionBarChart()],
      source_type: "sql",
      sql_query: `SELECT region, amount FROM ${table.name}`,
    });
    ids.vizzes.push(sqlViz.id);
    const detailChart = regionBarChart();
    detailChart.encodings.details = [{ ...detailChart.encodings.dimensions[0] }] as never[];
    const detailViz = await post("/catalog/visualizations", {
      name: `${name} detail chart`,
      spec: [detailChart],
      source_type: "table",
      catalog_table_id: table.id,
    });
    ids.vizzes.push(detailViz.id);
    return {
      tableId: table.id,
      tableName: table.name,
      tableVizId: viz.id,
      sqlVizId: sqlViz.id,
      detailVizId: detailViz.id,
      cleanup,
    };
  } catch (err) {
    await cleanup(request).catch(() => undefined);
    throw err;
  }
}
