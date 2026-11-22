import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "fs";
import { tmpdir } from "os";
import { join } from "path";
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { API_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";

// An AI-built page with a map, drawn for real with the AI routes stubbed (needs a worker, else skips).

type Json = Record<string, any>;

// DASHBOARD_AI_SCREENS=<dir> also writes screenshots there.
const SCREENS = process.env.DASHBOARD_AI_SCREENS;

async function shot(page: Page, name: string) {
  if (!SCREENS) return;
  mkdirSync(SCREENS, { recursive: true });
  await page.screenshot({ path: join(SCREENS, `${name}.png`) });
}

const DISTRICTS = ["Noord", "Oost", "Zuid", "West", "Centrum", "outside"];
const CUSTOMERS_CSV = [
  "customer,expected_district,lat,lon,order_value,km_to_depot",
  ...Array.from({ length: 36 }, (_, i) => {
    const district = DISTRICTS[i % DISTRICTS.length];
    const lat = (52.3 + (i % 7) * 0.02).toFixed(4);
    const lon = (4.85 + (i % 5) * 0.03).toFixed(4);
    return `c${i},${district},${lat},${lon},${100 + i * 7},${(2 + (i % 9) * 3.5).toFixed(1)}`;
  }),
].join("\n");

const BRIEFS = {
  map: "Map of points at latitude lat and longitude lon, coloured by expected_district, sized by order_value.",
  value: "Bar chart of the sum of order_value by expected_district, sorted descending.",
  count: "Bar chart of the count of rows by expected_district, sorted descending.",
  depot: "Map of the depot at depot_lat and depot_lon on the same map as the customers.",
  scatter: "Scatter plot of km_to_depot on x against order_value on y, one point per row.",
};

const check = (id: string, title: string, brief: string, tableId: number): Json => ({
  id,
  table_id: tableId,
  title,
  brief,
  purpose: `What ${title.toLowerCase()} shows.`,
  relation: null,
});

const planFor = (tableId: number): Json => ({
  plan: {
    title: "Customer locations and depot distance",
    kpis: [
      { id: "k1", table_id: tableId, label: "Customers", field: null, agg: "count" },
      { id: "k2", table_id: tableId, label: "Order value", field: "order_value", agg: "sum" },
      { id: "k3", table_id: tableId, label: "Median km", field: "km_to_depot", agg: "median" },
    ],
    checks: [
      check("c1", "Customer locations", BRIEFS.map, tableId),
      check("c2", "Order value by district", BRIEFS.value, tableId),
      check("c3", "Customers by district", BRIEFS.count, tableId),
      check("c4", "Depot on the map", BRIEFS.depot, tableId),
      check("c5", "Order value versus depot distance", BRIEFS.scatter, tableId),
    ],
    filters: [
      {
        id: "f1",
        table_id: tableId,
        field: "expected_district",
        kind: "categorical",
        label: "Expected district",
      },
    ],
  },
  explanation: null,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
  provider: "openai",
  model: "gpt-test-planner",
});

const chartAnswer = (chart: Json | null, explanation: string | null = null): Json => ({
  chart,
  title: null,
  explanation,
  warnings: [],
  degraded: chart === null,
  reason: chart === null ? "no_chart" : null,
  message: null,
  attempts: 1,
  repairs: [],
  provider: "openai",
  model: "gpt-test-charts",
});

const CHARTS: Record<string, Json> = {
  [BRIEFS.map]: chartAnswer({
    mark: "map",
    latitude: { field: "lat" },
    longitude: { field: "lon" },
    color: { field: "expected_district" },
    size: { field: "order_value" },
    aggregate: false,
  }),
  [BRIEFS.value]: chartAnswer({
    mark: "bar",
    x: { field: "expected_district" },
    y: { field: "order_value", aggregate: "sum" },
    sort: "descending",
  }),
  [BRIEFS.count]: chartAnswer({
    mark: "bar",
    x: { field: "expected_district" },
    y: { field: "*", aggregate: "count" },
    sort: "descending",
  }),
  [BRIEFS.depot]: chartAnswer(null, "A map shows one set of points; the depot cannot be added."),
  [BRIEFS.scatter]: chartAnswer({
    mark: "point",
    x: { field: "km_to_depot" },
    y: { field: "order_value" },
    aggregate: false,
  }),
};

const STORY: Json = {
  story: {
    title: "Customer locations and depot distance",
    summary_md: "Customers spread over six districts; order value does not follow distance.",
    charts: [
      {
        id: "c1",
        title: "Customer locations",
        finding: "Most customers sit near the centre.",
        width: "full",
      },
      { id: "c2", title: "Order value by district", finding: null, width: "half" },
      { id: "c3", title: "Customers by district", finding: null, width: "half" },
      { id: "c5", title: "Order value versus depot distance", finding: null, width: "full" },
    ],
    left_out: [],
  },
  grounded: true,
  evidence: [],
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
  provider: "openai",
  model: "gpt-test-planner",
};

async function stubAi(page: Page, tableId: number) {
  const answer = (route: Parameters<Parameters<Page["route"]>[1]>[0], body: Json) =>
    route
      .fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) })
      .catch(() => undefined);
  await page.route(/\/ai\/providers(\?|$)/, (route) =>
    answer(route, [
      {
        provider: "openai",
        supports_tools: true,
        supports_streaming: true,
        default_model: "gpt-test",
        surfaces: {},
        status: "configured",
        credential: null,
      },
    ] as unknown as Json),
  );
  await page.route(/\/ai\/plan_dashboard(\?|$)/, (route) => answer(route, planFor(tableId)));
  await page.route(/\/ai\/generate_chart(\?|$)/, (route) => {
    const body = route.request().postDataJSON();
    return answer(route, CHARTS[body.prompt] ?? chartAnswer(null, "unknown brief"));
  });
  await page.route(/\/ai\/dashboard_story(\?|$)/, (route) => answer(route, STORY));
}

async function seedCustomers(request: APIRequestContext, token: string) {
  const headers = authHeaders(token);
  const dir = mkdtempSync(join(tmpdir(), "flowfile-e2e-dash-map-"));
  const csvPath = join(dir, "customers.csv");
  writeFileSync(csvPath, CUSTOMERS_CSV);
  const ns = await request.get(`${API_URL}/catalog/default-namespace-id`, { headers });
  const created = await request.post(`${API_URL}/catalog/tables`, {
    headers,
    data: {
      name: `e2e_customers_${Date.now()}`,
      file_path: csvPath,
      namespace_id: ns.ok() ? await ns.json() : null,
    },
  });
  if (!created.ok()) throw new Error(`table: ${created.status()} ${await created.text()}`);
  const table = (await created.json()) as { id: number; name: string };
  return {
    table,
    cleanup: async (req: APIRequestContext) => {
      await req.delete(`${API_URL}/catalog/tables/${table.id}`, { headers });
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

const tiles = (page: Page) => page.locator(".vgl-item:not(.vgl-item--placeholder)");

test.describe("AI dashboard builder: a map page", () => {
  let token: string;
  let seeded: Awaited<ReturnType<typeof seedCustomers>>;
  let dashboardId: number | null = null;

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    seeded = await seedCustomers(request, token);
  });

  test.afterAll(async ({ request }) => {
    if (dashboardId != null) {
      await request.delete(`${API_URL}/catalog/dashboards/${dashboardId}`, {
        headers: authHeaders(token),
      });
    }
    await seeded?.cleanup(request);
  });

  test("builds the map, logs every step and leaves the bottom padding in view", async ({
    page,
    request,
  }) => {
    const probe = await request.post(`${API_URL}/catalog/visualizations/fields`, {
      headers: authHeaders(token),
      data: { source: { source_type: "table", table_id: seeded.table.id } },
    });
    const fields = probe.ok() ? await probe.json() : null;
    test.skip(!probe.ok() || !!fields?.error, "chart columns unavailable: no flowfile_worker");

    await stubAi(page, seeded.table.id);
    await login(page, token, "#/main/dashboards/new?ai=1");
    const builder = page.getByTestId("dashboard-ai-builder");
    await expect(builder).toBeVisible({ timeout: 30000 });
    await builder.getByTestId("dashboard-ai-sources").click();
    await page.keyboard.type(seeded.table.name);
    await page.locator(".el-select-dropdown__item:visible", { hasText: seeded.table.name }).click();
    const asked = builder.getByTestId("dashboard-ai-question");
    await expect(async () => {
      await asked.click();
      await asked.fill("Where are the customers and does depot distance matter?");
      await expect(asked).toHaveValue(/depot distance/, { timeout: 1000 });
    }).toPass({ timeout: 15000 });
    await builder.getByTestId("dashboard-ai-build").click();

    const result = builder.getByTestId("dashboard-ai-result");
    await expect(result).toBeVisible({ timeout: 60000 });
    await expect(builder.getByTestId("dashboard-ai-failed")).toContainText("Depot on the map");

    const log = builder.getByTestId("dashboard-ai-log");
    await expect(log).toContainText("8 steps · 1 failed");
    await log.locator("summary").click();
    const entries = log.getByTestId("dashboard-ai-log-entry");
    await expect(entries).toHaveCount(8);
    const declined = entries.filter({ hasText: "Depot on the map" });
    await expect(declined).toHaveAttribute("data-status", "failed");
    await expect(declined).toContainText("no_chart");
    await expect(declined).toContainText("openai (gpt-test-charts)");
    await expect(entries.filter({ hasText: "Customer locations" }).first()).toHaveAttribute(
      "data-status",
      "ok",
    );
    await expect(entries.last()).toContainText("Add to dashboard");
    await shot(page, "1-result-with-log");

    await builder.getByTestId("dashboard-ai-done").click();
    await expect(builder).toBeHidden();
    await expect(page).toHaveURL(/#\/main\/dashboards\/\d+\/edit/, { timeout: 20000 });
    dashboardId = Number(/dashboards\/(\d+)\/edit/.exec(page.url())?.[1]);

    // Summary, three KPIs and four charts.
    await expect(tiles(page)).toHaveCount(8);
    const map = page.locator(".leaflet-container").first();
    await expect(map).toBeVisible({ timeout: 30000 });
    // Graphic Walker draws the points on Leaflet's canvas overlay, so its pixels are checked.
    const overlay = map.locator(".leaflet-overlay-pane canvas");
    await expect
      .poll(
        () =>
          overlay.evaluate((el) => {
            const canvas = el as HTMLCanvasElement;
            const data = canvas
              .getContext("2d")!
              .getImageData(0, 0, canvas.width, canvas.height).data;
            let painted = 0;
            for (let i = 3; i < data.length; i += 4) if (data[i] > 0) painted += 1;
            return painted;
          }),
        { timeout: 30000 },
      )
      .toBeGreaterThan(200);
    await expect(map.getByText("outside")).toBeVisible();
    if (SCREENS) await map.screenshot({ path: join(SCREENS, "2b-map-tile.png") });
    await expect(page.locator(".kpi-value").first()).toHaveText("36", { timeout: 30000 });
    await shot(page, "2-page-top");

    const canvas = page.locator(".canvas").first();
    await canvas.evaluate((el) => el.scrollTo({ top: el.scrollHeight }));
    await page.waitForTimeout(300);
    const box = await canvas.evaluate((el) => {
      const rect = el.getBoundingClientRect();
      const tileBottoms = [...el.querySelectorAll(".vgl-item:not(.vgl-item--placeholder)")].map(
        (t) => t.getBoundingClientRect().bottom,
      );
      return {
        canvasBottom: rect.bottom,
        viewport: window.innerHeight,
        lastTileBottom: Math.max(...tileBottoms),
        scrolledToEnd: el.scrollTop + el.clientHeight >= el.scrollHeight - 1,
      };
    });
    expect(box.scrolledToEnd).toBe(true);
    expect(box.canvasBottom).toBeLessThanOrEqual(box.viewport + 1);
    expect(box.canvasBottom - box.lastTileBottom).toBeGreaterThanOrEqual(44);
    await shot(page, "3-page-bottom");
  });
});
