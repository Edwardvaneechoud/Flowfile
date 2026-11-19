import { mkdirSync } from "fs";
import { join } from "path";
import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { API_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";
import { seedCharts, type SeededCharts } from "./helpers/charts";

// The AI dashboard builder (`DashboardAiBuilder.vue`) in the real editor with live data from the
// worker. The three AI routes are stubbed (`/ai/plan_dashboard`, `/ai/generate_chart`,
// `/ai/dashboard_story`); the charts are converted, saved, placed and drawn for real. Without a
// worker a table's columns cannot be read, so the tests skip (they do not pass).
//
// DASHBOARD_AI_SCREENS=<dir> also writes screenshots of the dialog and the built page there.

type Json = Record<string, any>;

const SCREENS = process.env.DASHBOARD_AI_SCREENS;

async function shot(page: Page, name: string) {
  if (!SCREENS) return;
  mkdirSync(SCREENS, { recursive: true });
  await page.screenshot({ path: join(SCREENS, `${name}.png`) });
}

const provider = (status: "configured" | "unconfigured", name: string) => ({
  provider: name,
  supports_tools: true,
  supports_streaming: true,
  default_model: "gpt-test",
  surfaces: {},
  status,
  credential: null,
});

async function stubProviders(page: Page, configured: boolean) {
  await page.route(/\/ai\/providers(\?|$)/, (route) =>
    route
      .fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify([
          provider(configured ? "configured" : "unconfigured", "openai"),
          provider("unconfigured", "anthropic"),
        ]),
      })
      .catch(() => undefined),
  );
}

const BRIEFS = {
  region: "Bar chart of the sum of amount by region, sorted descending.",
  category: "Bar chart of the sum of amount by category.",
  trend: "Line chart of the sum of amount per month of order_date.",
};

const planFor = (tableId: number): Json => ({
  plan: {
    title: "Sales overview",
    kpis: [{ id: "k1", table_id: tableId, label: "Total amount", field: "amount", agg: "sum" }],
    checks: [
      {
        id: "c1",
        table_id: tableId,
        title: "Amount by region",
        brief: BRIEFS.region,
        purpose: "Which regions lead.",
        relation: null,
      },
      {
        id: "c2",
        table_id: tableId,
        title: "Amount by category",
        brief: BRIEFS.category,
        purpose: "Which categories lead.",
        relation: null,
      },
      {
        id: "c3",
        table_id: tableId,
        title: "Amount by month",
        brief: BRIEFS.trend,
        purpose: "Whether sales grow.",
        relation: null,
      },
    ],
    filters: [
      { id: "f1", table_id: tableId, field: "region", kind: "categorical", label: "Region" },
    ],
  },
  explanation: null,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
});

const chartAnswer = (chart: Json | null, reason: string | null = null): Json => ({
  chart,
  title: null,
  explanation: reason ? "The table has no date column." : null,
  warnings: [],
  degraded: chart === null,
  reason,
  message: null,
});

const CHARTS: Record<string, Json> = {
  [BRIEFS.region]: chartAnswer({
    mark: "bar",
    x: { field: "region" },
    y: { field: "amount", aggregate: "sum" },
    sort: "descending",
  }),
  [BRIEFS.category]: chartAnswer({
    mark: "bar",
    x: { field: "category" },
    y: { field: "amount", aggregate: "sum" },
  }),
  [BRIEFS.trend]: chartAnswer(null, "no_chart"),
};

const storyAnswer = (grounded: boolean): Json => ({
  story: {
    title: "Sales overview",
    summary_md: grounded
      ? "Sales total **270**. West brings in the most, 150."
      : "This page shows how the amount splits over the regions.",
    charts: [
      {
        id: "c1",
        title: "Amount by region",
        finding: grounded ? "West leads with 150; South is lowest with 20." : "Amount per region.",
        width: "full",
      },
    ],
    left_out: [{ id: "c2", reason: "It adds little to the answer." }],
  },
  grounded,
  evidence: [],
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
  attempts: 1,
});

/** Stubs the three AI routes and records each request body. */
async function stubBuilderRoutes(page: Page, tableId: number, grounded = true) {
  const bodies = { plan: [] as Json[], chart: [] as Json[], story: [] as Json[] };
  const answer = (route: Parameters<Parameters<Page["route"]>[1]>[0], body: Json) =>
    route
      .fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) })
      .catch(() => undefined);
  await page.route(/\/ai\/plan_dashboard(\?|$)/, (route) => {
    bodies.plan.push(route.request().postDataJSON());
    return answer(route, planFor(tableId));
  });
  await page.route(/\/ai\/generate_chart(\?|$)/, (route) => {
    const body = route.request().postDataJSON();
    bodies.chart.push(body);
    return answer(route, CHARTS[body.prompt] ?? chartAnswer(null, "empty_response"));
  });
  await page.route(/\/ai\/dashboard_story(\?|$)/, (route) => {
    bodies.story.push(route.request().postDataJSON());
    return answer(route, storyAnswer(grounded));
  });
  return bodies;
}

async function pickTableAndAsk(page: Page, tableName: string, question: string) {
  const builder = page.getByTestId("dashboard-ai-builder");
  await expect(builder).toBeVisible({ timeout: 30000 });
  await builder.getByTestId("dashboard-ai-sources").click();
  await page.keyboard.type(tableName);
  await page.locator(".el-select-dropdown__item:visible", { hasText: tableName }).click();
  await builder.getByTestId("dashboard-ai-question").fill(question);
  return builder;
}

const createdDashboardId = async (page: Page): Promise<number> => {
  await expect(page).toHaveURL(/#\/main\/dashboards\/\d+\/edit/, { timeout: 20000 });
  return Number(/dashboards\/(\d+)\/edit/.exec(page.url())?.[1]);
};

const deleteDashboard = async (request: APIRequestContext, token: string, id: number | null) => {
  if (id != null) {
    await request.delete(`${API_URL}/catalog/dashboards/${id}`, { headers: authHeaders(token) });
  }
};

const tiles = (page: Page) => page.locator(".vgl-item:not(.vgl-item--placeholder)");

test.describe("AI dashboard builder", () => {
  test.describe.configure({ mode: "serial" });

  let token: string;
  let seeded: SeededCharts;

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    seeded = await seedCharts(request, token, `e2e dashboard ai ${Date.now()}`);
  });

  test.afterAll(async ({ request }) => {
    await seeded?.cleanup(request);
  });

  test.beforeEach(async ({ request }) => {
    const fields = await request.post(
      `${API_URL}/catalog/visualizations/${seeded.tableVizId}/fields`,
      { headers: authHeaders(token) },
    );
    const body = fields.ok() ? await fields.json() : null;
    test.skip(
      !fields.ok() || !!body?.error,
      `chart columns unavailable (${fields.status()}): flowfile_worker is not running`,
    );
  });

  test("a question becomes a page: summary, KPI and chart land in one undo step, and a second build takes a tab", async ({
    page,
    request,
  }) => {
    await stubProviders(page, true);
    const bodies = await stubBuilderRoutes(page, seeded.tableId);
    let createdId: number | null = null;
    try {
      await login(page, token, "#/main/dashboards/new?ai=1");
      const builder = await pickTableAndAsk(page, seeded.tableName, "How are sales doing?");
      await expect(builder.getByTestId("dashboard-ai-data-line")).toContainText(
        "It also reads what each chart returns",
      );
      await shot(page, "1-form");
      await builder.getByTestId("dashboard-ai-build").click();

      const result = builder.getByTestId("dashboard-ai-result");
      await expect(result).toBeVisible({ timeout: 60000 });
      await shot(page, "2-result");
      await expect(result).toContainText(
        "“Sales overview” was added with 1 chart, 1 KPI and 1 filter.",
      );
      await expect(builder.getByTestId("dashboard-ai-filters")).toHaveText(
        "Filter in the strip above the page: Region.",
      );
      await expect(builder.getByTestId("dashboard-ai-left-out")).toContainText(
        "Amount by category: It adds little to the answer.",
      );
      await expect(builder.getByTestId("dashboard-ai-failed")).toContainText(
        "Amount by month: The AI did not make a chart: The table has no date column.",
      );

      expect(bodies.plan).toEqual([
        {
          table_ids: [seeded.tableId],
          prompt: "How are sales doing?",
          send_values: true,
          provider: "openai",
          model: "gpt-test",
        },
      ]);
      expect(bodies.chart.map((b) => b.prompt).sort()).toEqual(Object.values(BRIEFS).sort());
      expect(bodies.chart.every((b) => b.table_id === seeded.tableId)).toBe(true);
      const [story] = bodies.story;
      expect(story.title).toBe("Sales overview");
      expect(story.charts.map((c: Json) => c.id)).toEqual(["c1", "c2"]);
      expect(story.failed.map((f: Json) => f.id)).toEqual(["c3"]);
      // Each built chart goes with the aggregate query its tile runs.
      for (const chart of story.charts) {
        const view = chart.payload.workflow.find((step: Json) => step.type === "view");
        expect(view.query[0].op).toBe("aggregate");
        expect(view.query[0].measures[0]).toMatchObject({ field: "amount", agg: "sum" });
      }

      await builder.getByTestId("dashboard-ai-done").click();
      await expect(builder).toBeHidden();
      createdId = await createdDashboardId(page);

      // Summary, KPI and the one kept chart, drawn with live data.
      await expect(tiles(page)).toHaveCount(3);
      await expect(page.getByText("West brings in the most, 150.")).toBeVisible();
      await expect(page.locator(".kpi-label")).toHaveText("Total amount");
      await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
      await expect(page.locator(".tile-header", { hasText: "Amount by region" })).toBeVisible();
      await expect(page.locator(".tile-viz-body canvas").first()).toBeVisible({ timeout: 30000 });
      await expect(page.getByTestId("tile-note-button")).toHaveCount(1);
      await shot(page, "3-page");
      // A new dashboard takes the page's title as its name.
      await expect(page.getByPlaceholder("Dashboard name")).toHaveValue("Sales overview");

      const saved = await (
        await request.get(`${API_URL}/catalog/dashboards/${createdId}`, {
          headers: authHeaders(token),
        })
      ).json();
      expect(saved.name).toBe("Sales overview");
      expect(saved.layout.filters).toHaveLength(1);
      expect(saved.layout.filters[0]).toMatchObject({
        field_name: "region",
        label: "Region",
        kind: "categorical",
        state: { selected: [] },
        target: "all",
        datasource_id: seeded.tableId,
        scope: "datasource",
      });
      expect(saved.layout.tiles.map((t: Json) => t.type)).toEqual(["text", "kpi", "viz"]);
      const vizTile = saved.layout.tiles[2];
      expect(vizTile).toMatchObject({
        w: 48,
        note_md: "West leads with 150; South is lowest with 20.",
      });
      const viz = await (
        await request.get(`${API_URL}/catalog/visualizations/${vizTile.viz_id}`, {
          headers: authHeaders(token),
        })
      ).json();
      expect(viz).toMatchObject({
        name: "Amount by region",
        chart_type: "bar",
        source_type: "table",
        catalog_table_id: seeded.tableId,
      });

      // One undo takes the whole page back; redo brings it back.
      await page.getByTestId("dashboard-undo").click();
      await expect(tiles(page)).toHaveCount(0);
      await expect(page.getByPlaceholder("Dashboard name")).toHaveValue("Untitled dashboard");
      await expect(page.locator(".filter-chip")).toHaveCount(0);
      await expect(page.getByTestId("dashboard-undo")).toBeDisabled();
      await page.getByTestId("dashboard-redo").click();
      await expect(tiles(page)).toHaveCount(3);
      await expect(page.getByPlaceholder("Dashboard name")).toHaveValue("Sales overview");

      // The filter the plan asked for is in the strip and narrows the page's tiles.
      const chip = page.locator(".filter-chip");
      await expect(chip).toHaveCount(1);
      await expect(chip).toContainText("Region");
      await chip.locator(".filter-chip-input").click();
      await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
      await page.keyboard.press("Escape");
      await expect(page.locator(".kpi-value")).toHaveText("150", { timeout: 30000 });

      // A second build on a dashboard that holds tiles goes on a tab named after the page.
      await page.getByTestId("dashboard-ai-open").click();
      await expect(builder.getByTestId("dashboard-ai-question")).toHaveValue(
        "How are sales doing?",
      );
      await builder.getByTestId("dashboard-ai-build").click();
      await expect(builder.getByTestId("dashboard-ai-result")).toBeVisible({ timeout: 60000 });
      await builder.getByTestId("dashboard-ai-done").click();
      await expect(page.locator(".el-tabs__item", { hasText: "Tab 1" })).toBeVisible();
      await expect(page.locator(".el-tabs__item", { hasText: "Sales overview" })).toBeVisible();
      await expect(tiles(page)).toHaveCount(3);
      // The dashboard's filter already reaches every tile on the table, so no second one is added.
      await expect(chip).toHaveCount(1);
      await expect(page.locator(".kpi-value")).toHaveText("150", { timeout: 30000 });
    } finally {
      await deleteDashboard(request, token, createdId);
    }
  });

  test("with data values not shared no chart query is sent and the page says so", async ({
    page,
    request,
  }) => {
    await stubProviders(page, true);
    await page.route(/\/ai\/data_policy(\?|$)/, (route) =>
      route
        .fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ send_data_allowed: false }),
        })
        .catch(() => undefined),
    );
    const bodies = await stubBuilderRoutes(page, seeded.tableId, false);
    let createdId: number | null = null;
    try {
      await login(page, token, "#/main/dashboards/new?ai=1");
      const builder = await pickTableAndAsk(page, seeded.tableName, "How are sales doing?");
      await expect(builder.getByTestId("dashboard-ai-data-line")).toContainText(
        "The AI sees the column names and types only, no data values.",
      );
      await builder.getByTestId("dashboard-ai-build").click();
      await expect(builder.getByTestId("dashboard-ai-result")).toContainText(
        "without numbers read from the data",
        { timeout: 60000 },
      );
      expect(bodies.plan[0].send_values).toBe(false);
      expect(bodies.chart.every((b) => b.send_values === false)).toBe(true);
      expect(bodies.story[0].send_values).toBe(false);
      expect(bodies.story[0].charts.map((c: Json) => c.payload)).toEqual([null, null]);

      await builder.getByTestId("dashboard-ai-done").click();
      createdId = await createdDashboardId(page);
      await expect(page.getByText("Written without seeing the data values.")).toBeVisible();
    } finally {
      await deleteDashboard(request, token, createdId);
    }
  });

  test("Cancel stops a build and adds nothing", async ({ page }) => {
    await stubProviders(page, true);
    await stubBuilderRoutes(page, seeded.tableId);
    // Registered last, so it takes the plan request and never answers.
    await page.route(/\/ai\/plan_dashboard(\?|$)/, () => new Promise(() => undefined));
    await login(page, token, "#/main/dashboards/new?ai=1");
    const builder = await pickTableAndAsk(page, seeded.tableName, "How are sales doing?");
    await builder.getByTestId("dashboard-ai-build").click();
    await expect(builder.getByTestId("dashboard-ai-progress")).toContainText("Planning the page");
    await builder.getByTestId("dashboard-ai-cancel").click();
    await expect(builder.getByTestId("dashboard-ai-message")).toHaveText(
      "Stopped. Nothing was added to the dashboard.",
    );
    await builder.getByTestId("dashboard-ai-close").click();
    await expect(tiles(page)).toHaveCount(0);
    await expect(page).toHaveURL(/#\/main\/dashboards\/new/);
    await expect(page.getByTestId("dashboard-undo")).toBeDisabled();
  });

  test("the builder is not offered without a configured provider", async ({ page }) => {
    await stubProviders(page, false);
    await login(page, token, "#/main/dashboards/new?ai=1");
    await expect(page.getByTestId("insert-filter")).toBeVisible({ timeout: 30000 });
    await expect(page.getByTestId("dashboard-ai-open")).toHaveCount(0);
    await expect(page.getByTestId("dashboard-ai-builder")).toHaveCount(0);
  });
});
