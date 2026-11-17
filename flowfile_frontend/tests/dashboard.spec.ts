import { expect, test } from "@playwright/test";
import { API_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";
import { seedDashboard, type SeededDashboard } from "./helpers/dashboards";

// Dashboards draw through flowfile_worker: without one every tile stays empty, so the tests
// skip (they do not pass) when the chart's columns cannot be read.

test.describe("Dashboards", () => {
  test.describe.configure({ mode: "serial" });

  let token: string;
  let seeded: SeededDashboard;

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    seeded = await seedDashboard(request, token, `e2e dashboard ${Date.now()}`);
  });

  test.afterAll(async ({ request }) => {
    await seeded?.cleanup(request);
  });

  test.beforeEach(async ({ request }) => {
    const fields = await request.post(`${API_URL}/catalog/visualizations/${seeded.vizId}/fields`, {
      headers: authHeaders(token),
    });
    test.skip(
      !fields.ok(),
      `chart columns unavailable (${fields.status()}): flowfile_worker is not running, so tiles cannot draw`,
    );
  });

  test("a viewer sees titled cards, a drawn chart and a KPI, with one request per chart", async ({
    page,
  }) => {
    const chartRequests: string[] = [];
    page.on("request", (req) => {
      const path = new URL(req.url()).pathname.replace(/^.*\/catalog/, "");
      if (path.startsWith(`/visualizations/${seeded.vizId}`)) {
        chartRequests.push(`${req.method()} ${path}`);
      }
    });

    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);

    await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
    await expect(page.locator(".tile-card")).toHaveCount(3);
    await expect(page.locator(".tile-title")).toHaveText(["Revenue by region", "Orders"]);
    // The KPI still to be set up keeps its space and says nothing.
    await expect(page.locator(".tile-blank")).toHaveCount(1);
    await expect(page.getByText("KPI not configured")).toHaveCount(0);
    // Graphic Walker draws inside a shadow root; Playwright's CSS reaches into it.
    await expect(page.locator(".tile-viz-body canvas").first()).toBeVisible({ timeout: 30000 });
    await expect(page.locator(".tile-viz-body input")).toHaveCount(0);

    const count = (request: string) => chartRequests.filter((r) => r === request).length;
    expect(count(`GET /visualizations/${seeded.vizId}`)).toBe(1);
    expect(count(`POST /visualizations/${seeded.vizId}/fields`)).toBe(1);
  });

  test("a viewer's filter travels in the URL and Reset restores the saved one", async ({
    page,
  }) => {
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
    await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
    await expect(page.getByRole("button", { name: "Reset" })).toHaveCount(0);

    await page.locator(".filter-chip-input").click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
    await page.keyboard.press("Escape");

    await expect(page.locator(".kpi-value")).toHaveText("150");
    await expect(page).toHaveURL(/[?&]f=/);

    await page.reload();
    await page.waitForLoadState("load");
    await expect(page.locator(".kpi-value")).toHaveText("150", { timeout: 30000 });

    await page.getByRole("button", { name: "Reset" }).click();
    await expect(page.locator(".kpi-value")).toHaveText("270");
    await expect(page).not.toHaveURL(/[?&]f=/);
  });

  test("a duplicated tile and a description are saved", async ({ page, request }) => {
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}/edit`);
    await expect(page.locator(".tile-header")).toHaveCount(2, { timeout: 30000 });

    await page.getByPlaceholder("Description (optional)").fill("Orders by region");
    const chartHeader = page.locator(".tile-header", { hasText: "Revenue by region" });
    await chartHeader.locator(".tile-menu").click();
    await page.locator(".el-dropdown-menu__item:visible", { hasText: "Duplicate" }).click();
    await expect(page.locator(".tile-header")).toHaveCount(3);

    await expect
      .poll(
        async () => {
          const response = await request.get(
            `${API_URL}/catalog/dashboards/${seeded.dashboardId}`,
            {
              headers: authHeaders(token),
            },
          );
          const saved = await response.json();
          const charts = saved.layout.tiles.filter((t: { type: string }) => t.type === "viz");
          return [saved.description, charts.length, charts.map((t: { title: string }) => t.title)];
        },
        { timeout: 20000 },
      )
      .toEqual(["Orders by region", 2, ["Revenue by region", "Revenue by region"]]);
  });

  test("a viewer pages and sorts a table tile on the worker, and the region filter narrows it", async ({
    page,
  }) => {
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
    const table = page.locator(".rows-grid");
    const range = table.locator(".rows-range");
    const firstColumn = table.locator("tbody tr td:first-child");
    const amounts = table.locator("tbody tr td:nth-child(2)");

    // The saved default: largest amount first, five rows a page.
    await expect(range).toHaveText("1–5 of 7 rows", { timeout: 30000 });
    await expect(table.locator("thead th")).toHaveText(["Region", "amount"]);
    await expect(amounts).toHaveText(["100", "70", "50", "30", "20"]);

    await table.locator("thead th", { hasText: "Region" }).click();
    await expect(firstColumn).toHaveText(["Central", "Central", "East", "North", "South"]);
    await expect(table.locator("thead th").first()).toHaveAttribute("aria-sort", "ascending");

    await table.getByRole("button", { name: "Next page" }).click();
    await expect(range).toHaveText("6–7 of 7 rows");
    await expect(firstColumn).toHaveText(["West", "West"]);

    await page.locator(".filter-chip-input").click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
    await page.keyboard.press("Escape");

    await expect(range).toHaveText("1–2 of 2 rows");
    await expect(firstColumn).toHaveText(["West", "West"]);
  });
});
