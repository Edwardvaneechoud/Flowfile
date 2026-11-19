import { mkdirSync } from "fs";
import { join } from "path";
import { expect, test, type Page } from "@playwright/test";
import { API_URL, BASE_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";
import { seedCharts, type SeededCharts } from "./helpers/charts";

// The chart builder's AI prompt bar (`ChartPromptBar.vue`) in the real builder with live data
// from the worker. `/ai/providers` and `/ai/generate_chart` are stubbed, except in the one test
// that sends a real request to core. Without a worker the builder cannot load the table's
// columns, so the tests skip (they do not pass).
//
// CHART_AI_SCREENS=<dir> also writes the screenshots of the bar's states into that folder.

type Json = Record<string, unknown>;

const SCREENS = process.env.CHART_AI_SCREENS;

const provider = (status: "configured" | "unconfigured", name = "openai") => ({
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

const answer = (chart: Json, title: string, explanation: string): Json => ({
  chart,
  title,
  explanation,
  warnings: [],
  degraded: false,
  reason: null,
  message: null,
});

const degraded = (reason: string, explanation: string | null, message: string | null): Json => ({
  chart: null,
  title: null,
  explanation,
  warnings: [],
  degraded: true,
  reason,
  message,
});

/** Stubs the chart route; each request takes the next answer of `queue` and its body lands in `bodies`. */
async function stubChartRoute(page: Page) {
  const bodies: Json[] = [];
  const queue: (Json | (() => Promise<Json>))[] = [];
  await page.route(/\/ai\/generate_chart(\?|$)/, async (route) => {
    bodies.push(route.request().postDataJSON());
    const next = queue.shift() ?? degraded("empty_response", null, null);
    const body = typeof next === "function" ? await next() : next;
    await route
      .fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) })
      .catch(() => undefined);
  });
  return { bodies, queue };
}

/** Opens a catalog URL; the first hash navigation on a fresh page can be lost, so it goes twice. */
async function open(page: Page, token: string, hash: string) {
  await login(page, token, hash);
  await page.goto(`${BASE_URL}/${hash}`);
  await page.waitForLoadState("load");
}

async function openNewChart(page: Page, token: string, tableId: number) {
  await open(page, token, `#/main/catalog?tab=catalog&tableId=${tableId}`);
  await page.getByRole("button", { name: "New visualization" }).click();
  await expect(page.getByTestId("chart-prompt-bar")).toBeVisible({ timeout: 30000 });
}

async function openSavedChart(page: Page, token: string, vizId: number) {
  await open(page, token, `#/main/catalog?tab=catalog&vizId=${vizId}`);
  await expect(page.locator(".el-dialog .viz-viewer")).toBeVisible({ timeout: 30000 });
}

async function generate(page: Page, prompt: string) {
  await page.getByTestId("chart-prompt-input").fill(prompt);
  const button = page.getByTestId("chart-prompt-generate");
  // Enabled once the builder has mounted and loaded the table's columns.
  await expect(button).toBeEnabled({ timeout: 30000 });
  await button.click();
}

const getViz = async (request: Parameters<typeof getAuthToken>[0], token: string, id: number) => {
  const response = await request.get(`${API_URL}/catalog/visualizations/${id}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  return response.json();
};

const shelf = (fields: { fid: string; aggName?: string }[]) =>
  fields.map((f) => (f.aggName ? `${f.aggName}(${f.fid})` : f.fid));

test.describe("Chart builder AI prompt", () => {
  test.describe.configure({ mode: "serial" });

  let token: string;
  let seeded: SeededCharts;

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    seeded = await seedCharts(request, token, `e2e chart ai ${Date.now()}`);
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

  test("a prompt draws a chart, a follow-up changes it, undo takes it back and it saves with its title", async ({
    page,
    request,
  }) => {
    await stubProviders(page, true);
    const chartRoute = await stubChartRoute(page);
    chartRoute.queue.push(
      answer(
        {
          mark: "bar",
          x: { field: "region" },
          y: { field: "amount", aggregate: "sum" },
          sort: "descending",
        },
        "Revenue by region",
        "Total amount per region, largest first.",
      ),
      answer(
        { mark: "line", x: { field: "region" }, y: { field: "amount", aggregate: "sum" } },
        "Revenue by region",
        "The same totals as a line.",
      ),
      degraded("no_chart", "There is no date column for a monthly trend.", null),
    );

    await openNewChart(page, token, seeded.tableId);
    const input = page.getByTestId("chart-prompt-input");
    await expect(input).toHaveAttribute("placeholder", "Describe a chart");
    await expect(page.getByTestId("chart-prompt-sends")).toHaveText(
      "Sends this table's column names, types and a few example values to OpenAI.",
    );

    await generate(page, "revenue by region");
    await expect(page.getByTestId("chart-prompt-explanation")).toContainText(
      "Total amount per region, largest first.",
    );
    expect(chartRoute.bodies[0]).toEqual({
      table_id: seeded.tableId,
      prompt: "revenue by region",
      provider: "openai",
      model: "gpt-test",
    });
    // Drawn with live data: the shelves hold the request's fields and the worksheet paints.
    const dialog = page.locator(".el-dialog");
    await expect(dialog.getByText("amount sum", { exact: true })).toBeVisible();
    await expect(dialog.locator("canvas").first()).toBeVisible({ timeout: 30000 });
    await expect(input).toHaveAttribute("placeholder", "Describe a change");
    await expect(page.getByTestId("viz-name-input")).toHaveValue("Revenue by region");

    await generate(page, "make it a line");
    await expect(page.getByTestId("chart-prompt-explanation")).toContainText(
      "The same totals as a line.",
    );
    // The follow-up carried the chart on screen, as the builder exported it.
    expect(chartRoute.bodies[1].current_chart).toEqual({
      mark: "bar",
      x: { field: "region" },
      y: { field: "amount", aggregate: "sum" },
      sort: "descending",
    });

    // Graphic Walker's own undo (Ctrl+Z outside a text field) takes the AI's change back in one step.
    await page.locator(".el-dialog__title").click();
    await page.keyboard.press("Control+z");

    await generate(page, "a monthly trend");
    await expect(page.getByTestId("chart-prompt-message")).toHaveText(
      /The AI did not make a chart: There is no date column for a monthly trend\./,
    );
    // The chart on screen when that was asked is the bar again, not the line.
    expect((chartRoute.bodies[2].current_chart as Json).mark).toBe("bar");

    await page.getByTestId("viz-save-button").click();
    await expect(page.locator(".el-dialog .viz-editor")).toHaveCount(0, { timeout: 30000 });

    const listed = await request.get(`${API_URL}/catalog/tables/${seeded.tableId}/visualizations`, {
      headers: authHeaders(token),
    });
    const saved = ((await listed.json()) as Json[]).find((v) => v.name === "Revenue by region");
    expect(saved, "the chart saved under the model's title").toBeTruthy();
    expect(saved!.chart_type).toBe("bar");
    const chart = (saved!.spec as Json[])[0] as {
      config: { geoms: string[] };
      encodings: { columns: { fid: string; aggName?: string }[]; rows: Json[] };
    };
    expect(chart.config.geoms).toEqual(["bar"]);
    expect(shelf(chart.encodings.columns)).toEqual(["region"]);
    expect(shelf(chart.encodings.rows as { fid: string; aggName?: string }[])).toEqual([
      "sum(amount)",
    ]);

    await openSavedChart(page, token, saved!.id as number);
    await expect(page.getByTestId("chart-prompt-input")).toHaveAttribute(
      "placeholder",
      "Describe a change",
      { timeout: 30000 },
    );
    await expect(page.locator(".el-dialog").getByText("amount sum", { exact: true })).toBeVisible();
  });

  test("on a saved chart an AI change autosaves and Revert restores the opened chart", async ({
    page,
    request,
  }) => {
    await stubProviders(page, true);
    const chartRoute = await stubChartRoute(page);
    chartRoute.queue.push(
      answer(
        { mark: "arc", theta: { field: "amount", aggregate: "sum" }, color: { field: "region" } },
        "Share of revenue by region",
        "Each region's share of the total amount.",
      ),
    );

    const opened = await getViz(request, token, seeded.tableVizId);
    await openSavedChart(page, token, seeded.tableVizId);
    await generate(page, "as a pie");
    await expect(page.getByTestId("chart-prompt-explanation")).toContainText("Each region's share");
    expect(chartRoute.bodies[0].current_chart).toEqual({
      mark: "bar",
      x: { field: "amount", aggregate: "sum" },
      y: { field: "region" },
      sort: "descending",
    });

    await expect
      .poll(
        async () => {
          const viz = await getViz(request, token, seeded.tableVizId);
          return [viz.spec[0].config.geoms[0], viz.chart_type, viz.name];
        },
        { timeout: 20000 },
      )
      .toEqual(["arc", "arc", opened.name]);

    await page.getByRole("button", { name: "Revert" }).click();
    await page.locator(".el-message-box__btns button", { hasText: "Revert" }).click();
    await expect
      .poll(
        async () => {
          const viz = await getViz(request, token, seeded.tableVizId);
          return [viz.spec[0].config.geoms[0], viz.chart_type];
        },
        { timeout: 20000 },
      )
      .toEqual(["bar", "bar"]);
  });

  test("a degraded answer shows its message and leaves the chart alone", async ({
    page,
    request,
  }) => {
    await stubProviders(page, true);
    const chartRoute = await stubChartRoute(page);
    chartRoute.queue.push(
      degraded("timeout", null, "The model did not answer in time."),
      degraded("invalid_chart", null, 'y.field "revenu" is not a column. Closest: "amount".'),
    );
    const before = await getViz(request, token, seeded.tableVizId);

    await openSavedChart(page, token, seeded.tableVizId);
    const revert = page.getByRole("button", { name: "Revert" });
    await expect(revert).toBeDisabled();

    await generate(page, "something");
    const message = page.getByTestId("chart-prompt-message");
    await expect(message).toContainText("The AI took too long to answer.");

    await generate(page, "something else");
    await expect(message).toContainText("The AI's answer was not a chart this builder can draw.");
    await expect(message).toContainText('y.field "revenu" is not a column.');
    await expect(page.getByTestId("chart-prompt-explanation")).toHaveCount(0);

    // Nothing changed, so nothing autosaves and there is nothing to revert.
    await page.waitForTimeout(3000);
    await expect(revert).toBeDisabled();
    const after = await getViz(request, token, seeded.tableVizId);
    expect(after.updated_at).toBe(before.updated_at);
  });

  test("a chart the AI cannot carry over says which part and sends the prompt alone", async ({
    page,
  }) => {
    await stubProviders(page, true);
    const chartRoute = await stubChartRoute(page);
    chartRoute.queue.push(degraded("no_chart", "Not this time.", null));

    await openSavedChart(page, token, seeded.detailVizId);
    await expect(page.getByTestId("chart-prompt-carry-notice")).toContainText(
      "This chart has a detail field, which the AI cannot carry over.",
      { timeout: 30000 },
    );
    await generate(page, "count per category");
    await expect(page.getByTestId("chart-prompt-message")).toBeVisible();
    expect(chartRoute.bodies[0]).not.toHaveProperty("current_chart");
  });

  test("the bar is hidden without a provider and on a chart over a SQL query", async ({ page }) => {
    await stubProviders(page, false);
    await openSavedChart(page, token, seeded.tableVizId);
    await expect(page.locator(".el-dialog canvas").first()).toBeVisible({ timeout: 30000 });
    await expect(page.getByTestId("chart-prompt-bar")).toHaveCount(0);

    await page.unroute(/\/ai\/providers(\?|$)/);
    await stubProviders(page, true);
    await openSavedChart(page, token, seeded.sqlVizId);
    await expect(page.locator(".el-dialog canvas").first()).toBeVisible({ timeout: 30000 });
    await expect(page.getByTestId("chart-prompt-bar")).toHaveCount(0);
  });

  test("the real route's answer without a reachable provider is shown and changes nothing", async ({
    page,
  }) => {
    // Only the provider list is stubbed, so the bar shows; the request goes to the real core.
    await stubProviders(page, true);
    await openNewChart(page, token, seeded.tableId);
    const response = page.waitForResponse(/\/ai\/generate_chart(\?|$)/, { timeout: 120000 });
    await generate(page, "revenue by region");
    const body = await (await response).json();
    expect(body).toMatchObject({ degraded: true, reason: "provider_error", chart: null });

    const message = page.getByTestId("chart-prompt-message");
    await expect(message).toContainText(
      "The AI provider could not be reached or returned an error",
    );
    await expect(message).toContainText("Check it under Settings → AI and try again.");
    await expect(page.getByTestId("chart-prompt-input")).toHaveAttribute(
      "placeholder",
      "Describe a chart",
    );
    if (SCREENS) {
      mkdirSync(SCREENS, { recursive: true });
      await page
        .locator(".el-dialog")
        .screenshot({ path: join(SCREENS, "real-route-provider-error-light-1440.png") });
    }
  });

  test("screenshots of the bar's states", async ({ context }) => {
    test.skip(!SCREENS, "set CHART_AI_SCREENS to a folder to write them");
    mkdirSync(SCREENS!, { recursive: true });

    const shots: [string, number][] = [
      ["light", 1440],
      ["dark", 1440],
      ["light", 900],
    ];
    for (const [theme, width] of shots) {
      const suffix = `${theme}-${width}`;
      const page = await context.newPage();
      await page.setViewportSize({ width, height: 900 });
      await page.addInitScript(
        (mode) => localStorage.setItem("flowfile-theme-preference", mode),
        theme,
      );
      await stubProviders(page, true);
      const chartRoute = await stubChartRoute(page);
      let release: () => void = () => undefined;
      chartRoute.queue.push(
        () =>
          new Promise((resolve) => {
            release = () =>
              resolve(
                answer(
                  {
                    mark: "bar",
                    x: { field: "region" },
                    y: { field: "amount", aggregate: "sum" },
                    sort: "descending",
                  },
                  "Revenue by region",
                  "Total amount per region, largest first.",
                ),
              );
          }),
        degraded("timeout", null, "The model did not answer in time."),
      );
      const dialog = page.locator(".el-dialog");

      await openNewChart(page, token, seeded.tableId);
      await page.waitForTimeout(1500);
      await dialog.screenshot({ path: join(SCREENS!, `idle-empty-${suffix}.png`) });

      await generate(page, "revenue by region, largest first");
      await expect(page.getByTestId("chart-prompt-busy")).toBeVisible();
      await dialog.screenshot({ path: join(SCREENS!, `busy-${suffix}.png`) });
      release();
      await expect(page.getByTestId("chart-prompt-explanation")).toBeVisible();
      await expect(dialog.locator("canvas").first()).toBeVisible({ timeout: 30000 });
      await page.waitForTimeout(1500);
      await dialog.screenshot({ path: join(SCREENS!, `after-chart-${suffix}.png`) });

      await generate(page, "make it monthly");
      await expect(page.getByTestId("chart-prompt-message")).toBeVisible();
      await dialog.screenshot({ path: join(SCREENS!, `failure-${suffix}.png`) });

      await openSavedChart(page, token, seeded.detailVizId);
      await expect(page.getByTestId("chart-prompt-carry-notice")).toBeVisible({ timeout: 30000 });
      await page.waitForTimeout(1500);
      await dialog.screenshot({ path: join(SCREENS!, `cannot-carry-over-${suffix}.png`) });
      await page.close();
    }
  });
});
