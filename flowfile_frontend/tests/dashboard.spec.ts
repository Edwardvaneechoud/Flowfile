import { readFileSync } from "fs";
import { expect, test } from "@playwright/test";
import { API_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";
import {
  seedDashboard,
  seedTrendDashboard,
  seedWideFilterDashboard,
  type SeededDashboard,
} from "./helpers/dashboards";

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

  test("a filter dragged onto the canvas stays a tile, leaves the strip and narrows the KPI", async ({
    page,
    request,
  }) => {
    // Its own dashboard, so the strip filter the other tests use stays where it is.
    const placed = await seedDashboard(request, token, `e2e placed filter ${Date.now()}`);
    try {
      await login(page, token, `#/main/dashboards/${placed.dashboardId}/edit`);
      const chip = page.locator(".filter-chip", { hasText: "Region" });
      await expect(chip).toBeVisible({ timeout: 30000 });

      await chip
        .locator(".filter-chip-grip")
        .dragTo(page.locator(".canvas .vgl-layout"), { targetPosition: { x: 40, y: 40 } });
      await expect(page.locator(".tile-filter")).toHaveCount(1);
      await expect(page.locator(".filter-chip", { hasText: "Region" })).toHaveCount(0);

      await expect
        .poll(
          async () => {
            const response = await request.get(
              `${API_URL}/catalog/dashboards/${placed.dashboardId}`,
              { headers: authHeaders(token) },
            );
            const saved = await response.json();
            return saved.layout.tiles
              .filter((t: { type: string }) => t.type === "filter")
              .map((t: { filter_id: string }) => t.filter_id);
          },
          { timeout: 20000 },
        )
        .toEqual(["f-region"]);

      await page.reload();
      await page.waitForLoadState("load");
      await expect(page.locator(".tile-filter .tile-filter-name")).toHaveText("Region", {
        timeout: 30000,
      });
      await expect(page.locator(".filter-chip", { hasText: "Region" })).toHaveCount(0);

      await page.locator(".back-btn").click();
      await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
      // Its only filter is on the canvas, so the viewer gets no strip at all.
      await expect(page.locator(".filter-bar")).toHaveCount(0);

      await page.locator(".tile-filter .filter-chip-input").click();
      await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
      await page.keyboard.press("Escape");
      await expect(page.locator(".kpi-value")).toHaveText("150");
      await expect(page).toHaveURL(/[?&]f=/);

      await page.getByRole("button", { name: "Reset" }).click();
      await expect(page.locator(".kpi-value")).toHaveText("270");
    } finally {
      await placed.cleanup(request);
    }
  });

  test("removing a tile a filter targets keeps the dashboard saving", async ({ page, request }) => {
    const seededTargets = await seedDashboard(request, token, `e2e filter target ${Date.now()}`);
    const url = `${API_URL}/catalog/dashboards/${seededTargets.dashboardId}`;
    const headers = authHeaders(token);
    try {
      const current = await (await request.get(url, { headers })).json();
      const layout = {
        ...current.layout,
        filters: current.layout.filters.map((f: Record<string, unknown>) => ({
          ...f,
          target: "tiles",
          target_tile_ids: ["t-table"],
        })),
      };
      const put = await request.put(url, { headers, data: { layout } });
      expect(put.ok()).toBe(true);

      await login(page, token, `#/main/dashboards/${seededTargets.dashboardId}/edit`);
      const tableHeader = page.locator(".tile-header", { hasText: "Orders" });
      await expect(tableHeader).toBeVisible({ timeout: 30000 });
      await tableHeader.locator(".tile-menu").click();
      await page
        .locator(".el-dropdown-menu__item:visible", { hasText: "Remove from dashboard" })
        .click();
      await expect(page.locator(".filter-chip", { hasText: "Region" })).toContainText("no tiles");

      await expect
        .poll(
          async () => {
            const saved = await (await request.get(url, { headers })).json();
            return [
              saved.layout.tiles.some((t: { id: string }) => t.id === "t-table"),
              saved.layout.filters[0].target,
              saved.layout.filters[0].target_tile_ids,
            ];
          },
          { timeout: 20000 },
        )
        .toEqual([false, "tiles", []]);
    } finally {
      await seededTargets.cleanup(request);
    }
  });

  test("undo brings a removed tile back and saves it, redo removes it again", async ({
    page,
    request,
  }) => {
    const seededUndo = await seedDashboard(request, token, `e2e undo ${Date.now()}`);
    const url = `${API_URL}/catalog/dashboards/${seededUndo.dashboardId}`;
    const headers = authHeaders(token);
    const savedHasTable = async () => {
      const saved = await (await request.get(url, { headers })).json();
      return saved.layout.tiles.some((t: { id: string }) => t.id === "t-table");
    };
    try {
      await login(page, token, `#/main/dashboards/${seededUndo.dashboardId}/edit`);
      const tableHeader = page.locator(".tile-header", { hasText: "Orders" });
      await expect(tableHeader).toBeVisible({ timeout: 30000 });
      await expect(page.getByTestId("dashboard-undo")).toBeDisabled();

      await tableHeader.locator(".tile-menu").click();
      await page
        .locator(".el-dropdown-menu__item:visible", { hasText: "Remove from dashboard" })
        .click();
      await expect(tableHeader).toHaveCount(0);
      await expect.poll(savedHasTable, { timeout: 20000 }).toBe(false);

      await page.keyboard.press("ControlOrMeta+z");
      await expect(tableHeader).toBeVisible();
      await expect(page.getByTestId("dashboard-redo")).toBeEnabled();
      await expect.poll(savedHasTable, { timeout: 20000 }).toBe(true);

      await page.keyboard.press("ControlOrMeta+Shift+z");
      await expect(tableHeader).toHaveCount(0);
      await expect.poll(savedHasTable, { timeout: 20000 }).toBe(false);
    } finally {
      await seededUndo.cleanup(request);
    }
  });

  test("a chart tile's number format and axis title are saved on the tile, and undo takes them back in one step", async ({
    page,
    request,
  }) => {
    const seededFormat = await seedDashboard(request, token, `e2e format ${Date.now()}`);
    const headers = authHeaders(token);
    const savedFormat = async () => {
      const url = `${API_URL}/catalog/dashboards/${seededFormat.dashboardId}`;
      const saved = await (await request.get(url, { headers })).json();
      return saved.layout.tiles.find((t: { id: string }) => t.id === "t-chart")?.chart_format;
    };
    try {
      await login(page, token, `#/main/dashboards/${seededFormat.dashboardId}/edit`);
      const chartHeader = page.locator(".tile-header", { hasText: "Revenue by region" });
      await expect(chartHeader).toBeVisible({ timeout: 30000 });
      await chartHeader.locator(".tile-menu").click();
      await page.locator(".el-dropdown-menu__item:visible", { hasText: "Format" }).click();

      const dialog = page.locator(".el-dialog:visible", { hasText: "Format chart" });
      await expect(dialog).toBeVisible();
      await dialog.getByTestId("chart-format-number").click();
      await page
        .locator(".el-select-dropdown__item:visible", { hasText: "Compact (1.2M)" })
        .click();
      await expect(dialog.getByTestId("chart-format-number-sample")).toHaveText(
        "1234567.891 → 1.23M",
      );

      // A custom format starts from the preset it replaces and is checked as it is typed.
      await dialog.getByTestId("chart-format-number").click();
      await page.locator(".el-select-dropdown__item:visible", { hasText: "Custom…" }).click();
      const custom = dialog.getByTestId("chart-format-number-custom");
      await expect(custom).toHaveValue(".3~s");
      await custom.fill("not a format");
      await expect(dialog.getByTestId("chart-format-number-error")).toBeVisible();
      await expect(dialog.getByTestId("chart-format-save")).toBeDisabled();
      await custom.fill("$,.2f");
      await expect(dialog.getByTestId("chart-format-number-sample")).toHaveText(
        "1234567.891 → $1,234,567.89",
      );
      await dialog.getByTestId("chart-format-y-title").fill("Region");
      await dialog.getByTestId("chart-format-save").click();
      await expect(dialog).toHaveCount(0);

      await expect
        .poll(savedFormat, { timeout: 20000 })
        .toEqual({ number_format: "$,.2f", time_format: null, x_title: null, y_title: "Region" });
      // The chart itself is untouched: the format lives on the tile.
      const viz = await (
        await request.get(`${API_URL}/catalog/visualizations/${seededFormat.vizId}`, { headers })
      ).json();
      expect(viz.spec[0].layout.format).toEqual({});
      expect(viz.spec[0].encodings.rows.map((f: { name: string }) => f.name)).toEqual(["region"]);

      await page.getByTestId("dashboard-undo").click();
      await expect.poll(savedFormat, { timeout: 20000 }).toBeNull();
    } finally {
      await seededFormat.cleanup(request);
    }
  });

  test("each region's colour is stored with the author's next edit, never on its own, and undo keeps it", async ({
    page,
    request,
  }) => {
    const seededColors = await seedDashboard(request, token, `e2e colours ${Date.now()}`, {
      colorByRegion: true,
    });
    const headers = authHeaders(token);
    const saved = async () =>
      (
        await request.get(`${API_URL}/catalog/dashboards/${seededColors.dashboardId}`, { headers })
      ).json();
    const byRegion = ["Central", "East", "North", "South", "West"].map((value, slot) => ({
      field: "region",
      value,
      slot,
    }));
    try {
      await login(page, token, `#/main/dashboards/${seededColors.dashboardId}/edit`);
      await expect(page.locator(".tile-viz-body canvas").first()).toBeVisible({ timeout: 30000 });
      // Drawing the chart gives every region a colour, but no edit was made: nothing is saved.
      await page.waitForTimeout(1500);
      await expect(page.getByTestId("dashboard-undo")).toBeDisabled();
      const opened = await saved();
      expect(opened.layout_version).toBe(1);
      expect(opened.layout.color_assignments).toEqual([]);

      await page.getByPlaceholder("Description (optional)").fill("Coloured by region");
      await expect
        .poll(async () => (await saved()).layout.color_assignments, { timeout: 20000 })
        .toEqual(byRegion);

      await page.getByPlaceholder("Description (optional)").blur();
      await page.getByTestId("dashboard-undo").click();
      await expect
        .poll(
          async () => {
            const after = await saved();
            return [after.description, after.layout.color_assignments];
          },
          { timeout: 20000 },
        )
        .toEqual([null, byRegion]);

      // The chart in the catalog is not touched.
      const viz = await (
        await request.get(`${API_URL}/catalog/visualizations/${seededColors.vizId}`, { headers })
      ).json();
      expect(viz.spec[0].layout.colorPalette).toBeUndefined();
    } finally {
      await seededColors.cleanup(request);
    }
  });

  test("typing in a filter's value box finds a value beyond the first hundred", async ({
    page,
    request,
  }) => {
    const wide = await seedWideFilterDashboard(request, token, `e2e wide filter ${Date.now()}`);
    try {
      await login(page, token, `#/main/dashboards/${wide.dashboardId}`);
      const range = page.locator(".rows-grid .rows-range");
      await expect(range).toHaveText("1–5 of 150 rows", { timeout: 30000 });

      await page.locator(".filter-chip-input").click();
      const option = (text: string) =>
        page.locator(".el-select-dropdown__item:visible").getByText(text, { exact: true });
      await expect(option("C001")).toBeVisible();
      await expect(option("C149")).toHaveCount(0);
      await expect(page.getByText("Showing the first 100 values")).toBeVisible();

      const search = page.waitForRequest(
        (req) =>
          req.url().includes("/visualizations/compute") &&
          req.postData()?.includes("regexp") === true,
      );
      await page.keyboard.type("c149");
      await search;
      await option("C149").click();
      await page.keyboard.press("Escape");

      await expect(range).toHaveText("1–1 of 1 row");
    } finally {
      await wide.cleanup(request);
    }
  });

  test("a viewer expands a chart, sees the rows behind it narrowed by the filter and downloads it", async ({
    page,
  }) => {
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
    // An earlier test duplicated this chart; either copy will do.
    const chartTile = page
      .locator(".tile", { has: page.locator(".tile-title", { hasText: "Revenue by region" }) })
      .first();
    await expect(chartTile.locator(".tile-viz-body canvas").first()).toBeVisible({
      timeout: 30000,
    });
    // The KPI and the table get a menu; text and a KPI still to be set up do not.
    await expect(
      page.locator(".tile", { has: page.locator(".kpi-value") }).getByTestId("tile-actions"),
    ).toHaveCount(1);
    await expect(
      page.locator(".tile", { hasText: "Orders" }).getByTestId("tile-actions"),
    ).toHaveCount(1);
    await expect(page.locator(".tile-text").getByTestId("tile-actions")).toHaveCount(0);
    await expect(page.locator(".tile-blank").getByTestId("tile-actions")).toHaveCount(0);

    const choose = async (entry: string) => {
      await chartTile.hover();
      await chartTile.getByTestId("tile-actions").click();
      await page.locator(".el-dropdown-menu__item:visible", { hasText: entry }).click();
    };

    await choose("Expand");
    const expanded = page.getByTestId("tile-expand-dialog");
    await expect(expanded.locator("canvas").first()).toBeVisible({ timeout: 30000 });
    await page.keyboard.press("Escape");
    await expect(expanded).toBeHidden();

    const rowsDialog = page.getByTestId("tile-rows-dialog");
    await choose("See rows");
    await expect(rowsDialog.locator(".rows-range")).toHaveText("1–7 of 7 rows", {
      timeout: 30000,
    });
    await expect(rowsDialog.locator("thead th")).toHaveText(["region", "amount"]);
    await page.keyboard.press("Escape");
    await expect(rowsDialog).toBeHidden();

    await page.locator(".filter-chip-input").click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
    await page.keyboard.press("Escape");
    await expect(page.locator(".kpi-value")).toHaveText("150");

    await choose("See rows");
    await expect(rowsDialog.locator(".rows-range")).toHaveText("1–2 of 2 rows", {
      timeout: 30000,
    });
    await expect(rowsDialog.locator("tbody tr td:first-child")).toHaveText(["West", "West"]);
    await page.keyboard.press("Escape");
    await expect(rowsDialog).toBeHidden();

    // The chart's CSV is the data it draws: one aggregated row for the one region left.
    const csvDownload = page.waitForEvent("download");
    await choose("Download CSV");
    const csv = await csvDownload;
    expect(csv.suggestedFilename()).toBe("Revenue_by_region.csv");
    const text = readFileSync(await csv.path(), "utf8").replace(/^\uFEFF/, "");
    expect(text.trim().split("\n")).toEqual(["region,amount_sum", "West,150"]);

    const imageDownload = page.waitForEvent("download");
    await choose("Download image");
    const image = await imageDownload;
    expect(image.suggestedFilename()).toBe("Revenue_by_region.png");
    const png = readFileSync(await image.path());
    expect(png.subarray(1, 4).toString()).toBe("PNG");
  });

  test("a chart made from the dashboard is named before it saves and its tile shows the name", async ({
    page,
    request,
  }) => {
    const seededNew = await seedDashboard(request, token, `e2e name ${Date.now()}`);
    const headers = authHeaders(token);
    let newVizId: number | null = null;
    try {
      // The source picker lists only tables inside a catalog schema; the seed files none.
      const namespaceId = await (
        await request.get(`${API_URL}/catalog/default-namespace-id`, { headers })
      ).json();
      const moved = await request.put(`${API_URL}/catalog/tables/${seededNew.tableId}`, {
        headers,
        data: { namespace_id: namespaceId },
      });
      expect(moved.ok()).toBe(true);
      const table = await moved.json();
      await login(page, token, `#/main/dashboards/${seededNew.dashboardId}/edit`);
      await expect(page.locator(".tile-header", { hasText: "Orders" })).toBeVisible({
        timeout: 30000,
      });

      await page.getByRole("button", { name: "New visualization" }).click();
      const picker = page.locator(".el-dialog", { hasText: "From a catalog table" });
      await picker.locator(".picker-cascader input").fill(table.name);
      await page.locator(".el-cascader__suggestion-item", { hasText: table.name }).click();
      await picker.getByRole("button", { name: "Continue" }).click();

      const nameInput = page.getByTestId("viz-name-input");
      const save = page.getByTestId("viz-save-button");
      await expect(nameInput).toHaveValue(table.name);
      await expect(save).toBeEnabled({ timeout: 30000 });

      await nameInput.fill("  ");
      await expect(page.getByTestId("viz-name-error")).toHaveText("Enter a name to save");
      await expect(save).toBeDisabled();

      await nameInput.fill("Amount per region");
      await expect(page.getByTestId("viz-name-error")).toHaveCount(0);
      // Enter in the name field saves nothing: the builder stays open.
      await nameInput.press("Enter");
      await expect(save).toBeVisible();

      const created = page.waitForResponse(
        (r) => r.request().method() === "POST" && /\/catalog\/visualizations\/?$/.test(r.url()),
      );
      await save.click();
      const response = await created;
      newVizId = (await response.json()).id;
      expect((await response.json()).name).toBe("Amount per region");
      await expect(page.locator(".tile-title", { hasText: "Amount per region" })).toBeVisible({
        timeout: 30000,
      });
    } finally {
      if (newVizId != null) {
        await request.delete(`${API_URL}/catalog/dashboards/${seededNew.dashboardId}`, { headers });
        await request.delete(`${API_URL}/catalog/visualizations/${newVizId}`, { headers });
      }
      await seededNew.cleanup(request);
    }
  });

  test("a KPI's trend draws one point per month in view and the region filter redraws it", async ({
    page,
    request,
  }) => {
    const trend = await seedTrendDashboard(request, token, `e2e trend ${Date.now()}`);
    try {
      await login(page, token, `#/main/dashboards/${trend.dashboardId}`);
      await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });

      // Jan, Feb, Apr and May: the empty March and the undated row draw no point.
      const line = page.getByTestId("kpi-trend").getByRole("img");
      await expect(line).toHaveAttribute("data-points", "4", { timeout: 30000 });
      await expect(line).toHaveAttribute(
        "aria-label",
        "Total amount by month, Jan 2024 to May 2024: down 80%",
      );

      await line.focus();
      await expect(page.locator(".kpi-spark-tip")).toHaveText(/May 2024\s*20/);

      await page.locator(".filter-chip-input").click();
      await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
      await page.keyboard.press("Escape");

      await expect(page.locator(".kpi-value")).toHaveText("150");
      await expect(line).toHaveAttribute("data-points", "2");
      await expect(line).toHaveAttribute(
        "aria-label",
        "Total amount by month, Jan 2024 to Feb 2024: down 50%",
      );
    } finally {
      await trend.cleanup(request);
    }
  });

  test("on a phone the viewer reads its tiles in one column, at desktop width in the grid", async ({
    page,
  }) => {
    // The grid's own items, in screen order; the drag placeholder is not a tile.
    const tileBoxes = async () => {
      const boxes = await page
        .locator(".vgl-item:not(.vgl-item--placeholder)")
        .evaluateAll((items) =>
          items.map((el) => {
            const r = el.getBoundingClientRect();
            return { x: Math.round(r.left), y: Math.round(r.top), h: Math.round(r.height) };
          }),
        );
      return boxes.sort((a, b) => a.y - b.y || a.x - b.x);
    };

    await page.setViewportSize({ width: 390, height: 844 });
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
    await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
    await expect(page.locator(".canvas-stacked")).toHaveCount(1);
    await expect
      .poll(async () => {
        const boxes = await tileBoxes();
        const oneLeftEdge = new Set(boxes.map((b) => b.x)).size === 1;
        const downwards = boxes.every((b, i) => i === 0 || b.y >= boxes[i - 1].y + boxes[i - 1].h);
        return boxes.length > 1 && oneLeftEdge && downwards;
      })
      .toBe(true);

    await page.setViewportSize({ width: 1400, height: 900 });
    await expect(page.locator(".canvas-stacked")).toHaveCount(0);
    await expect
      .poll(async () => new Set((await tileBoxes()).map((b) => b.x)).size)
      .toBeGreaterThan(1);
  });

  test("Present hides the app around the dashboard and Esc brings it back", async ({ page }) => {
    await page.setViewportSize({ width: 1400, height: 900 });
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
    await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
    const sidebar = page.locator(".app-layout__sidebar-wrapper");
    await expect(sidebar).toBeVisible();

    // Present at once, inside the filter's URL debounce: the link must keep both.
    await page.locator(".filter-chip-input").click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
    await page.keyboard.press("Escape");
    await page.getByTestId("dashboard-present").click();
    await expect(sidebar).toHaveCount(0);
    await expect(page.locator(".catalog-tabs")).toBeHidden();
    await expect(page.getByRole("button", { name: "Edit", exact: true })).toHaveCount(0);
    await expect(page.getByTestId("dashboard-exit-present")).toBeVisible();
    await expect(page).toHaveURL(/[?&]present=1/);
    await expect(page).toHaveURL(/[?&]f=/);
    await expect(page.locator(".kpi-value")).toHaveText("150");

    await page.keyboard.press("Escape");
    await expect(sidebar).toBeVisible();
    await expect(page.locator(".catalog-tabs")).toBeVisible();
    await expect(page.getByRole("button", { name: "Edit", exact: true })).toBeVisible();
    await expect(page).not.toHaveURL(/[?&]present=1/);
    await expect(page).toHaveURL(/[?&]f=/);
  });

  test("?present=1 opens in present mode with working filters, and Exit keeps the filter", async ({
    page,
  }) => {
    await page.setViewportSize({ width: 1400, height: 900 });
    await login(page, token, `#/main/dashboards/${seeded.dashboardId}?present=1`);
    await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
    await expect(page.locator(".app-layout__sidebar-wrapper")).toHaveCount(0);

    await page.locator(".filter-chip-input").click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
    await page.keyboard.press("Escape");
    await expect(page.locator(".kpi-value")).toHaveText("150");
    // Esc closed the value list, not present mode.
    await expect(page.locator(".app-layout__sidebar-wrapper")).toHaveCount(0);
    await expect(page).toHaveURL(/[?&]f=/);

    await page.getByTestId("dashboard-exit-present").click();
    await expect(page.locator(".app-layout__sidebar-wrapper")).toBeVisible();
    await expect(page).not.toHaveURL(/[?&]present=1/);
    await expect(page).toHaveURL(/[?&]f=/);
    await expect(page.locator(".kpi-value")).toHaveText("150");
  });

  test("an author's auto-refresh interval is saved and a viewer's page refreshes on it", async ({
    page,
    request,
  }) => {
    const url = `${API_URL}/catalog/dashboards/${seeded.dashboardId}`;
    const headers = authHeaders(token);
    try {
      await login(page, token, `#/main/dashboards/${seeded.dashboardId}/edit`);
      await expect(page.locator(".tile-header").first()).toBeVisible({ timeout: 30000 });
      await page.getByTestId("dashboard-auto-refresh-setting").click();
      await page.locator(".el-select-dropdown__item:visible", { hasText: "Every 1 min" }).click();
      await expect
        .poll(
          async () =>
            (await (await request.get(url, { headers })).json()).layout.auto_refresh_seconds,
          { timeout: 20000 },
        )
        .toBe(60);

      // A fake clock, so the minute passes without waiting for it.
      await page.clock.install();
      await page.goto("about:blank");
      const computes: string[] = [];
      page.on("request", (req) => {
        if (
          req.method() === "POST" &&
          req.url().includes(`/visualizations/${seeded.vizId}/compute`)
        )
          computes.push(req.url());
      });
      await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
      await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
      await expect(page.getByTestId("dashboard-auto-refresh")).toHaveText(/Every 1 min/);
      await expect.poll(() => computes.length).toBeGreaterThan(0);
      const before = computes.length;

      await page.clock.runFor(61_000);
      await expect.poll(() => computes.length, { timeout: 15000 }).toBeGreaterThan(before);

      // A viewer's pause is their own: nothing is saved.
      await page.getByTestId("dashboard-auto-refresh").click();
      await page.locator(".el-dropdown-menu__item:visible", { hasText: "Pause" }).click();
      await expect(page.getByTestId("dashboard-auto-refresh")).toHaveText(/paused/);
      const layout = (await (await request.get(url, { headers })).json()).layout;
      expect(layout.auto_refresh_seconds).toBe(60);
    } finally {
      const current = await (await request.get(url, { headers })).json();
      await request.put(url, {
        headers,
        data: { layout: { ...current.layout, auto_refresh_seconds: null } },
      });
    }
  });

  test("an author moves a tile to a second tab; a viewer sees each tab's tiles there, with ?tab= and the filter kept", async ({
    page,
    request,
  }) => {
    const tabbed = await seedDashboard(request, token, `e2e tabs ${Date.now()}`);
    const url = `${API_URL}/catalog/dashboards/${tabbed.dashboardId}`;
    const headers = authHeaders(token);
    try {
      await page.setViewportSize({ width: 1400, height: 900 });
      await login(page, token, `#/main/dashboards/${tabbed.dashboardId}/edit`);
      const tableHeader = page.locator(".tile-header", { hasText: "Orders" });
      await expect(tableHeader).toBeVisible({ timeout: 30000 });

      await page.getByTestId("dashboard-add-tab").click();
      await expect(page.getByRole("tab")).toHaveText(["Tab 1", "Tab 2"]);
      await expect(page.getByRole("tab", { name: "Tab 2" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
      await expect(page.locator(".tile-header")).toHaveCount(0);

      await page.getByRole("tab", { name: "Tab 2" }).dblclick();
      await page.getByTestId("dashboard-tab-rename").fill("Details");
      await page.getByTestId("dashboard-tab-rename").press("Enter");
      await page.getByRole("tab", { name: "Tab 1" }).click();
      await expect(tableHeader).toBeVisible();
      await tableHeader.locator(".tile-menu").click();
      await page.locator(".el-dropdown-menu__item:visible", { hasText: "Move to Details" }).click();
      await expect(tableHeader).toHaveCount(0);
      await page.getByRole("tab", { name: "Details" }).click();
      await expect(tableHeader).toBeVisible();

      await expect
        .poll(
          async () => {
            const { layout } = await (await request.get(url, { headers })).json();
            const tabOf = (id: string) =>
              layout.tiles.find((t: { id: string }) => t.id === id)?.tab_id;
            const tabs = layout.tabs.map((t: { id: string; title: string }) => t.id);
            return [
              layout.tabs.map((t: { title: string }) => t.title),
              tabOf("t-chart") === tabs[0],
              tabOf("t-table") === tabs[1],
            ];
          },
          { timeout: 20000 },
        )
        .toEqual([["Tab 1", "Details"], true, true]);

      await login(page, token, `#/main/dashboards/${tabbed.dashboardId}`);
      await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
      await expect(page.locator(".rows-grid")).toHaveCount(0);
      await page.locator(".filter-chip-input").click();
      await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
      await page.keyboard.press("Escape");
      await expect(page.locator(".kpi-value")).toHaveText("150");

      // The tab list moves with the arrow keys, like any tablist.
      await page.getByRole("tab", { name: "Tab 1" }).focus();
      await page.keyboard.press("ArrowRight");
      await expect(page.getByRole("tab", { name: "Details" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
      await expect(page).toHaveURL(/[?&]tab=/);
      await expect(page.locator(".kpi-value")).toHaveCount(0);
      await expect(page.locator(".rows-range")).toHaveText("1–2 of 2 rows", { timeout: 30000 });

      await page.reload();
      await page.waitForLoadState("load");
      await expect(page.locator(".rows-range")).toHaveText("1–2 of 2 rows", { timeout: 30000 });
      await expect(page.getByRole("tab", { name: "Details" })).toHaveAttribute(
        "aria-selected",
        "true",
      );

      await page.getByRole("tab", { name: "Tab 1" }).click();
      await expect(page.locator(".kpi-value")).toHaveText("150", { timeout: 30000 });
    } finally {
      await tabbed.cleanup(request);
    }
  });

  test("undo takes back a tab deletion with its tiles", async ({ page, request }) => {
    const tabbed = await seedDashboard(request, token, `e2e tab undo ${Date.now()}`);
    const url = `${API_URL}/catalog/dashboards/${tabbed.dashboardId}`;
    const headers = authHeaders(token);
    const savedTabs = async () => {
      const { layout } = await (await request.get(url, { headers })).json();
      const table = layout.tiles.find((t: { id: string }) => t.id === "t-table");
      return [layout.tabs.map((t: { title: string }) => t.title), table?.tab_id ?? null];
    };
    try {
      const current = await (await request.get(url, { headers })).json();
      const layout = {
        ...current.layout,
        tabs: [
          { id: "tab-overview", title: "Overview" },
          { id: "tab-detail", title: "Detail" },
        ],
        tiles: current.layout.tiles.map((t: { id: string }) => ({
          ...t,
          tab_id: t.id === "t-table" ? "tab-detail" : "tab-overview",
        })),
      };
      expect((await request.put(url, { headers, data: { layout } })).ok()).toBe(true);

      await login(page, token, `#/main/dashboards/${tabbed.dashboardId}/edit`);
      await page.getByRole("tab", { name: "Detail" }).click({ timeout: 30000 });
      const tableHeader = page.locator(".tile-header", { hasText: "Orders" });
      await expect(tableHeader).toBeVisible({ timeout: 30000 });

      await page.getByTestId("dashboard-tab-menu").click();
      await page.locator(".el-dropdown-menu__item:visible", { hasText: "Delete tab" }).click();
      await page.getByRole("button", { name: "Delete", exact: true }).click();
      await page.getByRole("button", { name: "Keep the tab" }).click();
      await expect(page.getByRole("tab")).toHaveText(["Overview"]);
      await expect.poll(savedTabs, { timeout: 20000 }).toEqual([["Overview"], null]);

      await page.getByTestId("dashboard-undo").click();
      await expect(page.getByRole("tab")).toHaveText(["Overview", "Detail"]);
      await page.getByRole("tab", { name: "Detail" }).click();
      await expect(tableHeader).toBeVisible();
      await expect
        .poll(savedTabs, { timeout: 20000 })
        .toEqual([["Overview", "Detail"], "tab-detail"]);
    } finally {
      await tabbed.cleanup(request);
    }
  });

  // Last: it turns the shared seed's region filter single-select.
  test("a single-select filter picks one value at a time and narrows the KPI", async ({
    page,
    request,
  }) => {
    const url = `${API_URL}/catalog/dashboards/${seeded.dashboardId}`;
    const headers = authHeaders(token);
    const current = await (await request.get(url, { headers })).json();
    const layout = {
      ...current.layout,
      filters: current.layout.filters.map((f: Record<string, unknown>) => ({
        ...f,
        multiple: false,
      })),
    };
    const put = await request.put(url, { headers, data: { layout } });
    expect(put.ok()).toBe(true);
    expect((await put.json()).layout.filters[0].multiple).toBe(false);

    await login(page, token, `#/main/dashboards/${seeded.dashboardId}`);
    await expect(page.locator(".kpi-value")).toHaveText("270", { timeout: 30000 });
    const input = page.locator(".filter-chip-input");

    await input.click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "West" }).click();
    await expect(page.locator(".kpi-value")).toHaveText("150");

    // Picking another value replaces the first: East alone is 70, both would be 220.
    await input.click();
    await page.locator(".el-select-dropdown__item:visible", { hasText: "East" }).click();
    await expect(page.locator(".kpi-value")).toHaveText("70");
    await expect(input.locator(".el-tag")).toHaveCount(0);

    await page.getByRole("button", { name: "Reset" }).click();
    await expect(page.locator(".kpi-value")).toHaveText("270");
  });
});
