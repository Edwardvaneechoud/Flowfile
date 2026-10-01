import { test, expect, Locator, Page, APIRequestContext } from "@playwright/test";

import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { minimizePalette, openFlow } from "./helpers/canvas";

/**
 * The canvas notebook is the catalog NotebookPanel on the open flow, with no kernel: Run syncs
 * edited cells to the canvas and runs the cell's node there, Push syncs without running.
 *
 * Needs no Docker. Set SHOTS_DIR to keep a screenshot of each step.
 */

const SHOTS_DIR = process.env.SHOTS_DIR;

async function shot(page: Page, name: string) {
  if (SHOTS_DIR) await page.screenshot({ path: `${SHOTS_DIR}/${name}.png` });
}

const ROWS = 150;

/**
 * manual_input (150 rows) -> filter (salary > 60000, 149 rows) -> polars_code, plus one flow
 * parameter, through the editor API. Development mode keeps each node's rows for the preview.
 */
async function buildFlow(request: APIRequestContext, token: string, flowId: number) {
  const post = async (path: string, params: Record<string, unknown>, data?: unknown) => {
    const query = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]));
    const response = await request.post(`${API_URL}${path}?${query}`, {
      headers: authHeaders(token),
      data,
    });
    expect(response.ok(), `${path}: ${await response.text()}`).toBe(true);
  };
  const add = (id: number, type: string, x: number) =>
    post("/editor/add_node/", {
      flow_id: flowId,
      node_id: id,
      node_type: type,
      pos_x: x,
      pos_y: 200,
    });
  const connect = (from: number, to: number) =>
    post(
      "/editor/connect_node/",
      { flow_id: flowId },
      {
        input_connection: { node_id: to, connection_class: "input-0" },
        output_connection: { node_id: from, connection_class: "output-0" },
      },
    );
  await add(1, "manual_input", 100);
  await add(2, "filter", 400);
  await add(3, "polars_code", 700);
  const ids = Array.from({ length: ROWS }, (_, i) => i + 1);
  const salaries = [50000, 75000, 90000, 65000, ...Array(ROWS - 4).fill(70000)];
  await post(
    "/update_settings/",
    { node_type: "manual_input" },
    {
      flow_id: flowId,
      node_id: 1,
      raw_data_format: {
        columns: [
          { name: "id", data_type: "Integer" },
          { name: "salary", data_type: "Integer" },
        ],
        data: [ids, salaries],
      },
    },
  );
  await connect(1, 2);
  await post(
    "/update_settings/",
    { node_type: "filter" },
    {
      flow_id: flowId,
      node_id: 2,
      depending_on_id: 1,
      filter_input: {
        mode: "basic",
        basic_filter: { field: "salary", operator: ">", value: "60000" },
      },
    },
  );
  await connect(2, 3);
  await post(
    "/update_settings/",
    { node_type: "polars_code" },
    {
      flow_id: flowId,
      node_id: 3,
      depending_on_ids: [2],
      polars_code_input: {
        polars_code: "output_df = input_df.with_columns((pl.col('salary') * 2).alias('double'))\n",
      },
    },
  );
  const settings = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(settings.ok()).toBe(true);
  await post(
    "/flow_settings",
    {},
    {
      ...(await settings.json()),
      execution_mode: "Development",
      parameters: [{ name: "min_salary", default_value: "60000", type: "integer" }],
    },
  );
}

type RenderedCell = { cell_id: string; node_ids: number[]; kind: string; code: string };

/** The render route's cells; a cell may span several nodes, so tests locate cells by node id. */
async function renderedCells(request: APIRequestContext, token: string, flowId: number) {
  const response = await request.get(`${API_URL}/notebook/render?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  return (await response.json()).cells as RenderedCell[];
}

const cellOf = (cells: RenderedCell[], nodeId: number) =>
  cells.find((c) => c.node_ids.includes(nodeId))!;

const cellOfKind = (cells: RenderedCell[], kind: string) => cells.find((c) => c.kind === kind)!;

const NOTEBOOK = ".code-dock .code-notebook .notebook-panel";

const cellTexts = (page: Page) => page.locator(`${NOTEBOOK} .nb-cell .cm-content`).allInnerTexts();

/** The notebook is the code pane's Notebook mode; the pane sits beside the canvas, which narrows. */
async function openNotebook(page: Page) {
  await page.locator('[data-tutorial="generate-code-btn"]').click();
  // The Code button's tooltip sits over the pane's mode toggle until the pointer leaves it.
  await page.mouse.move(400, 500);
  await page.getByTestId("code-mode-notebook").click();
}

const syncState = (cell: Locator) => cell.getByTestId("nb-sync-state");

async function replaceCode(page: Page, cell: Locator, code: string) {
  await cell.locator(".cm-content").click();
  await page.keyboard.press("ControlOrMeta+A");
  await page.keyboard.insertText(code);
  await expect(syncState(cell)).toHaveAttribute("data-sync-state", "edited");
}

const responseTo = (page: Page, path: string) =>
  page.waitForResponse((r) => r.url().includes(path));

/** Let the app apply a response it has read: its promise chain, a Vue flush, then a paint. */
async function settle(page: Page) {
  await page.evaluate(
    () => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(resolve))),
  );
}

test.describe("Canvas notebook", () => {
  test.use({ viewport: { width: 1600, height: 1000 } });

  let token: string;
  let flowId: number;
  let apiCalls: string[];

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `canvas_notebook_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await buildFlow(request, token, flowId);
    await openFlow(page, token, flowId, name);
    apiCalls = [];
    page.on("request", (r) => {
      if (r.url().includes("/api/")) apiCalls.push(r.url());
    });
  });

  test.afterEach(async ({ request }) => {
    if (flowId) await closeFlow(request, token, flowId);
    const kernelCalls = apiCalls.filter((u) => /\/kernels|notebook\/status|flow-session/.test(u));
    expect(kernelCalls, "a flow tab never addresses a kernel").toEqual([]);
  });

  const called = (path: string, since = 0) => apiCalls.slice(since).some((u) => u.includes(path));

  // Read raw: the canvas reload after a sync or undo can briefly answer non-200.
  const filterSettings = async (request: APIRequestContext) =>
    (
      await request.get(`${API_URL}/node?flow_id=${flowId}&node_id=2&get_data=false`, {
        headers: authHeaders(token),
      })
    ).text();

  test("renders synced cells with no kernel and runs an unedited node cell", async ({
    page,
    request,
  }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cells = await renderedCells(request, token, flowId);
    for (const cell of cells) {
      const shown = panel.locator(`[data-cell-id="${cell.cell_id}"]`);
      await expect(shown).toHaveCount(1);
      await expect(syncState(shown)).toHaveAttribute("data-sync-state", "synced");
    }
    expect(cells.map((c) => c.kind)).toEqual(["imports", "parameters", "node", "node"]);
    await expect(panel.getByTestId("nb-session-status")).toHaveCount(0);
    await expect(panel.locator(".nb-kernel-select")).toHaveCount(0);
    const more = panel.getByRole("button", { name: "More actions" });
    await more.click();
    await expect(page.getByRole("menuitem", { name: "Clear outputs" })).toBeVisible();
    await expect(page.getByRole("menuitem", { name: "Reset session" })).toHaveCount(0);
    await more.click();
    await expect(page.getByRole("menuitem", { name: "Clear outputs" })).toBeHidden();
    await shot(page, "01-open");

    const filter = panel.locator(`[data-cell-id="${cellOf(cells, 2).cell_id}"]`);
    const lineage = responseTo(page, "/editor/notebook/run_lineage/");
    const rows = responseTo(page, `/node/data?flow_id=${flowId}&node_id=2`);
    await filter.locator(".nb-run").click();
    expect((await lineage).status()).toBe(200);
    // A lazy result past the 100-row sample has no known row count.
    const example = await (await rows).json();
    expect(example.data).toHaveLength(100);
    expect(example.number_of_records).toBeNull();
    await expect(filter.locator(".display-title")).toHaveText(
      "Node #2 · preview of up to 100 rows · total rows unknown",
      { timeout: 30_000 },
    );
    await expect(filter.getByRole("treegrid")).toHaveAttribute("aria-rowcount", "101");
    await expect(filter.locator(".cell-output")).not.toContainText("NaN");
    await expect(filter.locator(".display-table-footer")).not.toContainText("showing");
    await expect(syncState(filter)).toHaveAttribute("data-sync-state", "synced");
    expect(called("/editor/notebook/run_lineage/")).toBe(true);
    expect(called("/editor/notebook/push/")).toBe(false);
    await shot(page, "02-ran-unedited");
  });

  test("in Performance mode a node cell fetches its rows after the run", async ({
    page,
    request,
  }) => {
    const current = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, {
      headers: authHeaders(token),
    });
    const updated = await request.post(`${API_URL}/flow_settings`, {
      headers: authHeaders(token),
      data: { ...(await current.json()), execution_mode: "Performance" },
    });
    expect(updated.ok(), await updated.text()).toBe(true);

    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const filter = panel.locator(
      `[data-cell-id="${cellOf(await renderedCells(request, token, flowId), 2).cell_id}"]`,
    );
    const rowsPath = `/node/data?flow_id=${flowId}&node_id=2`;
    const lineage = responseTo(page, "/editor/notebook/run_lineage/");
    const noRows = responseTo(page, rowsPath);
    const fetch = responseTo(page, "/node/trigger_fetch_data");
    const rows = page.waitForResponse(
      async (r) => r.url().includes(rowsPath) && (await r.json()).has_example_data === true,
    );
    await filter.locator(".nb-run").click();
    expect((await lineage).status()).toBe(200);
    expect((await (await noRows).json()).has_example_data).toBe(false);
    const fetched = await fetch;
    expect(fetched.status()).toBe(200);
    const params = new URL(fetched.url()).searchParams;
    expect([params.get("flow_id"), params.get("node_id")]).toEqual([String(flowId), "2"]);
    expect(params.get("performance_mode")).not.toBe("true");
    expect((await (await rows).json()).data).toHaveLength(100);
    await expect(filter.locator(".display-title")).toHaveText(
      /^Node #2 · preview of up to 100 rows/,
      { timeout: 30_000 },
    );
    await expect(filter.getByRole("treegrid")).toHaveAttribute("aria-rowcount", "101");
    await expect(filter.locator(".cell-output")).not.toContainText("has no result yet");
    await expect(syncState(filter)).toHaveAttribute("data-sync-state", "synced");
    expect(apiCalls.filter((u) => u.includes("/node/trigger_fetch_data"))).toHaveLength(1);
    await shot(page, "02b-performance-fetched");
  });

  test("Run on an edited node cell syncs it and shows its rows; undo restores the canvas", async ({
    page,
    request,
  }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cellModel = cellOf(await renderedCells(request, token, flowId), 2);
    const filter = panel.locator(`[data-cell-id="${cellModel.cell_id}"]`);
    await replaceCode(page, filter, cellModel.code.replace("60000", "80000"));

    const pushed = responseTo(page, "/editor/notebook/push/");
    const rows = responseTo(page, `/node/data?flow_id=${flowId}&node_id=2`);
    await filter.locator(".nb-run").click();
    expect((await pushed).status()).toBe(200);
    expect((await rows).status()).toBe(200);
    await expect(filter.locator(".display-title")).toHaveText(
      "Node #2 · preview of up to 100 rows",
      { timeout: 30_000 },
    );
    await expect(filter.locator(".display-table .ag-center-cols-container .ag-row")).toHaveCount(1);
    await expect(filter.locator(".display-table")).toContainText("90000");
    await expect(syncState(filter)).toHaveAttribute("data-sync-state", "synced");
    await expect.poll(() => filterSettings(request)).toContain("80000");
    await shot(page, "03-ran-edited");

    await page.locator(".undo-redo-controls .control-btn").first().click();
    await expect.poll(() => filterSettings(request)).toContain("60000");
    const undone = cellOf(await renderedCells(request, token, flowId), 2);
    await expect(panel.locator(`[data-cell-id="${undone.cell_id}"] .cm-content`)).toContainText(
      "60000",
    );
    await expect(syncState(panel.locator(`[data-cell-id="${undone.cell_id}"]`))).toHaveAttribute(
      "data-sync-state",
      "synced",
    );
    await shot(page, "04-undone");
  });

  test("Run on the parameters cell syncs it and lists the parameters", async ({
    page,
    request,
  }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cellModel = cellOfKind(await renderedCells(request, token, flowId), "parameters");
    const params = panel.locator(`[data-cell-id="${cellModel.cell_id}"]`);
    await replaceCode(page, params, cellModel.code.replace("default=60000", "default=70000"));

    const pushed = responseTo(page, "/editor/notebook/push/");
    await params.locator(".nb-run").click();
    expect((await pushed).status()).toBe(200);
    await expect(params.locator(".display-title")).toHaveText("Flow parameters", {
      timeout: 30_000,
    });
    const row = params.locator(".display-table .ag-center-cols-container .ag-row");
    await expect(row).toHaveCount(1);
    await expect(row).toHaveText(/min_salary\s*integer\s*70000/);
    await expect(syncState(params)).toHaveAttribute("data-sync-state", "synced");
    const defaultValue = async () => {
      const response = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, {
        headers: authHeaders(token),
      });
      return response.ok() ? (await response.json()).parameters?.[0]?.default_value : null;
    };
    await expect.poll(defaultValue).toBe("70000");
    expect(called("/editor/notebook/run_lineage/")).toBe(false);
    await shot(page, "05-parameters");
  });

  test("a cell outside the dialect fails on its line and blocks the sync", async ({
    page,
    request,
  }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cellModel = cellOf(await renderedCells(request, token, flowId), 2);
    const filter = panel.locator(`[data-cell-id="${cellModel.cell_id}"]`);
    await replaceCode(page, filter, cellModel.code.replace("60000", "80000"));

    const last = panel.locator(".nb-cell").nth(await panel.locator(".nb-cell").count());
    await panel.getByRole("button", { name: "Add cell" }).click();
    const added = panel.locator(`[data-cell-id="${await last.getAttribute("data-cell-id")}"]`);
    await added.locator(".cm-content").click();
    await page.keyboard.insertText("threshold = 8\nprint(threshold)");
    const refused = responseTo(page, "/editor/notebook/push/");
    await added.locator(".nb-run").click();
    expect((await refused).status()).toBe(422);
    await expect(syncState(added)).toHaveAttribute("data-sync-state", "error");
    await expect(added.locator(".cm-line.nb-sync-error-line")).toHaveText("print(threshold)");
    await expect(added.locator(".output-error")).toContainText("Line 2:");
    await expect(added.locator(".output-error")).toContainText("this needs a kernel");
    await expect(syncState(filter)).toHaveAttribute("data-sync-state", "edited");
    expect(apiCalls.filter((u) => u.includes("/editor/notebook/push/"))).toHaveLength(1);
    expect(await filterSettings(request)).not.toContain("80000");
    await shot(page, "06-refused");

    // A plain value syncs and shows nothing.
    await replaceCode(page, added, "threshold = 8");
    const pushed = responseTo(page, "/editor/notebook/push/");
    await added.locator(".nb-run").click();
    expect((await pushed).status()).toBe(200);
    await expect(syncState(added)).toHaveAttribute("data-sync-state", "synced");
    await expect(syncState(filter)).toHaveAttribute("data-sync-state", "synced");
    await expect(added.locator(".cell-output")).toHaveCount(0);
    await expect(added.locator(".nb-sync-error-line")).toHaveCount(0);
    await expect.poll(() => filterSettings(request)).toContain("80000");
    expect(called("/editor/notebook/run_lineage/")).toBe(false);
    await shot(page, "07-plain-synced");
  });

  test("Push syncs without running; Run all runs the flow and fills every cell", async ({
    page,
    request,
  }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cells = await renderedCells(request, token, flowId);
    const cellAt = (cell: RenderedCell) => panel.locator(`[data-cell-id="${cell.cell_id}"]`);
    const filter = cellAt(cellOf(cells, 2));
    await replaceCode(page, filter, cellOf(cells, 2).code.replace("60000", "80000"));

    const pushed = responseTo(page, "/editor/notebook/push/");
    await panel.getByTestId("nb-push").click();
    expect((await pushed).status()).toBe(200);
    await expect(page.getByText("Pushed to the canvas")).toBeVisible();
    await expect(syncState(filter)).toHaveAttribute("data-sync-state", "synced");
    await expect.poll(() => filterSettings(request)).toContain("80000");
    await expect(filter.locator(".cell-output")).toHaveCount(0);
    expect(called("/editor/notebook/run_lineage/") || called("/flow/run/")).toBe(false);
    await shot(page, "08-pushed");

    const runAllStart = apiCalls.length;
    const run = responseTo(page, "/flow/run/");
    await panel.getByTestId("nb-run-all").click();
    expect((await run).status()).toBe(200);
    const polars = cellAt(cellOf(cells, 3));
    await expect(polars.locator(".display-title")).toHaveText(
      "Node #3 · preview of up to 100 rows",
      { timeout: 30_000 },
    );
    await expect(polars.locator(".display-table")).toContainText("double");
    await expect(polars.locator(".display-table")).toContainText("180000");
    await expect(filter.locator(".display-title")).toHaveText(
      "Node #2 · preview of up to 100 rows",
    );
    await expect(filter.locator(".display-table .ag-center-cols-container .ag-row")).toHaveCount(1);
    await expect(cellAt(cellOfKind(cells, "parameters")).locator(".display-title")).toHaveText(
      "Flow parameters",
    );
    await expect(cellAt(cellOfKind(cells, "imports")).locator(".cell-output")).toHaveCount(0);
    expect(called("/editor/notebook/push/", runAllStart)).toBe(false);
    await shot(page, "09-run-all");
  });

  test("moving a node keeps the cells; preview on canvas; a double-click closes the pane", async ({
    page,
    request,
  }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cells = await renderedCells(request, token, flowId);
    await expect(panel.locator(`[data-cell-id="${cells.at(-1)!.cell_id}"]`)).toHaveCount(1);

    await minimizePalette(page);
    const before = await cellTexts(page);
    const node = page.locator('.vue-flow__node[data-id="1"]');
    const box = await node.boundingBox();
    if (!box) throw new Error("node not visible");
    const rerender = responseTo(page, "/notebook/render");
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.mouse.down();
    await page.mouse.move(box.x + box.width / 2 + 60, box.y + box.height / 2 + 160, { steps: 10 });
    await page.mouse.up();
    await (await rerender).finished();
    await settle(page);
    expect(await cellTexts(page)).toEqual(before);
    await expect(panel.locator('[data-sync-state="edited"]')).toHaveCount(0);
    await shot(page, "10-moved");

    const polars = panel.locator(`[data-cell-id="${cellOf(cells, 3).cell_id}"]`);
    await polars.locator(".nb-cell-menu").click();
    await page.locator(".nb-cell-menu-popper:visible [data-action='run-on-canvas']").click();
    await expect(polars.locator(".display-table")).toContainText("double", { timeout: 30_000 });
    await expect(page.locator("#bottomDock").getByText("double", { exact: true })).toBeVisible();
    await shot(page, "11-preview-on-canvas");

    // A double-click on empty canvas closes the code pane; a single click keeps it.
    await minimizePalette(page);
    await page.locator(".vue-flow__pane").click({ position: { x: 350, y: 60 } });
    await expect(page.locator(".code-dock")).toHaveCount(1);
    await page.locator(".vue-flow__pane").dblclick({ position: { x: 350, y: 60 } });
    await expect(page.locator(".code-dock")).toHaveCount(0);
    await shot(page, "12-dblclick-closed");
  });
});
