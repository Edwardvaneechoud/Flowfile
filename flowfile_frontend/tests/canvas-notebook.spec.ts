import { test, expect, Page, APIRequestContext } from "@playwright/test";

import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { minimizePalette, openFlow } from "./helpers/canvas";

/**
 * The canvas notebook is the catalog NotebookPanel on the flow's session: render, run, push, undo.
 *
 * Needs no Docker. Set SHOTS_DIR to keep a screenshot of each step.
 */

const SHOTS_DIR = process.env.SHOTS_DIR;

async function shot(page: Page, name: string) {
  if (SHOTS_DIR) await page.screenshot({ path: `${SHOTS_DIR}/${name}.png` });
}

/** manual_input -> filter (salary > 60000) -> polars_code, through the editor API. */
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
        data: [
          [1, 2, 3, 4],
          [50000, 75000, 90000, 65000],
        ],
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
}

type RenderedCell = { cell_id: string; node_ids: number[]; code: string };

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

const NOTEBOOK = "#rightDrawer .code-notebook .notebook-panel";

const cellTexts = (page: Page) => page.locator(`${NOTEBOOK} .nb-cell .cm-content`).allInnerTexts();

/** The notebook is the Code drawer's Notebook mode; the drawer overlays the canvas's right side. */
async function openNotebook(page: Page) {
  await page.locator('[data-tutorial="generate-code-btn"]').click();
  await page.getByTestId("code-mode-notebook").click();
}

test.describe("Canvas notebook", () => {
  test.use({ viewport: { width: 1600, height: 1000 } });

  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `canvas_notebook_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await buildFlow(request, token, flowId);
    await openFlow(page, token, flowId, name);
  });

  test.afterEach(async ({ request }) => {
    if (flowId) await closeFlow(request, token, flowId);
  });

  test("render, run, push, undo and move", async ({ page, request }) => {
    const panel = page.locator(NOTEBOOK);
    await openNotebook(page);
    const cells = await renderedCells(request, token, flowId);
    for (const cell of cells) {
      await expect(panel.locator(`[data-cell-id="${cell.cell_id}"]`)).toHaveCount(1);
    }
    expect(await panel.locator(".nb-cell").count()).toBeGreaterThanOrEqual(cells.length);
    const filter = cellOf(cells, 2);
    const polarsCell = cellOf(cells, 3);
    await expect(panel.locator(".nb-banner")).toContainText("Flow session");
    await shot(page, "01-open");

    const added = panel.locator(".nb-cell").nth(await panel.locator(".nb-cell").count());
    await panel.getByRole("button", { name: "Add cell" }).click();
    await added.locator(".cm-content").click();
    await page.keyboard.insertText("x = fl.from_dict({'a': [1, 2]}); x");
    await page.keyboard.press("Shift+Enter");
    const rows = added.locator(".display-table .ag-center-cols-container .ag-row");
    await expect(rows).toHaveCount(2, { timeout: 60_000 });
    await shot(page, "02-ran");
    await added.locator(".nb-cell-menu").click();
    await page.locator(".nb-cell-menu-popper:visible [data-action='delete']").click();

    const filterCell = panel.locator(`[data-cell-id="${filter.cell_id}"] .cm-content`);
    await filterCell.click();
    await page.keyboard.press("ControlOrMeta+A");
    await page.keyboard.insertText(filter.code.replace("60000", "80000"));
    const pushed = page.waitForResponse((r) => r.url().includes("/editor/notebook/push/"));
    await panel.getByTestId("nb-push").click();
    expect((await pushed).status()).toBe(200);
    // Read raw: the canvas reload after a push or undo can briefly answer non-200.
    const filterValue = async () =>
      (
        await request.get(`${API_URL}/node?flow_id=${flowId}&node_id=2&get_data=false`, {
          headers: authHeaders(token),
        })
      ).text();
    await expect.poll(filterValue).toContain("80000");
    await expect(page.getByText("Pushed to the canvas")).toBeVisible();
    await shot(page, "03-pushed");

    await page.locator(".undo-redo-controls .control-btn").first().click();
    await expect.poll(filterValue).toContain("60000");
    const undone = cellOf(await renderedCells(request, token, flowId), 2);
    await expect(panel.locator(`[data-cell-id="${undone.cell_id}"] .cm-content`)).toContainText(
      "60000",
    );
    await shot(page, "04-undone");

    await minimizePalette(page);
    const before = await cellTexts(page);
    const node = page.locator('.vue-flow__node[data-id="1"]');
    const box = await node.boundingBox();
    if (!box) throw new Error("node not visible");
    const rerender = page.waitForResponse((r) => r.url().includes("/notebook/render"));
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.mouse.down();
    await page.mouse.move(box.x + box.width / 2 + 60, box.y + box.height / 2 + 160, { steps: 10 });
    await page.mouse.up();
    await rerender;
    await page.waitForTimeout(300);
    expect(await cellTexts(page)).toEqual(before);
    await shot(page, "05-moved");

    await panel.locator(`[data-cell-id="${polarsCell.cell_id}"] .nb-cell-menu`).click();
    await page.locator(".nb-cell-menu-popper:visible [data-action='run-on-canvas']").click();
    const preview = page.getByText("double", { exact: true });
    await expect(preview).toBeVisible({ timeout: 30_000 });
    await shot(page, "06-run-on-canvas");
  });
});
