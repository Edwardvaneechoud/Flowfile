import { test, expect, Page } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  nodeIdsByType,
} from "./helpers/api";
import { dragPaletteNode, minimizePalette, openFlow, openNodeSettings } from "./helpers/canvas";

/**
 * The read-only canvas notebook dock: canvas edits re-render its cells, layout moves do not.
 *
 * Needs a core started with FEATURE_FLAG_CANVAS_NOTEBOOK=1 (the spec skips when the status
 * route answers 503) and no Docker. Set SHOTS_DIR to keep a screenshot of each step.
 */

const SHOTS_DIR = process.env.SHOTS_DIR;

async function shot(page: Page, name: string) {
  if (SHOTS_DIR) await page.screenshot({ path: `${SHOTS_DIR}/${name}.png` });
}

const renderCount = async (page: Page) =>
  Number(await page.locator(".canvas-notebook-dock").getAttribute("data-render-count"));

test.describe("Canvas notebook dock", () => {
  test.use({ viewport: { width: 1600, height: 1000 } });

  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const status = await request.get(`${API_URL}/notebook/status`, { headers: authHeaders(token) });
    test.skip(status.status() === 503, "FEATURE_FLAG_CANVAS_NOTEBOOK is off on this core");
    const name = `canvas_notebook_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await openFlow(page, token, flowId, name);
  });

  test.afterEach(async ({ request }) => {
    if (flowId) await closeFlow(request, token, flowId);
  });

  test("drop, configure and move a node", async ({ page, request }) => {
    const dock = page.locator(".canvas-notebook-dock");

    await page.getByTestId("canvas-notebook-toggle").click();
    await expect(dock).toHaveAttribute("data-render-count", /\d+/);
    await expect(dock.locator('[data-cell-id="imports"]')).toBeVisible();
    await shot(page, "01-dock-open");

    await dragPaletteNode(page, "manual", "manual_input", 520, 420);
    const [nodeId] = (await nodeIdsByType(request, token, flowId)).manual_input;
    const cell = dock.locator(`[data-cell-id="node-${nodeId}"]`);
    await expect(cell).toHaveAttribute("data-status", "placeholder");
    await expect(cell.locator(".cn-cell-reason")).toBeVisible();
    await shot(page, "02-placeholder");

    await openNodeSettings(page, nodeId, ".manual-input-root");
    await page.getByRole("button", { name: "Add Column" }).click();
    await expect(page.locator(".manual-input-root .info-badge").first()).toHaveText("2 columns");
    await page.getByRole("button", { name: "Apply" }).click();
    await expect(cell).toHaveAttribute("data-status", "code", { timeout: 2000 });
    await expect(cell.locator(".cm-content")).toContainText("fl.");
    await shot(page, "03-configured");

    // Fold the drawer and palette off the node and let any close save settle before moving.
    await page.locator("#rightDrawer button[title='Minimize']").click();
    await minimizePalette(page);
    await page.waitForTimeout(1000);
    const before = await renderCount(page);

    const node = page.locator(`.vue-flow__node[data-id="${nodeId}"]`);
    const box = await node.boundingBox();
    if (!box) throw new Error("node not visible");
    const positionOf = async () => {
      const response = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
        headers: authHeaders(token),
      });
      const input = (await response.json()).node_inputs.find((n: any) => String(n.id) === nodeId);
      return [input.pos_x, input.pos_y];
    };
    const positionBefore = await positionOf();
    const rerender = page.waitForResponse((r) => r.url().includes("/notebook/render"));
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await page.mouse.down();
    await page.mouse.move(box.x + box.width / 2 + 120, box.y + box.height / 2 + 80, { steps: 10 });
    await page.mouse.up();
    await expect.poll(positionOf).not.toEqual(positionBefore);
    // The move is refetched after the debounce, and its unchanged fingerprint is ignored.
    await rerender;
    await page.waitForTimeout(300);
    expect(await renderCount(page)).toBe(before);
    await shot(page, "04-moved");
  });

  test("the split resizes by dragging and keeps its width", async ({ page }) => {
    await page.getByTestId("canvas-notebook-toggle").click();
    const dock = page.locator(".canvas-notebook-dock");
    const canvas = page.locator(".canvas-wrap main");
    const [dockBefore, canvasBefore] = [await dock.boundingBox(), await canvas.boundingBox()];
    if (!dockBefore || !canvasBefore) throw new Error("dock or canvas not visible");
    const handle = await page.locator(".cn-resizer").boundingBox();
    if (!handle) throw new Error("resizer not visible");
    await page.mouse.move(handle.x + handle.width / 2, handle.y + 200);
    await page.mouse.down();
    await page.mouse.move(handle.x + handle.width / 2 - 100, handle.y + 200, { steps: 5 });
    await page.mouse.up();
    const dockAfter = await dock.boundingBox();
    const canvasAfter = await canvas.boundingBox();
    expect(Math.round(dockAfter!.width - dockBefore.width)).toBe(100);
    expect(Math.round(canvasBefore.width - canvasAfter!.width)).toBe(100);
    const stored = await page.evaluate(() =>
      localStorage.getItem("flowfile.canvasNotebook.width.v1"),
    );
    expect(Number(stored)).toBe(Math.round(dockAfter!.width));
    await shot(page, "05-resized");
  });
});
