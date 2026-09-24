import { test, expect, Page } from "@playwright/test";

import { closeFlow, createFlow, getAuthToken, nodeIdsByType } from "./helpers/api";
import { openFlow } from "./helpers/canvas";

/**
 * Palette drops over a floating panel.
 *
 * The floating panels (Data actions, the settings drawer, the bottom dock) sit inside the
 * canvas's drop zone, so a node released over one used to be created on the canvas underneath
 * it, out of sight. Releasing over a panel now cancels the drop; the open canvas still takes it.
 *
 * The drags are real mouse gestures, so Chromium applies the native rule that a drop only fires
 * where dragover was cancelled. A synthetic drop is checked too, since it skips that rule.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578)
 *   2. Frontend: `npm run dev:web` (port 8080) or `npm run preview:web` (4173)
 *   3. Run: `npx playwright test tests/palette-drop.spec.ts`
 */

const ITEM = "sort";

async function searchPalette(page: Page) {
  await page.locator(".nodes-wrapper .search-input").fill(ITEM);
  await page.locator(`[data-tutorial-node='${ITEM}']`).first().waitFor();
}

async function dragPaletteItemTo(page: Page, x: number, y: number) {
  await searchPalette(page);
  const box = await page.locator(`[data-tutorial-node='${ITEM}']`).first().boundingBox();
  if (!box) throw new Error("palette item not visible");
  await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
  await page.mouse.down();
  await page.mouse.move(x, y, { steps: 12 });
  await page.mouse.up();
}

/** A point inside the Data actions panel, below the palette item being dragged. */
async function pointInPalette(page: Page) {
  const box = await page.locator("#dataActions").boundingBox();
  if (!box) throw new Error("Data actions panel not visible");
  return { x: box.x + box.width / 2, y: box.y + box.height - 40 };
}

test.describe("Palette drop over a floating panel", () => {
  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `palette_drop_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await openFlow(page, token, flowId, name);
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("releasing over Data actions adds no node", async ({ page, request }) => {
    const target = await pointInPalette(page);
    await dragPaletteItemTo(page, target.x, target.y);

    await page.waitForTimeout(500);
    await expect(page.locator(".vue-flow__node")).toHaveCount(0);
    expect(await nodeIdsByType(request, token, flowId)).toEqual({});
    // The refused drag still ends: the palette drag no longer blocks text selection.
    expect(await page.evaluate(() => document.body.style.userSelect)).toBe("");
  });

  test("a synthetic drop over Data actions adds no node", async ({ page, request }) => {
    await searchPalette(page);
    const target = await pointInPalette(page);
    const dropAllowed = await page.evaluate(
      ({ item, x, y }) => {
        const source = document.querySelector(`[data-tutorial-node='${item}']`)!;
        const panel = document.elementFromPoint(x, y)!;
        const dataTransfer = new DataTransfer();
        source.dispatchEvent(new DragEvent("dragstart", { bubbles: true, dataTransfer }));
        const init = { bubbles: true, cancelable: true, dataTransfer, clientX: x, clientY: y };
        // A cancelled dragover is what tells the browser the drop is allowed.
        const allowed = !panel.dispatchEvent(new DragEvent("dragover", init));
        panel.dispatchEvent(new DragEvent("drop", init));
        source.dispatchEvent(new DragEvent("dragend", { bubbles: true, dataTransfer }));
        return allowed;
      },
      { item: ITEM, ...target },
    );

    expect(dropAllowed).toBe(false);
    await page.waitForTimeout(500);
    await expect(page.locator(".vue-flow__node")).toHaveCount(0);
    expect(await nodeIdsByType(request, token, flowId)).toEqual({});
  });

  test("releasing on the open canvas still adds the node", async ({ page, request }) => {
    const panel = await page.locator("#dataActions").boundingBox();
    const canvas = await page.locator(".vue-flow").boundingBox();
    if (!panel || !canvas) throw new Error("canvas not laid out");
    const x = panel.x + panel.width + (canvas.x + canvas.width - panel.x - panel.width) / 2;
    await dragPaletteItemTo(page, x, canvas.y + 200);

    await expect(page.locator(".vue-flow__node")).toHaveCount(1);
    await expect(page.locator(".vue-flow__node").first()).toBeInViewport();
    expect(Object.keys(await nodeIdsByType(request, token, flowId))).toEqual([ITEM]);
  });
});
