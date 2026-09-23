import { expect, Locator, Page } from "@playwright/test";

import { BASE_URL } from "./api";

/** Designer interactions shared by specs that build flows through the real canvas. */

/**
 * Store the token the way the login flow does, then open `hash` (e.g. `#/main/designer`).
 * `flowId` becomes the designer's last-used flow, which it opens first.
 */
export async function login(page: Page, token: string, hash: string, flowId?: number) {
  await page.goto(BASE_URL);
  await page.waitForLoadState("networkidle");
  await page.evaluate(
    ({ token, expiration, flowId }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
      if (flowId !== undefined) sessionStorage.setItem("last_flow_id", String(flowId));
    },
    { token, expiration: Date.now() + 60 * 60 * 1000, flowId },
  );
  await page.goto(`${BASE_URL}/${hash}`);
  await page.waitForLoadState("networkidle");
}

/**
 * The designer route drops the flow id and restores whichever tab was last active, so the
 * flow is also picked from the tab strip by its (unique) name.
 */
export async function openFlow(page: Page, token: string, flowId: number, flowName: string) {
  await login(page, token, "#/main/designer", flowId);
  await page.locator(".flow-tab", { hasText: flowName }).first().click();
  await expect(page.locator(".flow-tab.active")).toContainText(flowName);
  await page.locator(".vue-flow").waitFor();
}

/** The node palette floats over the canvas and would swallow clicks on nodes. */
export async function minimizePalette(page: Page) {
  const minimize = page.locator("#dataActions button[title='Minimize']");
  if (await minimize.count()) await minimize.click();
}

/**
 * Drop a palette item on the canvas at (x, y) relative to the canvas. Palette items are native
 * HTML5 drag sources, so the app's own dragstart -> drop pipeline is driven with a real
 * DataTransfer; searching first expands the palette group that holds the item.
 */
export async function dragPaletteNode(
  page: Page,
  search: string,
  item: string,
  x: number,
  y: number,
) {
  const nodes = page.locator(".vue-flow__node");
  const nodesBefore = await nodes.count();
  const searchBox = page.locator(".nodes-wrapper .search-input");
  await searchBox.fill(search);
  await page.locator(`[data-tutorial-node='${item}']`).first().waitFor({ state: "attached" });
  await page.evaluate(
    ({ item, x, y }) => {
      const source = document.querySelector(`[data-tutorial-node='${item}']`);
      const canvas = document.querySelector(".vue-flow");
      if (!source || !canvas) throw new Error(`missing drag source or canvas for ${item}`);
      const dataTransfer = new DataTransfer();
      source.dispatchEvent(new DragEvent("dragstart", { bubbles: true, dataTransfer }));
      const rect = canvas.getBoundingClientRect();
      const init = {
        bubbles: true,
        cancelable: true,
        dataTransfer,
        clientX: rect.left + x,
        clientY: rect.top + y,
      };
      canvas.dispatchEvent(new DragEvent("dragover", init));
      canvas.dispatchEvent(new DragEvent("drop", init));
    },
    { item, x, y },
  );
  await expect(nodes).toHaveCount(nodesBefore + 1);
  await searchBox.fill("");
}

async function stableBox(page: Page, locator: Locator) {
  await locator.waitFor();
  let previous = await locator.boundingBox();
  for (let i = 0; i < 15; i++) {
    await page.waitForTimeout(200);
    const current = await locator.boundingBox();
    if (
      previous &&
      current &&
      Math.abs(current.x - previous.x) < 1 &&
      Math.abs(current.y - previous.y) < 1
    ) {
      return current;
    }
    previous = current;
  }
  return previous;
}

/** Connect two canvas nodes with a real mouse drag from the source's output to the target's input. */
export async function connectNodes(page: Page, sourceNodeId: string, targetNodeId: string) {
  const sourceHandle = page
    .locator(`.vue-flow__node[data-id='${sourceNodeId}'] .vue-flow__handle[data-handlepos='right']`)
    .first();
  const targetHandle = page
    .locator(`.vue-flow__node[data-id='${targetNodeId}'] .vue-flow__handle[data-handlepos='left']`)
    .first();
  const edges = page.locator(".vue-flow__edge");
  const edgesBefore = await edges.count();
  const src = await stableBox(page, sourceHandle);
  const dst = await stableBox(page, targetHandle);
  if (!src || !dst) throw new Error("handle not visible");
  await page.mouse.move(src.x + src.width / 2, src.y + src.height / 2);
  await page.mouse.down();
  await page.mouse.move((src.x + dst.x) / 2, (src.y + dst.y) / 2, { steps: 5 });
  await page.mouse.move(dst.x + dst.width / 2, dst.y + dst.height / 2, { steps: 5 });
  await page.mouse.up();
  await expect(edges).toHaveCount(edgesBefore + 1);
}

/**
 * Open a node's settings drawer and wait until it shows that node's loaded settings, then for
 * `readySelector`. The Node Reference field's `df_<id>` placeholder only renders once the node's
 * data is in, so a drawer still showing another node of the same type is never mistaken for it.
 * dispatchEvent rather than a mouse dblclick: nodes can overlap, and a real event lands on
 * whichever is on top.
 */
export async function openNodeSettings(page: Page, nodeId: string, readySelector: string) {
  await page.locator(`.vue-flow__node[data-id="${nodeId}"]`).dispatchEvent("dblclick");
  await page
    .locator(`.node-settings-drawer input[placeholder='df_${nodeId}']`)
    .waitFor({ state: "attached", timeout: 20000 });
  await page.locator(readySelector).first().waitFor({ timeout: 20000 });
}

export async function applySettings(page: Page) {
  await page.getByRole("button", { name: "Apply" }).click();
  await expect(page.getByRole("button", { name: "Applied ✓" })).toBeVisible();
}

export function nodeStatus(page: Page, nodeId: string): Locator {
  return page.locator(`.vue-flow__node[data-id="${nodeId}"] .status-indicator`);
}

export async function clickRun(page: Page) {
  await page.locator("[data-tutorial='run-btn'] button", { hasText: "Run" }).click();
}
