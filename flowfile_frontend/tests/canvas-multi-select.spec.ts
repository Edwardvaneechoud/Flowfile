import { test, expect, Page } from "@playwright/test";

import { closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { minimizePalette, openFlow } from "./helpers/canvas";
import { postEditor } from "./helpers/flows";

/**
 * Canvas multi-select: Alt (or the platform's Cmd/Ctrl) + click toggles a node in the selection
 * without opening its settings, and only the click's own modifiers decide it — an Alt keydown is
 * never cancelled and a modifier whose keyup was lost never latches the canvas into multi-select.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578, or API_URL)
 *   2. Frontend: `npm run dev:web` (port 8080, or TEST_URL)
 *   3. Run: `npx playwright test tests/canvas-multi-select.spec.ts`
 */

const node = (page: Page, id: string) => page.locator(`.vue-flow__node[data-id="${id}"]`);

const selectedIds = (page: Page) =>
  page
    .locator(".vue-flow__node.selected")
    .evaluateAll((els) => els.map((el) => el.getAttribute("data-id")).sort());

/** The node whose settings are open carries `.node-button.selected`. */
const settingsOpenFor = (page: Page) =>
  page
    .locator(".vue-flow__node:has(.node-button.selected)")
    .evaluateAll((els) => els.map((el) => el.getAttribute("data-id")));

/** Dispatch a keydown the app never sees released, as when the OS swallows the keyup. */
const pressWithoutRelease = (page: Page, key: string, code: string) =>
  page.evaluate(
    ({ key, code }) => {
      const event = new KeyboardEvent("keydown", {
        key,
        code,
        altKey: key === "Alt",
        metaKey: key === "Meta",
        ctrlKey: key === "Control",
        bubbles: true,
        cancelable: true,
      });
      document.body.dispatchEvent(event);
      return event.defaultPrevented;
    },
    { key, code },
  );

test.describe("Canvas multi-select", () => {
  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `multi_select_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    for (const [id, x] of [
      [1, 100],
      [2, 260],
      [3, 420],
    ]) {
      await postEditor(request, token, "/editor/add_node/", {
        flow_id: flowId,
        node_id: id,
        node_type: "manual_input",
        pos_x: x,
        pos_y: 200,
      });
    }
    await openFlow(page, token, flowId, name);
    await minimizePalette(page);
    await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("Alt+click adds and removes nodes without opening their settings", async ({ page }) => {
    await node(page, "1").click({ modifiers: ["Alt"] });
    await node(page, "2").click({ modifiers: ["Alt"] });
    await node(page, "3").click({ modifiers: ["Alt"] });
    await expect.poll(() => selectedIds(page)).toEqual(["1", "2", "3"]);

    await node(page, "2").click({ modifiers: ["Alt"] });
    await expect.poll(() => selectedIds(page)).toEqual(["1", "3"]);
    expect(await settingsOpenFor(page)).toEqual([]);
    await expect(page.locator(".node-settings-drawer")).toHaveCount(0);

    await node(page, "2").click();
    await expect.poll(() => selectedIds(page)).toEqual(["2"]);
    await expect.poll(() => settingsOpenFor(page)).toEqual(["2"]);
  });

  test("Cmd/Ctrl+click toggles the selection too", async ({ page }) => {
    await node(page, "1").click({ modifiers: ["ControlOrMeta"] });
    await node(page, "3").click({ modifiers: ["ControlOrMeta"] });
    await expect.poll(() => selectedIds(page)).toEqual(["1", "3"]);
    expect(await settingsOpenFor(page)).toEqual([]);

    await node(page, "1").click({ modifiers: ["ControlOrMeta"] });
    await expect.poll(() => selectedIds(page)).toEqual(["3"]);
  });

  // Meta/Control keydowns stay cancelled by VueFlow's own zoom-activation key, which is not ours.
  test("An Alt keydown is not cancelled", async ({ page }) => {
    expect(await pressWithoutRelease(page, "Alt", "AltLeft")).toBe(false);
  });

  test("A modifier whose keyup was lost does not latch multi-select", async ({ page }) => {
    await node(page, "1").click();
    await expect.poll(() => selectedIds(page)).toEqual(["1"]);

    await pressWithoutRelease(page, "Alt", "AltLeft");
    await pressWithoutRelease(page, "Meta", "MetaLeft");
    await pressWithoutRelease(page, "Control", "ControlLeft");

    // Keyboard selection reads the same flag a click does; a latched one would add node 2.
    await node(page, "2").focus();
    await page.keyboard.press("Enter");
    await expect.poll(() => selectedIds(page)).toEqual(["2"]);

    await node(page, "3").click();
    await expect.poll(() => selectedIds(page)).toEqual(["3"]);
  });
});
