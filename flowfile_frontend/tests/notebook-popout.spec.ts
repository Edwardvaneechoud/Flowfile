import { test, expect, Locator, Page } from "@playwright/test";

import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { openFlow } from "./helpers/canvas";
import { buildSalaryFlow, filterSettings, setFilterValue } from "./helpers/flows";

/**
 * The code dock's notebook pops out into its own window (`#/notebook?flow=<id>`), opened with
 * `window.open` in web mode. Popping out is a move: the dock closes and shows a stub for the flow
 * until the window comes back. Core's change feed keeps the two windows in step: a change made
 * elsewhere re-renders the pop-out's cells, and a push from the pop-out lands on the designer.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578, or API_URL)
 *   2. Frontend: `npm run dev:web` (port 8080, or TEST_URL)
 *   3. Run: `npx playwright test tests/notebook-popout.spec.ts`
 */

const DOCK_NOTEBOOK = ".code-dock .code-notebook .notebook-panel";
const WINDOW_NOTEBOOK = "[data-testid='notebook-window'] .notebook-panel";

async function openDockNotebook(page: Page) {
  await page.locator('[data-tutorial="generate-code-btn"]').click();
  // The Code button's tooltip sits over the pane's mode toggle until the pointer leaves it.
  await page.mouse.move(400, 500);
  await page.getByTestId("code-mode-notebook").click();
}

async function popOut(page: Page): Promise<Page> {
  const opened = page.waitForEvent("popup");
  await page.getByTestId("code-popout").click();
  const popup = await opened;
  await popup.waitForLoadState("load");
  await popup.locator(WINDOW_NOTEBOOK).waitFor({ timeout: 20_000 });
  return popup;
}

const filterCell = async (panel: Locator, token: string, flowId: number, request: any) => {
  const response = await request.get(`${API_URL}/notebook/render?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  const cells = (await response.json()).cells as { cell_id: string; node_ids: number[] }[];
  const cell = cells.find((c) => c.node_ids.includes(2))!;
  return panel.locator(`[data-cell-id="${cell.cell_id}"]`);
};

test.describe("Notebook pop-out", () => {
  test.use({ viewport: { width: 1600, height: 1000 } });

  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `notebook_popout_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await buildSalaryFlow(request, token, flowId);
    await openFlow(page, token, flowId, name);
    await openDockNotebook(page);
    await expect(page.locator(DOCK_NOTEBOOK)).toBeVisible();
  });

  let closedByTest = false;

  test.afterEach(async ({ request }) => {
    if (!closedByTest) await closeFlow(request, token, flowId);
    closedByTest = false;
  });

  test("pops out as a move, follows the canvas, pushes back, and comes back", async ({
    page,
    request,
  }) => {
    const popup = await popOut(page);
    // The pop-out is a bare window: the notebook and nothing of the app around it.
    await expect(popup.getByTestId("notebook-window")).toBeVisible();
    await expect(popup.locator('[data-tutorial="generate-code-btn"]')).toHaveCount(0);
    await expect(popup).toHaveTitle(/^Notebook – /);
    // The dock closed: one host per flow.
    await expect(page.locator(".code-dock")).toHaveCount(0);

    // A change made elsewhere re-renders the pop-out's cells through the change feed.
    const popupPanel = popup.locator(WINDOW_NOTEBOOK);
    const cell = await filterCell(popupPanel, token, flowId, request);
    await expect(cell.locator(".cm-content")).toContainText("> 60000");
    await setFilterValue(request, token, flowId, "61000");
    await expect(cell.locator(".cm-content")).toContainText("> 61000", { timeout: 15_000 });

    // An edit pushed from the pop-out lands on the canvas, and the designer reloads it.
    await cell.locator(".cm-content").click();
    await popup.keyboard.press("ControlOrMeta+A");
    const code = (await cell.locator(".cm-content").innerText()).replace("> 61000", "> 62000");
    await popup.keyboard.insertText(code);
    await expect(cell.getByTestId("nb-sync-state")).toHaveAttribute("data-sync-state", "edited");
    const designerReload = page.waitForResponse((r) => r.url().includes("/flow_data/v2"));
    const pushRequest = popup.waitForRequest((r) => r.url().includes("/editor/notebook/push/"));
    const pushed = popup.waitForResponse((r) => r.url().includes("/editor/notebook/push/"));
    await popupPanel.getByTestId("nb-push").click();
    expect(JSON.stringify((await pushRequest).postDataJSON().cells)).toContain("> 62000");
    expect((await pushed).status()).toBe(200);
    await expect(popup.getByText("Pushed to the canvas")).toBeVisible();
    await designerReload;
    // A pushed filter comes back as the expression the cell held, so read it whole.
    expect(JSON.stringify((await filterSettings(request, token, flowId)).filter_input)).toContain(
      "62000",
    );
    await expect(page.locator('.vue-flow__node[data-id="2"]')).toBeVisible();

    // Reopening the dock shows the stub, not a second notebook; "Bring back" closes the window.
    await openDockNotebook(page);
    await expect(page.getByTestId("notebook-popout-stub")).toBeVisible();
    await expect(page.locator(DOCK_NOTEBOOK)).toHaveCount(0);
    await page.getByTestId("nb-bring-back").click();
    await expect.poll(() => popup.isClosed()).toBe(true);
    await expect(page.locator(DOCK_NOTEBOOK)).toBeVisible();
    await expect(page.getByTestId("notebook-popout-stub")).toHaveCount(0);
  });

  test("closing the pop-out by hand hands the notebook back to the dock", async ({ page }) => {
    const popup = await popOut(page);
    await expect(page.locator(".code-dock")).toHaveCount(0);

    await popup.close();

    await openDockNotebook(page);
    await expect(page.locator(DOCK_NOTEBOOK)).toBeVisible({ timeout: 10_000 });
    await expect(page.getByTestId("notebook-popout-stub")).toHaveCount(0);
  });

  test("closing the flow elsewhere closes the pop-out and drops the designer's tab", async ({
    page,
    request,
  }) => {
    const popup = await popOut(page);
    await expect(popup.locator(WINDOW_NOTEBOOK)).toBeVisible();
    const tabs = page.locator(".flow-tab");
    const tabsBefore = await tabs.count();

    // Another client closes the flow: core's `closed` event reaches both windows.
    await closeFlow(request, token, flowId);
    closedByTest = true;

    await expect.poll(() => popup.isClosed(), { timeout: 15_000 }).toBe(true);
    await expect(tabs).toHaveCount(tabsBefore - 1, { timeout: 15_000 });
  });
});
