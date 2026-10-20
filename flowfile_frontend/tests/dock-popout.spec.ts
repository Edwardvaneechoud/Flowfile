import { test, expect, Page } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  lastRunStart,
  waitForRun,
} from "./helpers/api";
import { clickRun, minimizePalette, openFlow } from "./helpers/canvas";
import { buildSalaryFlow } from "./helpers/flows";

/**
 * The bottom dock's Data and Logs tabs pop out into their own windows (`#/popout/table?flow=<id>`,
 * `#/popout/logs?flow=<id>`), opened with `window.open` in web mode. Popping out is a move: the tab
 * leaves the dock, which no longer opens for it. The Data window follows the canvas: a node click in
 * the designer is forwarded to it, and a reloaded window asks for the current node again. The Logs
 * window streams the flow's runs.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578, or API_URL)
 *   2. Frontend: `npm run dev:web` (port 8080, or TEST_URL)
 *   3. Run: `npx playwright test tests/dock-popout.spec.ts`
 */

const DOCK = "#bottomDock";
const node = (page: Page, id: number) => page.locator(`.vue-flow__node[data-id="${id}"]`);
const dockTab = (page: Page, label: string) =>
  page.locator(`${DOCK} .dragitem-tab`, { hasText: label });
const windowStatus = (popup: Page) => popup.locator("[data-testid='table-window'] .dp-status-bar");
const windowLogs = (popup: Page) => popup.locator("[data-testid='logs-window'] .logs");

async function popOut(page: Page, tab: "data" | "logs"): Promise<Page> {
  const opened = page.waitForEvent("popup");
  await page.getByTestId(`dock-popout-${tab}`).click();
  const popup = await opened;
  await popup.waitForLoadState("load");
  return popup;
}

/** Development mode stores every step's rows; a new flow runs in Performance mode, which keeps none. */
async function useDevelopmentMode(request: any, token: string, flowId: number) {
  const read = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(read.ok()).toBe(true);
  const settings = await read.json();
  settings.execution_mode = "Development";
  const write = await request.post(`${API_URL}/flow_settings`, {
    headers: authHeaders(token),
    data: settings,
  });
  expect(write.ok()).toBe(true);
}

/**
 * A run from the designer's button, over both on the API and in the designer's own poll, which
 * shows the logs on the end.
 */
async function runFlow(page: Page, request: any, token: string, flowId: number) {
  const before = await lastRunStart(request, token, flowId);
  await clickRun(page);
  await waitForRun(request, token, flowId, before);
  await expect(page.locator("[data-tutorial='run-btn'] button", { hasText: "Run" })).toBeEnabled();
}

test.describe("Dock pop-outs", () => {
  test.use({ viewport: { width: 1600, height: 1000 } });

  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `dock_popout_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await buildSalaryFlow(request, token, flowId);
    await useDevelopmentMode(request, token, flowId);
    await openFlow(page, token, flowId, name);
    await minimizePalette(page);
    // The filter has rows only after a run; the run also opens the dock on Logs.
    await runFlow(page, request, token, flowId);
    await expect(page.locator(DOCK)).toBeVisible();
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("the Data window follows the canvas and Return reopens the dock on the node", async ({
    page,
  }) => {
    await node(page, 1).click();
    await expect(page.locator(`${DOCK} .dp-status-bar`)).toContainText("showing 4 of 4 rows");

    const popup = await popOut(page, "data");
    await expect(popup.getByTestId("table-window")).toBeVisible();
    await expect(popup).toHaveTitle(/^Data – dock_popout_\d+$/);
    await expect(windowStatus(popup)).toContainText("showing 4 of 4 rows", { timeout: 20_000 });
    // The Data tab left the dock, which stays for the logs: one host for the preview.
    await expect(dockTab(page, "Data")).toHaveCount(0);
    await expect(dockTab(page, "Logs")).toHaveCount(1);

    // A node click in the designer goes to the window; the dock shows no preview.
    await node(page, 2).click();
    await expect(windowStatus(popup)).toContainText("showing 3 of 3 rows", { timeout: 20_000 });
    await expect(dockTab(page, "Data")).toHaveCount(0);
    await expect(page.locator(`${DOCK} .dp-status-bar`)).toHaveCount(0);

    // Clicking the shown node again is a re-read of its rows in the window.
    const reread = popup.waitForResponse((response) =>
      new URL(response.url()).pathname.endsWith("/node/data"),
    );
    await node(page, 2).click();
    expect((await reread).ok()).toBe(true);

    // A reloaded window asks the designer for its node again (it was opened on node 1).
    await popup.reload();
    await popup.waitForLoadState("load");
    await expect(windowStatus(popup)).toContainText("showing 3 of 3 rows", { timeout: 20_000 });

    // Return reopens the dock on that node, and the window is gone.
    await popup.getByTestId("table-window-return").click();
    await expect.poll(() => popup.isClosed()).toBe(true);
    await expect(dockTab(page, "Data")).toHaveCount(1, { timeout: 10_000 });
    await expect(page.locator(`${DOCK} .dp-status-bar`)).toContainText("showing 3 of 3 rows", {
      timeout: 20_000,
    });
  });

  test("the Logs window streams the runs and the dock gets its tab back on close", async ({
    page,
    request,
  }) => {
    const popup = await popOut(page, "logs");
    await expect(popup.getByTestId("logs-window")).toBeVisible();
    await expect(popup).toHaveTitle(/^Logs – dock_popout_\d+$/);
    await expect(page.locator(DOCK)).toHaveCount(0);
    await expect
      .poll(() => windowLogs(popup).locator("div").count(), { timeout: 20_000 })
      .toBeGreaterThan(0);
    const logsBefore = await windowLogs(popup).innerText();

    // A designer run shows up in the window (core rewrites the log per run), not in the dock, and
    // the window settles on the finished run: its last line, with core's stream closed after it.
    await runFlow(page, request, token, flowId);
    await expect(windowLogs(popup).locator("div").last()).toContainText("Flow completed!", {
      timeout: 20_000,
    });
    await expect(popup.locator("[data-testid='logs-window'] .log-status")).toContainText(
      "Disconnected",
    );
    expect(await windowLogs(popup).innerText()).not.toBe(logsBefore);
    await expect(page.locator(DOCK)).toHaveCount(0);

    // A close by hand leaves the dock closed, run flag or not; the next run brings the logs back.
    await popup.close();
    await page.waitForTimeout(2_500); // past the opener's 1 s closed poll
    await expect(page.locator(DOCK)).toHaveCount(0);
    await runFlow(page, request, token, flowId);
    await expect(page.locator(DOCK)).toBeVisible({ timeout: 10_000 });
    await expect(page.locator(`${DOCK} .log-container`)).toBeVisible();
  });
});
