import { test, expect } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  lastRunStart,
  readNodeSettings,
  waitForRun,
} from "./helpers/api";
import { applySettings, clickRun, openFlow, openNodeSettings } from "./helpers/canvas";
import { buildSalaryFlow } from "./helpers/flows";

/**
 * A filter picked onto the default worker in its drawer, in a flow that runs in core. With core started
 * with FLOWFILE_COMPUTE_PROTOCOL=1 and FLOWFILE_EXCHANGE_URI but no exchange connection, placement puts
 * the filter on the worker, refuses the crossing into it before any node runs, and the canvas badges the
 * edge between the two backends; with the flag off the node's setting is not read and nothing is badged.
 */
test.describe("Compute backend picker", () => {
  let token: string;
  let flowId: number;
  let flowName: string;
  let protocolOn: boolean;

  test.beforeEach(async ({ request }) => {
    token = await getAuthToken(request);
    flowName = `placement_picker_${Date.now()}`;
    flowId = await createFlow(request, token, flowName);
    await buildSalaryFlow(request, token, flowId);
    const headers = authHeaders(token);
    const settings = await (
      await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, { headers })
    ).json();
    protocolOn = settings.execution_location === "auto";
    const moved = await request.post(`${API_URL}/flow_settings`, {
      headers,
      data: { ...settings, execution_location: "local" },
    });
    expect(moved.ok()).toBe(true);
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("a node picked onto the worker is saved there and the run badges its crossing", async ({
    page,
    request,
  }) => {
    await openFlow(page, token, flowId, flowName);
    await openNodeSettings(page, "2", "#tab-general");
    await page.locator("#tab-general").click();
    await page.locator("[data-testid=node-execution-location]").click();
    await page
      .locator(".el-select-dropdown:visible .el-select-dropdown__item", {
        hasText: "Default worker",
      })
      .click();
    await applySettings(page);
    await expect
      .poll(async () => (await readNodeSettings(request, token, flowId, "2")).execution_location)
      .toBe("worker");

    const previousStart = await lastRunStart(request, token, flowId);
    await clickRun(page);
    if (protocolOn) {
      await expect(page.locator(".compute-error-banner")).toContainText(
        'cannot run on backend "worker"',
        { timeout: 30_000 },
      );
      const badge = page.locator('[data-testid=boundary-badge][data-edge="1-2"]');
      await expect(badge).toBeVisible({ timeout: 30_000 });
      await badge.hover();
      await expect(badge.getByTestId("boundary-tooltip")).toContainText(/Node 1.*→.*Node 2/);
    } else {
      const run = await waitForRun(request, token, flowId, previousStart);
      expect(run.success).toBe(true);
      await expect(page.locator("[data-testid=boundary-badge]")).toHaveCount(0);
    }
  });
});
