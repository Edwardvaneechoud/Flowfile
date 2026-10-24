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
import { applySettings, clickRun, nodeStatus, openFlow, openNodeSettings } from "./helpers/canvas";
import { buildSalaryFlow } from "./helpers/flows";

/**
 * A filter picked onto the default worker in its drawer, in a flow posted onto `local`. The core under
 * test serves, so with FLOWFILE_COMPUTE_PROTOCOL=1 it saves the flow as `auto`, as a file saved `local`
 * opens, and with FLOWFILE_EXCHANGE_URI (each node placed by its own setting) both nodes run on the
 * worker. No edge is badged: a crossing needs two backends that compute, and a serving core has one, its
 * worker. With the flag off the flow stays on `local`, the node's setting is not read and nothing is
 * badged.
 */
test.describe("Compute backend picker", () => {
  let token: string;
  let flowId: number;
  let flowName: string;

  test.beforeEach(async ({ request }) => {
    token = await getAuthToken(request);
    flowName = `placement_picker_${Date.now()}`;
    flowId = await createFlow(request, token, flowName);
    await buildSalaryFlow(request, token, flowId);
    const headers = authHeaders(token);
    const settings = await (
      await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, { headers })
    ).json();
    const protocolOn = settings.execution_location === "auto";
    const moved = await request.post(`${API_URL}/flow_settings`, {
      headers,
      data: { ...settings, execution_location: "local" },
    });
    expect(moved.ok()).toBe(true);
    const saved = await (
      await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, { headers })
    ).json();
    expect(saved.execution_location).toBe(protocolOn ? "auto" : "local");
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("a node picked onto the worker is saved there and the flow runs", async ({
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
    const run = await waitForRun(request, token, flowId, previousStart);
    expect(run.success).toBe(true);
    // The badges come from the same run report as the node statuses, so wait until the canvas shows it.
    await expect(nodeStatus(page, "1")).toHaveClass(/success/);
    await expect(nodeStatus(page, "2")).toHaveClass(/success/);
    await expect(page.locator("[data-testid=boundary-badge]")).toHaveCount(0);
  });
});
