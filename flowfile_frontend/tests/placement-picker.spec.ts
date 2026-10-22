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
 * A filter picked onto the default worker in its drawer, in a flow moved onto `local`. The core under
 * test serves, so with FLOWFILE_COMPUTE_PROTOCOL=1 and FLOWFILE_EXCHANGE_URI (each node placed by its own
 * setting) placement puts the filter on the worker and refuses its input left on `local`, "this core
 * serves; compute runs on its worker", before any node runs. No edge is badged: a crossing needs two
 * backends that compute, and a serving core has one, its worker. With the flag off the node's setting is
 * not read and nothing is badged.
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

  test("a node picked onto the worker is saved there and its input left on local refuses the run", async ({
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
      const refusal = 'cannot run on backend "local": this core serves; compute runs on its worker';
      const banner = page.locator(".compute-error-banner");
      await expect(banner).toContainText(refusal, { timeout: 30_000 });
      await expect(banner).not.toContainText('cannot run on backend "worker"');
      const run = await waitForRun(request, token, flowId, previousStart);
      expect(run.success).toBe(false);
      expect(run.node_step_result.map((n) => [String(n.node_id), n.error])).toEqual([
        ["1", refusal],
      ]);
      await expect(page.locator("[data-testid=boundary-badge]")).toHaveCount(0);
    } else {
      const run = await waitForRun(request, token, flowId, previousStart);
      expect(run.success).toBe(true);
      await expect(page.locator("[data-testid=boundary-badge]")).toHaveCount(0);
    }
  });
});
