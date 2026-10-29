import { test, expect } from "@playwright/test";

import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { clickRun, openFlow } from "./helpers/canvas";

/**
 * A flow whose execution location names a backend nothing configures is refused before any node
 * runs, and the run card shows placement's reason in a banner. Skipped unless core runs with
 * FLOWFILE_COMPUTE_PROTOCOL=1 (a new flow's location is then "auto"): with it off nothing is placed.
 */
test.describe("Compute error banner", () => {
  let token: string;
  let flowId: number;
  let flowName: string;

  test.beforeEach(async ({ request }) => {
    token = await getAuthToken(request);
    flowName = `compute_banner_${Date.now()}`;
    flowId = await createFlow(request, token, flowName);
    const headers = authHeaders(token);
    const added = await request.post(
      `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=100&pos_y=100`,
      { headers },
    );
    expect(added.ok()).toBe(true);
    const seeded = await request.post(`${API_URL}/update_settings/?node_type=manual_input`, {
      headers,
      data: {
        flow_id: flowId,
        node_id: 1,
        raw_data_format: { columns: [{ name: "a", data_type: "Int64" }], data: [[1, 2, 3]] },
      },
    });
    expect(seeded.ok()).toBe(true);
    const settings = await (
      await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, { headers })
    ).json();
    test.skip(
      settings.execution_location !== "auto",
      "needs a core with FLOWFILE_COMPUTE_PROTOCOL=1",
    );
    const moved = await request.post(`${API_URL}/flow_settings`, {
      headers,
      data: { ...settings, execution_location: "worker:e2e-missing" },
    });
    expect(moved.ok()).toBe(true);
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("a run placement refuses shows the reason in a banner", async ({ page }) => {
    await openFlow(page, token, flowId, flowName);
    await expect(page.locator('.vue-flow__node[data-id="1"]')).toBeVisible({ timeout: 15_000 });
    await clickRun(page);
    const banner = page.locator(".compute-error-banner");
    await expect(banner).toBeVisible({ timeout: 30_000 });
    await expect(banner).toContainText("This flow cannot run on its compute backend");
    await expect(banner).toContainText('cannot run on backend "worker:e2e-missing"');
  });
});
