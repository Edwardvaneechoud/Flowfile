import { test, expect } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  lastRunStart,
  nodeResult,
  waitForRun,
} from "./helpers/api";
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

  test("a refused flow runs after its backend configuration is corrected", async ({
    page,
    request,
  }) => {
    await openFlow(page, token, flowId, flowName);
    const node = page.locator('.vue-flow__node[data-id="1"]');
    const badge = node.getByTestId("placement-badge");
    await expect(badge).toHaveText("can't run", { timeout: 15_000 });

    const previousStart = await lastRunStart(request, token, flowId);
    await clickRun(page);
    const refusedRun = await waitForRun(request, token, flowId, previousStart);
    expect(refusedRun.success).toBe(false);
    await expect(page.locator(".compute-error-banner")).toContainText("worker:e2e-missing");

    const headers = authHeaders(token);
    const settings = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, { headers });
    expect(settings.ok()).toBe(true);
    const corrected = await request.post(`${API_URL}/flow_settings`, {
      headers,
      data: { ...(await settings.json()), execution_location: "auto" },
    });
    expect(corrected.ok()).toBe(true);

    // Observe this flow's real preview, not another tab's placement or a fabricated response.
    const preview = page.waitForResponse((response) => {
      const url = new URL(response.url());
      return (
        url.pathname.endsWith("/flow/placement") &&
        url.searchParams.get("flow_id") === String(flowId) &&
        url.searchParams.get("probe") === "false"
      );
    });
    await page.reload();
    await expect(page.locator(".flow-tab.active")).toContainText(flowName);
    const response = await preview;
    expect(response.ok()).toBe(true);
    const placement = await response.json();
    expect(placement.available).toBe(true);
    expect(placement.assignment["1"]).toBe("worker");
    expect(placement.refusals).toEqual([]);
    await expect(badge).toHaveCount(0);

    await clickRun(page);
    const recoveredRun = await waitForRun(request, token, flowId, refusedRun.start_time);
    expect(recoveredRun.success).toBe(true);
    expect(nodeResult(recoveredRun, "1")?.success).toBe(true);
    await expect(page.locator(".compute-error-banner")).toHaveCount(0);
  });
});
