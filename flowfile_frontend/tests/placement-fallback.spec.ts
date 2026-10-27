import { test, expect } from "@playwright/test";

import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { clickRun, minimizePalette, openFlow } from "./helpers/canvas";

/**
 * A node its backend cannot run runs on the default backend when an exchange store is configured:
 * the canvas marks it before the run and the run card names it after. The node is set to `local`,
 * which a core that serves runs nothing on, so it falls back to the worker with nothing crossing.
 * Skipped unless core runs with FLOWFILE_COMPUTE_PROTOCOL=1 and FLOWFILE_EXCHANGE_URI, as e2e-tests.yml
 * starts it.
 */
test.describe("Placement fallback", () => {
  let token: string;
  let flowId: number;
  let flowName: string;

  test.beforeEach(async ({ request }) => {
    token = await getAuthToken(request);
    flowName = `placement_fallback_${Date.now()}`;
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
        execution_location: "local",
        raw_data_format: { columns: [{ name: "a", data_type: "Int64" }], data: [[1, 2, 3]] },
      },
    });
    expect(seeded.ok()).toBe(true);
    const placement = await (
      await request.get(`${API_URL}/flow/placement?flow_id=${flowId}`, { headers })
    ).json();
    test.skip(
      !placement.available || placement.fallbacks.length === 0,
      "needs a core with FLOWFILE_COMPUTE_PROTOCOL=1 and FLOWFILE_EXCHANGE_URI",
    );
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("the node is marked before the run and named on the run card after", async ({ page }) => {
    await openFlow(page, token, flowId, flowName);
    await minimizePalette(page);
    const node = page.locator('.vue-flow__node[data-id="1"]');
    await expect(node).toBeVisible({ timeout: 15_000 });
    const badge = node.getByTestId("placement-badge");
    await expect(badge).toHaveText("→ default worker", { timeout: 15_000 });
    const tooltip = node.getByTestId("placement-badge-tooltip");
    await expect(tooltip).toBeHidden();
    await badge.hover();
    await expect(tooltip).toBeVisible({ timeout: 1_000 });
    await expect(tooltip).toHaveText(
      'Runs on the default worker, because backend "local" doesn\'t support this node.',
    );

    await clickRun(page);

    const notice = page.getByTestId("placement-notice");
    await expect(notice).toBeVisible({ timeout: 30_000 });
    await expect(notice).toContainText(
      '1 node ran on the default worker because backend "local" doesn\'t support it',
    );
    await expect(notice.locator("li")).toHaveCount(0);
    await notice.getByRole("button", { name: "Show details" }).click();
    await expect(notice.locator("li")).toContainText(
      "manual_input #1 this core serves; compute runs on its worker",
    );
    await expect(page.locator(".compute-error-banner")).toHaveCount(0);
  });
});
