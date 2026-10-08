import { test, expect, APIRequestContext, Page } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  lastRunStart,
  waitForRun,
} from "./helpers/api";
import { clickRun, dragPaletteNode, nodeStatus, openFlow } from "./helpers/canvas";

/**
 * Two tabs on one flow follow each other through core's change feed (GET /editor/events): a node
 * dropped in one tab appears in the other, and a run started in one lands its result in the
 * other, with no reload by hand. The tabs share one browser context, the way two desktop windows
 * or two tabs of one browser share a session.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578, or API_URL)
 *   2. Frontend: `npm run dev:web` (port 8080, or TEST_URL)
 *   3. Run: `npx playwright test tests/two-tabs-sync.spec.ts`
 */

async function seedManualInput(request: APIRequestContext, token: string, flowId: number) {
  const added = await request.post(
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=100&pos_y=100`,
    { headers: authHeaders(token) },
  );
  expect(added.ok()).toBe(true);
  const settings = await request.post(`${API_URL}/update_settings/?node_type=manual_input`, {
    headers: authHeaders(token),
    data: {
      flow_id: flowId,
      node_id: 1,
      raw_data_format: {
        columns: [{ name: "a", data_type: "Int64" }],
        data: [[1, 2, 3]],
      },
    },
  });
  expect(settings.ok()).toBe(true);
}

test.describe("Two tabs on one flow", () => {
  let token: string;
  let flowId: number;
  let pageA: Page;
  let pageB: Page;

  test.beforeEach(async ({ context, request }) => {
    token = await getAuthToken(request);
    const name = `two_tabs_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    pageA = await context.newPage();
    pageB = await context.newPage();
    await openFlow(pageA, token, flowId, name);
    await openFlow(pageB, token, flowId, name);
  });

  test.afterEach(async ({ request }) => {
    await pageA.close();
    await pageB.close();
    await closeFlow(request, token, flowId);
  });

  test("a node dropped in one tab appears in the other without a reload", async () => {
    await dragPaletteNode(pageA, "sort", "sort", 320, 200);
    const dropped = await pageA.locator(".vue-flow__node").first().getAttribute("data-id");
    expect(dropped).not.toBeNull();
    await expect(pageB.locator(`.vue-flow__node[data-id="${dropped}"]`)).toBeVisible({
      timeout: 15_000,
    });
  });

  test("a run started in one tab lands its result in the other", async ({ request }) => {
    await seedManualInput(request, token, flowId);
    for (const page of [pageA, pageB]) {
      await expect(page.locator('.vue-flow__node[data-id="1"]')).toBeVisible({ timeout: 15_000 });
    }
    const before = await lastRunStart(request, token, flowId);
    await clickRun(pageA);
    await waitForRun(request, token, flowId, before);
    await expect(nodeStatus(pageB, "1")).toHaveClass(/success/, { timeout: 15_000 });
  });
});
