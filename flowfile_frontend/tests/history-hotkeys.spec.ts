import { test, expect, type APIRequestContext, type Page } from "@playwright/test";
import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { openFlow } from "./helpers/canvas";

// Real core/worker services, with two independent histories. No API responses are mocked.
test.describe("Designer history shortcuts", () => {
  test.use({ viewport: { width: 1600, height: 1000 } });
  let token: string;
  let a: number;
  let b: number;
  let nameA: string;
  let nameB: string;

  const snapshot = async (request: APIRequestContext, flowId: number) => {
    const response = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
      headers: authHeaders(token),
    });
    expect(response.ok()).toBe(true);
    const data = await response.json();
    return { nodes: data.node_inputs, edges: data.node_edges, history: data.history };
  };
  const ids = async (request: APIRequestContext, flowId: number) =>
    (await snapshot(request, flowId)).nodes.map((node: { id: number }) => node.id).sort();
  const select = async (page: Page, name: string, flowId: number) => {
    const loaded = page.waitForResponse(
      (r) =>
        new URL(r.url()).pathname.endsWith("/flow_data/v2") &&
        new URL(r.url()).searchParams.get("flow_id") === String(flowId) &&
        r.status() === 200,
    );
    await page.locator(".flow-tab", { hasText: name }).first().click();
    await loaded;
    await expect(page.locator(".flow-tab.active")).toContainText(name);
  };
  const shortcut = async (page: Page, action: "undo" | "redo") => {
    // A body target avoids native text-editor undo without adding canvas mutations.
    await page.locator("body").evaluate((el) => {
      el.setAttribute("tabindex", "-1");
      el.focus();
    });
    await page.keyboard.press(action === "undo" ? "ControlOrMeta+z" : "ControlOrMeta+Shift+z");
  };

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    nameA = `history_A_${Date.now()}`;
    nameB = `history_B_${Date.now()}`;
    a = await createFlow(request, token, nameA);
    b = await createFlow(request, token, nameB);
    for (const flowId of [a, b]) {
      for (const nodeId of [1, 2, 3]) {
        const response = await request.post(
          `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=${nodeId}&node_type=manual_input&pos_x=${nodeId * 200}&pos_y=200`,
          { headers: authHeaders(token) },
        );
        expect(response.ok()).toBe(true);
      }
    }
    await openFlow(page, token, a, nameA);
    await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  });
  test.afterEach(async ({ request }) => {
    for (const id of [a, b]) if (id) await closeFlow(request, token, id);
  });

  test("mixed shortcut and toolbar actions restore the right graph without changing the other flow", async ({
    page,
    request,
  }) => {
    const other = await snapshot(request, b);
    const actions = ["undo", "undo", "redo", "undo", "redo", "redo"] as const;
    const counts = [2, 1, 2, 1, 2, 3];
    for (const [i, action] of actions.entries()) {
      if (i % 2) {
        await page
          .locator(".undo-redo-controls .control-btn")
          .nth(action === "undo" ? 0 : 1)
          .click();
      } else {
        await shortcut(page, action);
      }
      await expect(page.locator(".vue-flow__node")).toHaveCount(counts[i]);
      expect(await ids(request, a)).toEqual([1, 2, 3].slice(0, counts[i]));
      expect(await snapshot(request, b)).toEqual(other);
    }
    // The redo boundary is a no-op, not a second mutation or navigation.
    const restored = await snapshot(request, a);
    await shortcut(page, "redo");
    expect(await snapshot(request, a)).toEqual(restored);
    await select(page, nameB, b);
    await expect(page.locator(".vue-flow__node")).toHaveCount(3);
    await shortcut(page, "undo");
    await expect(page.locator(".vue-flow__node")).toHaveCount(2);
    expect(await snapshot(request, a)).toEqual(restored);
    await shortcut(page, "redo");
    await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  });
});
