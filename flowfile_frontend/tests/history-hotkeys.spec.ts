import { test, expect, type APIRequestContext, type Page } from "@playwright/test";
import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { openFlow } from "./helpers/canvas";

// Real core/worker services, with two independent histories. No API responses are mocked.
// Dispatch clicks to the intended DOM handlers so missing icon fonts in headless runs
// cannot redirect pointer input to overlapping header controls. This tests action
// routing and backend state, not pointer hit-testing or header layout.
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
  const select = async (page: Page, name: string) => {
    const loaded = page.waitForResponse(
      (r) => r.url().includes("/flow_data/v2") && r.status() === 200,
    );
    await page.locator(".flow-tab", { hasText: name }).first().dispatchEvent("click");
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
          .dispatchEvent("click");
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
    await select(page, nameB);
    await expect(page.locator(".vue-flow__node")).toHaveCount(3);
    await shortcut(page, "undo");
    await expect(page.locator(".vue-flow__node")).toHaveCount(2);
    expect(await snapshot(request, a)).toEqual(restored);
    await shortcut(page, "redo");
    await expect(page.locator(".vue-flow__node")).toHaveCount(3);
  });

  test("queued A presses and a late A response cannot mutate or reload B", async ({
    page,
    request,
  }) => {
    const other = await snapshot(request, b);
    let release!: () => void;
    const gate = new Promise<void>((done) => {
      release = done;
    });
    let entered!: () => void;
    const started = new Promise<void>((done) => {
      entered = done;
    });
    const calls: string[] = [];
    await page.route(/\/editor\/(undo|redo)/, async (route) => {
      calls.push(route.request().url());
      // Execute the real backend request, delaying only its delivery to the browser.
      const response = await route.fetch();
      entered();
      await gate;
      await route.fulfill({ response });
    });
    try {
      await shortcut(page, "undo");
      await started;
      await shortcut(page, "redo");
      await shortcut(page, "undo");
      const loaded = page.waitForResponse(
        (r) => r.url().includes("/flow_data/v2") && r.status() === 200,
      );
      await page.locator(".flow-tab", { hasText: nameB }).first().dispatchEvent("click");
      await expect(page.locator(".flow-tab.active")).toContainText(nameB);
      // The new flow's load also waits for outstanding mutations; release A first.
      release();
      await loaded;
      await expect(page.locator(".vue-flow__node")).toHaveCount(3);
      // A fresh B action must wait behind A's queue; its response proves the queue drained.
      await shortcut(page, "undo");
      await expect(page.locator(".vue-flow__node")).toHaveCount(2);
      expect(calls).toHaveLength(2);
      expect(calls[0]).toContain(`flow_id=${a}`);
      expect(calls[1]).toContain(`flow_id=${b}`);
      expect(await ids(request, a)).toEqual([1, 2]);
      await shortcut(page, "redo");
      await expect(page.locator(".vue-flow__node")).toHaveCount(3);
      // Ignore revision counters: the real B undo/redo increments them, but restores its graph.
      const restored = await snapshot(request, b);
      expect(restored.nodes).toEqual(other.nodes);
      expect(restored.edges).toEqual(other.edges);
    } finally {
      release();
      await page.unroute(/\/editor\/(undo|redo)/);
    }
  });
});
