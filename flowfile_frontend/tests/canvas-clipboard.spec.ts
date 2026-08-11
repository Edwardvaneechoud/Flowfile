import { test, expect, APIRequestContext, Page } from "@playwright/test";

/**
 * Canvas clipboard routing tests.
 *
 * Regression suite for the copy/paste refactor (sentinel clipboard + panel
 * denylist): a paste aimed at settings-panel chrome must never mutate the
 * canvas, a stale node buffer must never be resurrected by non-matching
 * clipboard text, and a matching sentinel must paste the node.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578)
 *   2. Frontend dev server: `npm run dev:web` (port 8080)
 *   3. Run: `npx playwright test tests/canvas-clipboard.spec.ts`
 *      (NOT covered by `make test_e2e`, which runs only web-flow.spec.ts.)
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

const SENTINEL_ID = "e2e-clipboard-sentinel";
const SENTINEL_TEXT = `Flowfile nodes · 1 · ${SENTINEL_ID}`;

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  return (await tokenResponse.json()).access_token;
}

async function authPost(request: APIRequestContext, url: string, token: string) {
  return request.post(url, { headers: { Authorization: `Bearer ${token}` } });
}

/** Create a fresh flow with one manual_input node (node_id 1) and return its id. */
async function createFlowWithNode(request: APIRequestContext, token: string): Promise<number> {
  const createResponse = await authPost(
    request,
    `${API_URL}/editor/create_flow/?name=Clipboard_E2E&register_in_catalog=false`,
    token,
  );
  expect(createResponse.ok()).toBe(true);
  const flowId = await createResponse.json();
  const addResponse = await authPost(
    request,
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=120&pos_y=140`,
    token,
  );
  expect(addResponse.ok()).toBe(true);
  return flowId;
}

/** The armed node-copy buffer, in the useFlowClipboard localStorage format. */
function bufferSeed(flowId: number): string {
  return JSON.stringify({
    sentinelId: SENTINEL_ID,
    flowId,
    copiedAt: 0,
    single: {
      nodeIdToCopyFrom: 1,
      type: "manual_input",
      label: "Manual Input",
      description: "",
      numberOfInputs: 0,
      numberOfOutputs: 1,
      typeSnakeCase: "manual_input",
      flowIdToCopyFrom: flowId,
      multi: false,
      inputHandles: [],
      outputHandles: [{ id: "output-0", position: "right" }],
    },
  });
}

/** Boot the app authenticated, with the flow open and the buffer armed. */
async function openFlow(page: Page, token: string, flowId: number) {
  await page.goto(BASE_URL);
  await page.waitForLoadState("networkidle");
  const expiration = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration, flowId, buffer }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
      localStorage.setItem("flowfileNodeClipboard", buffer);
      sessionStorage.setItem("last_flow_id", String(flowId));
    },
    { token, expiration, flowId, buffer: bufferSeed(flowId) },
  );
  await page.reload();
  await page.waitForSelector(".vue-flow__node", { timeout: 15000 });
}

/** Dispatch a synthetic ClipboardEvent carrying `text` on `selector`. */
async function dispatchClipboardEvent(
  page: Page,
  selector: string,
  type: "copy" | "paste",
  text: string,
) {
  const dispatched = await page.evaluate(
    ({ selector, type, text }) => {
      const target = document.querySelector(selector);
      if (!target) return false;
      const clipboardData = new DataTransfer();
      if (type === "paste") clipboardData.setData("text/plain", text);
      const event = new ClipboardEvent(type, { clipboardData, bubbles: true, cancelable: true });
      target.dispatchEvent(event);
      return true;
    },
    { selector, type, text },
  );
  expect(dispatched).toBe(true);
}

const nodeCount = (page: Page) => page.locator(".vue-flow__node").count();

test.describe("Canvas clipboard routing", () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test("paste aimed at overlay-panel chrome never touches the canvas", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    const copyRequests: string[] = [];
    page.on("request", (req) => {
      if (req.url().includes("copy_node") || req.url().includes("add_node")) {
        copyRequests.push(req.url());
      }
    });

    // The palette ("Data actions") is a DraggableItem — same data-canvas-overlay
    // ancestry as the node-settings drawer the bug report targeted.
    await dispatchClipboardEvent(page, "[data-canvas-overlay] *", "paste", "[price] * 1.21");
    await page.waitForTimeout(500);

    expect(await nodeCount(page)).toBe(before);
    expect(copyRequests).toEqual([]);
  });

  test("non-tabular text on the pane does NOT resurrect the stale node buffer", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    await dispatchClipboardEvent(page, ".vue-flow__pane", "paste", "[price] * 1.21");
    await page.waitForTimeout(500);

    expect(await nodeCount(page)).toBe(before);
  });

  test("sentinel with a mismatched uuid does nothing", async ({ page, request }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    await dispatchClipboardEvent(
      page,
      ".vue-flow__pane",
      "paste",
      "Flowfile nodes · 1 · some-other-uuid",
    );
    await page.waitForTimeout(500);

    expect(await nodeCount(page)).toBe(before);
  });

  test("matching sentinel pastes the buffered node onto the canvas", async ({ page, request }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    await dispatchClipboardEvent(page, ".vue-flow__pane", "paste", SENTINEL_TEXT);
    await expect(page.locator(".vue-flow__node")).toHaveCount(before + 1, { timeout: 10000 });
  });

  test("tabular text on the pane creates a Manual Input node (not the buffered node)", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);
    const before = await nodeCount(page);

    const copyRequests: string[] = [];
    page.on("request", (req) => {
      if (req.url().includes("copy_node")) copyRequests.push(req.url());
    });

    await dispatchClipboardEvent(page, ".vue-flow__pane", "paste", "name\tage\nAlice\t30\nBob\t25");
    await expect(page.locator(".vue-flow__node")).toHaveCount(before + 1, { timeout: 10000 });
    expect(copyRequests).toEqual([]);
  });

  test("copy with text selected on the canvas defers to the native text copy", async ({
    page,
    request,
  }) => {
    // Node placed in open canvas (the default 120,140 sits under the palette
    // overlay and is unclickable), with a description so the selectable
    // <pre class="description-text"> actually renders.
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=Clipboard_E2E_Sel&register_in_catalog=false`,
      authToken,
    );
    const flowId = await createResponse.json();
    await authPost(
      request,
      `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=formula&pos_x=700&pos_y=400`,
      authToken,
    );
    await request.post(`${API_URL}/node/description/?flow_id=${flowId}&node_id=1`, {
      headers: { Authorization: `Bearer ${authToken}`, "Content-Type": "application/json" },
      data: JSON.stringify("joins revenue per region"),
    });
    await openFlow(page, authToken, flowId);

    // Canvas node text computes user-select:none, so a real mouse selection
    // over it cannot exist (Chromium's selection.toString() is "" for such
    // content even with a programmatic range) — the guard is parity/defense
    // for any selectable canvas text. Exercise its wiring by stubbing
    // getSelection the way genuinely selectable text would behave, then run
    // an unstubbed control proving the same dispatch arms the buffer.
    await page.locator(".vue-flow__node").first().click();
    await page.waitForSelector("pre.description-text", { timeout: 10000 });
    const bufferBefore = await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"));

    const withSelection = await page.evaluate(() => {
      const pre = document.querySelector("pre.description-text")!;
      const original = window.getSelection.bind(window);
      try {
        (window as { getSelection: unknown }).getSelection = () => ({
          toString: () => pre.textContent ?? "",
        });
        const event = new ClipboardEvent("copy", {
          clipboardData: new DataTransfer(),
          bubbles: true,
          cancelable: true,
        });
        pre.dispatchEvent(event);
        return { prevented: event.defaultPrevented };
      } finally {
        (window as { getSelection: unknown }).getSelection = original;
      }
    });
    const bufferAfterGuarded = await page.evaluate(() =>
      localStorage.getItem("flowfileNodeClipboard"),
    );
    expect(withSelection.prevented).toBe(false);
    expect(bufferAfterGuarded).toBe(bufferBefore);

    const control = await page.evaluate(() => {
      const pre = document.querySelector("pre.description-text")!;
      const event = new ClipboardEvent("copy", {
        clipboardData: new DataTransfer(),
        bubbles: true,
        cancelable: true,
      });
      pre.dispatchEvent(event);
      return { prevented: event.defaultPrevented };
    });
    const bufferAfterControl = await page.evaluate(() =>
      localStorage.getItem("flowfileNodeClipboard"),
    );
    expect(control.prevented).toBe(true);
    expect(bufferAfterControl).not.toBe(bufferBefore);
  });

  test("copy aimed at overlay-panel chrome leaves the node buffer untouched", async ({
    page,
    request,
  }) => {
    const flowId = await createFlowWithNode(request, authToken);
    await openFlow(page, authToken, flowId);

    const bufferBefore = await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"));
    await dispatchClipboardEvent(page, "[data-canvas-overlay] *", "copy", "");
    await page.waitForTimeout(200);
    const bufferAfter = await page.evaluate(() => localStorage.getItem("flowfileNodeClipboard"));

    expect(bufferAfter).toBe(bufferBefore);
  });
});
