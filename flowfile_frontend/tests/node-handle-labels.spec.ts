import { test, expect, APIRequestContext, Page } from "@playwright/test";

/**
 * Canvas handle labels.
 *
 * A node with more than one input handle renders a compact positional letter
 * (A, B, …) next to each handle, with the declared input name as the native
 * tooltip — mirroring how multi-output handles have always worked. Single-input
 * and `multi` nodes (union-style, collapsed onto one handle) render no letter.
 *
 * No pixel baselines: the geometry assertions are relations between measured
 * rects (letter inside the node body, input letters left, output letters right),
 * so they survive font, theme and zoom changes. The tooltip is asserted as a DOM
 * attribute because Chromium renders native tooltips outside the page.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578)
 *   2. Frontend: `npm run dev:web` (port 8080) or `npm run preview:web` (4173)
 *   3. Run: `npx playwright test tests/node-handle-labels.spec.ts`
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  const tokenData = await tokenResponse.json();
  return tokenData.access_token;
}

async function authPost(request: APIRequestContext, url: string, token: string, data?: unknown) {
  return request.post(url, { headers: { Authorization: `Bearer ${token}` }, data });
}

async function navigateWithAuth(page: Page, token: string, targetUrl: string) {
  await page.goto(targetUrl);
  await page.waitForLoadState("networkidle");
  const expirationTime = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration }: { token: string; expiration: number }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
    },
    { token, expiration: expirationTime },
  );
  await page.reload();
  await page.waitForLoadState("networkidle");
}

async function addNode(
  request: APIRequestContext,
  token: string,
  flowId: number,
  nodeId: number,
  nodeType: string,
  posX: number,
  posY: number,
) {
  const response = await authPost(
    request,
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=${nodeId}` +
      `&node_type=${nodeType}&pos_x=${posX}&pos_y=${posY}`,
    token,
  );
  expect(response.ok()).toBe(true);
}

// flow_id rides in the query string; the body keys the target handle
// ("input-0" is the left/main frame, "input-1" the right — input_schema.py).
async function connect(
  request: APIRequestContext,
  token: string,
  flowId: number,
  fromId: number,
  toId: number,
  inputHandle: string,
) {
  const response = await authPost(
    request,
    `${API_URL}/editor/connect_node/?flow_id=${flowId}`,
    token,
    {
      input_connection: { node_id: toId, connection_class: inputHandle },
      output_connection: { node_id: fromId, connection_class: "output-0" },
    },
  );
  expect(response.ok()).toBe(true);
}

// One flow holds every case, so all nodes render at the same canvas zoom and
// their node-relative pixels are directly comparable.
async function seedFlow(request: APIRequestContext, token: string, name: string): Promise<number> {
  const createResponse = await authPost(
    request,
    `${API_URL}/editor/create_flow/?name=${name}&register_in_catalog=false`,
    token,
  );
  expect(createResponse.ok()).toBe(true);
  const flowId: number = await createResponse.json();

  await addNode(request, token, flowId, 1, "manual_input", 300, 120);
  await addNode(request, token, flowId, 2, "manual_input", 300, 400);
  await addNode(request, token, flowId, 3, "join", 620, 240);
  await addNode(request, token, flowId, 4, "union", 620, 520);
  await addNode(request, token, flowId, 5, "filter", 900, 120);
  await addNode(request, token, flowId, 6, "random_split", 900, 360);
  await connect(request, token, flowId, 1, 3, "input-0");
  await connect(request, token, flowId, 2, 3, "input-1");

  return flowId;
}

interface LabelRect {
  text: string;
  left: number;
  right: number;
  top: number;
  bottom: number;
}

interface NodeGeometry {
  width: number;
  height: number;
  inputLabels: LabelRect[];
  outputLabels: LabelRect[];
  inputTitles: (string | null)[];
  outputTitles: (string | null)[];
  inputHandleCount: number;
}

// Rects are node-relative, so the canvas transform cancels out.
async function readNodeGeometry(page: Page, nodeId: string): Promise<NodeGeometry | null> {
  return page.evaluate((id: string) => {
    const body = document.querySelector(`.vue-flow__node[data-id="${id}"] .custom-node`);
    if (!body) return null;
    const b = body.getBoundingClientRect();
    const read = (selector: string) =>
      Array.from(body.querySelectorAll(selector)).map((el) => {
        const r = el.getBoundingClientRect();
        return {
          text: (el.textContent || "").trim(),
          left: r.left - b.left,
          right: r.right - b.left,
          top: r.top - b.top,
          bottom: r.bottom - b.top,
        };
      });
    const titles = (selector: string) =>
      Array.from(body.querySelectorAll(selector)).map((h) => h.getAttribute("title"));
    return {
      width: b.width,
      height: b.height,
      inputLabels: read(".handle-label--input"),
      outputLabels: read(".handle-label--output"),
      inputTitles: titles(".handle-input .vue-flow__handle"),
      outputTitles: titles(".handle-output .vue-flow__handle"),
      inputHandleCount: body.querySelectorAll(".handle-input").length,
    };
  }, nodeId);
}

test.describe("Canvas handle labels", () => {
  let authToken: string;
  let flowId: number;
  const flowName = `HandleLabels_${Date.now()}`;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    flowId = await seedFlow(request, authToken, flowName);
  });

  test.afterAll(async ({ request }) => {
    if (flowId) {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test.beforeEach(async ({ page }) => {
    await page.setViewportSize({ width: 1440, height: 900 });
    // Session restore can reopen a different flow; pin this one before loading.
    await page.goto(BASE_URL);
    await page.evaluate((id: number) => sessionStorage.setItem("last_flow_id", String(id)), flowId);
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
    await page
      .locator('.vue-flow__node[data-id="3"]')
      .waitFor({ state: "visible", timeout: 15000 });
  });

  test("a two-input node letters its handles from the names and titles them fully", async ({
    page,
  }) => {
    const join = await readNodeGeometry(page, "3");
    expect(join).not.toBeNull();
    // Initials, not positional letters — "Left"/"Right" reads as L/R.
    expect(join!.inputLabels.map((l) => l.text)).toEqual(["L", "R"]);
    // Names come from NodeTemplate.input_labels and match the settings drawer's
    // "Left data" / "Right data" panels.
    expect(join!.inputTitles).toEqual(["Left", "Right"]);
  });

  test("a single-input node renders no input letter", async ({ page }) => {
    const filter = await readNodeGeometry(page, "5");
    expect(filter).not.toBeNull();
    expect(filter!.inputHandleCount).toBe(1);
    expect(filter!.inputLabels).toEqual([]);
  });

  test("a multi node collapses to one handle and renders no input letter", async ({ page }) => {
    const union = await readNodeGeometry(page, "4");
    expect(union).not.toBeNull();
    expect(union!.inputHandleCount).toBe(1);
    expect(union!.inputLabels).toEqual([]);
  });

  test("multi-output handles keep their letters and gain name tooltips", async ({ page }) => {
    const randomSplit = await readNodeGeometry(page, "6");
    expect(randomSplit).not.toBeNull();
    // "train"/"test" share an initial, so this side falls back to positional letters.
    expect(randomSplit!.outputLabels.map((l) => l.text)).toEqual(["A", "B"]);
    // Regression guard: the template's output_names used to be clobbered to null
    // for an unconfigured node, leaving the letters with no tooltip at all.
    expect(randomSplit!.outputTitles).toEqual(["train", "test"]);
  });

  test("letters sit inside the node body on the side they belong to", async ({ page }) => {
    const join = await readNodeGeometry(page, "3");
    const randomSplit = await readNodeGeometry(page, "6");

    for (const label of join!.inputLabels) {
      expect(label.left).toBeGreaterThanOrEqual(0);
      expect(label.right).toBeLessThanOrEqual(join!.width);
      expect(label.top).toBeGreaterThanOrEqual(0);
      expect(label.bottom).toBeLessThanOrEqual(join!.height);
      // Left half: an input letter must never drift into the output band.
      expect(label.right).toBeLessThan(join!.width / 2);
    }

    for (const label of randomSplit!.outputLabels) {
      expect(label.left).toBeGreaterThanOrEqual(0);
      expect(label.right).toBeLessThanOrEqual(randomSplit!.width);
      expect(label.left).toBeGreaterThan(randomSplit!.width / 2);
    }
  });

  test("the two input letters do not overlap each other", async ({ page }) => {
    const join = await readNodeGeometry(page, "3");
    const [a, b] = join!.inputLabels;
    expect(a.bottom).toBeLessThan(b.top);
  });
});
