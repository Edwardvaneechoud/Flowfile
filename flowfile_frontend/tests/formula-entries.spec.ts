import { test, expect, APIRequestContext, Page } from "@playwright/test";
import * as path from "path";

/**
 * E2E coverage for the Formula node's ordered list of entries: an entry may
 * reference the output of the entries before it, and reordering is an ordinary
 * settings change that can legitimately produce a forward reference.
 *
 * Prerequisites: flowfile_core on :63578 and a web server on :8080.
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";
const COMPLEX_FLOW_FIXTURE = path.resolve(__dirname, "fixtures/complex-flow.yaml");

/** The fixture's formula node already writes `full_name_upper = uppercase([name])`. */
const FIRST_OUTPUT = "full_name_upper";
const SECOND_OUTPUT = "full_name_len";
const SECOND_EXPRESSION = `length([${FIRST_OUTPUT}])`;

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const response = await request.post(`${API_URL}/auth/token`);
  if (!response.ok()) throw new Error(`Failed to get auth token: ${response.status()}`);
  return (await response.json()).access_token;
}

async function importFixture(request: APIRequestContext, token: string): Promise<number> {
  const response = await request.get(
    `${API_URL}/import_flow/?flow_path=${encodeURIComponent(COMPLEX_FLOW_FIXTURE)}`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  if (!response.ok()) throw new Error(`Failed to import fixture: ${response.status()}`);
  return await response.json();
}

/** Resolve a node id by type, so the spec never depends on canvas label text. */
async function findNodeId(
  request: APIRequestContext,
  token: string,
  flowId: number,
  nodeType: string,
): Promise<string> {
  const response = await request.get(`${API_URL}/flow_data/v2?flow_id=${flowId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const flowData = await response.json();
  const node = (flowData.node_inputs ?? []).find((n: any) => n.item === nodeType);
  if (!node) throw new Error(`No ${nodeType} node in the fixture flow`);
  return String(node.id);
}

async function readSettings(
  request: APIRequestContext,
  token: string,
  flowId: number,
  nodeId: string,
): Promise<any> {
  const response = await request.get(`${API_URL}/node?flow_id=${flowId}&node_id=${nodeId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  expect(response.ok()).toBe(true);
  return (await response.json())?.setting_input;
}

/**
 * The designer route drops the flow id and restores whichever tab was last
 * active, so the flow has to be picked from the tab strip by name.
 */
async function openFlow(page: Page, token: string, flowName: string) {
  await page.goto(BASE_URL);
  await page.waitForLoadState("networkidle");
  await page.evaluate(
    ({ token, expiration }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
    },
    { token, expiration: Date.now() + 60 * 60 * 1000 },
  );
  await page.goto(`${BASE_URL}/#/main/designer`);
  await page.waitForLoadState("networkidle");
  await page.locator(".flow-tab", { hasText: flowName }).first().click();
  await page.locator(".vue-flow__node").first().waitFor({ state: "visible", timeout: 20000 });
  // The node palette floats over the canvas and would swallow clicks on nodes.
  const minimize = page.locator("#dataActions button[title='Minimize']");
  if (await minimize.count()) await minimize.click();
}

async function openNodeSettings(page: Page, nodeId: string) {
  // dispatchEvent rather than dblclick: nodes overlap on the fixture canvas, so
  // a real mouse event can land on whichever node is painted on top.
  await page.locator(`.vue-flow__node[data-id="${nodeId}"]`).dispatchEvent("dblclick");
  await page.locator(".formula-entry").first().waitFor({ timeout: 20000 });
}

/**
 * The drawer has no keyboard close, and re-opening the node that is already
 * showing does not re-run `loadNodeData`. Routing via another node is what
 * actually proves the settings came back from the backend.
 */
async function reopenNodeSettings(page: Page, nodeId: string, viaNodeId: string) {
  await page.locator(`.vue-flow__node[data-id="${viaNodeId}"]`).dispatchEvent("dblclick");
  await page.locator(".formula-entry").first().waitFor({ state: "detached", timeout: 20000 });
  await openNodeSettings(page, nodeId);
}

const rows = (page: Page) => page.locator(".formula-entry");

/** The row header line IS the output-name field (an el-autocomplete wrapper). */
const nameField = (page: Page, rowIndex: number) =>
  rows(page).nth(rowIndex).locator(".entry-name input");

const outputNames = (page: Page) => nameField(page, 0).inputValue();

const setOutputName = async (page: Page, rowIndex: number, name: string) => {
  const field = nameField(page, rowIndex);
  await field.click();
  await field.fill(name);
  await expect(field).toHaveValue(name);
};

const typeExpression = async (page: Page, rowIndex: number, expression: string) => {
  const editor = rows(page).nth(rowIndex).locator(".cm-content");
  await editor.click();
  await page.keyboard.type(expression);
  await expect(editor).toContainText(expression);
};

const apply = async (page: Page) => {
  await page.getByRole("button", { name: "Apply" }).click();
  await expect(page.getByRole("button", { name: "Applied ✓" })).toBeVisible();
};

/**
 * Chromium will not synthesize an HTML5 drag from mouse moves, so drive the
 * event sequence directly with one shared DataTransfer. `dragstart` has to come
 * from the row header — the only draggable handle — while the drop lands on the
 * row itself; clientY in the top half of the target means "insert before it".
 */
const dragRowOnto = (page: Page, fromIndex: number, toIndex: number) =>
  page.evaluate(
    ({ fromIndex, toIndex }) => {
      const entries = Array.from(document.querySelectorAll(".formula-entry"));
      const dataTransfer = new DataTransfer();
      const fire = (el: Element, type: string, clientY: number) =>
        el.dispatchEvent(
          new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer, clientY }),
        );
      const source = entries[fromIndex];
      const target = entries[toIndex];
      const targetTop = target.getBoundingClientRect().top + 2;
      fire(source.querySelector(".entry-grip")!, "dragstart", 0);
      fire(target, "dragover", targetTop);
      fire(target, "drop", targetTop);
      fire(source, "dragend", 0);
    },
    { fromIndex, toIndex },
  );

async function runFlow(request: APIRequestContext, token: string, flowId: number) {
  const headers = { Authorization: `Bearer ${token}` };
  const started = await request.post(`${API_URL}/flow/run/?flow_id=${flowId}`, { headers });
  expect(started.ok()).toBe(true);

  for (let attempt = 0; attempt < 120; attempt++) {
    const status = await request.get(`${API_URL}/flow/run_status/?flow_id=${flowId}`, { headers });
    if (status.ok()) {
      const info = await status.json();
      if (!info.is_running && info.success !== null) return info;
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  throw new Error("Flow run did not finish in time");
}

test.describe("Formula entries", () => {
  let authToken: string;
  let flowId: number;
  let formulaNodeId: string;

  test.beforeEach(async ({ request }) => {
    // A fresh import per test: these tests write node settings.
    authToken = await getAuthToken(request);
    flowId = await importFixture(request, authToken);
    formulaNodeId = await findNodeId(request, authToken, flowId, "formula");
  });

  test("a legacy single-formula node opens as one row with no list chrome", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, formulaNodeId);

    await expect(rows(page)).toHaveCount(1);
    expect(await outputNames(page)).toBe(FIRST_OUTPUT);
    // The header line is there (it holds the name), but none of the list chrome is.
    await expect(page.locator(".formula-entry .entry-head")).toHaveCount(1);
    await expect(page.locator(".entry-grip")).toHaveCount(0);
    await expect(page.locator(".entry-chevron")).toHaveCount(0);
    await expect(page.locator(".formula-add-row")).toBeVisible();
    await expect(page.locator(".formula-collapse-toggle")).toHaveCount(0);
  });

  test("a row collapses to its summary and keeps its editor text on expand", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, formulaNodeId);

    await page.locator(".formula-add-row").click();
    await expect(rows(page)).toHaveCount(2);
    await expect(page.getByRole("button", { name: "Collapse all" })).toBeVisible();

    const first = rows(page).nth(0);
    const editor = first.locator(".cm-content");
    const original = (await editor.innerText()).trim();
    expect(original).not.toBe("");

    await first.locator(".entry-chevron").click();
    await expect(first.locator(".entry-editor")).toBeHidden();
    // The name stays in its own field; the summary is the expression alone.
    await expect(first.locator(".entry-expr")).toHaveText(original);
    await expect(nameField(page, 0)).toHaveValue(FIRST_OUTPUT);

    // The chevron is the expand affordance; the summary line sits outside the header.
    await first.locator(".entry-chevron").click();
    await expect(first.locator(".entry-editor")).toBeVisible();
    await expect(editor).toHaveText(original);
  });

  test("a second entry can reference the first entry's output column", async ({
    page,
    request,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, formulaNodeId);

    await page.locator(".formula-add-row").click();
    await expect(rows(page)).toHaveCount(2);

    await setOutputName(page, 1, SECOND_OUTPUT);
    await typeExpression(page, 1, SECOND_EXPRESSION);
    await apply(page);

    const settings = await readSettings(request, authToken, flowId, formulaNodeId);
    expect(settings.functions.map((entry: any) => entry.field.name)).toEqual([
      FIRST_OUTPUT,
      SECOND_OUTPUT,
    ]);
    expect(settings.functions[1].function).toBe(SECOND_EXPRESSION);

    await runFlow(request, authToken, flowId);

    const preview = await request.get(
      `${API_URL}/node/data?flow_id=${flowId}&node_id=${formulaNodeId}`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    expect(preview.ok()).toBe(true);
    const table = await preview.json();
    expect(table.columns).toContain(FIRST_OUTPUT);
    expect(table.columns).toContain(SECOND_OUTPUT);
    for (const row of table.data) {
      expect(row[SECOND_OUTPUT]).toBe(String(row[FIRST_OUTPUT]).length);
    }
  });

  test("a reorder is never blocked, persists, and surfaces the forward reference", async ({
    page,
    request,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, formulaNodeId);

    await page.locator(".formula-add-row").click();
    await setOutputName(page, 1, SECOND_OUTPUT);
    await typeExpression(page, 1, SECOND_EXPRESSION);
    await apply(page);

    // Drag the dependent entry above the one that creates the column it reads.
    await dragRowOnto(page, 1, 0);
    await expect(nameField(page, 0)).toHaveValue(SECOND_OUTPUT);
    await expect(nameField(page, 1)).toHaveValue(FIRST_OUTPUT);

    // The reorder went through; the broken reference sits under the row's editor.
    await expect(rows(page).nth(0).locator(".entry-flag.is-error")).toBeVisible();
    const issue = rows(page).nth(0).locator(".entry-diagnostic.is-error");
    await expect(issue).toBeVisible({ timeout: 15000 });
    await expect(issue).toContainText(`column '${FIRST_OUTPUT}' not found`);

    await apply(page);
    const settings = await readSettings(request, authToken, flowId, formulaNodeId);
    expect(settings.functions.map((entry: any) => entry.field.name)).toEqual([
      SECOND_OUTPUT,
      FIRST_OUTPUT,
    ]);

    // Reopening the drawer shows the stored order, not the original one.
    await reopenNodeSettings(
      page,
      formulaNodeId,
      await findNodeId(request, authToken, flowId, "select"),
    );
    await expect(nameField(page, 0)).toHaveValue(SECOND_OUTPUT);
  });
});
