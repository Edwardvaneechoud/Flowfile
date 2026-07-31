import { test, expect, APIRequestContext, Page } from "@playwright/test";
import * as path from "path";

/**
 * E2E coverage for the column-picker surface in node settings.
 *
 * Also the first spec in the suite that opens a node settings drawer at all, so
 * it incidentally covers GenericNode.vue's string-interpolated dynamic import.
 *
 * Prerequisites: flowfile_core on :63578 and a web server on :8080.
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";
const COMPLEX_FLOW_FIXTURE = path.resolve(__dirname, "fixtures/complex-flow.yaml");

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
  await page.locator("table.column-list tbody tr").first().waitFor({ timeout: 20000 });
}

const rowNames = (page: Page) =>
  page.locator("table.column-list tbody tr .column-name-cell").allInnerTexts();

/**
 * Chromium will not synthesize an HTML5 drag from mouse moves, so drive the
 * event sequence directly with one shared DataTransfer. clientY sits in the top
 * half of the target row, which means "insert before it".
 */
const dragRowOnto = (page: Page, fromIndex: number, toIndex: number) =>
  page.evaluate(
    ({ fromIndex, toIndex }) => {
      const rows = Array.from(document.querySelectorAll("table.column-list tbody tr"));
      const dataTransfer = new DataTransfer();
      const fire = (el: Element, type: string, clientY: number) =>
        el.dispatchEvent(
          new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer, clientY }),
        );
      const source = rows[fromIndex];
      const target = rows[toIndex];
      const targetTop = target.getBoundingClientRect().top + 2;
      fire(source, "dragstart", source.getBoundingClientRect().top + 2);
      fire(target, "dragover", targetTop);
      fire(target, "drop", targetTop);
      fire(source, "dragend", 0);
    },
    { fromIndex, toIndex },
  );

test.describe("Column selector", () => {
  let authToken: string;
  let flowId: number;
  let selectNodeId: string;
  let joinNodeId: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    flowId = await importFixture(request, authToken);
    selectNodeId = await findNodeId(request, authToken, flowId, "select");
    joinNodeId = await findNodeId(request, authToken, flowId, "join");
  });

  test("shift-select highlights rows without painting a native text selection", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const rows = page.locator("table.column-list tbody tr");
    expect(await rows.count()).toBeGreaterThanOrEqual(3);

    await rows.nth(0).locator(".column-name-cell").click();
    await rows
      .nth(2)
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });

    expect(await page.evaluate(() => window.getSelection()?.toString() ?? "")).toBe("");
    await expect(page.locator("table.column-list tbody tr.is-selected")).toHaveCount(3);
  });

  test("moving a multi-row selection to the top moves every selected row", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const before = await rowNames(page);
    test.skip(before.length < 4, "needs at least 4 columns to be meaningful");

    const rows = page.locator("table.column-list tbody tr");
    await rows.nth(2).locator(".column-name-cell").click();
    await rows
      .nth(3)
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });
    await expect(page.locator("table.column-list tbody tr.is-selected")).toHaveCount(2);

    await rows.nth(3).locator(".column-name-cell").click({ button: "right" });
    await page.getByRole("button", { name: "Move to top" }).click();

    const after = await rowNames(page);
    expect(after.slice(0, 2)).toEqual([before[2], before[3]]);
    expect(after).toHaveLength(before.length);
    expect(new Set(after)).toEqual(new Set(before));
    // The highlight follows the rows rather than staying on the old indices.
    await expect(page.locator("table.column-list tbody tr.is-selected")).toHaveCount(2);
  });

  test("dragging a multi-row selection moves the whole block, not just one row", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const before = await rowNames(page);
    test.skip(before.length < 5, "needs at least 5 columns to be meaningful");

    const rows = page.locator("table.column-list tbody tr");
    await rows.nth(2).locator(".column-name-cell").click();
    await rows
      .nth(4)
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });
    await expect(page.locator("table.column-list tbody tr.is-selected")).toHaveCount(3);

    await dragRowOnto(page, 4, 0);

    const after = await rowNames(page);
    expect(after.slice(0, 3)).toEqual([before[2], before[3], before[4]]);
    expect(new Set(after)).toEqual(new Set(before));
    await expect(page.locator("table.column-list tbody tr.is-selected")).toHaveCount(3);
  });

  test("the header sort cycles asc -> desc -> original on a node that never bound sortedBy", async ({
    page,
  }) => {
    // Only the Select node binds v-model:sortedBy. Join's two pickers used to sit
    // on a permanently "none" prop, so every header click re-sorted ascending.
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, joinNodeId);

    const table = page.locator("table.column-list").first();
    const header = table.locator("thead th.is-sortable").first();
    const names = () => table.locator("tbody tr .column-name-cell").allInnerTexts();

    const original = await names();
    test.skip(original.length < 3, "needs at least 3 columns to be meaningful");

    await header.click();
    const ascending = await names();
    expect(ascending).toEqual([...original].sort((a, b) => a.localeCompare(b)));

    await header.click();
    expect(await names()).toEqual([...original].sort((a, b) => b.localeCompare(a)));

    await header.click();
    expect(await names()).toEqual(original);
  });

  test("clicking outside the rows clears the selection", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const rows = page.locator("table.column-list tbody tr");
    const selected = page.locator("table.column-list tbody tr.is-selected");
    const selectionGroup = page.locator(".column-list-selection");

    await rows.nth(1).locator(".column-name-cell").click();
    await expect(selected).toHaveCount(1);
    await expect(selectionGroup).toBeVisible();

    // A non-sortable header is inert, so it stands in for "blank chrome".
    await page.locator("table.column-list thead th", { hasText: "New column name" }).click();
    await expect(selected).toHaveCount(0);
    await expect(selectionGroup).toBeHidden();
  });

  test("the selection toolbar does not shift the table, and Clear selection works", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const firstRow = page.locator("table.column-list tbody tr").first();
    const toolbar = page.locator(".column-list-toolbar");
    const selectionGroup = page.locator(".column-list-selection");

    const rowY = (await firstRow.boundingBox())!.y;
    const toolbarHeight = (await toolbar.boundingBox())!.height;

    await page.locator("table.column-list tbody tr").nth(1).locator(".column-name-cell").click();
    await expect(selectionGroup).toBeVisible();

    // Swapping the kept-count for the taller selection pill must not move the rows.
    expect((await firstRow.boundingBox())!.y).toBe(rowY);
    expect((await toolbar.boundingBox())!.height).toBe(toolbarHeight);

    await page.getByRole("button", { name: "Clear selection" }).click();
    await expect(page.locator("table.column-list tbody tr.is-selected")).toHaveCount(0);
    await expect(selectionGroup).toBeHidden();
    expect((await firstRow.boundingBox())!.y).toBe(rowY);
  });

  test("the rename and filter fields opt out of password-manager autofill", async ({ page }) => {
    // A column named name/city/email reads as a contact form to LastPass.
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const fields = [
      page.locator("table.column-list tbody tr .inline-input").first(),
      page.locator(".column-list-search .search-input").first(),
    ];

    for (const field of fields) {
      await expect(field).toHaveAttribute("autocomplete", "off");
      await expect(field).toHaveAttribute("data-lpignore", "true");
      await expect(field).toHaveAttribute("data-1p-ignore", "true");
      await expect(field).toHaveAttribute("data-bwignore", "true");
      await expect(field).toHaveAttribute("data-form-type", "other");
    }
  });

  test("changing a data type marks that cell, and only that cell", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const changed = page.locator("table.column-list tbody td.is-changed");
    // A stored rename sets is_altered but not data_type_change, so nothing is
    // marked until a type actually differs from the source.
    await expect(changed).toHaveCount(0);

    const row = page.locator("table.column-list tbody tr").nth(1);
    await row.locator(".el-select").click();
    const option = page.getByRole("option", { name: "Int64", exact: true }).first();
    await option.scrollIntoViewIfNeeded();
    await option.click();

    await expect(changed).toHaveCount(1);
    await expect(row.locator(".change-marker")).toBeVisible();
    await expect(row.locator(".change-marker")).toHaveAttribute(
      "title",
      /Data type changed from String/,
    );
  });

  test("reordering persists the position field back through the API", async ({ page, request }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, selectNodeId);

    const before = await rowNames(page);
    const rows = page.locator("table.column-list tbody tr");
    await rows.nth(2).locator(".column-name-cell").click();
    await rows
      .nth(3)
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });
    await rows.nth(3).locator(".column-name-cell").click({ button: "right" });
    await page.getByRole("button", { name: "Move to top" }).click();

    const onScreen = await rowNames(page);

    await page.getByRole("button", { name: "Apply" }).click();
    await expect(page.getByRole("button", { name: "Applied ✓" })).toBeVisible();

    const response = await request.get(
      `${API_URL}/node?flow_id=${flowId}&node_id=${selectNodeId}&get_data=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    expect(response.ok()).toBe(true);
    const node = await response.json();
    const renames = node?.setting_input?.select_input ?? [];
    if (renames.length === 0) test.skip(true, "node has no select_input to compare");

    const byPosition = [...renames]
      .sort((a: any, b: any) => a.position - b.position)
      .map((r: any) => r.old_name);
    expect(byPosition).toEqual(onScreen);
    expect(byPosition).not.toEqual(before);
  });
});
