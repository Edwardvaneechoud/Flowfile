import { test, expect, APIRequestContext, Locator, Page } from "@playwright/test";

/**
 * E2E for notebook cell reordering and structural undo (Change 1) on both
 * surfaces: the catalog notebook (NotebookPanel + CatalogNotebookCell) and the
 * Python Script node's notebook editor (NotebookEditor + NotebookCell).
 *
 * Prerequisites (same as web-flow.spec.ts):
 * 1. Start backend: poetry run flowfile_core
 * 2. Start frontend: npm run dev:web (from flowfile_frontend)
 * 3. Run: npx playwright test tests/notebook-interactions.spec.ts
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

const NOTEBOOK_STORAGE_KEY = "flowfile.notebook.v1";
const NOTEBOOK_URL = `${BASE_URL}/#/main/catalog?tab=notebook`;
const CELL_SOURCES = ["# cell A", "# cell B", "# cell C", "# cell D"];
const CELL_IDS = CELL_SOURCES.map((_, i) => `e2e-cell-${i}`);
const SELECT_ALL = process.platform === "darwin" ? "Meta+a" : "Control+a";

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const response = await request.post(`${API_URL}/auth/token`);
  if (!response.ok()) throw new Error(`Failed to get auth token: ${response.status()}`);
  return (await response.json()).access_token;
}

interface SeededNotebook {
  id: number;
  name: string;
}

async function createNotebook(
  request: APIRequestContext,
  token: string,
  label: string,
): Promise<SeededNotebook> {
  const name = `NB_E2E_${label}_${Date.now()}`;
  const response = await request.post(`${API_URL}/catalog/notebooks`, {
    headers: { Authorization: `Bearer ${token}` },
    data: {
      name,
      namespace_id: null,
      description: null,
      cells: CELL_SOURCES.map((source, i) => ({
        id: CELL_IDS[i],
        type: "python",
        source,
        metadata: {},
      })),
      default_kernel_id: null,
    },
  });
  if (!response.ok()) throw new Error(`Failed to create notebook: ${response.status()}`);
  const body = await response.json();
  return { id: body.id, name: body.name };
}

async function notebookSources(
  request: APIRequestContext,
  token: string,
  id: number,
): Promise<string[]> {
  const response = await request.get(`${API_URL}/catalog/notebooks/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  if (!response.ok()) throw new Error(`Failed to read notebook: ${response.status()}`);
  return (await response.json()).cells.map((c: { source: string }) => c.source);
}

async function deleteNotebook(request: APIRequestContext, token: string, id: number) {
  await request.delete(`${API_URL}/catalog/notebooks/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
}

// Hash routes don't reload, so the token is injected and the page reloaded to
// let AuthService pick it up on init.
async function navigateWithAuth(page: Page, token: string, targetUrl: string) {
  await page.goto(targetUrl);
  await page.waitForLoadState("networkidle");
  await page.evaluate(
    ({ token, expiration, storageKey }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
      localStorage.setItem("flowfile-tutorial-dismissed", "true");
      localStorage.setItem("flowfile-tutorial-banner-dismissed", "true");
      // A tab left over from an earlier run would shadow the seeded notebook.
      localStorage.removeItem(storageKey);
    },
    { token, expiration: Date.now() + 60 * 60 * 1000, storageKey: NOTEBOOK_STORAGE_KEY },
  );
  await page.reload();
  await page.waitForLoadState("networkidle");
}

const cellRoots = (page: Page, host = ".nb-cells") => page.locator(`${host} > [data-cell-id]`);

const cellIds = (page: Page, host = ".nb-cells") =>
  cellRoots(page, host).evaluateAll((els) =>
    els.map((el) => el.getAttribute("data-cell-id") ?? ""),
  );

const cellTexts = async (page: Page, host = ".nb-cells") =>
  (await cellRoots(page, host).locator(".cm-content").allInnerTexts()).map((t) => t.trim());

async function expectCellIds(page: Page, expected: string[], host = ".nb-cells") {
  await expect.poll(() => cellIds(page, host), { timeout: 10000 }).toEqual(expected);
}

async function openNotebookByName(page: Page, name: string) {
  await expect(page.locator(".notebook-panel")).toBeVisible({ timeout: 20000 });
  await page.locator("button[title='New or open notebook']").click();
  await page.locator("li.el-dropdown-menu__item").filter({ hasText: name }).first().click();
  await expect(cellRoots(page)).toHaveCount(CELL_SOURCES.length, { timeout: 20000 });
}

interface DragOrigin {
  x: number;
  y: number;
}

/** Press the drag handle of `index` and return the pointer's start position. */
async function pressHandle(page: Page, cells: Locator, index: number): Promise<DragOrigin> {
  const handle = cells.nth(index).locator(".nb-drag-handle");
  await handle.scrollIntoViewIfNeeded();
  const box = await handle.boundingBox();
  if (!box) throw new Error(`No drag handle for cell ${index}`);
  const origin = { x: box.x + box.width / 2, y: box.y + box.height / 2 };
  await page.mouse.move(origin.x, origin.y);
  await page.mouse.down();
  return origin;
}

/** Vertical target below a cell's midpoint (drop after it). */
async function belowCell(cells: Locator, index: number): Promise<number> {
  const box = await cells.nth(index).boundingBox();
  if (!box) throw new Error(`No bounding box for cell ${index}`);
  return box.y + box.height - 4;
}

/** Vertical target above a cell's midpoint (drop before it). */
async function aboveCell(cells: Locator, index: number): Promise<number> {
  const box = await cells.nth(index).boundingBox();
  if (!box) throw new Error(`No bounding box for cell ${index}`);
  return box.y + 3;
}

test.describe("Catalog notebook — reorder and undo", () => {
  let authToken: string;
  const seeded: number[] = [];

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test.afterAll(async ({ request }) => {
    for (const id of seeded) await deleteNotebook(request, authToken, id);
  });

  test("drags a cell to the end and back with the pointer", async ({ page, request }) => {
    const nb = await createNotebook(request, authToken, "drag");
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);

    const cells = cellRoots(page);
    await expectCellIds(page, CELL_IDS);

    const origin = await pressHandle(page, cells, 0);
    await page.mouse.move(origin.x, await belowCell(cells, 3), { steps: 6 });
    await expect(cells.nth(0)).toHaveClass(/is-dragging/);
    await expect(page.locator(".nb-cells .nb-drop-line")).toBeVisible();
    await page.mouse.up();

    await expectCellIds(page, ["e2e-cell-1", "e2e-cell-2", "e2e-cell-3", "e2e-cell-0"]);
    // The code travelled with the id, not with the position.
    expect(await cellTexts(page)).toEqual(["# cell B", "# cell C", "# cell D", "# cell A"]);

    const backOrigin = await pressHandle(page, cells, 3);
    await page.mouse.move(backOrigin.x, await aboveCell(cells, 0), { steps: 6 });
    await expect(cells.nth(3)).toHaveClass(/is-dragging/);
    await page.mouse.up();

    await expectCellIds(page, CELL_IDS);
    expect(await cellTexts(page)).toEqual(CELL_SOURCES);
  });

  test("Escape mid-drag cancels without reordering", async ({ page, request }) => {
    const nb = await createNotebook(request, authToken, "escape");
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);

    const cells = cellRoots(page);
    const origin = await pressHandle(page, cells, 0);
    await page.mouse.move(origin.x, await belowCell(cells, 3), { steps: 6 });
    await expect(page.locator(".nb-cells .nb-drop-line")).toBeVisible();

    await page.keyboard.press("Escape");
    await page.mouse.up();

    await expect(page.locator(".nb-cells .nb-drop-line")).toHaveCount(0);
    await expect(cells.nth(0)).not.toHaveClass(/is-dragging/);
    await expectCellIds(page, CELL_IDS);
  });

  test("a drop inside the dragged cell's own span changes nothing and leaves the tab clean", async ({
    page,
    request,
  }) => {
    const nb = await createNotebook(request, authToken, "noop");
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);

    const cells = cellRoots(page);
    const dirtyMarker = page.locator(".nb-tabs .nb-dirty");
    await expect(dirtyMarker).toHaveCount(0);

    const origin = await pressHandle(page, cells, 0);
    await page.mouse.move(origin.x, await belowCell(cells, 0), { steps: 4 });
    await expect(cells.nth(0)).toHaveClass(/is-dragging/);
    await page.mouse.up();

    await expectCellIds(page, CELL_IDS);
    await page.waitForTimeout(500);
    await expect(dirtyMarker).toHaveCount(0);
  });

  test("undo restores a move and a delete", async ({ page, request }) => {
    const nb = await createNotebook(request, authToken, "undo");
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);

    const cells = cellRoots(page);
    await cells.nth(0).locator(".nb-drag-handle").focus();
    await page.keyboard.press("Alt+ArrowDown");

    await expect(page.locator(".nb-sr-only[role='status']")).toHaveText(
      "Cell 1 moved to position 2 of 4",
    );
    await expectCellIds(page, ["e2e-cell-1", "e2e-cell-0", "e2e-cell-2", "e2e-cell-3"]);

    await cells.nth(1).locator("[title='Delete cell']").click();
    await expectCellIds(page, ["e2e-cell-1", "e2e-cell-2", "e2e-cell-3"]);

    const undo = page.locator("button[title='Undo cell action (insert, delete, move, duplicate)']");
    await undo.click();
    await expectCellIds(page, ["e2e-cell-1", "e2e-cell-0", "e2e-cell-2", "e2e-cell-3"]);
    expect(await cellTexts(page)).toEqual(["# cell B", "# cell A", "# cell C", "# cell D"]);

    await undo.click();
    await expectCellIds(page, CELL_IDS);
    expect(await cellTexts(page)).toEqual(CELL_SOURCES);
  });

  test("a saved reorder survives a reload", async ({ page, request }) => {
    const nb = await createNotebook(request, authToken, "save");
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);

    const cells = cellRoots(page);
    const origin = await pressHandle(page, cells, 0);
    await page.mouse.move(origin.x, await belowCell(cells, 3), { steps: 6 });
    await page.mouse.up();
    await expectCellIds(page, ["e2e-cell-1", "e2e-cell-2", "e2e-cell-3", "e2e-cell-0"]);
    await expect(page.locator(".nb-tabs .nb-dirty")).toHaveCount(1);

    await page.locator(".nb-split-btn--main").click();
    await expect(page.locator(".nb-tabs .nb-dirty")).toHaveCount(0, { timeout: 20000 });

    expect(await notebookSources(request, authToken, nb.id)).toEqual([
      "# cell B",
      "# cell C",
      "# cell D",
      "# cell A",
    ]);

    // Reload with the open-tab cache cleared, so the reopen comes from the server.
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);
    await expectCellIds(page, ["e2e-cell-1", "e2e-cell-2", "e2e-cell-3", "e2e-cell-0"]);
    expect(await cellTexts(page)).toEqual(["# cell B", "# cell C", "# cell D", "# cell A"]);
  });

  test("delete is refused for the last remaining cell", async ({ page, request }) => {
    const nb = await createNotebook(request, authToken, "lastcell");
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name);

    const cells = cellRoots(page);
    for (let remaining = 4; remaining > 1; remaining--) {
      await cells.first().locator("[title='Delete cell']").click();
      await expect(cells).toHaveCount(remaining - 1);
    }
    await expect(cells.first().locator("[title='Delete cell']")).toBeDisabled();
  });
});

test.describe("Python Script node notebook — reorder and undo", () => {
  // The node cell list lives in the settings drawer; a taller viewport keeps all
  // three cells on screen so a pointer drag can reach past the last one.
  test.use({ viewport: { width: 1600, height: 1100 } });

  const NODE_HOST = ".notebook-cells";
  let authToken: string;
  let flowId: number;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    const created = await request.post(
      `${API_URL}/editor/create_flow/?name=NotebookNode_E2E_${Date.now()}&register_in_catalog=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    if (!created.ok()) throw new Error(`create_flow failed: ${created.status()}`);
    flowId = await created.json();
    const added = await request.post(
      `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=python_script&pos_x=320&pos_y=220`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    if (!added.ok()) throw new Error(`add_node failed: ${added.status()}`);
  });

  test.afterAll(async ({ request }) => {
    await request.post(`${API_URL}/editor/close_flow/?flow_id=${flowId}`, {
      headers: { Authorization: `Bearer ${authToken}` },
    });
  });

  test("drags a node cell past the last one and undoes it", async ({ page }) => {
    await page.goto(`${BASE_URL}/#/main/designer`);
    await page.waitForLoadState("networkidle");
    await page.evaluate(
      ({ token, expiration, flowId }) => {
        localStorage.setItem("auth_token", token);
        localStorage.setItem("auth_token_expiration", expiration.toString());
        localStorage.setItem("flowfile-tutorial-dismissed", "true");
        localStorage.setItem("flowfile-tutorial-banner-dismissed", "true");
        sessionStorage.setItem("last_flow_id", String(flowId));
      },
      { token: authToken, expiration: Date.now() + 60 * 60 * 1000, flowId },
    );
    await page.reload();
    await page.waitForLoadState("networkidle");
    await expect(page.locator('.vue-flow__node[data-id="1"]')).toBeVisible({ timeout: 20000 });

    // dispatchEvent rather than dblclick: the floating palette can cover the node.
    await page.locator('.vue-flow__node[data-id="1"]').dispatchEvent("dblclick");
    const cells = cellRoots(page, NODE_HOST);
    await expect(cells).toHaveCount(1, { timeout: 20000 });

    await page.locator("button.add-cell-button").click();
    await page.locator("button.add-cell-button").click();
    await expect(cells).toHaveCount(3);

    const sources = ["# node cell A", "# node cell B", "# node cell C"];
    for (let i = 0; i < sources.length; i++) {
      await cells.nth(i).locator(".cm-content").click();
      await page.keyboard.press(SELECT_ALL);
      await page.keyboard.type(sources[i]);
    }
    expect(await cellTexts(page, NODE_HOST)).toEqual(sources);

    const before = await cellIds(page, NODE_HOST);
    const origin = await pressHandle(page, cells, 0);
    await page.mouse.move(origin.x, await belowCell(cells, 2), { steps: 6 });
    await expect(cells.nth(0)).toHaveClass(/is-dragging/);
    await expect(page.locator(`${NODE_HOST} .nb-drop-line`)).toBeVisible();
    await page.mouse.up();

    await expectCellIds(page, [before[1], before[2], before[0]], NODE_HOST);
    expect(await cellTexts(page, NODE_HOST)).toEqual([sources[1], sources[2], sources[0]]);

    await page
      .locator("button[title='Undo cell action (insert, delete, move, duplicate)']")
      .click();
    await expectCellIds(page, before, NODE_HOST);
    expect(await cellTexts(page, NODE_HOST)).toEqual(sources);
  });
});
