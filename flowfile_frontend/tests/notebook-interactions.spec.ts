import { test, expect, APIRequestContext, Locator, Page } from "@playwright/test";

/**
 * E2E for notebook cell reordering and structural undo (Change 1) and for cell
 * actions, collapse and run-and-advance focus (Change 2) on both surfaces: the
 * catalog notebook (NotebookPanel + CatalogNotebookCell) and the Python Script
 * node's notebook editor (NotebookEditor + NotebookCell).
 *
 * No kernel is required: run-and-advance synthesises a "No kernel selected"
 * output and still advances, which is the focus path under test.
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
// Run and advance is Mod-Enter, i.e. Cmd on macOS and Ctrl elsewhere.
const RUN_ADVANCE = process.platform === "darwin" ? "Meta+Enter" : "Control+Enter";

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const response = await request.post(`${API_URL}/auth/token`);
  if (!response.ok()) throw new Error(`Failed to get auth token: ${response.status()}`);
  return (await response.json()).access_token;
}

interface SeededNotebook {
  id: number;
  name: string;
}

interface SeedCell {
  id: string;
  type: "python" | "markdown";
  source: string;
}

const defaultSeedCells = (): SeedCell[] =>
  CELL_SOURCES.map((source, i) => ({ id: CELL_IDS[i], type: "python", source }));

async function createNotebook(
  request: APIRequestContext,
  token: string,
  label: string,
  cells: SeedCell[] = defaultSeedCells(),
): Promise<SeededNotebook> {
  const name = `NB_E2E_${label}_${Date.now()}`;
  const response = await request.post(`${API_URL}/catalog/notebooks`, {
    headers: { Authorization: `Bearer ${token}` },
    data: {
      name,
      namespace_id: null,
      description: null,
      cells: cells.map((c) => ({ ...c, metadata: {} })),
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

async function openNotebookByName(page: Page, name: string, expectedCells = CELL_SOURCES.length) {
  await expect(page.locator(".notebook-panel")).toBeVisible({ timeout: 20000 });
  await page.locator("button[title='New or open notebook']").click();
  await page.locator("li.el-dropdown-menu__item").filter({ hasText: name }).first().click();
  await expect(cellRoots(page)).toHaveCount(expectedCells, { timeout: 20000 });
}

/** The one cell-action menu item currently on screen; every closed popper is filtered out. */
const menuItem = (page: Page, action: string) =>
  page.locator(`li.el-dropdown-menu__item[data-action="${action}"]:visible`);

async function openCellMenu(page: Page, cells: Locator, index: number) {
  await cells.nth(index).locator("button.nb-cell-menu").click();
  await expect(menuItem(page, "toggle-code")).toBeVisible();
}

async function cellMenuAction(page: Page, cells: Locator, index: number, action: string) {
  await openCellMenu(page, cells, index);
  const item = menuItem(page, action);
  await item.click();
  // Wait for the popper to close so Element Plus's focus handling has settled.
  await expect(menuItem(page, "toggle-code")).toBeHidden();
}

interface FocusInfo {
  cellId: string | null;
  inEditor: boolean;
}

async function focusedCell(page: Page): Promise<FocusInfo> {
  return page.evaluate(() => {
    const el = document.activeElement as HTMLElement | null;
    const root = el?.closest("[data-cell-id]") ?? null;
    return {
      cellId: root?.getAttribute("data-cell-id") ?? null,
      inEditor: !!el?.closest(".cm-content"),
    };
  });
}

async function expectEditorFocused(page: Page, cellId: string) {
  const expected: FocusInfo = { cellId, inEditor: true };
  await expect.poll(() => focusedCell(page), { timeout: 10000 }).toEqual(expected);
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

    await cellMenuAction(page, cells, 1, "delete");
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
      await cellMenuAction(page, cells, 0, "delete");
      await expect(cells).toHaveCount(remaining - 1);
    }
    await openCellMenu(page, cells, 0);
    const deleteItem = menuItem(page, "delete");
    await expect(deleteItem).toHaveAttribute("aria-disabled", "true");
    // The label greys out with the item instead of staying danger-red.
    const colours = await deleteItem.evaluate((el) => ({
      label: getComputedStyle(el.querySelector(".nb-menu-danger") as HTMLElement).color,
      item: getComputedStyle(el).color,
    }));
    expect(colours.label).toBe(colours.item);
  });
});

test.describe("Catalog notebook — cell actions and focus", () => {
  let authToken: string;
  const seeded: number[] = [];

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test.afterAll(async ({ request }) => {
    for (const id of seeded) await deleteNotebook(request, authToken, id);
  });

  async function seedAndOpen(
    page: Page,
    request: APIRequestContext,
    label: string,
    cells?: SeedCell[],
  ): Promise<SeededNotebook> {
    const nb = await createNotebook(request, authToken, label, cells);
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name, (cells ?? defaultSeedCells()).length);
    return nb;
  }

  test("run-and-advance on the last cell appends a blank cell and focuses it", async ({
    page,
    request,
  }) => {
    await seedAndOpen(page, request, "advance-end");

    const cells = cellRoots(page);
    await cells.nth(3).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells).toHaveCount(5);
    // The run really happened; without a kernel it lands as a synthesised error.
    await expect(cells.nth(3).locator(".cell-output")).toContainText("No kernel selected");

    const ids = await cellIds(page);
    expect(ids.slice(0, 4)).toEqual(CELL_IDS);
    await expectEditorFocused(page, ids[4]);
  });

  test("run-and-advance on a middle cell focuses the next editor", async ({ page, request }) => {
    await seedAndOpen(page, request, "advance-mid");

    const cells = cellRoots(page);
    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells).toHaveCount(4);
    await expectEditorFocused(page, CELL_IDS[1]);
  });

  test("run-and-advance renders a markdown cell and moves on", async ({ page, request }) => {
    const seedCells: SeedCell[] = [
      { id: "e2e-md-0", type: "python", source: "# cell A" },
      { id: "e2e-md-1", type: "markdown", source: "" },
      { id: "e2e-md-2", type: "python", source: "# cell C" },
    ];
    await seedAndOpen(page, request, "advance-md", seedCells);

    const cells = cellRoots(page);
    const mdCell = cells.nth(1);
    await mdCell.locator(".nb-md-rendered").dblclick();
    const textarea = mdCell.locator("textarea");
    await expect(textarea).toBeVisible();
    await textarea.click();
    await page.keyboard.type("# Hi");

    await page.keyboard.press(RUN_ADVANCE);

    await expect(mdCell.locator(".nb-md-rendered h1")).toHaveText("Hi");
    await expect(cells).toHaveCount(3);
    await expectEditorFocused(page, "e2e-md-2");
  });

  test("run-and-advance passes through a rendered markdown cell", async ({ page, request }) => {
    const seedCells: SeedCell[] = [
      { id: "e2e-pass-0", type: "python", source: "# cell A" },
      { id: "e2e-pass-1", type: "markdown", source: "## Middle" },
      { id: "e2e-pass-2", type: "python", source: "# cell C" },
    ];
    await seedAndOpen(page, request, "advance-md-pass", seedCells);

    const cells = cellRoots(page);
    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    // Command mode: the advance lands on the rendered cell's root, not in an editor.
    await expect
      .poll(() => focusedCell(page), { timeout: 10000 })
      .toEqual({ cellId: "e2e-pass-1", inEditor: false });
    await expect(cells.nth(1).locator(".nb-md-rendered")).toBeVisible();

    // From there Cmd/Ctrl+Enter must keep advancing, not un-render the cell.
    await page.keyboard.press(RUN_ADVANCE);
    await expect(cells.nth(1).locator(".nb-md-rendered h2")).toHaveText("Middle");
    await expectEditorFocused(page, "e2e-pass-2");
  });

  test("Enter on a rendered markdown cell opens its editor and takes the caret", async ({
    page,
    request,
  }) => {
    const seedCells: SeedCell[] = [
      { id: "e2e-cmd-0", type: "python", source: "# cell A" },
      { id: "e2e-cmd-1", type: "markdown", source: "## Middle" },
    ];
    await seedAndOpen(page, request, "md-command-mode", seedCells);

    const cells = cellRoots(page);
    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);
    await expect
      .poll(() => focusedCell(page), { timeout: 10000 })
      .toEqual({ cellId: "e2e-cmd-1", inEditor: false });

    await page.keyboard.press("Enter");
    const textarea = cells.nth(1).locator("textarea");
    await expect(textarea).toBeFocused();
    await page.keyboard.press(SELECT_ALL);
    await page.keyboard.type("## Edited");
    await expect(textarea).toHaveValue("## Edited");
  });

  test("run-and-advance expands a collapsed target and focuses its editor", async ({
    page,
    request,
  }) => {
    await seedAndOpen(page, request, "advance-collapsed");

    const cells = cellRoots(page);
    await cellMenuAction(page, cells, 1, "toggle-code");
    await expect(cells.nth(1).locator("button.nb-cell-collapsed")).toBeVisible();

    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells.nth(1).locator("button.nb-cell-collapsed")).toHaveCount(0);
    await expectEditorFocused(page, CELL_IDS[1]);
  });

  test("duplicate copies the code below the source, drops the output and focuses the copy", async ({
    page,
    request,
  }) => {
    await seedAndOpen(page, request, "duplicate");

    const cells = cellRoots(page);
    // Give the source cell an output first, so "no old result" is a real assertion.
    await cells.nth(1).locator(".cm-content").click();
    await cells.nth(1).locator("button.nb-run").click();
    await expect(cells.nth(1).locator(".cell-output")).toBeVisible();

    await cellMenuAction(page, cells, 1, "duplicate");

    await expect(cells).toHaveCount(5);
    const ids = await cellIds(page);
    expect(ids[0]).toBe(CELL_IDS[0]);
    expect(ids[1]).toBe(CELL_IDS[1]);
    expect(ids[2]).not.toBe(CELL_IDS[1]);
    expect(ids.slice(3)).toEqual([CELL_IDS[2], CELL_IDS[3]]);

    const copy = cells.nth(2);
    await expect(copy.locator(".cm-content")).toHaveText(CELL_SOURCES[1]);
    await expect(copy.locator(".cell-output")).toHaveCount(0);
    await expectEditorFocused(page, ids[2]);
  });

  test("collapsing code keeps the same editor instance and its text", async ({ page, request }) => {
    await seedAndOpen(page, request, "collapse-code");

    const cells = cellRoots(page);
    const cell = cells.nth(0);
    const editor = cell.locator(".cm-editor");
    await editor.evaluate((el) => {
      (el as HTMLElement & { __e2eStamp?: string }).__e2eStamp = "cm-editor-0";
    });

    await cellMenuAction(page, cells, 0, "toggle-code");
    await expect(cell.locator(".cm-content")).toBeHidden();
    const stub = cell.locator("button.nb-cell-collapsed");
    await expect(stub).toBeVisible();
    await expect(stub).toContainText("Code hidden");

    await stub.click();
    await expect(cell.locator(".cm-content")).toBeVisible();
    await expect(cell.locator(".cm-content")).toHaveText(CELL_SOURCES[0]);
    const stamp = await editor.evaluate(
      (el) => (el as HTMLElement & { __e2eStamp?: string }).__e2eStamp ?? null,
    );
    expect(stamp).toBe("cm-editor-0");

    // The restored view still takes input.
    await cell.locator(".cm-content").click();
    await page.keyboard.press(SELECT_ALL);
    await page.keyboard.type("# edited A");
    await expect(cell.locator(".cm-content")).toHaveText("# edited A");
  });

  test("collapse output is not offered for a cell without output", async ({ page, request }) => {
    await seedAndOpen(page, request, "no-output");

    const cells = cellRoots(page);
    await expect(cells.nth(0).locator(".cell-output")).toHaveCount(0);

    await openCellMenu(page, cells, 0);
    await expect(menuItem(page, "toggle-output")).toHaveCount(0);

    await cells.nth(0).locator(".cm-content").click();
    await expect(menuItem(page, "toggle-code")).toBeHidden();
  });

  test("delete moves focus to the next cell, or the previous one at the end", async ({
    page,
    request,
  }) => {
    await seedAndOpen(page, request, "delete-focus");

    const cells = cellRoots(page);
    await cellMenuAction(page, cells, 1, "delete");
    await expectCellIds(page, [CELL_IDS[0], CELL_IDS[2], CELL_IDS[3]]);
    await expectEditorFocused(page, CELL_IDS[2]);

    await cellMenuAction(page, cells, 2, "delete");
    await expectCellIds(page, [CELL_IDS[0], CELL_IDS[2]]);
    await expectEditorFocused(page, CELL_IDS[2]);
  });

  test("insert above and below land at the right index and focus the new cell", async ({
    page,
    request,
  }) => {
    await seedAndOpen(page, request, "insert");

    const cells = cellRoots(page);
    await cellMenuAction(page, cells, 0, "insert-above");
    await expect(cells).toHaveCount(5);
    let ids = await cellIds(page);
    expect(ids.slice(1)).toEqual(CELL_IDS);
    await expectEditorFocused(page, ids[0]);

    await cellMenuAction(page, cells, 1, "insert-below");
    await expect(cells).toHaveCount(6);
    ids = await cellIds(page);
    expect([ids[1], ids[3], ids[4], ids[5]]).toEqual(CELL_IDS);
    await expectEditorFocused(page, ids[2]);
  });

  test("undoing a delete restores the cell without stealing focus", async ({ page, request }) => {
    await seedAndOpen(page, request, "undo-focus");

    const cells = cellRoots(page);
    await cellMenuAction(page, cells, 1, "delete");
    await expectCellIds(page, [CELL_IDS[0], CELL_IDS[2], CELL_IDS[3]]);

    await cells.nth(0).locator(".cm-content").click();
    await expectEditorFocused(page, CELL_IDS[0]);

    // dispatchEvent, not click: a real press would move focus to the button itself.
    await page
      .locator("button[title='Undo cell action (insert, delete, move, duplicate)']")
      .dispatchEvent("click");

    await expectCellIds(page, CELL_IDS);
    expect(await cellTexts(page)).toEqual(CELL_SOURCES);
    expect(await focusedCell(page)).toEqual({ cellId: CELL_IDS[0], inEditor: true });
  });

  // Save As copies cell ids verbatim, so two open tabs can hold the same ids and Vue
  // (keyed on cell.id) reuses the very same cell components across owners.
  test("a second notebook with the same cell ids gets its own collapse state and editors", async ({
    page,
    request,
  }) => {
    const twinCells = (label: string): SeedCell[] => [
      { id: "e2e-twin-0", type: "python", source: `# twin ${label} 0` },
      { id: "e2e-twin-1", type: "python", source: `# twin ${label} 1` },
    ];
    const notebookB = await createNotebook(request, authToken, "twin-b", twinCells("B"));
    seeded.push(notebookB.id);
    await seedAndOpen(page, request, "twin-a", twinCells("A"));

    await page.locator("button[title='New or open notebook']").click();
    await page
      .locator("li.el-dropdown-menu__item")
      .filter({ hasText: notebookB.name })
      .first()
      .click();
    await expect(page.locator(".nb-tabs .el-tabs__item.is-active .nb-tab-name")).toHaveText(
      notebookB.name,
    );
    await expectCellIds(page, ["e2e-twin-0", "e2e-twin-1"]);

    const cells = cellRoots(page);
    await cellMenuAction(page, cells, 1, "toggle-code");
    await expect(cells.nth(1).locator("button.nb-cell-collapsed")).toBeVisible();

    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);
    await expect(cells.nth(1).locator("button.nb-cell-collapsed")).toHaveCount(0);
    await expectEditorFocused(page, "e2e-twin-1");
  });

  test("a second open notebook keeps the caret in its own tab", async ({ page, request }) => {
    const bCells: SeedCell[] = CELL_SOURCES.map((source, i) => ({
      id: `e2e-b-${i}`,
      type: "python",
      source: source.replace("cell", "b cell"),
    }));
    const bIds = bCells.map((c) => c.id);
    // Both notebooks exist before the page loads: the "+" list is fetched once on mount.
    const notebookB = await createNotebook(request, authToken, "tab-b", bCells);
    seeded.push(notebookB.id);
    await seedAndOpen(page, request, "tab-a");

    await page.locator("button[title='New or open notebook']").click();
    await page
      .locator("li.el-dropdown-menu__item")
      .filter({ hasText: notebookB.name })
      .first()
      .click();

    await expect(page.locator(".nb-tabs .el-tabs__item.is-active .nb-tab-name")).toHaveText(
      notebookB.name,
    );
    expect(await page.locator(".nb-tabs .el-tabs__item").count()).toBeGreaterThan(1);
    await expectCellIds(page, bIds);

    const cells = cellRoots(page);
    await cells.nth(0).locator(".cm-content").click();
    await expectEditorFocused(page, "e2e-b-0");

    // Nothing left running in notebook A may pull the caret back out of B.
    await page.waitForTimeout(500);
    expect(await focusedCell(page)).toEqual({ cellId: "e2e-b-0", inEditor: true });
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

test.describe("Python Script node notebook — cell actions and focus", () => {
  test.use({ viewport: { width: 1600, height: 1100 } });

  const NODE_HOST = ".notebook-cells";
  // One node per test: the drawer's cells are per-node state that outlives a reload.
  const NODE_IDS = { advance: 1, duplicate: 2, collapse: 3 };
  let authToken: string;
  let flowId: number;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    const created = await request.post(
      `${API_URL}/editor/create_flow/?name=NotebookNodeActions_E2E_${Date.now()}&register_in_catalog=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    if (!created.ok()) throw new Error(`create_flow failed: ${created.status()}`);
    flowId = await created.json();
    for (const [i, nodeId] of Object.values(NODE_IDS).entries()) {
      const added = await request.post(
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=${nodeId}&node_type=python_script&pos_x=${240 + i * 320}&pos_y=220`,
        { headers: { Authorization: `Bearer ${authToken}` } },
      );
      if (!added.ok()) throw new Error(`add_node ${nodeId} failed: ${added.status()}`);
    }
  });

  test.afterAll(async ({ request }) => {
    await request.post(`${API_URL}/editor/close_flow/?flow_id=${flowId}`, {
      headers: { Authorization: `Bearer ${authToken}` },
    });
  });

  async function openNodeNotebook(page: Page, nodeId: number) {
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
    const node = page.locator(`.vue-flow__node[data-id="${nodeId}"]`);
    await expect(node).toBeVisible({ timeout: 20000 });
    // dispatchEvent rather than dblclick: the floating palette can cover the node.
    await node.dispatchEvent("dblclick");
    await expect(cellRoots(page, NODE_HOST)).toHaveCount(1, { timeout: 20000 });
  }

  async function typeIntoCell(page: Page, cells: Locator, index: number, text: string) {
    await cells.nth(index).locator(".cm-content").click();
    await page.keyboard.press(SELECT_ALL);
    await page.keyboard.type(text);
  }

  test("run-and-advance on the last cell appends and focuses a new cell", async ({ page }) => {
    await openNodeNotebook(page, NODE_IDS.advance);

    const cells = cellRoots(page, NODE_HOST);
    await typeIntoCell(page, cells, 0, "# only cell");
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells).toHaveCount(2);
    const ids = await cellIds(page, NODE_HOST);
    await expectEditorFocused(page, ids[1]);
    // The appended cell is blank, so its editor shows only the placeholder.
    expect(await cellTexts(page, NODE_HOST)).toEqual(["# only cell", "# Enter code..."]);
  });

  test("duplicate copies the code and focuses the copy", async ({ page }) => {
    await openNodeNotebook(page, NODE_IDS.duplicate);

    const cells = cellRoots(page, NODE_HOST);
    await typeIntoCell(page, cells, 0, "# to duplicate");
    const before = await cellIds(page, NODE_HOST);

    await cellMenuAction(page, cells, 0, "duplicate");

    await expect(cells).toHaveCount(2);
    const ids = await cellIds(page, NODE_HOST);
    expect(ids[0]).toBe(before[0]);
    expect(ids[1]).not.toBe(before[0]);
    expect(await cellTexts(page, NODE_HOST)).toEqual(["# to duplicate", "# to duplicate"]);
    await expectEditorFocused(page, ids[1]);
  });

  test("collapsing code keeps the same editor instance and its text", async ({ page }) => {
    await openNodeNotebook(page, NODE_IDS.collapse);

    const cells = cellRoots(page, NODE_HOST);
    await typeIntoCell(page, cells, 0, "# collapse me");

    const cell = cells.nth(0);
    const editor = cell.locator(".cm-editor");
    await editor.evaluate((el) => {
      (el as HTMLElement & { __e2eStamp?: string }).__e2eStamp = "node-cm-editor-0";
    });

    await cellMenuAction(page, cells, 0, "toggle-code");
    await expect(cell.locator(".cm-content")).toBeHidden();
    const stub = cell.locator("button.nb-cell-collapsed");
    await expect(stub).toBeVisible();
    await expect(stub).toContainText("Code hidden");

    await stub.click();
    await expect(cell.locator(".cm-content")).toBeVisible();
    await expect(cell.locator(".cm-content")).toHaveText("# collapse me");
    const stamp = await editor.evaluate(
      (el) => (el as HTMLElement & { __e2eStamp?: string }).__e2eStamp ?? null,
    );
    expect(stamp).toBe("node-cm-editor-0");
  });
});
