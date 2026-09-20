import { test, expect, APIRequestContext, Locator, Page } from "@playwright/test";

/**
 * E2E for notebook cell reordering and structural undo (Change 1) and for cell
 * actions, collapse and run-and-advance focus (Change 2) on both surfaces: the
 * catalog notebook (NotebookPanel + CatalogNotebookCell) and the Python Script
 * node's notebook editor (NotebookEditor + NotebookCell).
 *
 * Docker is not required: the kernel is faked at the network layer (mockKernel),
 * so a run can succeed or fail on demand. Leaving that fixture off is what
 * exercises the synthesised "No kernel selected" failure.
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
// Jupyter/Databricks mapping: Shift+Enter runs and advances, Mod-Enter (Cmd on
// macOS, Ctrl elsewhere) runs in place.
const RUN_ADVANCE = "Shift+Enter";
const RUN_IN_PLACE = process.platform === "darwin" ? "Meta+Enter" : "Control+Enter";

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
  defaultKernelId: string | null = null,
): Promise<SeededNotebook> {
  const name = `NB_E2E_${label}_${Date.now()}`;
  const response = await request.post(`${API_URL}/catalog/notebooks`, {
    headers: { Authorization: `Bearer ${token}` },
    data: {
      name,
      namespace_id: null,
      description: null,
      cells: cells.map((c) => ({ ...c, metadata: {} })),
      default_kernel_id: defaultKernelId,
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

const KERNEL_ID = "e2e-kernel";
const KERNEL_NAME = "E2E Kernel";
const KERNEL_ID_2 = "e2e-kernel-2";
const KERNEL_NAME_2 = "E2E Kernel Two";
const KERNEL_STDOUT = "e2e kernel ran this cell";
const RUN_FAILURE = "RuntimeError: e2e kernel refused";
// One namespace identity per mocked kernel: the frontend drops schema metadata whose
// generation doesn't match the one the last execution reported.
const NAMESPACE_GENERATION = "e2e-namespace-1";

const STALE_CODE = "Code changed — rerun";
const STALE_UPSTREAM = "Earlier cells changed — rerun";
const STALE_PREVIOUS = "Previous session";

interface MockFrameColumn {
  name: string;
  dtype: string;
}

/** One entry of the dataframe_schemas answer: a frame the kernel holds after a run. */
interface MockFrame {
  name: string;
  kind?: string;
  state?: string;
  columns: MockFrameColumn[];
}

interface KernelMockOptions {
  /** When set, execute_cell answers success:false with this error text. */
  failWith?: string;
  /** Delay every execute_cell answer, so a batch stays observably in flight. */
  delayMs?: number;
  /** Offer a second kernel, so a test can change the session from the picker. */
  twoKernels?: boolean;
  /** Frames the dataframe_schemas endpoint reports; empty means the kernel holds none. */
  dataframes?: MockFrame[];
}

/** A gate over execute_cell answers; `release` opens it for held and later calls alike. */
interface ExecuteHold {
  /** Resolves once the fixture has received at least `n` execute_cell requests. */
  waitForRequests(n: number): Promise<void>;
  release(): void;
}

interface KernelControl {
  /** execute_cell requests received so far. */
  executeCount(): number;
  /** lsp/dataframe_schemas requests received so far. */
  schemaCount(): number;
  /** Hold every execute_cell answer from now on. */
  hold(): ExecuteHold;
}

/**
 * Answer every /kernels/* call with one ready kernel, so run-and-advance can be
 * driven through a real success or a real failure without Docker. The returned
 * handle counts execute_cell calls and can hold their answers open, which is what
 * makes "in flight" a state a test can stand in.
 * Must be called before the page navigates.
 */
async function mockKernel(page: Page, options: KernelMockOptions = {}): Promise<KernelControl> {
  const makeKernel = (id: string, name: string) => ({
    id,
    name,
    state: "idle",
    container_id: `${id}-container`,
    port: 19000,
    packages: [],
    resolved_packages: [],
    memory_gb: 2,
    cpu_cores: 1,
    gpu: false,
    image_flavour: "base",
    custom_image: null,
    image: "flowfile/kernel:base",
    created_at: new Date().toISOString(),
    error_message: null,
    kernel_version: "0.1.0",
  });
  const kernels = [makeKernel(KERNEL_ID, KERNEL_NAME)];
  if (options.twoKernels) kernels.push(makeKernel(KERNEL_ID_2, KERNEL_NAME_2));

  let executeCount = 0;
  let schemaCount = 0;
  let namespaceRevision = 0;
  let gate: Promise<void> | null = null;
  const frames = options.dataframes ?? [];

  // Code intelligence is a core-level probe, not a kernel call: without it the schema
  // fetch never leaves the frontend.
  await page.route(/\/lsp\/capabilities(\?|$)/, (route) =>
    route
      .fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          enabled: true,
          version: "",
          features: ["complete", "dataframe_schemas"],
        }),
      })
      .catch(() => undefined),
  );

  await page.route(/\/kernels(\/|$)/, async (route) => {
    const path = new URL(route.request().url()).pathname;
    // A held answer can outlive the page, and a fulfil that then fails means nothing.
    const json = (body: unknown) =>
      route
        .fulfill({ status: 200, contentType: "application/json", body: JSON.stringify(body) })
        .catch(() => undefined);

    if (path.endsWith("/docker-status")) {
      return json({ available: true, image_available: true, images: [], error: null });
    }
    if (path.endsWith("/kernels/") || path.endsWith("/kernels")) return json(kernels);
    if (path.endsWith("/execute_cell")) {
      executeCount += 1;
      namespaceRevision += 1;
      if (options.delayMs) await new Promise((resolve) => setTimeout(resolve, options.delayMs));
      if (gate) await gate;
      return json({
        success: !options.failWith,
        output_paths: [],
        artifacts_published: [],
        artifacts_deleted: [],
        display_outputs: [],
        stdout: options.failWith ? "" : KERNEL_STDOUT,
        stderr: "",
        error: options.failWith ?? null,
        execution_time_ms: 7,
        namespace_generation: NAMESPACE_GENERATION,
        revision: namespaceRevision,
      });
    }
    if (path.endsWith("/lsp/dataframe_schemas")) {
      schemaCount += 1;
      return json({
        namespace_generation: NAMESPACE_GENERATION,
        revision: namespaceRevision,
        state: "ready",
        dataframes: frames.map((frame) => ({
          name: frame.name,
          kind: frame.kind ?? "DataFrame",
          state: frame.state ?? "ready",
          columns: frame.columns,
          truncated: false,
        })),
      });
    }
    if (path.endsWith("/clear_namespace")) return json(null);
    if (path.endsWith("/artifacts")) return json({});
    if (path.endsWith("/memory")) return json(null);
    // Code intelligence is failure-safe by design; let it fall back to empty.
    if (path.includes("/lsp/")) {
      return route
        .fulfill({ status: 404, contentType: "application/json", body: "{}" })
        .catch(() => undefined);
    }
    return json([]);
  });

  return {
    executeCount: () => executeCount,
    schemaCount: () => schemaCount,
    hold() {
      let open!: () => void;
      const held = new Promise<void>((resolve) => {
        open = resolve;
      });
      gate = held;
      return {
        async waitForRequests(n: number) {
          const deadline = Date.now() + 20000;
          while (executeCount < n) {
            if (Date.now() > deadline) {
              throw new Error(`Saw ${executeCount} execute_cell requests, expected ${n}`);
            }
            await page.waitForTimeout(50);
          }
        },
        release() {
          open();
          if (gate === held) gate = null;
        },
      };
    },
  };
}

interface MockCatalogTable {
  name: string;
  columns: MockFrameColumn[];
}

/**
 * Answer the strict table resolve with one table's metadata; every other name 404s, which
 * is what "unresolved" looks like to the completion source. No read, no collect.
 */
async function mockCatalogResolve(page: Page, table: MockCatalogTable) {
  await page.route(/\/catalog\/tables\/resolve(\?|$)/, (route) => {
    const q = new URL(route.request().url()).searchParams.get("q");
    if (q !== table.name) {
      return route
        .fulfill({ status: 404, contentType: "application/json", body: "{}" })
        .catch(() => undefined);
    }
    return route
      .fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          table: { id: 1, name: table.name, schema_columns: table.columns },
        }),
      })
      .catch(() => undefined);
  });
}

interface MockNodeInput {
  name: string;
  sourceNodeId: number;
  columns: { name: string; data_type: string }[];
}

/** Two upstream inputs with their own schemas, without building the upstream flow. */
async function mockNodeInputs(page: Page, inputs: MockNodeInput[]) {
  await page.route(/\/node\/input_names(\?|$)/, (route) =>
    route
      .fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(
          inputs.map((input) => ({
            name: input.name,
            source_node_id: input.sourceNodeId,
            source_node_type: "manual_input",
          })),
        ),
      })
      .catch(() => undefined),
  );
  await page.route(/\/node\/data(\?|$)/, (route) => {
    const nodeId = Number(new URL(route.request().url()).searchParams.get("node_id"));
    const input = inputs.find((i) => i.sourceNodeId === nodeId);
    const columns = input?.columns ?? [];
    return route
      .fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          node_id: nodeId,
          number_of_records: 0,
          number_of_columns: columns.length,
          name: input?.name ?? "",
          table_schema: columns,
          columns: columns.map((c) => c.name),
          data: [],
          has_example_data: true,
          has_run_with_current_setup: true,
        }),
      })
      .catch(() => undefined);
  });
}

interface CompletionRow {
  label: string;
  detail: string;
}

/** Every row the completion popup currently renders; empty when it is closed. */
async function completionRows(page: Page): Promise<CompletionRow[]> {
  return page.evaluate(() =>
    Array.from(document.querySelectorAll(".cm-tooltip-autocomplete li")).map((li) => ({
      label: li.querySelector(".cm-completionLabel")?.textContent ?? "",
      detail: li.querySelector(".cm-completionDetail")?.textContent ?? "",
    })),
  );
}

/** `label → detail` for every popup row naming one of `names`, sorted. */
const columnRowKeys = async (page: Page, names: string[]) =>
  (await completionRows(page))
    .filter((row) => names.includes(row.label))
    .map((row) => `${row.label} → ${row.detail}`)
    .sort();

/** Wait until the popup's column rows are exactly `expected` — details included. */
async function expectColumnRows(page: Page, names: string[], expected: string[]) {
  await expect
    .poll(() => columnRowKeys(page, names), { timeout: 15000 })
    .toEqual([...expected].sort());
}

/** No column row may appear: settle first, so an empty popup isn't a race. */
async function expectNoColumnRows(page: Page, names: string[]) {
  await page.waitForTimeout(800);
  expect(await columnRowKeys(page, names)).toEqual([]);
}

/** Retype a cell and ask for completions at the caret; typing alone already opens them. */
async function typeForCompletions(page: Page, cells: Locator, index: number, text: string) {
  await cells.nth(index).locator(".cm-content").click();
  await page.keyboard.press(SELECT_ALL);
  await page.keyboard.type(text);
  await page.keyboard.press("Control+Space");
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

/** The mocked kernel really answered: an output block, and nothing failed in it. */
async function expectRanCleanly(cell: Locator) {
  const output = cell.locator(".cell-output");
  await expect(output).toBeVisible();
  await expect(output.locator(".output-error")).toHaveCount(0);
  await expect(output).toContainText(KERNEL_STDOUT);
}

const staleBadge = (cell: Locator) => cell.locator(".nb-status-badge.is-stale");

async function expectStale(cell: Locator, reason: string, label: string) {
  const badge = staleBadge(cell);
  await expect(badge).toBeVisible({ timeout: 10000 });
  await expect(badge).toHaveAttribute("data-reason", reason);
  await expect(badge).toHaveText(label);
}

/** Open a saved notebook in a second tab; both must exist before the page loaded. */
async function openSavedNotebook(page: Page, name: string) {
  await page.locator("button[title='New or open notebook']").click();
  await page.locator("li.el-dropdown-menu__item").filter({ hasText: name }).first().click();
  await expect(page.locator(".nb-tabs .el-tabs__item.is-active .nb-tab-name")).toHaveText(name);
}

async function switchNotebookTab(page: Page, name: string) {
  // dispatchEvent, not click: the strip scrolls, so an inactive tab can sit off it.
  await page.locator(".nb-tabs .el-tabs__item").filter({ hasText: name }).dispatchEvent("click");
  await expect(page.locator(".nb-tabs .el-tabs__item.is-active .nb-tab-name")).toHaveText(name);
}

/** Nothing moved: same cells, same caret. The wait lets a stray advance land first. */
async function expectStayedPut(page: Page, cellId: string, count: number, host = ".nb-cells") {
  await page.waitForTimeout(500);
  await expect(cellRoots(page, host)).toHaveCount(count);
  expect(await focusedCell(page)).toEqual({ cellId, inEditor: true });
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
    defaultKernelId: string | null = null,
  ): Promise<SeededNotebook> {
    const nb = await createNotebook(request, authToken, label, cells, defaultKernelId);
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name, (cells ?? defaultSeedCells()).length);
    return nb;
  }

  test("run-and-advance on the last cell appends a blank cell and focuses it", async ({
    page,
    request,
  }) => {
    await mockKernel(page);
    await seedAndOpen(page, request, "advance-end", defaultSeedCells(), KERNEL_ID);

    const cells = cellRoots(page);
    await cells.nth(3).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells).toHaveCount(5);
    await expectRanCleanly(cells.nth(3));

    const ids = await cellIds(page);
    expect(ids.slice(0, 4)).toEqual(CELL_IDS);
    await expectEditorFocused(page, ids[4]);
  });

  test("run-and-advance on a middle cell focuses the next editor", async ({ page, request }) => {
    await mockKernel(page);
    await seedAndOpen(page, request, "advance-mid", defaultSeedCells(), KERNEL_ID);

    const cells = cellRoots(page);
    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells).toHaveCount(4);
    await expectRanCleanly(cells.nth(0));
    await expectEditorFocused(page, CELL_IDS[1]);
  });

  test("a failed run still advances", async ({ page, request }) => {
    await mockKernel(page, { failWith: RUN_FAILURE });
    await seedAndOpen(page, request, "fail-mid", defaultSeedCells(), KERNEL_ID);

    const cells = cellRoots(page);
    await cells.nth(1).locator(".cm-content").click();
    await expectEditorFocused(page, CELL_IDS[1]);
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells.nth(1).locator(".cell-output .output-error")).toContainText(RUN_FAILURE);
    await expect(cells).toHaveCount(4);
    await expectEditorFocused(page, CELL_IDS[2]);
  });

  test("a failed run on the last cell still appends a blank cell", async ({ page, request }) => {
    await mockKernel(page, { failWith: RUN_FAILURE });
    await seedAndOpen(page, request, "fail-end", defaultSeedCells(), KERNEL_ID);

    const cells = cellRoots(page);
    await cells.nth(3).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells.nth(3).locator(".cell-output .output-error")).toContainText(RUN_FAILURE);
    await expect(cells).toHaveCount(5);
    const ids = await cellIds(page);
    expect(ids.slice(0, 4)).toEqual(CELL_IDS);
    await expectEditorFocused(page, ids[4]);
  });

  test("no kernel: run-and-advance still advances", async ({ page, request }) => {
    // No kernel fixture at all — the store synthesises its own failure.
    await seedAndOpen(page, request, "no-kernel");

    const cells = cellRoots(page);
    await cells.nth(3).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells.nth(3).locator(".cell-output .output-error")).toContainText(
      "No kernel selected",
    );
    await expect(cells).toHaveCount(5);
    const ids = await cellIds(page);
    expect(ids.slice(0, 4)).toEqual(CELL_IDS);
    await expectEditorFocused(page, ids[4]);
  });

  test("run in place keeps the caret in the cell", async ({ page, request }) => {
    await mockKernel(page);
    await seedAndOpen(page, request, "run-in-place", defaultSeedCells(), KERNEL_ID);

    const cells = cellRoots(page);
    await cells.nth(3).locator(".cm-content").click();
    await page.keyboard.press(RUN_IN_PLACE);

    await expectRanCleanly(cells.nth(3));
    await expectStayedPut(page, CELL_IDS[3], 4);
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
    await mockKernel(page);
    await seedAndOpen(page, request, "advance-md-pass", seedCells, KERNEL_ID);

    const cells = cellRoots(page);
    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);
    await expectRanCleanly(cells.nth(0));

    // Command mode: the advance lands on the rendered cell's root, not in an editor.
    await expect
      .poll(() => focusedCell(page), { timeout: 10000 })
      .toEqual({ cellId: "e2e-pass-1", inEditor: false });
    await expect(cells.nth(1).locator(".nb-md-rendered")).toBeVisible();

    // From there Shift+Enter must keep advancing, not un-render the cell.
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
    await mockKernel(page);
    await seedAndOpen(page, request, "md-command-mode", seedCells, KERNEL_ID);

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
    await mockKernel(page);
    await seedAndOpen(page, request, "advance-collapsed", defaultSeedCells(), KERNEL_ID);

    const cells = cellRoots(page);
    await cellMenuAction(page, cells, 1, "toggle-code");
    await expect(cells.nth(1).locator("button.nb-cell-collapsed")).toBeVisible();

    await cells.nth(0).locator(".cm-content").click();
    await page.keyboard.press(RUN_ADVANCE);
    await expectRanCleanly(cells.nth(0));

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
    await mockKernel(page);
    const notebookB = await createNotebook(request, authToken, "twin-b", twinCells("B"), KERNEL_ID);
    seeded.push(notebookB.id);
    await seedAndOpen(page, request, "twin-a", twinCells("A"), KERNEL_ID);

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

test.describe("Catalog notebook — outdated outputs and execution identity", () => {
  let authToken: string;
  const seeded: number[] = [];

  const PAIR_IDS = ["e2e-run-0", "e2e-run-1"];
  const pairCells = (label: string): SeedCell[] =>
    PAIR_IDS.map((id, i) => ({ id, type: "python", source: `# ${label} ${i}` }));

  // Two frames sharing a column name with different dtypes — the case a single flat
  // column list used to collapse onto the first source's type.
  const ORDERS_FRAME: MockFrame = {
    name: "orders",
    columns: [
      { name: "id", dtype: "Int64" },
      { name: "amount", dtype: "Float64" },
    ],
  };
  const CUSTOMERS_FRAME: MockFrame = {
    name: "customers",
    columns: [{ name: "id", dtype: "String" }],
  };
  const SALES_TABLE: MockCatalogTable = {
    name: "sales",
    columns: [
      { name: "amount", dtype: "Float64" },
      { name: "city", dtype: "String" },
    ],
  };
  const SALES_COLUMNS = ["amount", "city", "total"];

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
    cells: SeedCell[] = pairCells(label),
  ): Promise<SeededNotebook> {
    const nb = await createNotebook(request, authToken, label, cells, KERNEL_ID);
    seeded.push(nb.id);
    await navigateWithAuth(page, authToken, NOTEBOOK_URL);
    await openNotebookByName(page, nb.name, cells.length);
    return nb;
  }

  /** Replace a cell's code and leave the caret in its editor. */
  async function retypeCell(page: Page, cells: Locator, index: number, text: string) {
    await cells.nth(index).locator(".cm-content").click();
    await page.keyboard.press(SELECT_ALL);
    await page.keyboard.type(text);
  }

  test("an edit during a slow run keeps the result and marks it outdated", async ({
    page,
    request,
  }) => {
    const kernel = await mockKernel(page);
    await seedAndOpen(page, request, "editrun");

    const cells = cellRoots(page);
    await retypeCell(page, cells, 0, "# before the run");

    const hold = kernel.hold();
    await page.keyboard.press(RUN_IN_PLACE);
    await hold.waitForRequests(1);

    // The caret never left the editor, so this lands while the request is still out.
    await page.keyboard.type(" # edited mid-run");
    hold.release();

    await expectRanCleanly(cells.nth(0));
    await expectStale(cells.nth(0), "code-changed", STALE_CODE);
  });

  test("editing a cell outdates its result and every later one; rerunning clears only its own", async ({
    page,
    request,
  }) => {
    await mockKernel(page);
    await seedAndOpen(page, request, "chain");

    const cells = cellRoots(page);
    await cells.nth(0).locator("button.nb-run").click();
    await expectRanCleanly(cells.nth(0));
    await cells.nth(1).locator("button.nb-run").click();
    await expectRanCleanly(cells.nth(1));
    await expect(page.locator(".nb-cells .nb-status-badge")).toHaveCount(0);

    await retypeCell(page, cells, 0, "# edited A");
    await expectStale(cells.nth(0), "code-changed", STALE_CODE);
    await expectStale(cells.nth(1), "upstream-changed", STALE_UPSTREAM);

    await cells.nth(0).locator("button.nb-run").click();
    await expect(staleBadge(cells.nth(0))).toHaveCount(0, { timeout: 15000 });
    await expectStale(cells.nth(1), "upstream-changed", STALE_UPSTREAM);
  });

  test("Run All finishes in the notebook it started in after a tab switch", async ({
    page,
    request,
  }) => {
    const kernel = await mockKernel(page);
    const otherIds = ["e2e-other-0", "e2e-other-1"];
    const otherCells: SeedCell[] = otherIds.map((id, i) => ({
      id,
      type: "python",
      source: `# other ${i}`,
    }));
    // Both notebooks exist before the page loads: the "+" list is fetched once on mount.
    const other = await createNotebook(request, authToken, "batch-other", otherCells, KERNEL_ID);
    seeded.push(other.id);
    const origin = await seedAndOpen(page, request, "batch-origin");

    await openSavedNotebook(page, other.name);
    await switchNotebookTab(page, origin.name);
    await expectCellIds(page, PAIR_IDS);

    const hold = kernel.hold();
    await page.locator(".nb-run-all").click();
    await hold.waitForRequests(1);

    await switchNotebookTab(page, other.name);
    await expectCellIds(page, otherIds);
    hold.release();

    await expect.poll(() => kernel.executeCount(), { timeout: 20000 }).toBe(PAIR_IDS.length);
    await page.waitForTimeout(500);
    // The tab in view was never the batch's target.
    await expect(page.locator(".nb-cells .cell-output")).toHaveCount(0);

    await switchNotebookTab(page, origin.name);
    await expectRanCleanly(cellRoots(page).nth(0));
    await expectRanCleanly(cellRoots(page).nth(1));
    await expect(page.locator(".nb-batch-progress")).toHaveCount(0);
    await expect(page.locator(".nb-run-all")).toBeEnabled();
  });

  test("two Run All clicks in one tick run the notebook once", async ({ page, request }) => {
    const kernel = await mockKernel(page, { delayMs: 400 });
    await seedAndOpen(page, request, "dupe");

    // dispatchEvent, not click: a second real click would wait for the button to re-enable.
    const runAll = page.locator(".nb-run-all");
    await runAll.dispatchEvent("click");
    await runAll.dispatchEvent("click");

    const cells = cellRoots(page);
    await expectRanCleanly(cells.nth(0));
    await expectRanCleanly(cells.nth(1));
    await expect(page.locator(".nb-batch-progress")).toHaveCount(0, { timeout: 20000 });
    await page.waitForTimeout(600);
    expect(kernel.executeCount()).toBe(PAIR_IDS.length);
  });

  test("Reset session clears outputs and their stale marks", async ({ page, request }) => {
    await mockKernel(page);
    await seedAndOpen(page, request, "reset");

    const cells = cellRoots(page);
    await cells.nth(0).locator("button.nb-run").click();
    await expectRanCleanly(cells.nth(0));
    await retypeCell(page, cells, 0, "# edited after running");
    await expectStale(cells.nth(0), "code-changed", STALE_CODE);

    await page.locator("button[title='More actions']").click();
    await page
      .locator("li.el-dropdown-menu__item:visible")
      .filter({ hasText: "Reset session" })
      .click();

    await expect(page.locator(".el-message--success")).toContainText("Session reset");
    await expect(page.locator(".nb-cells .cell-output")).toHaveCount(0);
    await expect(page.locator(".nb-cells .nb-status-badge")).toHaveCount(0);
  });

  // Reset session is disabled while a batch runs, so the in-flight half of that rule is
  // reached through the other epoch bump the plan names: changing the kernel.
  test("a response in flight when the session changes cannot revive the result", async ({
    page,
    request,
  }) => {
    const kernel = await mockKernel(page, { twoKernels: true });
    await seedAndOpen(page, request, "inflight");

    const cells = cellRoots(page);
    await cells.nth(0).locator("button.nb-run").click();
    await expectRanCleanly(cells.nth(0));

    const hold = kernel.hold();
    await cells.nth(0).locator("button.nb-run").click();
    await hold.waitForRequests(2);

    await page.locator(".nb-kernel-select").click();
    await page
      .locator(".el-select-dropdown__item:visible")
      .filter({ hasText: KERNEL_NAME_2 })
      .click();
    await expectStale(cells.nth(0), "previous-session", STALE_PREVIOUS);

    hold.release();
    await page.waitForTimeout(1000);
    // Applying the answer would have cleared the mark; discarding it leaves it standing.
    await expectStale(cells.nth(0), "previous-session", STALE_PREVIOUS);
  });

  test("a running batch disables reordering and the structural cell actions", async ({
    page,
    request,
  }) => {
    const kernel = await mockKernel(page);
    await seedAndOpen(page, request, "busy");

    const cells = cellRoots(page);
    const hold = kernel.hold();
    await page.locator(".nb-run-all").click();
    await hold.waitForRequests(1);

    await expect(page.locator(".nb-batch-progress")).toHaveText("Running cell 1 of 2");
    await expect(cells.nth(0).locator("button.nb-drag-handle")).toBeDisabled();
    await expect(page.locator("button.nb-add-btn")).toBeDisabled();

    await openCellMenu(page, cells, 1);
    for (const action of ["insert-above", "insert-below", "duplicate", "delete"]) {
      await expect(menuItem(page, action)).toHaveAttribute("aria-disabled", "true");
    }
    await cells.nth(1).locator("button.nb-cell-menu").click();
    await expect(menuItem(page, "toggle-code")).toBeHidden();

    hold.release();
    await expect(page.locator(".nb-batch-progress")).toHaveCount(0, { timeout: 20000 });
    await expect(page.locator(".nb-run-all")).toBeEnabled();
    await expect(cells.nth(0).locator("button.nb-drag-handle")).toBeEnabled();
  });

  test("each run frame keeps its own dtype for the same column name", async ({ page, request }) => {
    const kernel = await mockKernel(page, { dataframes: [ORDERS_FRAME, CUSTOMERS_FRAME] });
    await seedAndOpen(page, request, "runframes");

    const cells = cellRoots(page);
    await cells.nth(0).locator("button.nb-run").click();
    await expectRanCleanly(cells.nth(0));
    // The frames are fetched when the run settles, never while typing.
    await expect.poll(() => kernel.schemaCount(), { timeout: 20000 }).toBeGreaterThan(0);
    await page.waitForTimeout(500);

    await typeForCompletions(page, cells, 1, 'orders.select("');
    await expectColumnRows(page, ["id"], ["id → Int64 · orders (last run)"]);

    await page.keyboard.press("Escape");
    await expect(page.locator(".cm-tooltip-autocomplete")).toHaveCount(0);

    await typeForCompletions(page, cells, 1, 'customers.select("');
    await expectColumnRows(page, ["id"], ["id → String · customers (last run)"]);
  });

  test("a catalog-backed rename offers the new name without running anything", async ({
    page,
    request,
  }) => {
    const kernel = await mockKernel(page);
    await mockCatalogResolve(page, SALES_TABLE);
    await seedAndOpen(page, request, "catalogstatic");

    const cells = cellRoots(page);
    await typeForCompletions(page, cells, 0, 'lf = flowfile_ctx.read_catalog_table("sales")');
    await page.keyboard.press("Escape");

    const schemasBefore = kernel.schemaCount();
    await typeForCompletions(page, cells, 1, 'lf.rename({"amount": "total"}).select("');
    await expectColumnRows(page, SALES_COLUMNS, ["total → Float64 · lf", "city → String · lf"]);

    // Catalog metadata alone got there: nothing executed, no kernel metadata was fetched.
    expect(kernel.executeCount()).toBe(0);
    expect(kernel.schemaCount()).toBe(schemasBefore);
  });

  test("an unsupported transform and a bare pl.col offer nothing on a catalog notebook", async ({
    page,
    request,
  }) => {
    await mockKernel(page);
    await mockCatalogResolve(page, SALES_TABLE);
    const joinCells: SeedCell[] = [
      {
        id: "e2e-join-0",
        type: "python",
        source: 'lf = flowfile_ctx.read_catalog_table("sales")\nx = lf.join(lf, on="city")',
      },
      { id: "e2e-join-1", type: "python", source: "" },
    ];
    await seedAndOpen(page, request, "unsupported", joinCells);

    const cells = cellRoots(page);
    // The receiver of the join does resolve, so the silence below is about the join alone.
    await typeForCompletions(page, cells, 1, 'lf.select("');
    await expectColumnRows(page, SALES_COLUMNS, ["amount → Float64 · lf", "city → String · lf"]);

    await page.keyboard.press("Escape");
    await typeForCompletions(page, cells, 1, 'x.select("');
    await expectNoColumnRows(page, SALES_COLUMNS);

    await page.keyboard.press("Escape");
    await typeForCompletions(page, cells, 1, 'pl.col("');
    await expectNoColumnRows(page, SALES_COLUMNS);
  });

  test("typing inside a column string never fetches kernel metadata", async ({ page, request }) => {
    const kernel = await mockKernel(page, { dataframes: [ORDERS_FRAME] });
    await seedAndOpen(page, request, "nofetch");

    const cells = cellRoots(page);
    await cells.nth(0).locator("button.nb-run").click();
    await expectRanCleanly(cells.nth(0));
    await expect.poll(() => kernel.schemaCount(), { timeout: 20000 }).toBeGreaterThan(0);
    await page.waitForTimeout(500);

    await typeForCompletions(page, cells, 1, 'orders.select("');
    const before = kernel.schemaCount();
    await page.keyboard.type("abcdefghijklmnopqrstuvwxyz0123");
    await page.waitForTimeout(800);
    expect(kernel.schemaCount()).toBe(before);
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
  const NODE_IDS = { advance: 1, duplicate: 2, collapse: 3, fail: 4, overlap: 5 };
  const NODE_Y = 220;
  const nodeX = (nodeId: number) => 240 + (nodeId - 1) * 320;
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
    for (const nodeId of Object.values(NODE_IDS)) {
      const added = await request.post(
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=${nodeId}&node_type=python_script&pos_x=${nodeX(nodeId)}&pos_y=${NODE_Y}`,
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

  /** Persist the kernel on the node itself — the drawer reads it back from the settings. */
  async function seedNodeKernel(request: APIRequestContext, nodeId: number) {
    const response = await request.post(`${API_URL}/update_settings/`, {
      headers: { Authorization: `Bearer ${authToken}` },
      params: { node_type: "python_script" },
      data: {
        flow_id: flowId,
        node_id: nodeId,
        pos_x: nodeX(nodeId),
        pos_y: NODE_Y,
        depending_on_ids: [],
        cache_results: false,
        output_names: ["main"],
        python_script_input: {
          code: "",
          kernel_id: KERNEL_ID,
          cells: [{ id: `node-cell-${nodeId}`, code: "" }],
        },
      },
    });
    if (!response.ok()) throw new Error(`seed node kernel failed: ${response.status()}`);
  }

  async function typeIntoCell(page: Page, cells: Locator, index: number, text: string) {
    await cells.nth(index).locator(".cm-content").click();
    await page.keyboard.press(SELECT_ALL);
    await page.keyboard.type(text);
  }

  test("run-and-advance on the last cell appends and focuses a new cell", async ({
    page,
    request,
  }) => {
    await mockKernel(page);
    await seedNodeKernel(request, NODE_IDS.advance);
    await openNodeNotebook(page, NODE_IDS.advance);

    const cells = cellRoots(page, NODE_HOST);
    await typeIntoCell(page, cells, 0, "# only cell");
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells).toHaveCount(2);
    await expectRanCleanly(cells.nth(0));
    const ids = await cellIds(page, NODE_HOST);
    await expectEditorFocused(page, ids[1]);
    // The appended cell is blank, so its editor shows only the placeholder.
    expect(await cellTexts(page, NODE_HOST)).toEqual(["# only cell", "# Enter code..."]);
  });

  test("a failed run still appends and advances", async ({ page, request }) => {
    await mockKernel(page, { failWith: RUN_FAILURE });
    await seedNodeKernel(request, NODE_IDS.fail);
    await openNodeNotebook(page, NODE_IDS.fail);

    const cells = cellRoots(page, NODE_HOST);
    await typeIntoCell(page, cells, 0, "# boom");
    await page.keyboard.press(RUN_ADVANCE);

    await expect(cells.nth(0).locator(".cell-output .output-error")).toContainText(RUN_FAILURE);
    await expect(cells).toHaveCount(2);
    const ids = await cellIds(page, NODE_HOST);
    await expectEditorFocused(page, ids[1]);
  });

  test("run-and-advance while a cell is running neither advances nor runs", async ({
    page,
    request,
  }) => {
    await mockKernel(page, { delayMs: 1500 });
    await seedNodeKernel(request, NODE_IDS.overlap);
    await openNodeNotebook(page, NODE_IDS.overlap);

    const cells = cellRoots(page, NODE_HOST);
    await typeIntoCell(page, cells, 0, "# slow");
    await page.keyboard.press(RUN_ADVANCE);

    // The advance lands immediately; the first cell is still in flight.
    await expect(cells).toHaveCount(2);
    const ids = await cellIds(page, NODE_HOST);
    await expectEditorFocused(page, ids[1]);
    const running = cells.nth(0).locator(".cell-executing");
    await expect(running).toBeVisible();

    // A second submit on the freshly focused cell must not start an overlapping run.
    await page.keyboard.type("# second");
    await page.keyboard.press(RUN_ADVANCE);
    await expectStayedPut(page, ids[1], 2, NODE_HOST);
    await expect(running).toBeVisible();

    await expectRanCleanly(cells.nth(0));
    await expect(running).toHaveCount(0);
    await expect(cells.nth(1).locator(".cell-output")).toHaveCount(0);
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

test.describe("Python Script node notebook — outdated outputs", () => {
  test.use({ viewport: { width: 1600, height: 1100 } });

  const NODE_HOST = ".notebook-cells";
  // One node per test: the drawer's cells are per-node state that outlives a reload.
  const NODE_IDS = { stale: 1, reset: 2, columns: 3 };
  const NODE_Y = 220;
  const nodeX = (nodeId: number) => 240 + (nodeId - 1) * 320;
  let authToken: string;
  let flowId: number;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    const created = await request.post(
      `${API_URL}/editor/create_flow/?name=NotebookNodeStale_E2E_${Date.now()}&register_in_catalog=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    if (!created.ok()) throw new Error(`create_flow failed: ${created.status()}`);
    flowId = await created.json();
    for (const nodeId of Object.values(NODE_IDS)) {
      const added = await request.post(
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=${nodeId}&node_type=python_script&pos_x=${nodeX(nodeId)}&pos_y=${NODE_Y}`,
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

  /** Persist the kernel on the node itself — the drawer reads it back from the settings. */
  async function seedNodeKernel(request: APIRequestContext, nodeId: number) {
    const response = await request.post(`${API_URL}/update_settings/`, {
      headers: { Authorization: `Bearer ${authToken}` },
      params: { node_type: "python_script" },
      data: {
        flow_id: flowId,
        node_id: nodeId,
        pos_x: nodeX(nodeId),
        pos_y: NODE_Y,
        depending_on_ids: [],
        cache_results: false,
        output_names: ["main"],
        python_script_input: {
          code: "",
          kernel_id: KERNEL_ID,
          cells: [{ id: `stale-cell-${nodeId}`, code: "" }],
        },
      },
    });
    if (!response.ok()) throw new Error(`seed node kernel failed: ${response.status()}`);
  }

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

  /** Replace a cell's code and leave the caret in its editor. */
  async function retypeCell(page: Page, cells: Locator, index: number, text: string) {
    await cells.nth(index).locator(".cm-content").click();
    await page.keyboard.press(SELECT_ALL);
    await page.keyboard.type(text);
  }

  test("an edit during a slow run marks the result outdated above its output", async ({
    page,
    request,
  }) => {
    const kernel = await mockKernel(page);
    await seedNodeKernel(request, NODE_IDS.stale);
    await openNodeNotebook(page, NODE_IDS.stale);

    const cells = cellRoots(page, NODE_HOST);
    await retypeCell(page, cells, 0, "# before the run");

    const hold = kernel.hold();
    await page.keyboard.press(RUN_IN_PLACE);
    await hold.waitForRequests(1);

    await page.keyboard.type(" # edited mid-run");
    hold.release();

    await expectRanCleanly(cells.nth(0));
    await expectStale(cells.nth(0), "code-changed", STALE_CODE);

    // The node toolbar is hover-only, so the badge has to sit with the result.
    const badgePrecedesOutput = await cells.nth(0).evaluate((el) => {
      const badge = el.querySelector(".nb-status-badge");
      const output = el.querySelector(".cell-output");
      if (!badge || !output) return false;
      return !!(badge.compareDocumentPosition(output) & Node.DOCUMENT_POSITION_FOLLOWING);
    });
    expect(badgePrecedesOutput).toBe(true);
  });

  test("Reset session clears the node notebook's outputs and stale marks", async ({
    page,
    request,
  }) => {
    await mockKernel(page);
    await seedNodeKernel(request, NODE_IDS.reset);
    await openNodeNotebook(page, NODE_IDS.reset);

    const cells = cellRoots(page, NODE_HOST);
    await retypeCell(page, cells, 0, "# node reset");
    await page.keyboard.press(RUN_IN_PLACE);
    await expectRanCleanly(cells.nth(0));

    await page.keyboard.type(" # edited");
    await expectStale(cells.nth(0), "code-changed", STALE_CODE);

    await page.locator("button[title^='Reset session']").click();
    await expect(page.locator(`${NODE_HOST} .cell-output`)).toHaveCount(0);
    await expect(page.locator(`${NODE_HOST} .nb-status-badge`)).toHaveCount(0);
  });

  test("column completions label every input and scope a read_input receiver", async ({
    page,
    request,
  }) => {
    await mockKernel(page);
    await mockNodeInputs(page, [
      { name: "main", sourceNodeId: 90, columns: [{ name: "id", data_type: "Int64" }] },
      { name: "lookup", sourceNodeId: 91, columns: [{ name: "id", data_type: "String" }] },
    ]);
    await seedNodeKernel(request, NODE_IDS.columns);
    await openNodeNotebook(page, NODE_IDS.columns);

    const cells = cellRoots(page, NODE_HOST);
    // No resolvable receiver: every input's column, one row each, so the dtypes stay apart.
    await typeForCompletions(page, cells, 0, 'pl.col("');
    await expectColumnRows(page, ["id"], ["id → Int64 · input main", "id → String · input lookup"]);

    await page.keyboard.press("Escape");
    await typeForCompletions(page, cells, 0, 'main = flowfile_ctx.read_input("main")');
    await page.keyboard.press("Escape");

    await page.locator("button.add-cell-button").click();
    await expect(cells).toHaveCount(2);

    await typeForCompletions(page, cells, 1, 'main.select("');
    await expectColumnRows(page, ["id"], ["id → Int64 · main"]);
  });
});
