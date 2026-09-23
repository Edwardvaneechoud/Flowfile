import { test, expect, APIRequestContext, Page } from "@playwright/test";
import * as path from "path";

/**
 * E2E coverage for the Group By drawer: the two-pane layout that keeps the
 * settings on screen under a long column list, the explicit add actions, the
 * drop zones, the drag auto-scroll, the sash / fold that resize the split, and
 * multi-select removal in the settings table.
 *
 * Prerequisites: flowfile_core on :63578 and a web server on :8080.
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";
const COMPLEX_FLOW_FIXTURE = path.resolve(__dirname, "fixtures/complex-flow.yaml");
/** Short enough that the old single-column layout pushed the settings below the fold. */
const SHORT_VIEWPORT = { width: 1280, height: 620 };
/** Tall enough that the settings pane has room to grow before it hits the column list's floor. */
const TALL_VIEWPORT = { width: 1280, height: 900 };

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
  const minimize = page.locator("#dataActions button[title='Minimize']");
  if (await minimize.count()) await minimize.click();
}

async function openGroupBySettings(page: Page, nodeId: string) {
  // dispatchEvent rather than dblclick: nodes overlap on the fixture canvas.
  await page.locator(`.vue-flow__node[data-id="${nodeId}"]`).dispatchEvent("dblclick");
  await page.locator(".picker-column-table tbody tr").first().waitFor({ timeout: 20000 });
}

const columnRows = (page: Page) => page.locator(".picker-column-table tbody tr:not(.is-empty)");
const aggRows = (page: Page) => page.locator(".group-by-agg-table tbody tr");

/** First input column no settings row uses yet, so adding it must change the row count. */
async function firstUnusedColumn(page: Page) {
  const rows = columnRows(page);
  const count = await rows.count();
  for (let index = 0; index < count; index++) {
    const row = rows.nth(index);
    if ((await row.locator(".usage-chip").count()) > 0) continue;
    const name = (await row.locator(".column-name").innerText()).trim();
    const dataType = (await row.locator(".column-type").innerText()).trim();
    return { row, index, name, dataType };
  }
  throw new Error("every input column is already in use");
}

/**
 * Chromium will not synthesize an HTML5 drag from mouse moves, so drive the
 * event sequence directly with one shared DataTransfer. The left half of the
 * settings pane is the group-by zone, the right half the aggregate zone.
 */
const dragColumnToZone = (page: Page, columnIndex: number, zone: "groupby" | "aggregate") =>
  page.evaluate(
    ({ columnIndex, zone }) => {
      const source = document.querySelectorAll(".picker-column-table tbody tr")[columnIndex];
      const pane = document.querySelector(".picker-settings") as HTMLElement;
      const dataTransfer = new DataTransfer();
      const fire = (el: Element, type: string, clientX: number, clientY: number) =>
        el.dispatchEvent(
          new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer, clientX, clientY }),
        );
      const s = source.getBoundingClientRect();
      fire(source, "dragstart", s.left + 10, s.top + s.height / 2);
      const p = pane.getBoundingClientRect();
      const x = p.left + p.width * (zone === "groupby" ? 0.25 : 0.75);
      const y = p.top + p.height / 2;
      const target = document.elementFromPoint(x, y) ?? pane;
      fire(target, "dragover", x, y);
      fire(target, "drop", x, y);
      fire(source, "dragend", 0, 0);
    },
    { columnIndex, zone },
  );

test.describe("Group By drawer", () => {
  let authToken: string;
  let flowId: number;
  let groupByNodeId: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    flowId = await importFixture(request, authToken);
    groupByNodeId = await findNodeId(request, authToken, flowId, "group_by");
  });

  test("keeps the settings pane on screen under the column list in a short drawer", async ({
    page,
  }) => {
    await page.setViewportSize(SHORT_VIEWPORT);
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const body = (await page.locator(".node-settings-body").boundingBox())!;
    const header = (await page.locator(".picker-settings-header").boundingBox())!;
    expect(header.y).toBeGreaterThanOrEqual(body.y);
    expect(header.y + header.height).toBeLessThanOrEqual(body.y + body.height);

    await expect(columnRows(page).first()).toBeVisible();
    await expect(aggRows(page).first()).toBeVisible();
    await expect(page.locator(".picker-settings-count")).toHaveText(
      /\d+ keys? · \d+ aggregations?/,
    );
  });

  test("+ Group adds a key row, marks the column, and re-adding reveals instead of duplicating", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const before = await aggRows(page).count();
    const { row, name } = await firstUnusedColumn(page);
    const addKey = row.getByRole("button", { name: `Group by ${name}` });

    await addKey.click();
    await expect(aggRows(page)).toHaveCount(before + 1);
    await expect(aggRows(page).last().locator(".agg-field-cell")).toHaveText(name);
    await expect(aggRows(page).last().locator(".inline-input")).toHaveValue(name);
    await expect(row.locator(".usage-chip.is-key")).toHaveText("key");

    await addKey.click();
    await expect(aggRows(page)).toHaveCount(before + 1);
    await expect(aggRows(page).last()).toHaveClass(/is-new/);
  });

  test("the selection toolbar aggregates every selected column at once", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const rows = columnRows(page);
    test.skip((await rows.count()) < 3, "needs at least 3 columns to be meaningful");

    let expectedNew = 0;
    for (let index = 0; index < 3; index++) {
      const chips = await rows.nth(index).locator(".usage-chip").allInnerTexts();
      if (!chips.includes("median")) expectedNew += 1;
    }
    const before = await aggRows(page).count();

    await rows.nth(0).locator(".column-name-cell").click();
    await rows
      .nth(2)
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });
    await expect(page.locator(".column-list-selection-count")).toHaveText("3 selected");

    await page.getByRole("button", { name: "Aggregate ▾" }).click();
    await page.locator(".context-menu li", { hasText: "Median" }).click();

    await expect(aggRows(page)).toHaveCount(before + expectedNew);
    await expect(page.locator(".column-list-selection")).toBeHidden();
  });

  test("dropping a column on the aggregate zone adds a typed default that survives Apply", async ({
    page,
    request,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const before = await aggRows(page).count();
    const { index, name, dataType } = await firstUnusedColumn(page);
    const agg = /^(u?int|float|decimal)/i.test(dataType) ? "sum" : "count";

    await dragColumnToZone(page, index, "aggregate");

    await expect(aggRows(page)).toHaveCount(before + 1);
    const added = aggRows(page).last();
    await expect(added.locator(".agg-field-cell")).toHaveText(name);
    await expect(added.locator(".inline-input")).toHaveValue(`${name}_${agg}`);
    await expect(page.locator(".drop-pill")).toHaveCount(0);

    await page.getByRole("button", { name: "Apply" }).click();
    await expect(page.getByRole("button", { name: "Applied ✓" })).toBeVisible();

    const response = await request.get(
      `${API_URL}/node?flow_id=${flowId}&node_id=${groupByNodeId}&get_data=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    expect(response.ok()).toBe(true);
    const aggCols = (await response.json())?.setting_input?.groupby_input?.agg_cols ?? [];
    expect(aggCols).toContainEqual(
      expect.objectContaining({ old_name: name, agg, new_name: `${name}_${agg}` }),
    );
  });

  test("the column list auto-scrolls while a drag hovers its bottom edge", async ({ page }) => {
    await page.setViewportSize(SHORT_VIEWPORT);
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const scroller = page.locator(".picker-columns .picker-scroll");
    const scrollable = await scroller.evaluate((el) => el.scrollHeight > el.clientHeight + 1);
    test.skip(!scrollable, "the column list fits without scrolling at this size");

    await page.evaluate(() => {
      const source = document.querySelector(".picker-column-table tbody tr")!;
      const scroller = document.querySelector(".picker-columns .picker-scroll")!;
      const dataTransfer = new DataTransfer();
      const s = source.getBoundingClientRect();
      source.dispatchEvent(
        new DragEvent("dragstart", {
          bubbles: true,
          cancelable: true,
          dataTransfer,
          clientX: s.left + 10,
          clientY: s.top + 5,
        }),
      );
      const r = scroller.getBoundingClientRect();
      document.dispatchEvent(
        new DragEvent("dragover", {
          bubbles: true,
          cancelable: true,
          dataTransfer,
          clientX: r.left + 40,
          clientY: r.bottom - 6,
        }),
      );
    });

    await expect.poll(() => scroller.evaluate((el) => el.scrollTop)).toBeGreaterThan(0);

    await page.evaluate(() => {
      document
        .querySelector(".picker-column-table tbody tr")!
        .dispatchEvent(new DragEvent("dragend", { bubbles: true }));
    });
    await expect(page.locator(".drop-pill")).toHaveCount(0);
  });

  test("dragging the Settings strip resizes the split and double-click resets it", async ({
    page,
  }) => {
    await page.setViewportSize(TALL_VIEWPORT);
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const pane = page.locator(".picker-settings");
    const scroller = page.locator(".picker-columns .picker-scroll");
    const scrollable = await scroller.evaluate((el) => el.scrollHeight > el.clientHeight + 1);
    const paneBefore = (await pane.boundingBox())!;
    const scrollerBefore = (await scroller.boundingBox())!;
    // The pin is clamped so the column list keeps its 84px floor.
    const available = await page.locator(".picker-card").evaluate((el) => {
      const style = getComputedStyle(el);
      return el.clientHeight - parseFloat(style.paddingTop) - parseFloat(style.paddingBottom);
    });
    const expected = Math.min(paneBefore.height + 60, available - 84);
    expect(expected).toBeGreaterThan(paneBefore.height + 40);
    const grip = (await page.locator(".picker-sash-grip").boundingBox())!;
    const x = grip.x + grip.width / 2;
    const y = grip.y + grip.height / 2;

    await page.mouse.move(x, y);
    await page.mouse.down();
    await page.mouse.move(x, y - 60, { steps: 6 });
    await expect(pane).toHaveClass(/is-resizing/);
    await page.mouse.up();

    await expect(pane).not.toHaveClass(/is-resizing/);
    await expect(pane).toHaveClass(/is-sized/);
    const paneAfter = (await pane.boundingBox())!;
    expect(Math.abs(paneAfter.height - expected)).toBeLessThan(2);
    if (scrollable) {
      expect((await scroller.boundingBox())!.height).toBeLessThan(scrollerBefore.height - 40);
    }

    await page.locator(".picker-settings-header").dblclick();
    await expect(pane).not.toHaveClass(/is-sized/);
    const paneReset = (await pane.boundingBox())!;
    expect(Math.abs(paneReset.height - paneBefore.height)).toBeLessThan(2);

    // Dragged shut, the pane folds instead of staying open at strip height.
    const gripAgain = (await page.locator(".picker-sash-grip").boundingBox())!;
    await page.mouse.move(gripAgain.x + gripAgain.width / 2, gripAgain.y + gripAgain.height / 2);
    await page.mouse.down();
    await page.mouse.move(gripAgain.x + gripAgain.width / 2, gripAgain.y + 600, { steps: 6 });
    await page.mouse.up();
    await expect(pane).toHaveClass(/is-collapsed/);
    await page.getByRole("button", { name: "Show the settings" }).click();
    await expect(pane).not.toHaveClass(/is-collapsed/);
    expect((await pane.boundingBox())!.height).toBeGreaterThan(100);
  });

  test("a shift-click range while filtering selects only the visible columns", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const rows = columnRows(page);
    const names = await rows.locator(".column-name").allTextContents();
    const letter = "aeiou".split("").find((candidate) => {
      const visible = names.filter((name) => name.toLowerCase().includes(candidate));
      return visible.length >= 2 && visible.length < names.length;
    })!;
    const visible = names.filter((name) => name.toLowerCase().includes(letter));
    await page.getByLabel("Filter columns").fill(letter);
    await expect(rows).toHaveCount(visible.length);
    await rows.first().locator(".column-name-cell").click();
    await rows
      .last()
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });
    await expect(page.locator(".column-list-selection-count")).toHaveText(
      `${visible.length} selected`,
    );
  });

  test("the chevron folds the settings to its strip and adding a column unfolds them", async ({
    page,
  }) => {
    await page.setViewportSize(SHORT_VIEWPORT);
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const pane = page.locator(".picker-settings");
    const scroller = page.locator(".picker-columns .picker-scroll");
    const scrollable = await scroller.evaluate((el) => el.scrollHeight > el.clientHeight + 1);
    const scrollerBefore = (await scroller.boundingBox())!;
    const before = await aggRows(page).count();

    await page.getByRole("button", { name: "Hide the settings" }).click();
    await expect(page.locator(".group-by-agg-table")).toHaveCount(0);
    await expect(page.locator(".picker-settings-count")).toBeVisible();
    expect((await pane.boundingBox())!.height).toBeLessThanOrEqual(40);
    if (scrollable) {
      expect((await scroller.boundingBox())!.height).toBeGreaterThan(scrollerBefore.height + 40);
    }

    const { row, name } = await firstUnusedColumn(page);
    await row.getByRole("button", { name: `Group by ${name}` }).click();

    await expect(aggRows(page)).toHaveCount(before + 1);
    await expect(aggRows(page).last()).toHaveClass(/is-new/);
    await expect(page.getByRole("button", { name: "Hide the settings" })).toBeVisible();
  });

  test("settings rows multi-select and remove via the strip pill, Delete, and the context menu", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openGroupBySettings(page, groupByNodeId);

    const rows = aggRows(page);
    const before = await rows.count();
    const addKey = async () => {
      const { row, name } = await firstUnusedColumn(page);
      await row.getByRole("button", { name: `Group by ${name}` }).click();
    };

    await addKey();
    await addKey();
    await expect(rows).toHaveCount(before + 2);
    await rows.nth(before).locator(".agg-field-cell").click();
    await rows
      .nth(before + 1)
      .locator(".agg-field-cell")
      .click({ modifiers: ["Shift"] });
    await expect(page.locator(".picker-row-selection .column-list-selection-count")).toHaveText(
      "2 selected",
    );
    await page.locator(".picker-row-selection").getByRole("button", { name: "Remove" }).click();
    await expect(rows).toHaveCount(before);
    await expect(page.locator(".picker-row-selection")).toHaveCount(0);

    await addKey();
    await rows.nth(before).locator(".agg-field-cell").click();
    await expect(rows.nth(before)).toHaveClass(/is-selected/);
    await page.keyboard.press("Delete");
    await expect(rows).toHaveCount(before);

    // Backspace removes the row, never the node the canvas has selected.
    await addKey();
    const canvasNode = page.locator(`.vue-flow__node[data-id="${groupByNodeId}"]`);
    // A synthetic click selects the node; a synthetic mousedown would trip d3-drag on `event.view`.
    await canvasNode.dispatchEvent("click", { button: 0 });
    await expect(canvasNode).toHaveClass(/selected/);
    await rows.nth(before).locator(".agg-field-cell").click();
    await page.keyboard.press("Backspace");
    await expect(rows).toHaveCount(before);
    await expect(canvasNode).toHaveCount(1);

    await addKey();
    await rows.nth(before).locator(".agg-field-cell").click({ button: "right" });
    await expect(page.locator(".context-menu")).toHaveCount(1);
    await page.keyboard.press("Escape");
    await expect(page.locator(".context-menu")).toHaveCount(0);
    await rows.nth(before).locator(".agg-field-cell").click({ button: "right" });
    await page.locator(".context-menu li", { hasText: "Remove" }).click();
    await expect(rows).toHaveCount(before);
  });
});
