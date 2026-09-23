import { test, expect, APIRequestContext, Page } from "@playwright/test";
import * as path from "path";

/**
 * E2E coverage for the Pivot and Unpivot drawers on the shared column picker:
 * role rows derived from the saved settings, single-holder role swaps, the
 * three drop zones, multi-row removal, and Unpivot's data-type mode.
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

async function openNodeSettings(page: Page, nodeId: string) {
  // dispatchEvent rather than dblclick: nodes overlap on the fixture canvas.
  await page.locator(`.vue-flow__node[data-id="${nodeId}"]`).dispatchEvent("dblclick");
  await page.locator(".picker-column-table tbody tr").first().waitFor({ timeout: 20000 });
}

const columnRows = (page: Page) => page.locator(".picker-column-table tbody tr:not(.is-empty)");
const roleRows = (page: Page) => page.locator(".picker-settings tbody tr");
const roleRow = (page: Page, name: string) =>
  roleRows(page).filter({ has: page.locator(`.picker-field-cell:text-is("${name}")`) });

async function chooseRole(page: Page, name: string, label: string) {
  await roleRow(page, name).locator(".el-select").click();
  await page
    .locator(".el-select-dropdown:visible .el-select-dropdown__item", { hasText: label })
    .click();
}

/**
 * Chromium will not synthesize an HTML5 drag from mouse moves, so drive the
 * event sequence directly with one shared DataTransfer. `fraction` is the
 * horizontal position inside the settings pane (0 = left edge, 1 = right).
 */
const dragColumnTo = (page: Page, columnIndex: number, fraction: number, drop = true) =>
  page.evaluate(
    ({ columnIndex, fraction, drop }) => {
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
      const x = p.left + p.width * fraction;
      const y = p.top + p.height / 2;
      const target = document.elementFromPoint(x, y) ?? pane;
      fire(target, "dragover", x, y);
      if (drop) {
        fire(target, "drop", x, y);
        fire(source, "dragend", 0, 0);
      }
    },
    { columnIndex, fraction, drop },
  );

/** Same event sequence, aimed at the centre of a host-marked drop area. */
const dragColumnToArea = (page: Page, columnIndex: number, zone: string) =>
  page.evaluate(
    ({ columnIndex, zone }) => {
      const source = document.querySelectorAll(".picker-column-table tbody tr")[columnIndex];
      const area = document.querySelector(`[data-drop-zone="${zone}"]`) as HTMLElement;
      const dataTransfer = new DataTransfer();
      const fire = (el: Element, type: string, clientX: number, clientY: number) =>
        el.dispatchEvent(
          new DragEvent(type, { bubbles: true, cancelable: true, dataTransfer, clientX, clientY }),
        );
      const s = source.getBoundingClientRect();
      fire(source, "dragstart", s.left + 10, s.top + s.height / 2);
      const a = area.getBoundingClientRect();
      const x = a.left + a.width / 2;
      const y = a.top + a.height / 2;
      const target = document.elementFromPoint(x, y) ?? area;
      fire(target, "dragover", x, y);
      fire(target, "drop", x, y);
      fire(source, "dragend", 0, 0);
    },
    { columnIndex, zone },
  );

const areaRows = (page: Page, zone: string) => page.locator(`[data-drop-zone="${zone}"] tbody tr`);
/** Row texts include the hover icons' ligature names, so name checks read the field cell. */
const areaFields = (page: Page, zone: string) => areaRows(page, zone).locator(".picker-field-cell");

test.describe("Pivot and Unpivot drawers", () => {
  let authToken: string;
  let flowId: number;
  let pivotNodeId: string;
  let unpivotNodeId: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    flowId = await importFixture(request, authToken);
    pivotNodeId = await findNodeId(request, authToken, flowId, "pivot");
    unpivotNodeId = await findNodeId(request, authToken, flowId, "unpivot");
  });

  test("pivot shows the saved roles and swaps single-holder roles on reassignment", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, pivotNodeId);

    await expect(roleRows(page)).toHaveCount(3);
    await expect(page.locator(".picker-settings-count")).toHaveText(
      "1 index key · pivot quarter · value sales_data",
    );
    await expect(columnRows(page).filter({ hasText: "Country" }).locator(".usage-chip")).toHaveText(
      "index",
    );

    await chooseRole(page, "quarter", "Value column");
    await expect(roleRow(page, "quarter").locator(".el-select")).toContainText("Value column");
    await expect(roleRow(page, "sales_data").locator(".el-select")).toContainText("Pivot column");
    await expect(page.locator(".picker-settings-count")).toHaveText(
      "1 index key · pivot sales_data · value quarter",
    );
    await expect(page.locator(".picker-flag")).toHaveCount(0);
  });

  test("pivot drops on the index band, flags what is missing, and greys single zones for a multi-drag", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, pivotNodeId);

    const rows = columnRows(page);
    const valueIndex = await rows.evaluateAll((els) =>
      els.findIndex((el) => el.querySelector(".column-name")?.textContent?.trim() === "sales_data"),
    );
    expect(valueIndex).toBeGreaterThanOrEqual(0);

    await dragColumnTo(page, valueIndex, 1 / 6);
    await expect(roleRow(page, "sales_data").locator(".el-select")).toContainText("Index key");
    await expect(page.locator(".picker-flag")).toHaveText("Missing: value column");
    await expect(page.locator(".picker-settings-count")).toHaveText(
      "2 index keys · pivot quarter · value —",
    );

    await rows.nth(0).locator(".column-name-cell").click();
    await rows
      .nth(1)
      .locator(".column-name-cell")
      .click({ modifiers: ["Shift"] });
    await dragColumnTo(page, 0, 0.5, false);
    await expect(page.locator(".drop-pill")).toHaveCount(3);
    await expect(page.locator(".drop-pill.is-disabled")).toHaveCount(2);
    await page.evaluate(() => {
      document
        .querySelector(".picker-column-table tbody tr")!
        .dispatchEvent(new DragEvent("dragend", { bubbles: true }));
    });
    await expect(page.locator(".drop-pill")).toHaveCount(0);
  });

  test("unpivot keeps index keys and unpivot columns in separate areas, removes and drops per area", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, unpivotNodeId);

    await expect(roleRows(page)).toHaveCount(4);
    await expect(areaRows(page, "index")).toHaveCount(1);
    await expect(areaRows(page, "value")).toHaveCount(3);
    await expect(page.locator(".picker-settings-count")).toHaveText(
      "1 index key · 3 columns to unpivot",
    );

    await roleRows(page).nth(1).locator(".picker-field-cell").click();
    await roleRows(page)
      .nth(3)
      .locator(".picker-field-cell")
      .click({ modifiers: ["Shift"] });
    await expect(page.locator(".picker-row-selection .column-list-selection-count")).toHaveText(
      "3 selected",
    );
    await page.locator(".picker-row-selection").getByRole("button", { name: "Remove" }).click();

    await expect(roleRows(page)).toHaveCount(1);
    await expect(areaRows(page, "value")).toHaveCount(0);
    await expect(page.locator(".picker-flag")).toHaveText("Missing: value columns");

    await columnRows(page)
      .filter({ hasText: "first_quarter" })
      .getByRole("button", { name: "Unpivot first_quarter" })
      .click();
    await expect(areaFields(page, "value")).toHaveText(["first_quarter"]);
    await expect(areaRows(page, "value").first()).toHaveClass(/is-new/);

    const rows = columnRows(page);
    const secondIndex = await rows.evaluateAll((els) =>
      els.findIndex(
        (el) => el.querySelector(".column-name")?.textContent?.trim() === "second_quarter",
      ),
    );
    await areaRows(page, "value").first().locator(".picker-field-cell").click();
    await dragColumnToArea(page, secondIndex, "index");
    await expect(areaFields(page, "index")).toHaveText(["Country", "second_quarter"]);
    // The moved row is the one that flashes, and a selection made before the move is dropped.
    await expect(areaRows(page, "index").filter({ hasText: "second_quarter" })).toHaveClass(
      /is-new/,
    );
    await expect(page.locator(".picker-row-selection")).toHaveCount(0);

    await areaRows(page, "index")
      .filter({ hasText: "second_quarter" })
      .getByRole("button", { name: "Unpivot second_quarter instead" })
      .click();
    await expect(areaFields(page, "value")).toHaveText(["first_quarter", "second_quarter"]);
    await expect(page.locator(".drop-pill")).toHaveCount(0);
  });

  test("unpivot's data-type mode hides the value rows and survives Apply", async ({
    page,
    request,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openNodeSettings(page, unpivotNodeId);

    await page.getByRole("button", { name: "By type" }).click();
    await expect(roleRows(page)).toHaveCount(1);
    await expect(page.locator(".unpivot-type-select")).toContainText("All other columns");
    await expect(page.locator(".picker-settings-count")).toHaveText(
      "1 index key · unpivot all other columns",
    );
    await expect(page.locator(".drop-pill")).toHaveCount(0);

    await page.getByRole("button", { name: "Apply" }).click();
    await expect(page.getByRole("button", { name: "Applied ✓" })).toBeVisible();

    const response = await request.get(
      `${API_URL}/node?flow_id=${flowId}&node_id=${unpivotNodeId}&get_data=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    expect(response.ok()).toBe(true);
    expect((await response.json())?.setting_input?.unpivot_input).toMatchObject({
      index_columns: ["Country"],
      value_columns: [],
      data_type_selector: "all",
      data_type_selector_mode: "data_type",
    });
  });
});
