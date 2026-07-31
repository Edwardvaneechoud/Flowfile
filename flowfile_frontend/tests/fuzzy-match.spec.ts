import { test, expect, APIRequestContext, Page } from "@playwright/test";
import * as path from "path";

/**
 * E2E coverage for the Fuzzy Match settings drawer.
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

async function openFuzzySettings(page: Page, nodeId: string) {
  // dispatchEvent rather than dblclick: nodes overlap on the fixture canvas.
  await page.locator(`.vue-flow__node[data-id="${nodeId}"]`).dispatchEvent("dblclick");
  await page.locator(".rule-card").first().waitFor({ timeout: 20000 });
}

test.describe("Fuzzy match settings", () => {
  let authToken: string;
  let flowId: number;
  let fuzzyNodeId: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    flowId = await importFixture(request, authToken);
    fuzzyNodeId = await findNodeId(request, authToken, flowId, "fuzzy_match");
  });

  test("the segmented control swaps the match rules for the column pickers", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openFuzzySettings(page, fuzzyNodeId);

    await expect(page.locator(".rule-card")).toHaveCount(1);
    await expect(page.locator("table.column-list")).toHaveCount(0);

    await page.locator(".fuzzy-tab", { hasText: "Output columns" }).click();
    await expect(page.locator(".rule-card")).toHaveCount(0);
    // One picker per side, left before right — same order as the rule fields.
    await expect(page.locator(".listbox-subtitle")).toHaveText(["Left data", "Right data"]);

    await page.locator(".fuzzy-tab", { hasText: "Match rules" }).click();
    await expect(page.locator(".rule-card")).toHaveCount(1);
  });

  test("a rule pointing at a missing column reports it; an unfinished one does not", async ({
    page,
  }) => {
    await openFlow(page, authToken, "complex-flow");
    await openFuzzySettings(page, fuzzyNodeId);

    const firstRule = page.locator(".rule-card").first();
    await expect(firstRule).not.toHaveClass(/is-invalid/);
    await expect(page.locator(".fuzzy-tab-alert")).toHaveCount(0);

    // A brand-new rule is unfinished, not broken: a nudge, no red.
    await page.locator(".add-rule").click();
    const secondRule = page.locator(".rule-card").nth(1);
    await expect(secondRule).not.toHaveClass(/is-invalid/);
    await expect(secondRule.locator(".rule-hint")).toHaveText("Pick a column on both sides");
    await expect(secondRule.locator(".rule-alert")).toHaveCount(0);
    await expect(page.locator(".fuzzy-tab-alert")).toHaveCount(1);

    // A column that no longer exists upstream is an actual error.
    await firstRule.locator("input.select-box").first().fill("gone_column");
    await page.locator(".tab-hint").click();
    await expect(firstRule).toHaveClass(/is-invalid/);
    await expect(firstRule.locator(".rule-alert")).toContainText("gone_column");
  });

  test("the similarity threshold persists as a number", async ({ page, request }) => {
    // v-model on <input type="range"> hands back a string; the backend field is a
    // float, so the .number modifier is what keeps the saved value a number.
    await openFlow(page, authToken, "complex-flow");
    await openFuzzySettings(page, fuzzyNodeId);

    await page.locator(".threshold-input").first().fill("91");
    await page.locator(".threshold-input").first().blur();

    await page.getByRole("button", { name: "Apply" }).click();
    await expect(page.getByRole("button", { name: "Applied ✓" })).toBeVisible();

    const response = await request.get(
      `${API_URL}/node?flow_id=${flowId}&node_id=${fuzzyNodeId}&get_data=false`,
      { headers: { Authorization: `Bearer ${authToken}` } },
    );
    expect(response.ok()).toBe(true);
    const node = await response.json();
    const mapping = node?.setting_input?.join_input?.join_mapping ?? [];
    expect(mapping.length).toBeGreaterThan(0);
    expect(mapping[0].threshold_score).toBe(91);
  });

  test("out-of-range threshold input is clamped to 0-100", async ({ page }) => {
    await openFlow(page, authToken, "complex-flow");
    await openFuzzySettings(page, fuzzyNodeId);

    const threshold = page.locator(".threshold-input").first();
    await threshold.fill("250");
    await threshold.blur();
    await expect(threshold).toHaveValue("100");

    await threshold.fill("-40");
    await threshold.blur();
    await expect(threshold).toHaveValue("0");
  });
});
