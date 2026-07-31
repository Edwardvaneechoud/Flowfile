// E2E for the data-preview row count + on-demand column stats:
// the status bar shows a real count (never the old 999 sentinel), the ⓘ header
// button opens the stats panel, and the custom header still sorts on click.
//
// Prerequisites (same as web-flow.spec.ts): core on :63578, web server on :8080.
import { test, expect, APIRequestContext, Page } from "@playwright/test";

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  return (await tokenResponse.json()).access_token;
}

async function authPost(request: APIRequestContext, url: string, token: string, data?: unknown) {
  return request.post(url, { headers: { Authorization: `Bearer ${token}` }, data });
}

async function authGet(request: APIRequestContext, url: string, token: string) {
  return request.get(url, { headers: { Authorization: `Bearer ${token}` } });
}

async function navigateWithAuth(page: Page, token: string, targetUrl: string) {
  await page.goto(targetUrl);
  await page.waitForLoadState("networkidle");
  const expiration = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration }: { token: string; expiration: number }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
    },
    { token, expiration },
  );
  await page.reload();
  await page.waitForLoadState("networkidle");
}

/** Creates a flow with one 4-row manual-input node (id 1) and runs it. */
async function createAndRunManualInputFlow(
  request: APIRequestContext,
  token: string,
  flowName: string,
): Promise<number> {
  const createResponse = await authPost(
    request,
    `${API_URL}/editor/create_flow/?name=${flowName}`,
    token,
  );
  expect(createResponse.ok()).toBe(true);
  const flowId = await createResponse.json();

  // pos_x clears the left Data-actions palette so the node is clickable.
  const addNode = await authPost(
    request,
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=600&pos_y=300`,
    token,
  );
  expect(addNode.ok()).toBe(true);

  const settings = await authPost(
    request,
    `${API_URL}/update_settings/?node_type=manual_input`,
    token,
    {
      flow_id: flowId,
      node_id: 1,
      raw_data_format: {
        columns: [
          { name: "name", data_type: "String" },
          { name: "city", data_type: "String" },
          { name: "age", data_type: "Int64" },
        ],
        data: [
          ["John", "Jane", "Edward", "Courtney"],
          ["New York", "Los Angeles", "Chicago", "Chicago"],
          [40, 30, 20, 30],
        ],
      },
    },
  );
  expect(settings.ok()).toBe(true);

  // Development mode stores preview samples; Performance mode (a common
  // default) skips them and would leave the dock in fetch-button state.
  const settingsResponse = await authGet(
    request,
    `${API_URL}/flow_settings?flow_id=${flowId}`,
    token,
  );
  expect(settingsResponse.ok()).toBe(true);
  const flowSettings = await settingsResponse.json();
  flowSettings.execution_mode = "Development";
  const settingsUpdate = await authPost(request, `${API_URL}/flow_settings`, token, flowSettings);
  expect(settingsUpdate.ok()).toBe(true);

  const run = await authPost(request, `${API_URL}/flow/run/?flow_id=${flowId}`, token);
  expect(run.ok()).toBe(true);

  await expect
    .poll(
      async () => {
        const status = await authGet(request, `${API_URL}/flow/run_status/?flow_id=${flowId}`, token);
        if (!status.ok()) return "pending";
        const body = await status.json();
        return body.is_running ? "running" : "done";
      },
      { timeout: 30000 },
    )
    .toBe("done");

  return flowId;
}

/** Opens the bottom-dock data preview for node 1 via its right-click menu.
 * Fits the view first — a restored session can leave the canvas panned so the
 * node sits under the left palette, which intercepts the click. */
async function openNodeDataPreview(page: Page, flowName: string) {
  await page.waitForSelector("main", { timeout: 10000 });
  const flowTab = page.getByText(flowName, { exact: true });
  await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
  await flowTab.first().click();

  const node = page.locator('.vue-flow__node[data-id="1"]');
  await node.waitFor({ state: "attached", timeout: 10000 });

  await page.locator(".vue-flow__pane").click({ button: "right", position: { x: 700, y: 200 } });
  await page.getByText("Fit View", { exact: true }).click();
  await node.waitFor({ state: "visible", timeout: 10000 });

  await node.click({ button: "right" });
  await page.getByText("View Data", { exact: true }).click();
}

test.describe("Data preview stats", () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test("status bar shows the real row count, ⓘ opens stats, sorting still works", async ({
    page,
    request,
  }) => {
    const flowName = `Preview_Stats_${Date.now()}`;
    const flowId = await createAndRunManualInputFlow(request, authToken, flowName);

    try {
      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await openNodeDataPreview(page, flowName);

      // Real count — 4 rows, 3 columns; never the old 999 sentinel or "?".
      const statusBar = page.locator(".dp-status-bar");
      await statusBar.waitFor({ state: "visible", timeout: 15000 });
      await expect(statusBar).toHaveText(/showing 4 of 4 rows · 3 columns/);

      // Headers carry the data-type pill from the preview schema.
      await expect(page.locator(".dp-col-header__dtype").first()).toHaveText("String");

      // The ⓘ button opens the stats panel with computed values.
      const infoButton = page.locator(".dp-col-header__info").first();
      await infoButton.waitFor({ state: "visible", timeout: 10000 });
      await infoButton.click();

      const panel = page.locator(".column-stats-panel");
      await panel.waitFor({ state: "visible", timeout: 10000 });
      await expect(panel).toContainText("name");
      // 4 rows, all filled, all unique names → the unique-key badge shows.
      await expect(panel.locator(".meta-value").first()).toHaveText("4", { timeout: 10000 });
      await expect(panel).toContainText("Unique key");

      // Escape dismisses the panel.
      await page.keyboard.press("Escape");
      await expect(panel).toHaveCount(0);

      // A numeric column additionally shows the average.
      await page.locator(".dp-col-header__info").nth(2).click();
      await panel.waitFor({ state: "visible", timeout: 10000 });
      await expect(panel).toContainText("Avg");
      await expect(panel).toContainText("30");
      await page.keyboard.press("Escape");
      await expect(panel).toHaveCount(0);

      // Clicking the header label still sorts (the custom header must wire
      // progressSort by hand — this catches it silently dying). Target the
      // label: the container's center can land on the ⓘ button instead.
      const firstHeader = page.locator(".dp-col-header__label").first();
      await firstHeader.click();
      await expect(page.locator(".dp-col-header__icon").first()).toBeVisible();
      // AG Grid positions rows via transforms — DOM order is not visual order,
      // so target the row AG Grid labels as visually first.
      const firstRow = page.locator('.ag-center-cols-container .ag-row[row-index="0"]');
      await expect(firstRow).toContainText("Courtney");
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("a slow stats request opens the panel once, and it never moves", async ({
    page,
    request,
  }) => {
    const flowName = `Preview_Stats_Slow_${Date.now()}`;
    const flowId = await createAndRunManualInputFlow(request, authToken, flowName);

    try {
      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);

      // Force the grace window open so there is a window to observe at all.
      await page.route("**/node/column_stats*", async (route) => {
        await new Promise((resolve) => setTimeout(resolve, 600));
        await route.continue();
      });

      await openNodeDataPreview(page, flowName);

      const infoButton = page.locator(".dp-col-header__info").first();
      await infoButton.waitFor({ state: "visible", timeout: 15000 });
      const anchorBox = (await infoButton.boundingBox())!;
      await infoButton.click();

      // The popover is not mounted synchronously — it waits out the grace window.
      const panel = page.locator(".column-stats-panel");
      await expect(panel).toHaveCount(0);

      // Once it does appear, the skeleton→stats swap must not move it: placement
      // is derived from the anchor alone and pins the edge touching the ⓘ, so
      // only the far edge travels as the body grows.
      await panel.waitFor({ state: "visible", timeout: 10000 });
      const before = (await panel.boundingBox())!;
      const droppedBelow = before.y > anchorBox.y;
      const pinnedEdge = (box: { y: number; height: number }) =>
        droppedBelow ? box.y : box.y + box.height;

      await expect(panel).toContainText("Unique key");
      const after = (await panel.boundingBox())!;
      expect(after.x).toBe(before.x);
      expect(pinnedEdge(after)).toBe(pinnedEdge(before));
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("stats panel explains itself for a node that has not run", async ({ page, request }) => {
    const flowName = `Preview_Stats_NotRun_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      await authPost(
        request,
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=600&pos_y=300`,
        authToken,
      );
      await authPost(request, `${API_URL}/update_settings/?node_type=manual_input`, authToken, {
        flow_id: flowId,
        node_id: 1,
        raw_data_format: {
          columns: [{ name: "name", data_type: "String" }],
          data: [["John"]],
        },
      });
      // New flows can default to Performance mode, which blocks stats with its
      // own message — this test is about the not-run reason.
      const settingsResponse = await authGet(
        request,
        `${API_URL}/flow_settings?flow_id=${flowId}`,
        authToken,
      );
      const flowSettings = await settingsResponse.json();
      flowSettings.execution_mode = "Development";
      await authPost(request, `${API_URL}/flow_settings`, authToken, flowSettings);

      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await openNodeDataPreview(page, flowName);

      const infoButton = page.locator(".dp-col-header__info").first();
      await infoButton.waitFor({ state: "visible", timeout: 15000 });
      await infoButton.click();

      const panel = page.locator(".column-stats-panel");
      await panel.waitFor({ state: "visible", timeout: 10000 });
      // The panel surfaces the server's 409 reason verbatim.
      await expect(panel).toContainText("Run the flow first.");
      // And it opened straight into that state: the 409 beat the grace window, so
      // the loading skeleton was never mounted to flash.
      await expect(panel.locator(".csp-skeleton")).toHaveCount(0);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });
});
