import { test, expect, Page } from "@playwright/test";

import { closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { minimizePalette, openFlow, openNodeSettings } from "./helpers/canvas";
import { buildSalaryFlow, filterSettings, setFilterValue } from "./helpers/flows";

/**
 * A settings drawer left open while another window changes the same node: its save is refused
 * (409 NODE_SETTINGS_CHANGED, the fingerprint the drawer loaded with no longer matches) and the
 * user picks between the draft and the other window's settings. Nothing is overwritten silently.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578, or API_URL)
 *   2. Frontend: `npm run dev:web` (port 8080, or TEST_URL)
 *   3. Run: `npx playwright test tests/drawer-conflict.spec.ts`
 */

const VALUE_INPUT =
  '.node-settings-drawer .filter-field:has(label:text-is("Value")) input.input-field';

/** Open the filter's drawer, type a new threshold, then let another window change the node. */
async function draftOverAForeignChange(page: Page, request: any, token: string, flowId: number) {
  await openNodeSettings(page, "2", VALUE_INPUT);
  const input = page.locator(VALUE_INPUT).first();
  await expect(input).toHaveValue("60000");
  await input.fill("70000");

  // The other window's save reaches this one through the change feed as a canvas reload.
  const reloaded = page.waitForResponse((r) => r.url().includes("/flow_data/v2"));
  await setFilterValue(request, token, flowId, "65000");
  await reloaded;
  await expect(input).toHaveValue("70000");
}

test.describe("Drawer conflict", () => {
  let token: string;
  let flowId: number;

  test.beforeEach(async ({ page, request }) => {
    token = await getAuthToken(request);
    const name = `drawer_conflict_${Date.now()}`;
    flowId = await createFlow(request, token, name);
    await buildSalaryFlow(request, token, flowId);
    await openFlow(page, token, flowId, name);
    await minimizePalette(page);
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("Discard mine closes the drawer and keeps the other window's settings", async ({
    page,
    request,
  }) => {
    await draftOverAForeignChange(page, request, token, flowId);

    await page.locator(".vue-flow__pane").click({ position: { x: 300, y: 500 } });
    await expect(page.locator(".el-message-box__title")).toHaveText("Settings changed in another window");
    await page.getByRole("button", { name: "Discard mine" }).click();

    await expect(page.locator(".node-settings-drawer")).toHaveCount(0);
    expect((await filterSettings(request, token, flowId)).filter_input.basic_filter.value).toBe(
      "65000",
    );
  });

  test("Keep mine overwrites with the draft", async ({ page, request }) => {
    await draftOverAForeignChange(page, request, token, flowId);

    await page.locator(".vue-flow__pane").click({ position: { x: 300, y: 500 } });
    await expect(page.locator(".el-message-box__title")).toHaveText("Settings changed in another window");
    await page.getByRole("button", { name: "Keep mine" }).click();

    await expect(page.locator(".node-settings-drawer")).toHaveCount(0);
    await expect
      .poll(
        async () => (await filterSettings(request, token, flowId)).filter_input.basic_filter.value,
      )
      .toBe("70000");
  });

  test("a drawer nobody else touched saves as before", async ({ page, request }) => {
    await openNodeSettings(page, "2", VALUE_INPUT);
    const input = page.locator(VALUE_INPUT).first();
    await input.fill("72000");
    await page.locator(".vue-flow__pane").click({ position: { x: 300, y: 500 } });
    await expect(page.locator(".node-settings-drawer")).toHaveCount(0);
    await expect
      .poll(
        async () => (await filterSettings(request, token, flowId)).filter_input.basic_filter.value,
      )
      .toBe("72000");
  });
});
