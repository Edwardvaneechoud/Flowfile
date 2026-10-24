import { test, expect, Page } from "@playwright/test";

import { closeFlow, createFlow, getAuthToken, readNodeSettings } from "./helpers/api";
import { applySettings, openFlow, openNodeSettings } from "./helpers/canvas";
import { postEditor } from "./helpers/flows";

/**
 * The Catalog Reader, Catalog Writer, Write Data and Explore Data drawers render the general
 * settings section: a compute backend picked there is saved with the drawer's own settings and
 * shows again after a reload. Core saves a node's execution_location in both flag states, so
 * nothing skips.
 */
const drawers = [
  {
    title: "Catalog Reader",
    nodeType: "catalog_reader",
    ready: ".mode-toggle",
    saved: (settings: any) => expect(settings.cdc_mode).toBe("off"),
  },
  {
    title: "Catalog Writer",
    nodeType: "catalog_writer",
    ready: ".writer-tabs",
    fill: async (page: Page) => {
      await page.locator("input[placeholder='Enter table name']").fill("general_settings_e2e");
    },
    saved: (settings: any) =>
      expect(settings.catalog_write_settings.table_name).toBe("general_settings_e2e"),
  },
  {
    title: "Write Data",
    nodeType: "output",
    ready: ".file-type-row",
    saved: (settings: any) => expect(settings.output_settings.directory).toBeTruthy(),
  },
  {
    title: "Explore Data",
    nodeType: "explore_data",
    ready: ".explore-data-container",
    saved: (settings: any) =>
      expect(settings.graphic_walker_input?.dataModel?.fields ?? []).toEqual([]),
  },
];

for (const drawer of drawers) {
  test.describe(`General settings in the ${drawer.title} drawer`, () => {
    let token: string;
    let flowId: number;
    let flowName: string;

    test.beforeEach(async ({ request }) => {
      token = await getAuthToken(request);
      flowName = `general_settings_${drawer.nodeType}_${Date.now()}`;
      flowId = await createFlow(request, token, flowName);
      await postEditor(request, token, "/editor/add_node/", {
        flow_id: flowId,
        node_id: 1,
        node_type: drawer.nodeType,
        pos_x: 300,
        pos_y: 200,
      });
    });

    test.afterEach(async ({ request }) => {
      await closeFlow(request, token, flowId);
    });

    test("a compute backend picked there is saved with the node and shows after a reload", async ({
      page,
      request,
    }) => {
      await openFlow(page, token, flowId, flowName);
      await openNodeSettings(page, "1", drawer.ready);
      await drawer.fill?.(page);
      await page.locator("#tab-general").click();
      await page.locator("[data-testid=node-execution-location]").click();
      await page
        .locator(".el-select-dropdown:visible .el-select-dropdown__item", {
          hasText: "Default worker",
        })
        .click();
      await applySettings(page);
      await expect
        .poll(async () => (await readNodeSettings(request, token, flowId, "1")).execution_location)
        .toBe("worker");
      const settings = await readNodeSettings(request, token, flowId, "1");
      expect(settings.is_setup).toBe(true);
      drawer.saved(settings);

      await page.reload();
      await page.locator(".flow-tab", { hasText: flowName }).first().click();
      await expect(page.locator(".flow-tab.active")).toContainText(flowName);
      await page.locator(".vue-flow").waitFor();
      await openNodeSettings(page, "1", drawer.ready);
      await page.locator("#tab-general").click();
      await expect(page.locator("[data-testid=node-execution-location]")).toContainText(
        "Default worker",
      );
    });
  });
}
