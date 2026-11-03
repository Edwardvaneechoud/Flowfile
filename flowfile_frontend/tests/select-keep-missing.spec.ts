import { test, expect, APIRequestContext, Page } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  readNodeSettings,
} from "./helpers/api";
import { applySettings, openFlow, openNodeSettings } from "./helpers/canvas";
import { postEditor } from "./helpers/flows";

/**
 * The Select drawer shows and saves the node's Keep missing columns flag: a Select added on the
 * canvas starts with it on, as core's default, and the notebook renders a node that only unticks
 * columns as `.drop([...])` with it on and as `.select([...])` with it off.
 */

const keepMissing = (page: Page) => page.getByRole("switch", { name: "Keep missing columns" });

/** manual_input (1: id, name, city) -> select (2), through the editor API. */
async function buildSelectFlow(request: APIRequestContext, token: string, flowId: number) {
  for (const [id, type, x] of [
    [1, "manual_input", 100],
    [2, "select", 400],
  ] as const) {
    await postEditor(request, token, "/editor/add_node/", {
      flow_id: flowId,
      node_id: id,
      node_type: type,
      pos_x: x,
      pos_y: 200,
    });
  }
  await postEditor(
    request,
    token,
    "/update_settings/",
    { node_type: "manual_input" },
    {
      flow_id: flowId,
      node_id: 1,
      raw_data_format: {
        columns: [
          { name: "id", data_type: "Integer" },
          { name: "name", data_type: "String" },
          { name: "city", data_type: "String" },
        ],
        data: [
          [1, 2],
          ["a", "b"],
          ["x", "y"],
        ],
      },
    },
  );
  await postEditor(
    request,
    token,
    "/editor/connect_node/",
    { flow_id: flowId },
    {
      input_connection: { node_id: 2, connection_class: "input-0" },
      output_connection: { node_id: 1, connection_class: "output-0" },
    },
  );
}

async function selectCell(request: APIRequestContext, token: string, flowId: number) {
  const response = await request.get(`${API_URL}/notebook/render?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  const cells = (await response.json()).cells as { node_ids: number[]; code: string }[];
  return cells.find((cell) => cell.node_ids.includes(2))!.code;
}

test.describe("Keep missing columns in the Select drawer", () => {
  let token: string;
  let flowId: number;
  let flowName: string;

  test.beforeEach(async ({ request }) => {
    token = await getAuthToken(request);
    flowName = `select_keep_missing_${Date.now()}`;
    flowId = await createFlow(request, token, flowName);
    await buildSelectFlow(request, token, flowId);
  });

  test.afterEach(async ({ request }) => {
    await closeFlow(request, token, flowId);
  });

  test("starts on, saves when turned off and decides how the notebook renders the node", async ({
    page,
    request,
  }) => {
    await openFlow(page, token, flowId, flowName);
    await openNodeSettings(page, "2", "[data-testid=select-keep-missing]");
    await expect(keepMissing(page)).toHaveAttribute("aria-checked", "true");
    await page.locator('.el-checkbox[aria-label="Keep name"]').click();
    await expect(page.getByRole("checkbox", { name: "Keep name" })).not.toBeChecked();
    await applySettings(page);
    await expect
      .poll(async () => {
        const rows = (await readNodeSettings(request, token, flowId, "2")).select_input;
        return rows.find((row: any) => row.old_name === "name")?.keep;
      })
      .toBe(false);
    expect((await readNodeSettings(request, token, flowId, "2")).keep_missing).toBe(true);
    expect(await selectCell(request, token, flowId)).toContain('.drop(["name"])');

    await page.locator("[data-testid=select-keep-missing]").click();
    await expect(keepMissing(page)).toHaveAttribute("aria-checked", "false");
    await applySettings(page);
    await expect
      .poll(async () => (await readNodeSettings(request, token, flowId, "2")).keep_missing)
      .toBe(false);
    const code = await selectCell(request, token, flowId);
    expect(code).toContain(".select([");
    expect(code).not.toContain(".drop(");

    await page.reload();
    await page.locator(".flow-tab", { hasText: flowName }).first().click();
    await expect(page.locator(".flow-tab.active")).toContainText(flowName);
    await page.locator(".vue-flow").waitFor();
    await openNodeSettings(page, "2", "[data-testid=select-keep-missing]");
    await expect(keepMissing(page)).toHaveAttribute("aria-checked", "false");
  });
});
