import { expect, test, type APIRequestContext, type Page } from "@playwright/test";
import { promises as fs } from "fs";
import { basename } from "path";
import { API_URL, authHeaders, closeFlow, createFlow, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";

async function call(request: APIRequestContext, token: string, path: string, data?: object) {
  const response = await request.fetch(`${API_URL}${path}`, {
    method: data === undefined ? "GET" : "POST",
    headers: authHeaders(token),
    ...(data === undefined ? {} : { data }),
  });
  if (!response.ok()) throw new Error(`${path}: ${response.status()} ${await response.text()}`);
  return response.json();
}

async function slowFlow(request: APIRequestContext, token: string, name: string) {
  const id = await createFlow(request, token, name);
  // The Polars code sandbox provides time; imports are not allowed in node code.
  for (const [nodeId, nodeType, x] of [
    [1, "manual_input", 100],
    [2, "polars_code", 450],
  ] as const) {
    const response = await request.post(
      `${API_URL}/editor/add_node/?flow_id=${id}&node_id=${nodeId}&node_type=${nodeType}&pos_x=${x}&pos_y=200`,
      { headers: authHeaders(token) },
    );
    expect(response.ok(), `add ${nodeType}: ${response.status()}`).toBe(true);
  }
  await call(request, token, "/update_settings/?node_type=manual_input", {
    flow_id: id,
    node_id: 1,
    raw_data_format: { columns: [{ name: "Column 1", data_type: "String" }], data: [["data"]] },
  });
  await call(request, token, `/editor/connect_node/?flow_id=${id}`, {
    input_connection: { node_id: 2, connection_class: "input-0" },
    output_connection: { node_id: 1, connection_class: "output-0" },
  });
  await call(request, token, "/update_settings/?node_type=polars_code", {
    flow_id: id,
    node_id: 2,
    depending_on_ids: [1],
    polars_code_input: { polars_code: "time.sleep(10)\noutput_df = input_df\n" },
  });
  return id;
}

const settings = (request: APIRequestContext, token: string, id: number) =>
  call(request, token, `/flow_settings?flow_id=${id}`);
const status = (request: APIRequestContext, token: string, id: number) =>
  call(request, token, `/flow/run_status/?flow_id=${id}`);
const buttons = (page: Page) => page.locator("[data-tutorial='run-btn'].button-group");
const run = (page: Page) => buttons(page).locator("button").first();
const cancel = (page: Page) => buttons(page).getByRole("button", { name: "Cancel", exact: true });
const cancelling = (page: Page) => run(page).filter({ hasText: "Cancelling…" });

async function select(page: Page, name: string) {
  const tab = page.locator(".flow-tab", { hasText: name }).first();
  await tab.waitFor();
  // A crowded tab strip can cover an already-active tab with its scroll arrows.
  if (!(await tab.evaluate((el) => el.classList.contains("active"))))
    await tab.dispatchEvent("click");
  await expect(page.locator(".flow-tab.active")).toContainText(name);
}

const unique = (label: string) => `e2e_cancel_${label}_${Date.now()}`;

test.describe("canvas cancellation against live core and worker", () => {
  test.setTimeout(70_000);

  test("Cancel and refresh keeps the run locked until terminal node results", async ({
    page,
    request,
  }) => {
    const token = await getAuthToken(request);
    const name = unique("refresh");
    const id = await slowFlow(request, token, name);
    try {
      await login(page, token, "#/main/designer", id);
      await select(page, name);
      await run(page).click();
      await expect(cancel(page)).toBeVisible();
      await expect.poll(async () => (await settings(request, token, id)).is_running).toBe(true);
      await cancel(page).click();
      await expect.poll(async () => (await settings(request, token, id)).is_canceled).toBe(true);
      await page.reload();
      if ((await settings(request, token, id)).is_running) {
        await expect(cancelling(page)).toBeVisible();
        await expect(cancel(page)).toHaveCount(0);
      }
      await expect
        .poll(async () => (await settings(request, token, id)).is_running, { timeout: 40_000 })
        .toBe(false);
      await expect(run(page)).toBeEnabled();
      await expect(cancelling(page)).toHaveCount(0);
      const terminal = await status(request, token, id);
      expect(terminal.success).toBe(false);
      expect(terminal.node_step_result).toEqual(
        expect.arrayContaining([expect.objectContaining({ node_id: 2, success: null })]),
      );
    } finally {
      if (!(await settings(request, token, id)).is_running) await closeFlow(request, token, id);
    }
  });

  test("A cancels while B runs: switching canvases does not leak state or errors", async ({
    page,
    request,
  }) => {
    const token = await getAuthToken(request);
    const aName = unique("A");
    const bName = unique("B");
    const a = await slowFlow(request, token, aName);
    const b = await slowFlow(request, token, bName);
    try {
      await login(page, token, "#/main/designer", a);
      await select(page, aName);
      await run(page).click();
      await expect(cancel(page)).toBeVisible();
      await cancel(page).click();
      await expect.poll(async () => (await settings(request, token, a)).is_canceled).toBe(true);
      await select(page, bName);
      await expect(run(page)).toBeEnabled();
      await expect(cancelling(page)).toHaveCount(0);
      await run(page).click();
      await expect(cancel(page)).toBeVisible();
      await expect.poll(async () => (await settings(request, token, b)).is_running).toBe(true);
      await expect
        .poll(async () => (await settings(request, token, a)).is_running, { timeout: 40_000 })
        .toBe(false);
      await expect
        .poll(async () => (await settings(request, token, b)).is_running, { timeout: 40_000 })
        .toBe(false);
      await expect(run(page)).toBeEnabled();
      expect((await status(request, token, a)).success).toBe(false);
      expect((await status(request, token, b)).success).toBe(true);
      await expect(
        page.locator(".el-notification", { hasText: "There were issues with the operation" }),
      ).toHaveCount(0);
      await select(page, aName);
      await expect(run(page)).toBeEnabled();
      await expect(cancelling(page)).toHaveCount(0);
    } finally {
      for (const id of [a, b]) {
        if (!(await settings(request, token, id)).is_running) await closeFlow(request, token, id);
      }
    }
  });

  test("Save As rekeys the canvas before the next Run", async ({ page, request }) => {
    const token = await getAuthToken(request);
    const name = unique("source");
    const copyName = unique("copy");
    const oldId = await slowFlow(request, token, name);
    let newId: number | undefined;
    let savedPath: string | undefined;
    try {
      await login(page, token, "#/main/designer", oldId);
      await select(page, name);
      await page.getByRole("button", { name: "More save options" }).click();
      await page.locator('[data-tutorial="save-as-btn"]').click();
      const saveDialog = page.getByRole("dialog", { name: "Save Flow" });
      await saveDialog.getByText("Also register in catalog").click();
      // Save As starts with the source file selected; clear it before creating a copy.
      await saveDialog.locator(".browser-main").dispatchEvent("click");
      await saveDialog.getByRole("button", { name: "Create New File Here" }).click();
      const createDialog = page.getByRole("dialog", { name: "Create New File" });
      await createDialog.getByPlaceholder("Enter file name (e.g., my_flow)").fill(copyName);
      const saved = page.waitForResponse(
        (r) => r.url().includes("/save_flow?") && r.request().method() === "POST",
      );
      await createDialog.getByRole("button", { name: "Create", exact: true }).click();
      const response = await saved;
      expect(response.ok()).toBe(true);
      const savedId: number = await response.json();
      newId = savedId;
      expect(savedId).not.toBe(oldId);
      savedPath = (await settings(request, token, savedId)).path;
      expect(savedPath && basename(savedPath)).toBe(`${copyName}.yaml`);
      await expect(saveDialog).toBeHidden();
      await run(page).click();
      await expect
        .poll(async () => (await settings(request, token, savedId)).is_running)
        .toBe(true);
      await expect
        .poll(async () => (await settings(request, token, savedId)).is_running, { timeout: 40_000 })
        .toBe(false);
      expect((await status(request, token, savedId)).success).toBe(true);
    } finally {
      if (newId && !(await settings(request, token, newId)).is_running) {
        await closeFlow(request, token, newId);
        // Only remove this test's unique file; never a directory or an existing flow.
        if (savedPath && basename(savedPath) === `${copyName}.yaml`) await fs.unlink(savedPath);
      }
    }
  });
});
