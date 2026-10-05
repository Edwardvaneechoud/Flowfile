import * as fs from "fs";
import * as path from "path";

import { test, expect, Locator, Page, APIRequestContext } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  createFlow,
  flowEdges,
  getAuthToken,
  lastRunStart,
  nodeIdsByType,
  readNodeSettings,
  waitForRun,
} from "./helpers/api";
import { BASE_URL } from "./helpers/api";
import { clickRun, minimizePalette, openFlow } from "./helpers/canvas";

/**
 * Visual walk-through of the canvas notebook on a notebook kernel: one PNG per check, plus
 * results.json, for a maintainer to look at. Not part of CI: it needs Docker, the image
 * `flowfile-kernel-notebook:dev`, a core in electron mode, and these env vars:
 *   SHOTS_DIR  folder for the PNGs and results.json
 *   CSV_PATH   absolute path of a small CSV (id, quantity, amount, region); core reads it,
 *              never the kernel
 */

const SHOTS_DIR = process.env.SHOTS_DIR ?? "";
const CSV_PATH = process.env.CSV_PATH ?? "";
const KERNEL_ID = "nbvis";
const KERNEL_NAME = "Notebook visual";
const KERNEL_IMAGE = "flowfile-kernel-notebook:dev";

const NOTEBOOK = ".code-dock .code-notebook .notebook-panel";

type Result = { check: string; status: "pass" | "problem"; note: string; shot: string };
const results: Result[] = [];
const consoleErrors: string[] = [];
const network: string[] = [];

test.skip(!SHOTS_DIR || !CSV_PATH, "set SHOTS_DIR and CSV_PATH");

async function shot(page: Page, name: string, fullPage = false) {
  const file = `${name}.png`;
  await page.screenshot({ path: path.join(SHOTS_DIR, file), fullPage });
  return file;
}

/** Run one check; a failure is recorded as a problem with the error text, and the walk goes on. */
async function check(
  page: Page,
  id: string,
  title: string,
  body: () => Promise<{ note: string; shots: string[] }>,
) {
  await test.step(`${id} ${title}`, async () => {
    try {
      const { note, shots } = await body();
      results.push({ check: `${id} ${title}`, status: "pass", note, shot: shots.join(", ") });
    } catch (e) {
      const file = await shot(page, `${id}-FAILED`).catch(() => "");
      const message = (e as Error).message.split("\n").slice(0, 6).join(" ").slice(0, 600);
      results.push({ check: `${id} ${title}`, status: "problem", note: message, shot: file });
    }
  });
}

async function api(
  request: APIRequestContext,
  token: string,
  method: "get" | "post" | "patch" | "delete",
  p: string,
  data?: unknown,
) {
  const response = await request[method](`${API_URL}${p}`, {
    headers: authHeaders(token),
    data,
  });
  return response;
}

async function buildFlow(request: APIRequestContext, token: string, flowId: number) {
  const post = async (p: string, params: Record<string, unknown>, data?: unknown) => {
    const query = new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]));
    const response = await request.post(`${API_URL}${p}?${query}`, {
      headers: authHeaders(token),
      data,
    });
    expect(response.ok(), `${p}: ${await response.text()}`).toBe(true);
  };
  await post("/editor/add_node/", {
    flow_id: flowId,
    node_id: 1,
    node_type: "read",
    pos_x: 100,
    pos_y: 200,
  });
  await post("/editor/add_node/", {
    flow_id: flowId,
    node_id: 2,
    node_type: "filter",
    pos_x: 400,
    pos_y: 200,
  });
  await post(
    "/update_settings/",
    { node_type: "read" },
    {
      flow_id: flowId,
      node_id: 1,
      received_file: {
        name: path.basename(CSV_PATH),
        path: CSV_PATH,
        directory: path.dirname(CSV_PATH),
        file_type: "csv",
        table_settings: { file_type: "csv", delimiter: ",", has_headers: true },
      },
    },
  );
  await post(
    "/editor/connect_node/",
    { flow_id: flowId },
    {
      input_connection: { node_id: 2, connection_class: "input-0" },
      output_connection: { node_id: 1, connection_class: "output-0" },
    },
  );
  await post(
    "/update_settings/",
    { node_type: "filter" },
    {
      flow_id: flowId,
      node_id: 2,
      depending_on_id: 1,
      filter_input: {
        mode: "basic",
        basic_filter: { field: "quantity", operator: ">=", value: "8" },
      },
    },
  );
  const settings = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  await post("/flow_settings", {}, { ...(await settings.json()), execution_mode: "Development" });
}

type RenderedCell = { cell_id: string; node_ids: number[]; kind: string; code: string };

async function renderedCells(request: APIRequestContext, token: string, flowId: number) {
  const response = await request.get(`${API_URL}/notebook/render?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  return (await response.json()).cells as RenderedCell[];
}

const varOf = (code: string) => /^(\w+)\s*=/m.exec(code)?.[1] ?? "";

async function openNotebook(page: Page) {
  if (!(await page.locator(".code-dock").count())) {
    await page.locator('[data-tutorial="generate-code-btn"]').click();
  }
  await page.mouse.move(400, 500);
  await page.getByTestId("code-mode-notebook").click();
  await page.locator(`${NOTEBOOK} .nb-cell`).first().waitFor();
}

const responseTo = (page: Page, p: string) => page.waitForResponse((r) => r.url().includes(p));

/** Append a python cell at the end of the notebook and type `code` into it. */
async function addCell(page: Page, code: string): Promise<Locator> {
  const panel = page.locator(NOTEBOOK);
  const before = await panel.locator(".nb-cell").count();
  await panel.locator(".nb-add-btn").last().click();
  await expect(panel.locator(".nb-cell")).toHaveCount(before + 1);
  const added = panel.locator(".nb-cell").nth(before);
  const id = await added.getAttribute("data-cell-id");
  const cell = panel.locator(`[data-cell-id="${id}"]`);
  await cell.locator(".cm-content").click();
  await page.keyboard.insertText(code);
  return cell;
}

async function waitKernelState(
  request: APIRequestContext,
  token: string,
  state: string,
  timeout = 180_000,
) {
  await expect
    .poll(
      async () => {
        const r = await api(request, token, "get", `/kernels/${KERNEL_ID}`);
        return r.ok() ? (await r.json()).state : `http ${r.status()}`;
      },
      { timeout, intervals: [1000] },
    )
    .toBe(state);
}

async function scrollCellIntoView(cell: Locator) {
  await cell.scrollIntoViewIfNeeded();
}

test.describe("Notebook on a kernel, visual inspection", () => {
  test.afterAll(() => {
    if (!SHOTS_DIR) return;
    fs.writeFileSync(
      path.join(SHOTS_DIR, "results.json"),
      JSON.stringify({ results, consoleErrors, network }, null, 2),
    );
  });

  test.use({ viewport: { width: 1440, height: 900 }, colorScheme: "light" });

  test("walk-through", async ({ page, request }) => {
    test.setTimeout(20 * 60_000);
    fs.mkdirSync(SHOTS_DIR, { recursive: true });
    page.on("console", (m) => {
      if (m.type() === "error") consoleErrors.push(`[console] ${m.text().slice(0, 400)}`);
    });
    page.on("pageerror", (e) => consoleErrors.push(`[pageerror] ${e.message.slice(0, 400)}`));
    page.on("response", async (r) => {
      const url = r.url();
      if (r.request().method() === "GET" ? r.status() < 400 : !/notebook|kernels/.test(url)) return;
      let body = "";
      if (r.status() >= 400 || /notebook\/plan|notebook\/push/.test(url)) {
        body = (await r.text().catch(() => "")).slice(0, 700);
      }
      network.push(
        `${r.request().method()} ${url.replace(/^.*?\/api/, "")} -> ${r.status()} ${body}`,
      );
    });

    const token = await getAuthToken(request);
    // A kernel left over from an earlier walk-through would hide "Create notebook kernel…".
    await api(request, token, "post", `/kernels/${KERNEL_ID}/stop`);
    await api(request, token, "delete", `/kernels/${KERNEL_ID}`);

    const flowName = `nb_kernel_visual_${Date.now()}`;
    const flowId = await createFlow(request, token, flowName);
    await buildFlow(request, token, flowId);
    let cells: RenderedCell[] = [];
    let sourceVar = "";
    let filterVar = "";
    const panel = page.locator(NOTEBOOK);
    const cellAt = (c: RenderedCell) => panel.locator(`[data-cell-id="${c.cell_id}"]`);

    await check(page, "01", "Kernels page: create form", async () => {
      await openFlow(page, token, flowId, flowName);
      await page.goto(`${BASE_URL}/#/main/compute?tab=kernels`);
      const header = page.getByRole("button", { name: /Create new kernel/ });
      if ((await header.getAttribute("aria-expanded")) !== "true") await header.click();
      await page.locator("#kernel-id").fill("formcheck");
      await page.locator("#kernel-name").fill("Create form check");
      const file = await shot(page, "01-kernel-create-form");
      return { note: "create form expanded and filled, not submitted", shots: [file] };
    });

    await check(page, "13", "Create notebook kernel… dialog from a flow tab's picker", async () => {
      await openFlow(page, token, flowId, flowName);
      await minimizePalette(page);
      await openNotebook(page);
      await page.getByTestId("nb-kernel-select").click();
      const create = page.locator(".nb-kernel-footer-create:visible");
      await expect(create).toBeVisible();
      const pickerShot = await shot(page, "13a-picker-no-notebook-kernel");
      await create.click();
      const dialog = page.locator(".el-dialog:visible").last();
      await expect(dialog).toBeVisible();
      await page.waitForTimeout(500);
      const text = (await dialog.innerText()).replace(/\s+/g, " ").slice(0, 300);
      const file = await shot(page, "13-create-notebook-kernel-dialog");
      await page.keyboard.press("Escape");
      await expect(dialog).toBeHidden();
      return { note: `dialog text starts: "${text.slice(0, 160)}…"`, shots: [pickerShot, file] };
    });

    await check(page, "03", "No kernel: Run the filter cell (sync + rows)", async () => {
      const previous = await lastRunStart(request, token, flowId);
      await clickRun(page);
      const run = await waitForRun(request, token, flowId, previous);
      expect(run.success).toBe(true);
      cells = await renderedCells(request, token, flowId);
      const source = cells.find((c) => c.node_ids.includes(1))!;
      const filter = cells.find((c) => c.node_ids.includes(2))!;
      sourceVar = varOf(source.code);
      filterVar = varOf(
        filter.code
          .split("\n")
          .filter((l) => /^\w+\s*=/.test(l))
          .at(-1) ?? "",
      );
      const cell = cellAt(filter);
      await page.waitForTimeout(3000);
      const lineage = page.waitForResponse(
        (r) => r.url().includes("/editor/notebook/run_lineage/"),
        {
          timeout: 60_000,
        },
      );
      await cell.locator(".nb-run").click();
      expect((await lineage).status()).toBe(200);
      await expect(cell.locator(".display-title")).toContainText("Node #2", { timeout: 60_000 });
      await expect(cell.locator(".display-table .ag-center-cols-container .ag-row")).toHaveCount(
        10,
      );
      await scrollCellIntoView(cell);
      const file = await shot(page, "03-no-kernel-run-filter");
      return {
        note: `canvas run ok; filter cell shows "${await cell.locator(".display-title").innerText()}", 10 rows; vars ${sourceVar}/${filterVar}`,
        shots: [file],
      };
    });

    // The notebook kernel, through the API.
    const created = await api(request, token, "post", "/kernels/", {
      id: KERNEL_ID,
      name: KERNEL_NAME,
      image_flavour: "custom",
      custom_image: KERNEL_IMAGE,
      packages: [],
    });
    expect(created.ok(), await created.text()).toBe(true);
    const started = await api(request, token, "post", `/kernels/${KERNEL_ID}/start`);
    expect(started.ok(), await started.text()).toBe(true);
    await waitKernelState(request, token, "idle");

    await check(page, "02", "Kernel details modal", async () => {
      await page.goto(`${BASE_URL}/#/main/compute?tab=kernels`);
      const card = page.locator(".kernel-card", { hasText: KERNEL_NAME });
      await card.getByRole("button", { name: "Details" }).click();
      const modal = page.locator(".km-details-modal");
      await expect(modal).toBeVisible();
      const running = await shot(page, "02a-kernel-details-running");
      await modal.locator(".modal-close").click();
      return { note: "details modal of the running kernel", shots: [running] };
    });

    await check(page, "04", "Kernel picker open in flow mode", async () => {
      await openFlow(page, token, flowId, flowName);
      await minimizePalette(page);
      await openNotebook(page);
      const select = page.getByTestId("nb-kernel-select");
      await select.click();
      const option = page.locator(".el-select-dropdown__item:visible", { hasText: KERNEL_NAME });
      await expect(option).toBeVisible({ timeout: 30_000 });
      await expect(
        page.locator(".el-select-dropdown__item:visible", { hasText: "No kernel" }),
      ).toBeVisible();
      const texts = await page.locator(".el-select-dropdown__item:visible").allInnerTexts();
      const file = await shot(page, "04-kernel-picker-open");
      await option.click();
      await expect(select).toContainText(KERNEL_NAME);
      const picked = await shot(page, "04b-kernel-picked");
      return {
        note: `options: ${texts.map((t) => t.replace(/\s+/g, " ")).join(" | ")}`,
        shots: [file, picked],
      };
    });

    await check(page, "05", "Kernel: Run the unedited source cell, display(source)", async () => {
      const source = cellAt(cells.find((c) => c.node_ids.includes(1))!);
      const exec = responseTo(page, "/notebook/session/execute");
      await source.locator(".nb-run").click();
      const r = await exec;
      const body = await r.json();
      expect(r.status(), JSON.stringify(body).slice(0, 400)).toBe(200);
      await expect(source.locator(".cell-output")).toBeVisible({ timeout: 60_000 });
      await scrollCellIntoView(source);
      const ran = await shot(page, "05a-kernel-run-source-cell");
      const sourceError = body.error ? String(body.error).trim().split("\n").at(-1) : "";
      const display = await addCell(page, "display(source_1)");
      const exec2 = responseTo(page, "/notebook/session/execute");
      await display.locator(".nb-run").click();
      const r2 = await exec2;
      expect(r2.status(), (await r2.text()).slice(0, 400)).toBe(200);
      await expect(display.locator(".cell-output")).toBeVisible({ timeout: 60_000 });
      await page.waitForTimeout(1000);
      await scrollCellIntoView(display);
      const shown = await shot(page, "05b-kernel-display-source");
      const out = (await display.locator(".cell-output").innerText())
        .replace(/\s+/g, " ")
        .slice(0, 200);
      expect(sourceError, "the unedited source cell runs on the kernel").toBe("");
      return {
        note: `source cell ran with no error; display(source) output: "${out}"`,
        shots: [ran, shown],
      };
    });

    const pricedCell = () => panel.locator(".nb-cell", { hasText: "import re" }).first();
    const PRICED_CODE = () =>
      [
        "import re",
        "",
        `priced = ${filterVar || "filtered_2"}`,
        "for name in priced.columns:",
        '    if re.search(r"amount$", name):',
        '        priced = priced.with_columns((ff.col(name) * 1.21).alias(f"{name}_incl_vat"))',
        "print(priced.columns)",
        "display(priced)",
      ].join("\n");
    await check(page, "06", "Kernel: import re, for loop, print, display(new frame)", async () => {
      const code = [
        "import re",
        "",
        `priced = ${filterVar}`,
        "for name in priced.columns:",
        '    if re.search(r"amount$", name):',
        '        priced = priced.with_columns((ff.col(name) * 1.21).alias(f"{name}_incl_vat"))',
        "print(priced.columns)",
        "display(priced)",
      ].join("\n");
      const cell = await addCell(page, code);
      const exec = responseTo(page, "/notebook/session/execute");
      await cell.locator(".nb-run").click();
      const r = await exec;
      expect(r.status(), (await r.text()).slice(0, 400)).toBe(200);
      await expect(cell.locator(".output-stdout")).toContainText("amount_incl_vat", {
        timeout: 60_000,
      });
      await expect(cell.locator(".output-error")).toHaveCount(0);
      await cell
        .locator(".ag-center-cols-container .ag-row")
        .first()
        .waitFor({ timeout: 15_000 })
        .catch(() => undefined);
      await page.waitForTimeout(800);
      await scrollCellIntoView(cell);
      const file = await shot(page, "06-kernel-loop-print-display");
      const ids = await nodeIdsByType(request, token, flowId);
      const count = Object.values(ids).flat().length;
      expect(count, "canvas unchanged").toBe(2);
      const out = (await cell.locator(".cell-output").innerText())
        .replace(/\s+/g, " ")
        .slice(0, 220);
      const rows = await cell.locator(".ag-center-cols-container .ag-row").count();
      expect(rows, "display(priced) shows rows computed in the kernel").toBeGreaterThan(0);
      return {
        note: `stdout + output shown; ${rows} rows; canvas still ${count} nodes. Output: "${out}"`,
        shots: [file],
      };
    });

    await check(page, "07", "Kernel: error cell shows traceback and marks the line", async () => {
      const cell = await addCell(page, "x = 1\ny = x / 0");
      const exec = responseTo(page, "/notebook/session/execute");
      await cell.locator(".nb-run").click();
      await exec;
      await expect(cell.locator(".output-error")).toContainText("ZeroDivisionError", {
        timeout: 60_000,
      });
      await scrollCellIntoView(cell);
      const marked = await cell.locator(".nb-sync-error-line").count();
      const file = await shot(page, "07-kernel-error-cell");
      const text = (await cell.locator(".output-error").innerText())
        .replace(/\s+/g, " ")
        .slice(0, 200);
      expect(marked, "a marked line").toBeGreaterThan(0);
      const markedText = await cell.locator(".nb-sync-error-line").first().innerText();
      // Remove it again: Push runs every cell and would stop on it.
      await cell.locator(".nb-cell-menu").click();
      await page.locator(".nb-cell-menu-popper:visible [data-action='delete']").click();
      await expect(cell).toHaveCount(0);
      return { note: `error: "${text}"; marked line: "${markedText}"`, shots: [file] };
    });

    const nodeCount = async () =>
      Object.values(await nodeIdsByType(request, token, flowId)).flat().length;

    /** Push, screenshot the review dialog when one opens, then the pane; `prefix` names the PNGs. */
    const push = async (dialogName: string, afterName: string) => {
      const shots: string[] = [];
      const filterBefore = JSON.stringify(
        (await readNodeSettings(request, token, flowId, "2"))?.filter_input,
      );
      // The first push carries `trigger: "push"`; core answers `applied: false` when it needs review.
      const firstPush = page.waitForResponse((r) => r.url().includes("/editor/notebook/push"), {
        timeout: 120_000,
      });
      await page.evaluate(() => {
        const w = window as any;
        w.__toasts = [];
        w.__obs?.disconnect();
        w.__obs = new MutationObserver((muts) => {
          for (const m of muts)
            m.addedNodes.forEach((n: any) => {
              if (n.classList?.contains("el-message")) w.__toasts.push(n.innerText);
            });
        });
        w.__obs.observe(document.body, { childList: true, subtree: true });
      });
      await panel.getByTestId("nb-push").click();
      const pushResponse = await firstPush;
      const pushBody = await pushResponse.json().catch(() => ({}));
      const dialog = page.locator(".el-message-box:visible");
      const toast = page.locator(".el-message", {
        hasText: /Pushed to the canvas|Nothing to push/,
      });
      const first = await Promise.race([
        dialog.waitFor({ timeout: 30_000 }).then(() => "dialog" as const),
        toast
          .first()
          .waitFor({ timeout: 30_000 })
          .then(() => "toast" as const),
      ]).catch(() => "none" as const);
      const dialogShown = first === "dialog";
      if (dialogShown) {
        await page.waitForTimeout(600);
        shots.push(await shot(page, dialogName));
        await dialog.getByRole("button", { name: "Push" }).click();
        await toast
          .first()
          .waitFor({ timeout: 30_000 })
          .catch(() => undefined);
      }
      await page.waitForTimeout(700);
      shots.push(await shot(page, afterName));
      const messages: string[] = await page.evaluate(() => (window as any).__toasts ?? []);
      const filterAfter = JSON.stringify(
        (await readNodeSettings(request, token, flowId, "2"))?.filter_input,
      );
      return {
        shots,
        pushBody,
        pushStatus: pushResponse.status(),
        dialogShown,
        filterBefore,
        filterAfter,
        messages,
      };
    };

    const errorLines = async () =>
      (await panel.locator(".output-error").allInnerTexts()).map((t) =>
        t.trim().split("\n").at(-1)?.slice(0, 160),
      );

    await check(page, "10", "⋯ menus (Run and preview on canvas, Reset session)", async () => {
      const shots: string[] = [];
      const filter = cellAt(cells.find((c) => c.node_ids.includes(2))!);
      await scrollCellIntoView(filter);
      await filter.locator(".nb-cell-menu").click();
      await expect(
        page.locator(".nb-cell-menu-popper:visible [data-action='run-on-canvas']"),
      ).toBeVisible();
      await page.waitForTimeout(300);
      shots.push(await shot(page, "10a-cell-menu"));
      await page.keyboard.press("Escape");
      await page.mouse.click(700, 60);
      const more = panel.getByRole("button", { name: "More actions" });
      await more.click();
      const reset = page.getByRole("menuitem", { name: "Reset session" });
      await expect(reset).toBeVisible();
      shots.push(await shot(page, "10b-toolbar-menu-reset-session"));
      const resetCall = responseTo(page, "/notebook/session/");
      await reset.click();
      const status = (await resetCall).status();
      await page.waitForTimeout(1000);
      shots.push(await shot(page, "10c-after-reset-session"));
      expect(status).toBe(200);
      return { note: `Reset session -> ${status}`, shots };
    });

    await check(
      page,
      "16",
      "Run all after Reset session, file invisible: every cell passes, display(priced) has rows",
      async () => {
        const shots: string[] = [];
        const executes: number[] = [];
        const onResponse = (r: { url: () => string; status: () => number }) => {
          if (r.url().includes("/notebook/session/execute")) executes.push(r.status());
        };
        page.on("response", onResponse);
        await panel.getByTestId("nb-run-all").click();
        await expect
          .poll(
            async () =>
              (await panel.getByTestId("nb-run-all").isDisabled()) === false && executes.length > 0,
            { timeout: 120_000 },
          )
          .toBe(true);
        await page.waitForTimeout(1500);
        page.off("response", onResponse);
        const python = await panel.locator(".nb-cell").count();
        await panel.locator(".nb-cell").first().scrollIntoViewIfNeeded();
        shots.push(await shot(page, "16a-run-all-top"));
        await scrollCellIntoView(pricedCell());
        shots.push(await shot(page, "16b-run-all-priced"));
        const errors = await errorLines();
        const rows = await pricedCell().locator(".ag-center-cols-container .ag-row").count();
        expect(errors, "no error outputs").toEqual([]);
        expect(rows, "display(priced) rows").toBeGreaterThan(0);
        return {
          note: `${executes.length} executes for ${python} cells (statuses ${[...new Set(executes)].join(",")}); priced shows ${rows} rows; errors ${JSON.stringify(errors)}`,
          shots,
        };
      },
    );

    await check(page, "11", 'Column completions for ff.col("', async () => {
      const cell = await addCell(page, `${filterVar || "filtered_2"}.select(ff.col(`);
      await page.keyboard.type('"', { delay: 50 });
      const popup0 = page.locator(".cm-tooltip-autocomplete");
      if (!(await popup0.isVisible().catch(() => false))) {
        await page.waitForTimeout(800);
        if (!(await popup0.isVisible().catch(() => false)))
          await page.keyboard.press("Control+Space");
      }
      const popup = page.locator(".cm-tooltip-autocomplete");
      await expect(popup).toBeVisible({ timeout: 15_000 });
      const options = await popup.locator("li").allInnerTexts();
      const file = await shot(page, "11-column-completions");
      await page.keyboard.press("Escape");
      expect(options.join(" ")).toContain("quantity");
      await cell.locator(".nb-cell-menu").click();
      await page.locator(".nb-cell-menu-popper:visible [data-action='delete']").click();
      return {
        note: `suggestions: ${options
          .map((o) => o.replace(/\s+/g, " "))
          .join(" | ")
          .slice(0, 300)}`,
        shots: [file],
      };
    });

    await check(
      page,
      "08",
      "Push (file invisible): review, new node below the filter, source unchanged",
      async () => {
        const r = await push("08a-push-review-dialog", "08b-after-push");
        expect(r.pushStatus, JSON.stringify(r.pushBody).slice(0, 400)).toBe(200);
        await expect.poll(nodeCount, { timeout: 30_000 }).toBe(3);
        const byType = await nodeIdsByType(request, token, flowId);
        const edges = await flowEdges(request, token, flowId);
        await page.mouse.click(150, 600);
        await page.waitForTimeout(1200);
        r.shots.push(await shot(page, "08c-pushed-canvas"));
        const settings = await readNodeSettings(request, token, flowId, "1");
        expect(settings?.received_file?.path).toBe(CSV_PATH);
        await page.locator('.vue-flow__node[data-id="1"]').dispatchEvent("dblclick");
        await page.locator(".node-settings-drawer").first().waitFor({ timeout: 20_000 });
        await page.waitForTimeout(1500);
        r.shots.push(await shot(page, "08d-source-settings-after-push"));
        await page.mouse.click(150, 600);
        const newId = Object.values(byType)
          .flat()
          .find((id) => id !== "1" && id !== "2");
        expect(edges).toContain(`2->${newId}`);
        return {
          note: `dialog ${r.dialogShown}; push warnings ${JSON.stringify(r.pushBody.warnings ?? [])}; messages ${JSON.stringify(r.messages)}; nodes ${JSON.stringify(byType)}; edges ${edges.join(", ")}; filter unchanged ${r.filterBefore === r.filterAfter}; source path unchanged`,
          shots: r.shots,
        };
      },
    );

    await check(page, "09", "Undo removes the pushed node; unedited Push", async () => {
      const shots: string[] = [];
      expect(await nodeCount(), "only undo a push that added a node").toBe(3);
      await page.mouse.click(150, 600);
      await page.locator(".undo-redo-controls .control-btn").first().click();
      await expect.poll(nodeCount, { timeout: 30_000 }).toBe(2);
      await page.waitForTimeout(2500);
      shots.push(await shot(page, "09a-undo-removed-node"));
      const cellsLeft = await panel.locator(".nb-cell").count();
      const loopKept = await pricedCell().count();
      const filterUndone = JSON.stringify(
        (await readNodeSettings(request, token, flowId, "2"))?.filter_input,
      );
      const r = await push("09b-unedited-push-dialog", "09b-unedited-push");
      expect(r.messages.join(" ")).toMatch(/^Nothing to push/);
      return {
        note: `after undo ${await nodeCount()} nodes, ${cellsLeft} cells (loop cell kept: ${loopKept > 0}), filter ${filterUndone}; next Push ${JSON.stringify(r.pushBody).slice(0, 200)}; messages ${JSON.stringify(r.messages)}`,
        shots: [...shots, ...r.shots],
      };
    });

    await check(page, "17", "Push of the loop cell with the file invisible", async () => {
      if (!(await pricedCell().count())) await addCell(page, PRICED_CODE());
      const r = await push("17a-push-review-dialog", "17b-after-push");
      expect(r.pushStatus, JSON.stringify(r.pushBody).slice(0, 400)).toBe(200);
      await expect.poll(nodeCount, { timeout: 30_000 }).toBe(3);
      await page.mouse.click(150, 600);
      await page.waitForTimeout(1200);
      r.shots.push(await shot(page, "17c-canvas-new-node"));
      const edges = await flowEdges(request, token, flowId);
      return {
        note: `dialog ${r.dialogShown}; warnings ${JSON.stringify(r.pushBody.warnings ?? [])}; applied first ${r.pushBody.applied}; messages ${JSON.stringify(r.messages)}; edges ${edges.join(", ")}`,
        shots: r.shots,
      };
    });

    await check(page, "18", "Unedited Push shows Nothing to push", async () => {
      await page.waitForTimeout(3500);
      const r = await push("18a-unexpected-dialog", "18-nothing-to-push");
      const nodes = await nodeCount();
      expect(r.messages.join(" ")).toMatch(/^Nothing to push/);
      expect(nodes).toBe(3);
      return {
        note: `push ${JSON.stringify(r.pushBody).slice(0, 200)}; messages ${JSON.stringify(r.messages)}; ${nodes} nodes`,
        shots: r.shots,
      };
    });

    await check(page, "14", "Dark mode notebook with outputs", async () => {
      await page.emulateMedia({ colorScheme: "dark" });
      await page.evaluate(() => {
        localStorage.setItem("flowfile-theme-preference", "dark");
        document.documentElement.setAttribute("data-theme", "dark");
      });
      await page.waitForTimeout(800);
      if (await pricedCell().count()) await scrollCellIntoView(pricedCell());
      const file = await shot(page, "14-dark-mode-notebook");
      await page.emulateMedia({ colorScheme: "light" });
      await page.evaluate(() => localStorage.setItem("flowfile-theme-preference", "light"));
      await openFlow(page, token, flowId, flowName);
      await minimizePalette(page);
      await openNotebook(page);
      const theme = await page.evaluate(() => document.documentElement.getAttribute("data-theme"));
      expect(theme, "back to light").not.toBe("dark");
      return { note: "data-theme=dark for the shot, light again after a reload", shots: [file] };
    });

    await check(
      page,
      "15",
      "Extra: a not-yet-configured node on the canvas and the kernel session",
      async () => {
        const add = await request.post(
          `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=9&node_type=filter&pos_x=400&pos_y=420`,
          { headers: authHeaders(token) },
        );
        expect(add.ok()).toBe(true);
        await openFlow(page, token, flowId, flowName);
        await minimizePalette(page);
        await openNotebook(page);
        const cell = await addCell(page, "display(filtered_2)");
        const exec = page.waitForResponse((r) => r.url().includes("/notebook/session/"), {
          timeout: 60_000,
        });
        await cell.locator(".nb-run").click();
        await exec;
        await expect(cell.locator(".cell-output")).toBeVisible({ timeout: 60_000 });
        await page.waitForTimeout(1000);
        await scrollCellIntoView(cell);
        const file = await shot(page, "15-unconfigured-node-session");
        const text = (await cell.locator(".cell-output").innerText())
          .replace(/\s+/g, " ")
          .slice(0, 300);
        await request.post(`${API_URL}/editor/delete_node/?flow_id=${flowId}&node_id=9`, {
          headers: authHeaders(token),
        });
        expect(text, "the cell runs with an unconfigured node elsewhere on the canvas").not.toMatch(
          /Error/,
        );
        return { note: `cell output: "${text}"`, shots: [file] };
      },
    );
  });
});
