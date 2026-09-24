import { test, expect, APIRequestContext, Page } from "@playwright/test";
import { execFileSync } from "child_process";
import path from "path";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  flowEdges,
  getAuthToken,
  lastRunStart,
  nodeIdsByType,
  nodeResult,
  readNodeSettings,
  RunInfo,
  waitForRun,
} from "./helpers/api";
import {
  applySettings,
  clickRun,
  connectNodes,
  dragPaletteNode,
  login,
  minimizePalette,
  nodeStatus,
  openFlow,
  openNodeSettings,
} from "./helpers/canvas";

/**
 * The cloud storage flow from the "TypeError: 'None' is not an instance of 'str'" report, built
 * through the real UI against a real core + worker + MinIO: a cloud reader, two formulas and a
 * partitioned Delta writer, configured the way the reporter did and run with the writer's
 * settings drawer still open (no Apply).
 *
 * Prerequisites: MinIO on :9000 seeded with `poetry run seed_cloud_e2e`, and a core + worker and a
 * web server (TEST_URL / API_URL). Everything is written under s3://flowfile-test/cloud-e2e-<run id>/,
 * which afterAll deletes. The spec skips without an explicit API_URL, so it never writes connections
 * and flows into a developer's live core by default.
 */

const RUN_ID = process.env.E2E_RUN_ID || Date.now().toString(36);
const SOURCE = "s3://flowfile-test/cloud-e2e/source.parquet";
const PREFIX = `s3://flowfile-test/cloud-e2e-${RUN_ID}`;
// Unique per run: a dev core may already hold a real "minio connection" that cleanup must not touch.
const CONNECTION = `minio connection ${RUN_ID}`;
const REPO_ROOT = path.resolve(__dirname, "../..");

/** Best effort: the spec has no S3 client, so the Python seed module deletes this run's prefix. */
function deleteRunData() {
  try {
    execFileSync(
      "poetry",
      ["run", "python", "-m", "test_utils.s3.cloud_e2e_seed", "--delete", `cloud-e2e-${RUN_ID}`],
      { cwd: REPO_ROOT, stdio: "inherit", timeout: 120_000 },
    );
  } catch (error) {
    console.warn(`Could not delete ${PREFIX}/: ${error}`);
  }
}

interface UserFlow {
  flowId: number;
  name: string;
  reader: string;
  categoryFormula: string;
  outputFormula: string;
  writer: string;
}

let token: string;
let flow: UserFlow | undefined;
const createdFlowIds: number[] = [];

async function connectionExists(request: APIRequestContext): Promise<boolean> {
  const response = await request.get(`${API_URL}/cloud_connections/cloud_connections`, {
    headers: authHeaders(token),
  });
  expect(response.ok()).toBe(true);
  return (await response.json()).some((c: any) => c.connection_name === CONNECTION);
}

async function deleteConnection(request: APIRequestContext) {
  await request.delete(
    `${API_URL}/cloud_connections/cloud_connection?connection_name=${encodeURIComponent(CONNECTION)}`,
    { headers: authHeaders(token) },
  );
}

/** The payload the Cloud Connections form posts, for tests that only need the connection to exist. */
async function ensureConnection(request: APIRequestContext) {
  if (await connectionExists(request)) return;
  const response = await request.post(`${API_URL}/cloud_connections/cloud_connection`, {
    headers: authHeaders(token),
    data: {
      connection_name: CONNECTION,
      storage_type: "s3",
      auth_method: "access_key",
      aws_region: "us-east-1",
      aws_access_key_id: "minioadmin",
      aws_secret_access_key: "minioadmin",
      aws_allow_unsafe_html: true,
      endpoint_url: "http://localhost:9000",
      verify_ssl: true,
    },
  });
  expect(response.ok(), await response.text()).toBe(true);
}

async function deltaInfo(request: APIRequestContext, resourcePath: string) {
  const response = await request.post(`${API_URL}/cloud_storage/delta/info`, {
    headers: authHeaders(token),
    data: { connection_name: CONNECTION, auth_mode: "access_key", resource_path: resourcePath },
  });
  expect(response.ok(), await response.text()).toBe(true);
  return response.json();
}

async function setFormula(page: Page, name: string, expression: string) {
  const entry = page.locator(".formula-entry").first();
  const nameField = entry.locator(".entry-name input");
  await nameField.click();
  await nameField.fill(name);
  await expect(nameField).toHaveValue(name);
  const editor = entry.locator(".cm-content");
  await editor.click();
  await page.keyboard.type(expression);
  await expect(editor).toContainText(expression);
}

async function pickPartitionColumn(page: Page, column: string) {
  await page.locator("#partition-by").click();
  await page
    .locator(".el-select-dropdown__item:visible", { hasText: new RegExp(`^${column}$`) })
    .click();
  await page.keyboard.press("Escape");
  await expect(
    page.locator(".el-select .el-tag:visible", { hasText: column }).first(),
  ).toBeVisible();
}

/** Drag a connection unless the canvas already made it (a drop next to a node auto-connects). */
async function ensureEdge(
  page: Page,
  request: APIRequestContext,
  flowId: number,
  from: string,
  to: string,
) {
  if (!(await flowEdges(request, token, flowId)).includes(`${from}->${to}`)) {
    await connectNodes(page, from, to);
  }
  await expect.poll(() => flowEdges(request, token, flowId)).toContain(`${from}->${to}`);
}

/**
 * Build reader -> formula -> formula -> writer through the palette and canvas, or reuse the flow
 * an earlier test in this run built. The reader and both formulas are configured and applied;
 * the writer is left at its factory settings for each test to configure.
 */
async function ensureUserFlow(page: Page, request: APIRequestContext): Promise<UserFlow> {
  const sessions = await request.get(`${API_URL}/active_flowfile_sessions/`, {
    headers: authHeaders(token),
  });
  const open = (await sessions.json()).some((f: any) => f.flow_id === flow?.flowId);
  if (flow && open) {
    await openFlow(page, token, flow.flowId, flow.name);
    await minimizePalette(page);
    return flow;
  }

  const name = `cloud_ui_e2e_${RUN_ID}_${Date.now().toString(36)}`;
  const flowId = await createFlow(request, token, name);
  createdFlowIds.push(flowId);
  await openFlow(page, token, flowId, name);
  await dragPaletteNode(page, "cloud provider", "cloud_storage_reader", 360, 220);
  await dragPaletteNode(page, "formula", "formula", 540, 220);
  await dragPaletteNode(page, "formula", "formula", 720, 220);
  await dragPaletteNode(page, "cloud provider", "cloud_storage_writer", 900, 220);
  await minimizePalette(page);

  let ids: Record<string, string[]> = {};
  await expect
    .poll(async () => {
      ids = await nodeIdsByType(request, token, flowId);
      return [
        ids.cloud_storage_reader?.length,
        ids.formula?.length,
        ids.cloud_storage_writer?.length,
      ];
    })
    .toEqual([1, 2, 1]);
  const built: UserFlow = {
    flowId,
    name,
    reader: ids.cloud_storage_reader[0],
    categoryFormula: ids.formula[0],
    outputFormula: ids.formula[1],
    writer: ids.cloud_storage_writer[0],
  };
  await ensureEdge(page, request, flowId, built.reader, built.categoryFormula);
  await ensureEdge(page, request, flowId, built.categoryFormula, built.outputFormula);
  await ensureEdge(page, request, flowId, built.outputFormula, built.writer);

  await openNodeSettings(page, built.reader, "#connection-select");
  await page.locator("#connection-select").selectOption(CONNECTION);
  await page.locator("#file-path").fill(SOURCE);
  await page.locator("#file-format").selectOption("parquet");
  await page.locator("#scan-mode").selectOption("single_file");
  await expect(page.getByTestId("cloud-path-warning")).toBeHidden();
  await applySettings(page);

  await openNodeSettings(page, built.categoryFormula, ".formula-entry");
  await setFormula(page, "category", 'ifnull([category], "na")');
  await applySettings(page);

  await openNodeSettings(page, built.outputFormula, ".formula-entry");
  await setFormula(page, "output_field", '"test"');
  await applySettings(page);

  flow = built;
  return built;
}

/** A writer on "No connection": the flow's own when it was never configured, else a fresh one. */
async function ensureNoConnectionWriter(
  page: Page,
  request: APIRequestContext,
  userFlow: UserFlow,
) {
  const writers = (await nodeIdsByType(request, token, userFlow.flowId)).cloud_storage_writer ?? [];
  for (const id of writers.slice().reverse()) {
    const settings = await readNodeSettings(request, token, userFlow.flowId, id);
    if (!settings?.cloud_storage_settings?.connection_name) return id;
  }
  const palette = page.locator("#dataActions button[title='Maximize']");
  if (await palette.count()) await palette.click();
  // Well away from every handle, so the drop does not auto-connect to the wrong node.
  await dragPaletteNode(page, "cloud provider", "cloud_storage_writer", 720, 480);
  await minimizePalette(page);
  let writer: string | undefined;
  await expect
    .poll(async () => {
      const after =
        (await nodeIdsByType(request, token, userFlow.flowId)).cloud_storage_writer ?? [];
      writer = after.find((id) => !writers.includes(id));
      return writer;
    })
    .toBeTruthy();
  if (!writer) throw new Error("the dropped writer did not reach the backend");
  await ensureEdge(page, request, userFlow.flowId, userFlow.outputFormula, writer);
  return writer;
}

const ranNodes = (info: RunInfo) => info.node_step_result.map((n) => String(n.node_id));

/** Run with the Run button and wait for that run; the open drawer is saved by Run itself. */
async function runFromUi(page: Page, request: APIRequestContext, flowId: number): Promise<RunInfo> {
  const previousStart = await lastRunStart(request, token, flowId);
  await clickRun(page);
  return waitForRun(request, token, flowId, previousStart);
}

test.describe("cloud storage flow built through the UI", () => {
  test.skip(!process.env.API_URL, "set API_URL (and TEST_URL) to a disposable core + web server");

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
  });

  test.afterAll(async ({ request }) => {
    for (const flowId of createdFlowIds.splice(0)) await closeFlow(request, token, flowId);
    flow = undefined;
    await deleteConnection(request);
    deleteRunData();
  });

  test("creates the MinIO connection on the Connections page", async ({ page, request }) => {
    await deleteConnection(request);
    await login(page, token, "#/main/connections?tab=cloud");
    await page.getByRole("button", { name: "Add Connection" }).click();
    await page.locator("#connection-name").fill(CONNECTION);
    await page.locator("#storage-type").selectOption("s3");
    await page.locator("#auth-method").selectOption("access_key");
    await page.locator("#aws-region").fill("us-east-1");
    await page.locator("#aws-access-key-id").fill("minioadmin");
    await page.locator("#aws-secret-access-key").fill("minioadmin");
    await page.locator("#aws-allow-unsafe-html").check();
    await page.locator("#endpoint-url").fill("http://localhost:9000");
    await page.getByRole("button", { name: "Create Connection" }).click();
    await expect(page.locator(".connection-item", { hasText: CONNECTION })).toBeVisible();

    const listed = await request.get(`${API_URL}/cloud_connections/cloud_connections`, {
      headers: authHeaders(token),
    });
    const saved = (await listed.json()).find((c: any) => c.connection_name === CONNECTION);
    expect(saved).toMatchObject({
      storage_type: "s3",
      auth_method: "access_key",
      aws_region: "us-east-1",
      aws_access_key_id: "minioadmin",
      aws_allow_unsafe_html: true,
      endpoint_url: "http://localhost:9000",
    });
  });

  test("runs reader -> formulas -> partitioned Delta writer with the writer drawer still open", async ({
    page,
    request,
  }, testInfo) => {
    await ensureConnection(request);
    const userFlow = await ensureUserFlow(page, request);
    const target = `${PREFIX}/table-${testInfo.retry}`;

    await openNodeSettings(page, userFlow.writer, "#connection-select");
    await page.locator("#connection-select").selectOption(CONNECTION);
    await page.locator("#file-path").fill(target);
    await page.locator("#file-format").selectOption("delta");
    await page.locator("#write-mode").selectOption("append");
    await pickPartitionColumn(page, "output_field");
    await expect(page.getByTestId("cloud-path-warning")).toBeHidden();

    const info = await runFromUi(page, request, userFlow.flowId);
    const nodes = [
      userFlow.reader,
      userFlow.categoryFormula,
      userFlow.outputFormula,
      userFlow.writer,
    ];
    // A writer whose drawer edits never reached the backend is not set up and silently skipped.
    expect(ranNodes(info), "every node of the flow ran").toEqual(expect.arrayContaining(nodes));
    expect(info.success, JSON.stringify(info.node_step_result)).toBe(true);
    for (const id of nodes) {
      expect(nodeResult(info, id)?.success, `node ${id}`).toBe(true);
      await expect(nodeStatus(page, id)).toHaveClass(/success/);
    }
    await expect(page.locator(".el-notification", { hasText: "Success" }).first()).toBeVisible();
    // Run saved the drawer without closing it.
    await expect(page.locator("#file-path")).toHaveValue(target);

    const saved = await readNodeSettings(request, token, userFlow.flowId, userFlow.writer);
    expect(saved.cloud_storage_settings).toMatchObject({
      connection_name: CONNECTION,
      auth_mode: "access_key",
      resource_path: target,
      file_format: "delta",
      write_mode: "append",
      partition_by: ["output_field"],
    });

    const created = await deltaInfo(request, target);
    expect(created).toMatchObject({
      exists: true,
      current_version: 0,
      partition_columns: ["output_field"],
    });
    expect(created.columns.map((c: any) => c.name)).toEqual(
      expect.arrayContaining(["category", "output_field"]),
    );
  });

  test("a writer left on 'No connection' with no path fails with a path error", async ({
    page,
    request,
  }) => {
    await ensureConnection(request);
    const userFlow = await ensureUserFlow(page, request);
    const writer = await ensureNoConnectionWriter(page, request, userFlow);

    await openNodeSettings(page, writer, "#connection-select");
    await page.locator("#connection-select").selectOption("");
    await page.locator("#file-path").fill("");
    const warning = page.getByTestId("cloud-path-warning");
    await expect(warning).toBeVisible();
    await expect(warning).toContainText(/path/i);
    await page.locator("#file-format").selectOption("delta");
    await page.locator("#write-mode").selectOption("append");
    await pickPartitionColumn(page, "output_field");

    const info = await runFromUi(page, request, userFlow.flowId);
    const result = nodeResult(info, writer);
    expect(ranNodes(info), "the writer ran").toContain(writer);
    expect(info.success).toBe(false);
    expect(result?.success, JSON.stringify(info.node_step_result)).toBe(false);
    expect(result?.error).toMatch(/path/i);
    expect(result?.error).not.toMatch(/TypeError|is not an instance of/);

    const status = nodeStatus(page, writer);
    await expect(status).toHaveClass(/failure/);
    await expect(status.locator(".tooltip-text")).toContainText(/path/i);
    await expect(page.locator(".el-notification", { hasText: "Error" }).first()).toBeVisible();

    const saved = await readNodeSettings(request, token, userFlow.flowId, writer);
    expect(saved.cloud_storage_settings.connection_name ?? null).toBeNull();
    expect(saved.cloud_storage_settings).toMatchObject({
      auth_mode: "aws-cli",
      resource_path: "",
      file_format: "delta",
      write_mode: "append",
    });
  });
});
