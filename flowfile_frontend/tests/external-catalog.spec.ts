import { test, expect, type APIRequestContext, type Page } from "@playwright/test";

import {
  API_URL,
  BASE_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  lastRunStart,
  nodeResult,
  readNodeSettings,
  waitForRun,
} from "./helpers/api";
import { applySettings, clickRun, login, openFlow, openNodeSettings } from "./helpers/canvas";
import { postEditor } from "./helpers/flows";

/**
 * A person adds an Iceberg REST catalog to Flowfile's catalog: Apache Polaris with a client id and secret, then an
 * open Lakekeeper. Test, the schemas and live tables in the tree, a table's columns, a wrong secret, a schema added
 * over there and brought in line by Refresh, and no save picker offering either catalog.
 *
 * Run by `python -m test_utils.iceberg_rest ui`, which starts both catalogs' seeding, a core, a worker and a preview
 * server and sets the variables below in this process only; the spec skips without them. Secrets are only ever
 * passed in variables, so no failure output can repeat one.
 */

const URI = process.env.EXTERNAL_CATALOG_URI ?? "";
const WAREHOUSE = process.env.EXTERNAL_CATALOG_WAREHOUSE ?? "";
const CLIENT_ID = process.env.EXTERNAL_CATALOG_CLIENT_ID ?? "";
const CLIENT_SECRET = process.env.EXTERNAL_CATALOG_CLIENT_SECRET ?? "";
const SCOPE = process.env.EXTERNAL_CATALOG_SCOPE ?? "";
const OPEN_URI = process.env.OPEN_CATALOG_URI ?? "";
const OPEN_WAREHOUSE = process.env.OPEN_CATALOG_WAREHOUSE ?? "";
const SHOTS = process.env.EXTERNAL_CATALOG_SHOTS ?? "";

test.use({ trace: "off", video: "off", screenshot: "off", viewport: { width: 1440, height: 900 } });
test.skip(!URI || !CLIENT_SECRET || !OPEN_URI, "set by python -m test_utils.iceberg_rest ui");

let token: string;
const responses: string[] = [];

async function shot(page: Page, name: string) {
  if (SHOTS) await page.screenshot({ path: `${SHOTS}/${name}` });
}

const treeRow = (page: Page, name: string) =>
  page.locator(".tree-row", {
    has: page.locator(".ns-name", { hasText: new RegExp(`^${name}$`) }),
  });

async function openAddForm(page: Page) {
  await page.locator('[data-testid="catalog-new"] button').click();
  await page.locator('[data-testid="catalog-new-external"]').click();
  await expect(page.locator('[data-testid="external-catalog-modal"]')).toBeVisible();
}

async function fillSigned(page: Page, name: string, secret: string) {
  await page.locator('[data-testid="external-name"]').fill(name);
  await page.locator('[data-testid="external-uri"]').fill(URI);
  await page.locator('[data-testid="external-warehouse"]').fill(WAREHOUSE);
  await page.locator('[data-testid="external-signin-client"]').check();
  await page.locator('[data-testid="external-client-id"]').fill(CLIENT_ID);
  await page.locator('[data-testid="external-client-secret"]').fill(secret);
  await page.getByRole("button", { name: "More" }).click();
  await page.locator('[data-testid="external-scope"]').fill(SCOPE);
}

function watchResponses(page: Page) {
  page.on("response", async (response) => {
    if (response.url().includes("/catalog/")) responses.push(await response.text().catch(() => ""));
  });
}

/** In an open Catalog Reader drawer: pick `polaris / sales`, then a table of it from the live list. */
async function pickOutsideTable(page: Page, table: string) {
  await page.locator('[data-testid="catalog-reader-namespace"]').click();
  await page
    .locator(".el-select-dropdown:visible .el-select-dropdown__item", {
      hasText: /^polaris \/ sales$/,
    })
    .click();
  await page.locator('[data-testid="catalog-reader-outside-table"]').click();
  // Each option holds an icon before the name, so its text is the name with white space around it.
  await page
    .locator(".el-select-dropdown:visible .el-select-dropdown__item", {
      hasText: new RegExp(`^\\s*${table}\\s*$`),
    })
    .click();
}

/** A field of the open node drawer, by its label. */
const drawerField = (page: Page, label: string) =>
  page.locator(".node-settings-drawer .catalog-field", {
    has: page.locator(".catalog-label", { hasText: new RegExp(`^${label}$`) }),
  });

/** Development mode keeps each node's preview rows after a run; Performance mode, the default, keeps none. */
async function useDevelopmentMode(request: APIRequestContext, flowId: number) {
  const read = await request.get(`${API_URL}/flow_settings?flow_id=${flowId}`, {
    headers: authHeaders(token),
  });
  expect(read.ok()).toBe(true);
  const settings = await read.json();
  settings.execution_mode = "Development";
  const write = await request.post(`${API_URL}/flow_settings`, {
    headers: authHeaders(token),
    data: settings,
  });
  expect(write.ok()).toBe(true);
}

async function holdsSecret(page: Page): Promise<boolean> {
  const html = await page.content();
  return html.includes(CLIENT_SECRET) || responses.some((text) => text.includes(CLIENT_SECRET));
}

/** A namespace made on the other catalog by someone else, with the catalog's own REST API. */
async function createNamespaceOverThere(request: APIRequestContext, name: string) {
  const basic = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString("base64");
  const issued = await request.post(`${URI}/v1/oauth/tokens`, {
    headers: { Authorization: `Basic ${basic}` },
    form: { grant_type: "client_credentials", scope: SCOPE },
  });
  expect(issued.status()).toBe(200);
  const bearer = { Authorization: `Bearer ${(await issued.json()).access_token}` };
  const config = await (
    await request.get(`${URI}/v1/config?warehouse=${WAREHOUSE}`, { headers: bearer })
  ).json();
  const prefix = config.overrides?.prefix ?? config.defaults?.prefix;
  const made = await request.post(`${URI}/v1/${prefix}/namespaces`, {
    headers: bearer,
    data: { namespace: [name], properties: {} },
  });
  expect([200, 409]).toContain(made.status());
}

async function removeAdded(request: APIRequestContext) {
  const tree = await (
    await request.get(`${API_URL}/catalog/namespaces/tree`, { headers: authHeaders(token) })
  ).json();
  for (const node of tree as { id: number; name: string; kept_elsewhere?: boolean }[]) {
    if (node.kept_elsewhere && ["polaris", "lakekeeper"].includes(node.name)) {
      await request.delete(`${API_URL}/catalog/external/${node.id}`, {
        headers: authHeaders(token),
      });
    }
  }
}

test.describe("a catalog from elsewhere", () => {
  // Each test builds on the catalogs the first one adds, and a new worker's beforeAll removes them.
  test.describe.configure({ mode: "serial" });

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    await removeAdded(request);
  });

  test("a person adds Polaris and Lakekeeper and browses their schemas and tables", async ({
    page,
    request,
  }) => {
    watchResponses(page);
    await login(page, token, "#/main/catalog?tab=catalog");
    await page.locator(".tree-container").waitFor();

    // 1. The form, filled with a client id and secret, and Test.
    await openAddForm(page);
    await fillSigned(page, "polaris", CLIENT_SECRET);
    await page.locator('[data-testid="external-test"]').click();
    await expect(page.locator('[data-testid="external-test-result"]')).toHaveText(
      "Signed in. 2 schemas.",
    );
    expect(await page.locator('[data-testid="external-client-secret"]').getAttribute("type")).toBe(
      "password",
    );
    await shot(page, "01-form-tested.png");

    // 2. Add: the catalog is in the tree with its badge and its two schemas.
    await page.locator('[data-testid="external-submit"]').click();
    await expect(page.locator('[data-testid="external-catalog-modal"]')).toBeHidden();
    const polaris = treeRow(page, "polaris");
    await expect(polaris).toBeVisible();
    await expect(polaris.locator('[data-testid="external-badge"]')).toHaveText(/Iceberg REST/);
    await expect(treeRow(page, "finance")).toBeVisible();
    await expect(treeRow(page, "sales")).toBeVisible();
    await expect(page.locator('[data-testid="external-panel"]')).toContainText(URI);

    // 3. A schema's tables, listed live.
    await expect(page.locator('[data-testid="external-table-customers"]')).toBeVisible();
    await expect(page.locator('[data-testid="external-table-orders"]')).toBeVisible();
    await shot(page, "02-schemas-and-tables.png");

    // 4. A table's columns from the catalog's own description; no row count.
    await page.locator('[data-testid="external-table-orders"]').click();
    const columns = page.locator('[data-testid="external-columns"] tbody tr td.col-name');
    await expect(columns).toHaveText(["id", "grp", "val", "name"]);
    await expect(page.locator('[data-testid="external-table-detail"]')).not.toContainText(
      /\d[\d,]*\s+rows|row count/i,
    );
    await expect(page.locator('[data-testid="external-snippet"]')).toContainText(
      'namespace_full_name="polaris.sales"',
    );
    await shot(page, "03-table-columns.png");

    // 5. A wrong secret is one sentence.
    await openAddForm(page);
    await fillSigned(page, "polaris_wrong", "not-the-secret");
    await page.locator('[data-testid="external-test"]').click();
    await expect(page.locator('[data-testid="external-test-result"]')).toHaveText(
      `The catalog at ${URI} refused the client id and secret.`,
    );
    await shot(page, "04-wrong-secret.png");
    await page.getByRole("button", { name: "Cancel" }).click();

    // 6. A schema made over there shows after Refresh.
    await createNamespaceOverThere(request, "marketing");
    await polaris.hover();
    await polaris.locator('[data-testid="external-refresh"]').click();
    await expect(treeRow(page, "marketing")).toBeVisible();
    await shot(page, "05-kept-in-line.png");

    // 7. The open Lakekeeper through the same form, beside Flowfile's own General.
    await openAddForm(page);
    await page.locator('[data-testid="external-name"]').fill("lakekeeper");
    await page.locator('[data-testid="external-uri"]').fill(OPEN_URI);
    await page.locator('[data-testid="external-warehouse"]').fill(OPEN_WAREHOUSE);
    await page.locator('[data-testid="external-signin-none"]').check();
    await page.locator('[data-testid="external-submit"]').click();
    await expect(page.locator('[data-testid="external-catalog-modal"]')).toBeHidden();
    await expect(treeRow(page, "lakekeeper")).toBeVisible();
    await expect(treeRow(page, "General")).toBeVisible();
    await expect(treeRow(page, "demo")).toBeVisible();
    await shot(page, "06-three-catalogs.png");

    // 8. No save picker offers a catalog kept elsewhere.
    await page.locator('[data-testid="catalog-new"] button').click();
    await page.getByText("Register table").click();
    const offered = await page.locator(".modal-card select option").allTextContents();
    expect(offered.length).toBeGreaterThan(0);
    expect(offered.filter((label) => /polaris|lakekeeper/.test(label))).toEqual([]);

    expect(await holdsSecret(page)).toBe(false);
  });

  // Slice R3: the Catalog Reader takes a table Polaris keeps, by name. Needs the catalog the test above added.
  test("a Catalog Reader reads a table Polaris keeps: its columns before a run, its rows after", async ({
    page,
    request,
  }) => {
    const flowName = `outside_reader_${Date.now()}`;
    const flowId = await createFlow(request, token, flowName);
    try {
      await postEditor(request, token, "/editor/add_node/", {
        flow_id: flowId,
        node_id: 1,
        node_type: "catalog_reader",
        pos_x: 300,
        pos_y: 200,
      });
      await useDevelopmentMode(request, flowId);
      watchResponses(page);
      await openFlow(page, token, flowId, flowName);
      await openNodeSettings(page, "1", ".mode-toggle");

      // The outside schema is offered beside Flowfile's own, and its tables are listed live.
      await pickOutsideTable(page, "orders");

      // Its columns, from the catalog's description, before anything ran; no row count.
      const drawerColumns = page.locator(
        '[data-testid="catalog-reader-outside-columns"] .schema-col .col-name',
      );
      await expect(drawerColumns).toHaveText(["id", "grp", "val", "name"]);
      // No Read (change tracking) or Version control for such a table, and no sentence in their place.
      await expect(drawerField(page, "Read")).toHaveCount(0);
      await expect(drawerField(page, "Version")).toHaveCount(0);
      const drawer = page.locator(".node-settings-drawer");
      await expect(drawer).not.toContainText("reading its changes is not available there");
      await expect(drawer).not.toContainText("reading it at a version is not available there");
      await expect(page.locator(".table-meta")).not.toContainText(/Rows/);
      await expect(
        drawerField(page, "Catalog / Schema").locator('[data-testid="external-badge"]'),
      ).toBeVisible();
      await shot(page, "07-reader-drawer.png");

      // Saved by name, with no table id of this install's.
      await applySettings(page);
      await expect
        .poll(
          async () => (await readNodeSettings(request, token, flowId, "1")).catalog_full_table_name,
        )
        .toBe("polaris.sales.orders");
      expect((await readNodeSettings(request, token, flowId, "1")).catalog_table_id).toBeNull();

      // Run: the node reads the table on the worker and its rows show in the preview.
      const before = await lastRunStart(request, token, flowId);
      await clickRun(page);
      const run = await waitForRun(request, token, flowId, before);
      expect(nodeResult(run, "1")?.success, nodeResult(run, "1")?.error ?? "").toBe(true);
      expect(run.success).toBe(true);

      await page.locator(".vue-flow__pane").click({ position: { x: 700, y: 500 } });
      const node = page.locator('.vue-flow__node[data-id="1"]');
      await node.click({ button: "right" });
      await page.getByText("View Data", { exact: true }).click();
      await expect(page.locator(".dp-status-bar")).toContainText(
        "showing 100 of 1,000 rows · 4 columns",
        {
          timeout: 20000,
        },
      );
      await expect(page.locator(".ag-center-cols-container .ag-row").first()).toBeVisible();
      await shot(page, "08-reader-rows.png");
      expect(await holdsSecret(page)).toBe(false);
    } finally {
      await closeFlow(request, token, flowId);
    }
  });

  // Slice W2: the Catalog Writer appends to a table Polaris keeps; the tree, the table's panel and the flow's page
  // then name each other. Needs the catalog the first test added.
  test("a Catalog Writer appends to a table Polaris keeps, and the catalog shows it with its writer", async ({
    page,
    request,
  }) => {
    const flowName = `outside_writer_${Date.now()}`;
    const flowId = await createFlow(request, token, flowName);
    const refusal = (label: string) =>
      `polaris keeps this table. Flowfile can Append to it or Overwrite it; ${label} is not available there.`;
    try {
      for (const [nodeId, nodeType, x] of [
        [1, "catalog_reader", 250],
        [2, "catalog_writer", 550],
      ] as const) {
        await postEditor(request, token, "/editor/add_node/", {
          flow_id: flowId,
          node_id: nodeId,
          node_type: nodeType,
          pos_x: x,
          pos_y: 200,
        });
      }
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
      watchResponses(page);
      await openFlow(page, token, flowId, flowName);
      await openNodeSettings(page, "1", ".mode-toggle");
      await pickOutsideTable(page, "orders");
      await expect(
        page.locator('[data-testid="catalog-reader-outside-columns"] .schema-col .col-name'),
      ).toHaveText(["id", "grp", "val", "name"]);
      await applySettings(page);

      // The writer: the same schema, a new table name, Append; the modes polaris refuses are disabled and say why.
      await openNodeSettings(page, "2", ".writer-tabs");
      await drawerField(page, "Table name").locator("input").fill("orders_copy");
      await drawerField(page, "Catalog / Schema").locator(".el-select").click();
      await page
        .locator(".el-select-dropdown:visible")
        .getByRole("option", { name: "polaris / sales", exact: true })
        .click();
      await expect(
        drawerField(page, "Catalog / Schema").locator('[data-testid="external-badge"]'),
      ).toHaveText(/Iceberg REST/);
      // No line under the select lists what is not available; each refused mode says why on hover only.
      await expect(drawerField(page, "Write mode").locator(".partition-hint")).toHaveCount(0);
      await expect(page.locator(".node-settings-drawer")).not.toContainText(
        "is not available there",
      );
      await drawerField(page, "Write mode").locator(".el-select").click();
      const modes = page.locator(".el-select-dropdown:visible");
      for (const [label, sentence] of [
        ["Upsert", "Upsert"],
        ["Update", "Update"],
        ["Delete", "Delete"],
        ["Error if exists", "Error if exists"],
        ["SCD2 (slowly changing dimension)", "SCD2"],
      ]) {
        const option = modes.getByRole("option", { name: label, exact: true });
        await expect(option).toHaveClass(/is-disabled/);
        await expect(option).toHaveAttribute("title", refusal(sentence));
      }
      for (const label of ["Overwrite", "Append"]) {
        await expect(modes.getByRole("option", { name: label, exact: true })).not.toHaveClass(
          /is-disabled/,
        );
      }
      await expect(page.locator(".el-popper[class*='-enter-active']")).toHaveCount(0);
      await shot(page, "09-writer-drawer.png");
      await modes.getByRole("option", { name: "Append", exact: true }).click();
      await expect(
        page.locator(".node-settings-drawer .writer-tabs .el-tabs__item", { hasText: "Virtual" }),
      ).toHaveClass(/is-disabled/);
      await applySettings(page);
      await expect
        .poll(
          async () => (await readNodeSettings(request, token, flowId, "2")).catalog_write_settings,
        )
        .toMatchObject({
          table_name: "orders_copy",
          namespace_full_name: "polaris.sales",
          write_mode: "append",
        });

      // Run: the reader reads on the worker and the writer appends to polaris.
      const before = await lastRunStart(request, token, flowId);
      await clickRun(page);
      const run = await waitForRun(request, token, flowId, before);
      expect(nodeResult(run, "2")?.success, nodeResult(run, "2")?.error ?? "").toBe(true);
      expect(run.success).toBe(true);
      // This tab's own status poll sees the run end, which marks its lists of outside schemas stale.
      await expect(
        page.locator("[data-tutorial='run-btn'] button", { hasText: "Run" }),
      ).toBeEnabled();

      // The tree lists the new table under polaris / sales without a ↻, though a drawer listed it just now.
      await page.goto(`${BASE_URL}/#/main/catalog?tab=catalog`);
      await page.locator(".tree-container").waitFor();
      const sales = treeRow(page, "sales").locator("xpath=..");
      await expect(sales.locator('[data-testid="external-table-orders"]')).toBeVisible();
      const copy = sales.locator('[data-testid="external-table-orders_copy"]');
      await expect(copy).toBeVisible();
      await expect(sales.locator('[data-testid="external-tables-refresh"]')).toBeVisible();
      await expect(sales.locator('[data-testid="external-listed"]')).toContainText(
        /listed \d\d:\d\d/,
      );
      await copy.scrollIntoViewIfNeeded();
      await shot(page, "10-tree-orders-copy.png");

      // Its panel: one line for the write (commit and change are one moment), the snapshots, the writer.
      await copy.click();
      const detail = page.locator('[data-testid="external-table-detail"]');
      await expect(detail.locator("h2")).toHaveText("orders_copy");
      await expect(detail.locator('[data-testid="external-last-changed"]')).toHaveText(
        /^(\d{4}-\d\d-\d\d )?\d\d:\d\d:\d\d$/,
      );
      await expect(detail.locator('[data-testid="external-last-commit"]')).toHaveCount(0);
      await expect(detail.locator('[data-testid="external-snapshots"]')).toHaveText(
        /^\d+ snapshots? kept$/,
      );
      await expect(detail.locator('[data-testid="external-written-by"]')).toHaveText(flowName);
      await expect(detail.locator('[data-testid="external-notes"]')).toHaveCount(0);
      await expect(detail).not.toContainText("What this table does not have here");
      await expect(
        detail.locator('[data-testid="external-columns"] tbody tr td.col-name'),
      ).toHaveText(["id", "grp", "val", "name"]);
      await shot(page, "11-table-panel.png");

      // What such a catalog is and its limits, only on a click of the badge; Esc closes it.
      const popover = page.locator('[data-testid="external-badge-popover"]');
      await expect(popover).toHaveCount(0);
      await expect(page.getByText("Kept by another catalog")).toHaveCount(0);
      await expect(page.getByText("Writing: Append and Overwrite only.")).toHaveCount(0);
      await detail.locator('[data-testid="external-badge"]').click();
      await expect(popover.getByRole("heading", { name: "Kept by another catalog" })).toBeVisible();
      await expect(popover).toContainText("Writing: Append and Overwrite only.");
      await expect(page.locator(".el-popper[class*='-enter-active']")).toHaveCount(0);
      const box = (await popover.boundingBox()) ?? { x: -1, y: -1, width: 0, height: 0 };
      const viewport = page.viewportSize() ?? { width: 0, height: 0 };
      expect(box.x).toBeGreaterThanOrEqual(0);
      expect(box.y).toBeGreaterThanOrEqual(0);
      expect(box.x + box.width).toBeLessThanOrEqual(viewport.width);
      expect(box.y + box.height).toBeLessThanOrEqual(viewport.height);
      await shot(page, "13-badge-popover.png");
      await page.keyboard.press("Escape");
      await expect(popover).toHaveCount(0);
      await expect(detail).toBeVisible();

      // The flow's page lists the table it read and the table it wrote.
      await detail.locator('[data-testid="external-written-by"] .flow-link').click();
      const lineage = page.locator(".cs-trigger", { hasText: "Data Lineage" });
      if ((await lineage.getAttribute("aria-expanded")) !== "true") await lineage.click();
      const read = page.locator(".lineage-item.read-item .lineage-item-name");
      const produced = page.locator(".lineage-item.produced-item .lineage-item-name");
      await expect(read).toHaveText(["orders"]);
      await expect(produced).toHaveText(["orders_copy"]);
      await expect(read).toBeVisible();
      await expect(produced).toBeVisible();
      // The section slides open; the screenshot waits until the transition has let go of its height.
      await expect(
        page.locator(".collapsible-section", { has: lineage }).locator(".cs-body"),
      ).not.toHaveAttribute("style", /max-height|overflow/);
      await produced.scrollIntoViewIfNeeded();
      await shot(page, "12-flow-tables.png");
      expect(await holdsSecret(page)).toBe(false);
    } finally {
      await closeFlow(request, token, flowId);
    }
  });

  test.afterAll(async ({ request }) => {
    await removeAdded(request);
  });
});
