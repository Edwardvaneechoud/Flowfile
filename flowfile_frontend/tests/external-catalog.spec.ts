import { test, expect, type APIRequestContext, type Page } from "@playwright/test";

import { API_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";

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
  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    await removeAdded(request);
  });

  test("a person adds Polaris and Lakekeeper and browses their schemas and tables", async ({
    page,
    request,
  }) => {
    page.on("response", async (response) => {
      if (response.url().includes("/catalog/"))
        responses.push(await response.text().catch(() => ""));
    });
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
      'catalog="polaris"',
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

  test.afterAll(async ({ request }) => {
    await removeAdded(request);
  });
});
