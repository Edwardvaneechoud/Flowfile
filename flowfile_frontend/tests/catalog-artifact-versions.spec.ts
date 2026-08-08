// E2E coverage for the catalog artifact version manager: server-paginated version
// history, latest-version protection, per-version delete, and namespace isolation
// of same-named models.
//
// STATUS: written but NEVER EXECUTED — the selectors and the seeding ladder still
// need a real run to shake out. Fix in the validation stage before wiring into CI.
//
// Prerequisites (same as web-flow.spec.ts — no webServer block in the config):
// 1. Start backend: poetry run flowfile_core (worker not needed)
// 2. Start frontend: npm run dev:web (from flowfile_frontend)
// 3. Run: npx playwright test tests/catalog-artifact-versions.spec.ts
// Seeding writes artifact blobs straight to the path core hands back from
// /artifacts/prepare-upload, so core must run on this machine (not in Docker).
import { test, expect, APIRequestContext, Page } from "@playwright/test";
import { createHash } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

const MODEL_NAME = "e2e_versioned_model";
const SCHEMA_A = "e2e-models-a";
const SCHEMA_B = "e2e-models-b";
const VERSIONS_IN_A = 45; // > 25 so the pager has two pages
const VERSIONS_IN_B = 2;
const PAGE_SIZE = 25;

interface Seeded {
  namespaceId: number;
  registrationId: number;
  latestArtifactId: number;
}

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  return (await tokenResponse.json()).access_token;
}

async function navigateWithAuth(page: Page, token: string, targetUrl: string) {
  await page.goto(targetUrl);
  await page.waitForLoadState("networkidle");
  const expirationTime = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration }: { token: string; expiration: number }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
    },
    { token, expiration: expirationTime },
  );
  await page.reload();
  await page.waitForLoadState("networkidle");
}

function authHeaders(token: string) {
  return { Authorization: `Bearer ${token}` };
}

/** Create (or reuse) a schema under the "General" catalog. */
async function ensureSchema(
  request: APIRequestContext,
  token: string,
  name: string,
): Promise<number> {
  const listed = await request.get(`${API_URL}/catalog/namespaces`, {
    headers: authHeaders(token),
  });
  const namespaces = await listed.json();
  const general = namespaces.find((n: any) => n.name === "General" && n.parent_id === null);
  if (!general) throw new Error('No "General" catalog to seed into');

  const children = await request.get(`${API_URL}/catalog/namespaces`, {
    headers: authHeaders(token),
    params: { parent_id: general.id },
  });
  const existing = (await children.json()).find((n: any) => n.name === name);
  if (existing) return existing.id;

  const created = await request.post(`${API_URL}/catalog/namespaces`, {
    headers: authHeaders(token),
    data: { name, parent_id: general.id },
  });
  return (await created.json()).id;
}

async function registerFlow(
  request: APIRequestContext,
  token: string,
  name: string,
  namespaceId: number,
): Promise<number> {
  const response = await request.post(`${API_URL}/catalog/flows`, {
    headers: authHeaders(token),
    data: { name, flow_path: `/tmp/${name}.flowfile`, namespace_id: namespaceId },
  });
  return (await response.json()).id;
}

/** prepare-upload -> write the blob at the path core hands back -> finalize. */
async function publishVersion(
  request: APIRequestContext,
  token: string,
  registrationId: number,
  namespaceId: number,
  payload: string,
): Promise<number> {
  const prepared = await request.post(`${API_URL}/artifacts/prepare-upload`, {
    headers: authHeaders(token),
    data: {
      name: MODEL_NAME,
      source_registration_id: registrationId,
      serialization_format: "pickle",
      namespace_id: namespaceId,
      python_type: "builtins.dict",
      python_module: "builtins",
    },
  });
  if (!prepared.ok()) throw new Error(`prepare-upload failed: ${prepared.status()}`);
  const { artifact_id, storage_key, path } = await prepared.json();

  const bytes = Buffer.from(payload, "utf-8");
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, bytes);

  const finalized = await request.post(`${API_URL}/artifacts/finalize`, {
    headers: authHeaders(token),
    data: {
      artifact_id,
      storage_key,
      sha256: createHash("sha256").update(bytes).digest("hex"),
      size_bytes: bytes.length,
    },
  });
  if (!finalized.ok()) throw new Error(`finalize failed: ${finalized.status()}`);
  return artifact_id;
}

async function seedNamespace(
  request: APIRequestContext,
  token: string,
  schemaName: string,
  versionCount: number,
): Promise<Seeded> {
  const namespaceId = await ensureSchema(request, token, schemaName);
  const registrationId = await registerFlow(request, token, `${schemaName}-producer`, namespaceId);
  const artifactIds: number[] = [];
  for (let i = 1; i <= versionCount; i++) {
    artifactIds.push(await publishVersion(request, token, registrationId, namespaceId, `v${i}`));
  }
  return {
    namespaceId,
    registrationId,
    latestArtifactId: artifactIds[artifactIds.length - 1],
  };
}

/** Drop the artifacts a run published; safe to call before anything is seeded. */
async function cleanupArtifacts(request: APIRequestContext, token: string, schemaName: string) {
  const ref = `General.${schemaName}::${MODEL_NAME}`;
  await request.delete(`${API_URL}/artifacts/by-name/${encodeURIComponent(ref)}`, {
    headers: authHeaders(token),
  });
}

/**
 * Undo the whole seeding ladder in reverse: artifacts, then the flow registration
 * that owns them, then the schema. Without this every run grows the catalog DB.
 */
async function cleanupSeeded(
  request: APIRequestContext,
  token: string,
  schemaName: string,
  seeded: Seeded | undefined,
) {
  await cleanupArtifacts(request, token, schemaName);
  if (!seeded) return;
  await request.delete(`${API_URL}/catalog/flows/${seeded.registrationId}`, {
    headers: authHeaders(token),
  });
  await request.delete(`${API_URL}/catalog/namespaces/${seeded.namespaceId}`, {
    headers: authHeaders(token),
  });
}

/** Version numbers rendered in the versions table, in DOM order. */
async function renderedVersions(page: Page): Promise<string[]> {
  return page.locator(".versions-row .col-version").allInnerTexts();
}

test.describe("Catalog artifact versions", () => {
  let authToken: string;
  let seededA: Seeded;
  let seededB: Seeded;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    await cleanupArtifacts(request, authToken, SCHEMA_A);
    await cleanupArtifacts(request, authToken, SCHEMA_B);
    seededA = await seedNamespace(request, authToken, SCHEMA_A, VERSIONS_IN_A);
    seededB = await seedNamespace(request, authToken, SCHEMA_B, VERSIONS_IN_B);
  });

  test.afterAll(async ({ request }) => {
    await cleanupSeeded(request, authToken, SCHEMA_A, seededA);
    await cleanupSeeded(request, authToken, SCHEMA_B, seededB);
  });

  test("paginates version history and protects the latest version", async ({ page }) => {
    await navigateWithAuth(
      page,
      authToken,
      `${BASE_URL}/#/main/catalog?tab=catalog&artifactId=${seededA.latestArtifactId}`,
    );

    const panel = page.locator(".artifact-detail");
    await expect(panel).toBeVisible();

    // "Total Versions" is the server count, not the page length.
    await expect(panel.locator(".meta-card", { hasText: "Total Versions" })).toContainText(
      String(VERSIONS_IN_A),
    );

    // First page: newest first, exactly one page worth of rows.
    await expect(page.locator(".versions-row")).toHaveCount(PAGE_SIZE);
    const firstPage = await renderedVersions(page);
    expect(firstPage[0]).toContain(`v${VERSIONS_IN_A}`);
    expect(firstPage[0]).toContain("latest");

    // The latest row offers neither restore nor delete.
    await expect(page.locator(".versions-row").first().locator(".col-actions button")).toHaveCount(
      0,
    );
    await expect(page.locator(".versions-row").nth(1).locator(".col-actions button")).toHaveCount(
      2,
    );

    // Pager advances to a disjoint second page.
    const pager = page.locator(".pagination-bar");
    await expect(pager).toBeVisible();
    await expect(pager.locator(".page-info")).toHaveText("Page 1 of 2");
    await pager.locator(".page-btn").nth(3).click(); // next
    await expect(pager.locator(".page-info")).toHaveText("Page 2 of 2");

    const secondPage = await renderedVersions(page);
    expect(secondPage).toHaveLength(VERSIONS_IN_A - PAGE_SIZE);
    expect(secondPage.some((row) => firstPage.includes(row))).toBe(false);
  });

  test("deletes a single non-latest version", async ({ page }) => {
    await navigateWithAuth(
      page,
      authToken,
      `${BASE_URL}/#/main/catalog?tab=catalog&artifactId=${seededA.latestArtifactId}`,
    );
    await expect(page.locator(".artifact-detail")).toBeVisible();

    const before = await renderedVersions(page);
    const target = before[1]; // second row: newest deletable version

    await page.locator(".versions-row").nth(1).locator(".col-actions button").last().click();
    await page.locator(".el-message-box__btns button", { hasText: "Delete" }).click();

    await expect(page.locator(".versions-row").first()).toContainText("latest");
    await expect.poll(async () => (await renderedVersions(page)).includes(target)).toBe(false);
    await expect(
      page.locator(".artifact-detail .meta-card", { hasText: "Total Versions" }),
    ).toContainText(String(VERSIONS_IN_A - 1));
  });

  test("keeps same-named models in different schemas apart", async ({ page }) => {
    // Both artifacts share a name; the version list is fetched with a
    // namespace-qualified ref, so neither may show the other's history.
    await navigateWithAuth(
      page,
      authToken,
      `${BASE_URL}/#/main/catalog?tab=catalog&artifactId=${seededB.latestArtifactId}`,
    );
    await expect(page.locator(".artifact-detail")).toBeVisible();
    await expect(
      page.locator(".artifact-detail .meta-card", { hasText: "Total Versions" }),
    ).toContainText(String(VERSIONS_IN_B));
    await expect(page.locator(".versions-row")).toHaveCount(VERSIONS_IN_B);

    // The tree lists the two families as separate rows under their own schemas.
    await page.locator(".search-input").first().fill(MODEL_NAME);
    await expect(page.locator(".tree-artifact", { hasText: MODEL_NAME })).toHaveCount(2);
  });
});
