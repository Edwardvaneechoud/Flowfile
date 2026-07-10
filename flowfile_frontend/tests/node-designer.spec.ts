// E2E coverage for the Node Designer: WYSIWYG build -> dry-run test -> save ->
// palette placement, browse/edit round-trip with the dirty guard, and the
// Catalog "Custom Nodes" tab deep link.
//
// Prerequisites (same as web-flow.spec.ts — no webServer block in the config):
// 1. Start backend: poetry run flowfile_core AND poetry run flowfile_worker (dry-run executes in the worker)
// 2. Start frontend: npm run dev:web (from flowfile_frontend)
// 3. Run: npx playwright test tests/node-designer.spec.ts
// Not wired into make targets or CI yet — run explicitly.
import { test, expect, APIRequestContext, Page } from '@playwright/test';

const BASE_URL = process.env.TEST_URL || 'http://localhost:8080';
const API_URL = process.env.API_URL || 'http://localhost:63578';

const NODE_NAME = 'E2E Upper';
const NODE_FILE = 'e2e_upper.py';
const NODE_CATEGORY = 'E2E Tools';

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  const tokenData = await tokenResponse.json();
  return tokenData.access_token;
}

async function navigateWithAuth(page: Page, token: string, targetUrl: string) {
  await page.goto(targetUrl);
  await page.waitForLoadState('networkidle');
  const expirationTime = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration }: { token: string; expiration: number }) => {
      localStorage.setItem('auth_token', token);
      localStorage.setItem('auth_token_expiration', expiration.toString());
      // A stale designer draft would trigger the restore banner and change the flow.
      Object.keys(localStorage)
        .filter((key) => key.startsWith('nodeDesigner.draft.'))
        .forEach((key) => localStorage.removeItem(key));
    },
    { token, expiration: expirationTime },
  );
  await page.reload();
  await page.waitForLoadState('networkidle');
}

async function deleteNodeIfExists(request: APIRequestContext, token: string) {
  await request.delete(`${API_URL}/user_defined_components/delete-custom-node/${NODE_FILE}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
}

test.describe('Node Designer E2E', () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
    await deleteNodeIfExists(request, authToken);
  });

  test.afterAll(async ({ request }) => {
    await deleteNodeIfExists(request, authToken);
  });

  test('build a node visually, dry-run it, save it, and find it in the palette', async ({
    page,
    request,
  }) => {
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/main/nodeDesigner`);

    // Identity: name + created-on-the-fly category.
    await page.locator('#node-name').fill(NODE_NAME);
    await page.locator('[data-testid="node-category-select"]').click();
    await page.locator('[data-testid="node-category-select"] input').fill(NODE_CATEGORY);
    await page.keyboard.press('Enter');

    // WYSIWYG form: add a TextInput control to the seeded Settings group.
    await page.locator('[data-testid="add-control-trigger"]').first().click();
    await page.locator('[data-testid="add-control-TextInput"]').click();

    // Code tab: body-only editor under the read-only signature header.
    await page.getByRole('tab', { name: 'Code' }).click();
    await expect(page.locator('.signature-header')).toContainText('def process');
    const editor = page.locator('.code-tab-layout .cm-content');
    await editor.click();
    await page.keyboard.press(process.platform === 'darwin' ? 'Meta+A' : 'Control+A');
    await page.keyboard.type(
      '    return inputs[0].with_columns(pl.col("name").str.to_uppercase().alias("upper"))',
    );

    // Test tab: paste sample data, run the dry-run, assert the output.
    await page.getByRole('tab', { name: 'Test' }).click();
    await page.getByRole('button', { name: /Paste CSV/i }).first().click();
    await page.locator('.csv-textarea').first().fill('name\nalice\nbob');
    await page.locator('.csv-apply').first().click();
    await page.getByRole('button', { name: /Run test/i }).click();
    await expect(page.locator('[data-testid="dry-run-results"]')).toBeVisible({ timeout: 60000 });
    await expect(page.locator('[data-testid="result-cell-upper-0"]')).toHaveText(/ALICE/);

    // Save and verify backend registration + palette group.
    await page.locator('[data-testid="designer-save"]').click();
    await expect
      .poll(
        async () => {
          const response = await request.get(
            `${API_URL}/user_defined_components/list-custom-nodes`,
            { headers: { Authorization: `Bearer ${authToken}` } },
          );
          if (!response.ok()) return [];
          const nodes = await response.json();
          return nodes
            .filter((node: any) => node.node_name === NODE_NAME)
            .map((node: any) => node.node_category);
        },
        { timeout: 15000 },
      )
      .toContain(NODE_CATEGORY);

    const nodeListResponse = await request.get(`${API_URL}/node_list`, {
      headers: { Authorization: `Bearer ${authToken}` },
    });
    expect(nodeListResponse.ok()).toBeTruthy();
    const templates = await nodeListResponse.json();
    const template = templates.find((t: any) => t.item === 'e2e_upper');
    expect(template).toBeTruthy();
    expect(template.node_group).toBe('e2e_tools');
    expect(template.node_group_label).toBe(NODE_CATEGORY);

    // Palette shows the dynamic category group with the node inside.
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/main/designer`);
    const group = page.getByText(NODE_CATEGORY, { exact: true }).first();
    await expect(group).toBeVisible({ timeout: 15000 });
    await group.click();
    await expect(page.getByText(NODE_NAME, { exact: true }).first()).toBeVisible();
  });

  test('browse -> edit restores state and dirty changes trigger the leave guard', async ({
    page,
  }) => {
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/main/nodeDesigner`);

    await page.locator('[data-testid="designer-browse"]').click();
    await page.locator(`[data-testid="node-edit-${NODE_FILE}"]`).click();

    await expect(page.locator('#node-name')).toHaveValue(NODE_NAME, { timeout: 15000 });

    await page.locator('#node-name').fill(`${NODE_NAME} edited`);
    await page.locator('[data-testid="designer-new"]').click();
    // ElMessageBox confirm guards the unsaved changes.
    const guard = page.locator('.el-message-box');
    await expect(guard).toBeVisible();
    await expect(guard).toContainText(/unsaved/i);
    await guard.getByRole('button', { name: /cancel/i }).click();
    await expect(page.locator('#node-name')).toHaveValue(`${NODE_NAME} edited`);
  });

  test('catalog Custom Nodes tab lists the node and deep-links into the designer', async ({
    page,
  }) => {
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/main/catalog`);

    await page.getByText('Custom Nodes', { exact: true }).first().click();
    const row = page.getByText(NODE_NAME, { exact: true }).first();
    await expect(row).toBeVisible({ timeout: 15000 });

    await page.getByRole('button', { name: 'Open', exact: true }).first().click();
    await expect(page).toHaveURL(new RegExp(`openFile=${NODE_FILE}`));
    await expect(page.locator('#node-name')).toHaveValue(NODE_NAME, { timeout: 15000 });
  });
});
