// Web-mode E2E for the "Share link" sender UI (RightActionCluster button ->
// ShareLinkDialog). Not part of `npm run test:web` (that script runs only
// web-flow.spec.ts); it runs under `npm run test:all`.
//
// Prerequisites (same as web-flow.spec.ts):
// 1. Start backend: poetry run flowfile_core (from root)
// 2. Start frontend: npm run dev:web (from flowfile_frontend)
// 3. Run tests: npx playwright test tests/share-link.spec.ts
import { test, expect, APIRequestContext, Page } from '@playwright/test';

const BASE_URL = process.env.TEST_URL || 'http://localhost:8080';
const API_URL = process.env.API_URL || 'http://localhost:63578';

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  const tokenData = await tokenResponse.json();
  return tokenData.access_token;
}

async function authPost(request: APIRequestContext, url: string, token: string, data?: any) {
  return request.post(url, {
    headers: { 'Authorization': `Bearer ${token}` },
    data
  });
}

// Vue Router hash routes don't reload the page, so AuthService only picks up the
// token on a full load — inject it, then reload.
async function navigateWithAuth(page: Page, token: string, targetUrl: string) {
  await page.goto(targetUrl);
  await page.waitForLoadState('networkidle');

  const expirationTime = Date.now() + (60 * 60 * 1000);
  await page.evaluate(({ token, expiration }: { token: string; expiration: number }) => {
    localStorage.setItem('auth_token', token);
    localStorage.setItem('auth_token_expiration', expiration.toString());
  }, { token, expiration: expirationTime });

  await page.reload();
  await page.waitForLoadState('networkidle');
}

async function createSharableFlow(request: APIRequestContext, token: string): Promise<number> {
  const createResponse = await authPost(
    request,
    `${API_URL}/editor/create_flow/?name=Share_Link_Test_Flow&register_in_catalog=false`,
    token
  );
  expect(createResponse.ok()).toBe(true);
  const flowId = await createResponse.json();

  await authPost(
    request,
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=manual_input&pos_x=100&pos_y=100`,
    token
  );
  await authPost(
    request,
    `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=2&node_type=select&pos_x=300&pos_y=100`,
    token
  );

  return flowId;
}

test.describe('Share link E2E Tests', () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test('share_link endpoint answers on the exact path (no trailing slash)', async ({ request }) => {
    const flowId = await createSharableFlow(request, authToken);

    const response = await request.get(`${API_URL}/editor/share_link?flow_id=${flowId}`, {
      headers: { 'Authorization': `Bearer ${authToken}` },
      maxRedirects: 0,
    });

    expect(response.status()).toBe(200);

    const body = await response.json();
    expect(body).toHaveProperty('url');
    expect(body).toHaveProperty('compatible');
    expect(Array.isArray(body.nodes_report)).toBe(true);
    expect(Array.isArray(body.warnings)).toBe(true);
    console.log(`✓ share_link returned ${body.hash_chars} hash chars, compatible=${body.compatible}`);
  });

  test('Share link button opens the dialog and reports the flow', async ({ page, request }) => {
    const flowId = await createSharableFlow(request, authToken);

    await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);

    const shareButton = page.getByRole('button', { name: 'Share flow as a browser link' });
    await expect(shareButton).toBeVisible({ timeout: 20000 });
    await shareButton.click();

    const dialog = page.getByRole('dialog').filter({ hasText: 'Share flow as a browser link' });
    await expect(dialog).toBeVisible();

    // The backend either mints a link or refuses because the flow is too large;
    // both are valid outcomes for this spec.
    const urlInput = dialog.getByLabel('Share link');
    const tooLarge = dialog.getByText('too large to share as a link');
    await expect(urlInput.or(tooLarge).first()).toBeVisible({ timeout: 20000 });

    if (await urlInput.isVisible()) {
      const value = await urlInput.inputValue();
      expect(value).toContain('#flow=');
      await expect(dialog.getByRole('button', { name: 'Copy' })).toBeVisible();
      await expect(dialog.getByRole('button', { name: 'Open in browser' })).toBeVisible();
      console.log(`✓ Minted share link of ${value.length} chars`);
    } else {
      console.log('✓ Backend refused the link as too large (still a valid report state)');
    }

    // The privacy line is unconditional.
    await expect(dialog.getByText('your data files do not')).toBeVisible();
  });
});
