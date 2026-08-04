import { test, expect, APIRequestContext, Page } from '@playwright/test';

/**
 * E2E for the canvas OS-file drop: overlay hint, upload confirmation dialog,
 * node creation, and the unsupported-extension rejection.
 *
 * Prerequisites (same as web-flow.spec.ts):
 * 1. Start backend: poetry run flowfile_core
 * 2. Start frontend: npm run dev:web (from flowfile_frontend)
 * 3. Run: npx playwright test tests/file-drop.spec.ts
 */

const BASE_URL = process.env.TEST_URL || 'http://localhost:8080';
const API_URL = process.env.API_URL || 'http://localhost:63578';

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  return (await tokenResponse.json()).access_token;
}

async function createFreshFlow(request: APIRequestContext, token: string): Promise<number> {
  const response = await request.post(
    `${API_URL}/editor/create_flow/?name=FileDrop_E2E_${Date.now()}&register_in_catalog=false`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  if (!response.ok()) throw new Error(`create_flow failed: ${response.status()}`);
  return response.json();
}

async function openDesigner(page: Page, token: string, flowId: number) {
  await page.goto(`${BASE_URL}/#/main/designer`);
  await page.waitForLoadState('networkidle');
  await page.evaluate(
    ({ token, expiration, flowId }: { token: string; expiration: number; flowId: number }) => {
      localStorage.setItem('auth_token', token);
      localStorage.setItem('auth_token_expiration', expiration.toString());
      // Keep the Getting Started tour out of the way of the canvas.
      localStorage.setItem('flowfile-tutorial-dismissed', 'true');
      localStorage.setItem('flowfile-tutorial-banner-dismissed', 'true');
      sessionStorage.setItem('last_flow_id', String(flowId));
    },
    { token, expiration: Date.now() + 60 * 60 * 1000, flowId },
  );
  await page.reload();
  await page.waitForLoadState('networkidle');
  await expect(page.locator('.vue-flow')).toBeVisible({ timeout: 15000 });
}

// The DataTransfer is stashed on window so drop can reuse the very object the
// dragenter/dragover pair carried, the way a real OS drag does.
async function startFileDrag(page: Page, name: string, type: string, content: string) {
  await page.evaluate(
    ({ name, type, content }: { name: string; type: string; content: string }) => {
      const canvas = document.querySelector('.vue-flow');
      if (!canvas) throw new Error('canvas not found');
      const dt = new DataTransfer();
      dt.items.add(new File([content], name, { type }));
      (window as unknown as { __ffDropDt: DataTransfer }).__ffDropDt = dt;
      const rect = canvas.getBoundingClientRect();
      const clientX = rect.left + rect.width / 2;
      const clientY = rect.top + rect.height / 2;
      for (const kind of ['dragenter', 'dragover']) {
        canvas.dispatchEvent(
          new DragEvent(kind, { bubbles: true, cancelable: true, dataTransfer: dt, clientX, clientY }),
        );
      }
    },
    { name, type, content },
  );
}

async function dropFile(page: Page) {
  await page.evaluate(() => {
    const canvas = document.querySelector('.vue-flow');
    const dt = (window as unknown as { __ffDropDt: DataTransfer }).__ffDropDt;
    if (!canvas || !dt) throw new Error('drag was not started');
    const rect = canvas.getBoundingClientRect();
    canvas.dispatchEvent(
      new DragEvent('drop', {
        bubbles: true,
        cancelable: true,
        dataTransfer: dt,
        clientX: rect.left + rect.width / 2,
        clientY: rect.top + rect.height / 2,
      }),
    );
  });
}

test.describe('Canvas file drop', () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test('drops a CSV and adds a Read node', async ({ page, request }) => {
    const flowId = await createFreshFlow(request, authToken);
    await openDesigner(page, authToken, flowId);

    const fileName = `drop_${Date.now()}.csv`;
    const nodesBefore = await page.locator('.vue-flow__node').count();

    await startFileDrag(page, fileName, 'text/csv', 'a,b\n1,2\n');

    const overlay = page.locator('.file-drop-overlay');
    await expect(overlay).toBeVisible();
    await expect(overlay).toContainText('1');

    await dropFile(page);

    const dialog = page.locator('.file-drop-dialog');
    await expect(dialog).toBeVisible();
    await expect(dialog).toContainText(fileName);
    await expect(dialog).toContainText(' B');
    await expect(dialog).toContainText('Max');

    await dialog.getByRole('button', { name: /Upload and add/ }).click();

    await expect(page.locator('.vue-flow__node')).toHaveCount(nodesBefore + 1, { timeout: 20000 });
  });

  test('rejects an unsupported extension', async ({ page, request }) => {
    const flowId = await createFreshFlow(request, authToken);
    await openDesigner(page, authToken, flowId);

    const nodesBefore = await page.locator('.vue-flow__node').count();

    await startFileDrag(page, `drop_${Date.now()}.json`, 'application/json', '{}');
    await dropFile(page);

    await expect(page.locator('.el-message--error')).toBeVisible();
    await expect(page.locator('.file-drop-dialog')).toHaveCount(0);
    await expect(page.locator('.vue-flow__node')).toHaveCount(nodesBefore);
  });
});
