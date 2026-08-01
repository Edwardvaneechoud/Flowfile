import { test, expect, APIRequestContext, Page } from '@playwright/test';

/**
 * E2E test for the interactive Getting Started tutorial.
 *
 * Walks the whole tour end-to-end and exercises the self-healing behaviors:
 * blocked backdrop on reading steps, wrong-node drag rejection, auto-skip of
 * already-satisfied steps, paused pill on route change, and completion
 * persistence.
 *
 * Prerequisites (same as web-flow.spec.ts):
 * 1. Start backend: poetry run flowfile_core (fresh instance recommended)
 * 2. Start frontend: npm run dev:web (from flowfile_frontend)
 * 3. Run: npx playwright test tests/tutorial.spec.ts
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

// Pin a freshly created (empty) flow as the designer session so accumulated
// backend state from earlier runs can't satisfy tutorial steps prematurely.
async function createFreshFlow(request: APIRequestContext, token: string): Promise<number> {
  const response = await request.post(
    `${API_URL}/editor/create_flow/?name=Tutorial_E2E_${Date.now()}&register_in_catalog=false`,
    { headers: { Authorization: `Bearer ${token}` } },
  );
  if (!response.ok()) throw new Error(`create_flow failed: ${response.status()}`);
  return response.json();
}

async function navigateWithAuth(page: Page, token: string, targetUrl: string, flowId?: number) {
  await page.goto(targetUrl);
  await page.waitForLoadState('networkidle');
  const expirationTime = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({ token, expiration, flowId }: { token: string; expiration: number; flowId?: number }) => {
      localStorage.setItem('auth_token', token);
      localStorage.setItem('auth_token_expiration', expiration.toString());
      localStorage.removeItem('flowfile-tutorial-completed');
      localStorage.removeItem('flowfile-tutorial-banner-dismissed');
      localStorage.removeItem('flowfile-tutorial-dismissed');
      if (flowId !== undefined) sessionStorage.setItem('last_flow_id', String(flowId));
    },
    { token, expiration: expirationTime, flowId },
  );
  await page.reload();
  await page.waitForLoadState('networkidle');
}

async function expectStep(page: Page, title: string) {
  await expect(page.locator('.tutorial-tooltip .tooltip-title')).toHaveText(title, {
    timeout: 15000,
  });
}

// Palette items are native HTML5 drag sources; drive the app's own dragstart ->
// drop pipeline with a real DataTransfer so the backend insert happens too.
async function dragPaletteNode(page: Page, item: string, x: number, y: number) {
  await page.evaluate(
    ({ item, x, y }: { item: string; x: number; y: number }) => {
      const source = document.querySelector(`[data-tutorial-node='${item}']`);
      const canvas = document.querySelector('.vue-flow');
      if (!source || !canvas) throw new Error(`missing drag source or canvas for ${item}`);
      const dt = new DataTransfer();
      source.dispatchEvent(new DragEvent('dragstart', { bubbles: true, dataTransfer: dt }));
      const rect = canvas.getBoundingClientRect();
      const cx = rect.left + x;
      const cy = rect.top + y;
      canvas.dispatchEvent(
        new DragEvent('dragover', { bubbles: true, cancelable: true, dataTransfer: dt, clientX: cx, clientY: cy }),
      );
      canvas.dispatchEvent(
        new DragEvent('drop', { bubbles: true, cancelable: true, dataTransfer: dt, clientX: cx, clientY: cy }),
      );
    },
    { item, x, y },
  );
}

// Connect two canvas nodes with a real mouse drag between their handles.
// Connect steps fit the viewport on entry (300ms animation), so wait for the
// handle position to stabilize before reading coordinates.
async function stableBox(page: Page, locator: ReturnType<Page['locator']>) {
  let prev = await locator.boundingBox();
  for (let i = 0; i < 15; i++) {
    await page.waitForTimeout(200);
    const cur = await locator.boundingBox();
    if (prev && cur && Math.abs(cur.x - prev.x) < 1 && Math.abs(cur.y - prev.y) < 1) return cur;
    prev = cur;
  }
  return prev;
}

async function connectNodes(page: Page, sourceNodeId: string, targetNodeId: string) {
  const sourceHandle = page.locator(
    `.vue-flow__node[data-id='${sourceNodeId}'] .vue-flow__handle[data-handlepos='right']`,
  );
  const targetHandle = page.locator(
    `.vue-flow__node[data-id='${targetNodeId}'] .vue-flow__handle[data-handlepos='left']`,
  );
  const src = await stableBox(page, sourceHandle);
  const dst = await stableBox(page, targetHandle);
  if (!src || !dst) throw new Error('handle not visible');
  await page.mouse.move(src.x + src.width / 2, src.y + src.height / 2);
  await page.mouse.down();
  await page.mouse.move((src.x + dst.x) / 2, (src.y + dst.y) / 2, { steps: 5 });
  await page.mouse.move(dst.x + dst.width / 2, dst.y + dst.height / 2, { steps: 5 });
  await page.mouse.up();
}

async function nodeIdsByItem(page: Page): Promise<Record<string, string[]>> {
  return page.evaluate(() => {
    const app = (document.querySelector('#app') as any).__vue_app__;
    const pinia = app.config.globalProperties.$pinia;
    const tutorial = pinia._s.get('tutorial');
    const byItem: Record<string, string[]> = {};
    const ctx = tutorial.context;
    for (const [item, ids] of Object.entries(ctx.nodesByItem as Record<string, number[]>)) {
      byItem[item] = ids.map(String);
    }
    return byItem;
  });
}

test.describe('Interactive tutorial', () => {
  test.use({ permissions: ['clipboard-read', 'clipboard-write'] });
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test('happy path with recovery behaviors', async ({ page, request }) => {
    test.slow();
    const flowId = await createFreshFlow(request, authToken);
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/main/home`, flowId);

    // Landing-page banner is the promoted entry point
    const banner = page.locator('.tutorial-banner');
    await expect(banner).toBeVisible();
    await banner.locator('.banner-cta').click();

    // Welcome step: centered, veiled, blocking
    await expectStep(page, 'Welcome to Flowfile!');
    await expect(page.locator('.tutorial-blocker--full')).toBeVisible();

    // A real click on the palette must be swallowed by the backdrop
    const inputCategory = page.locator("[data-tutorial-category='input'] .category-header");
    await inputCategory.click({ force: true, timeout: 5000 }).catch(() => undefined);
    await expect(
      page.locator("[data-tutorial-category='input'] .category-content"),
    ).toBeVisible();

    await page.locator('.tutorial-tooltip .next-btn').click();

    // The designer auto-opens a flow, so create-flow is auto-skipped
    await expectStep(page, 'Your Workspace');
    await page.locator('.tutorial-tooltip .next-btn').click();

    // Drag step: wrong node first -> hint, no advance; context remembers it
    await expectStep(page, 'Add Your Data');
    await expect(page.locator('.tutorial-tooltip .next-btn')).toHaveCount(0);
    await expect(page.locator('.tutorial-tooltip .skip-step-btn')).toBeVisible();

    await dragPaletteNode(page, 'group_by', 620, 140);
    await expect(page.locator('.tutorial-tooltip .tooltip-notice--warn')).toContainText(
      'different node',
    );
    await expectStep(page, 'Add Your Data');

    await dragPaletteNode(page, 'manual_input', 420, 320);
    await expectStep(page, 'Open the Node Settings');

    const ids = await nodeIdsByItem(page);
    const manualId = ids['manual_input'][0];
    const groupById = ids['group_by'][0];

    // The spotlight must wrap the node the user actually created
    await expect(page.locator('.tutorial-spotlight')).toBeVisible();
    await page.locator(`.vue-flow__node[data-id='${manualId}']`).click();
    await expectStep(page, 'Add Sample Data');

    // The copy button puts the sample JSON on the clipboard
    await page.locator('.tutorial-tooltip .tutorial-copy-btn').click();
    await expect(page.locator('.tutorial-tooltip .tutorial-copy-btn')).toContainText('Copied');
    const clipboard = await page.evaluate(() => navigator.clipboard.readText());
    const rows = JSON.parse(clipboard);
    expect(rows).toHaveLength(6);
    expect(rows[0]).toEqual({ country: 'USA', product: 'Widget', revenue: 1000 });

    await page.locator('.tutorial-tooltip .next-btn').click();

    // add-group-by is auto-skipped: the "wrong" drag already satisfied it
    await expectStep(page, 'Connect Your Nodes');
    await connectNodes(page, manualId, groupById);
    await expectStep(page, 'Open Group By');

    await page.locator(`.vue-flow__node[data-id='${groupById}']`).click();
    await expectStep(page, 'Configure the Aggregation');
    await page.locator('.tutorial-tooltip .next-btn').click();

    await expectStep(page, 'Add a Write Data Node');
    await dragPaletteNode(page, 'output', 820, 320);
    await expectStep(page, 'Connect the Output');

    const outputId = (await nodeIdsByItem(page))['output'][0];
    await connectNodes(page, groupById, outputId);
    await expectStep(page, 'Open Write Data');

    await page.locator(`.vue-flow__node[data-id='${outputId}']`).click();
    await expectStep(page, 'Set the Output File');
    await page.locator('.tutorial-tooltip .next-btn').click();

    // Flow settings: gear opens the modal, explainer step, close via Escape
    await expectStep(page, 'Check the Flow Settings');
    await page.locator("[data-tutorial='settings-btn']").click();
    await expectStep(page, 'Execution Settings');
    await page.keyboard.press('Escape');
    await page.locator('.tutorial-tooltip .next-btn').click();

    // Run: spotlight mode, advance on flow-run-started, then run-completed
    await expectStep(page, 'Run Your Flow');
    await page.locator("[data-tutorial='run-btn'] button").first().click();
    await expectStep(page, 'Watch It Execute');
    await expectStep(page, 'Explore Your Results');
    await page.locator('.tutorial-tooltip .next-btn').click();

    await expectStep(page, 'Tidy Up with Layout Controls');
    await page.locator('.tutorial-tooltip .next-btn').click();

    // Silent save fires flow-saved: save-flow advances and the satisfied
    // save-location (dialog) step auto-skips
    await expectStep(page, 'Save Your Flow');
    await page.locator("[data-tutorial='save-main-btn']").click();
    await expectStep(page, 'Generate Python Code');

    await page.locator("[data-tutorial='generate-code-btn']").click();
    await expectStep(page, 'Your Python Code');
    await page.locator('.tutorial-tooltip .next-btn').click();

    await expectStep(page, 'Congratulations!');
    await page.locator('.tutorial-tooltip .next-btn').click();

    // Clean teardown + persisted completion
    await expect(page.locator('.tutorial-overlay')).toHaveCount(0);
    await expect(page.locator('.tutorial-blocker')).toHaveCount(0);
    const completed = await page.evaluate(() =>
      localStorage.getItem('flowfile-tutorial-completed'),
    );
    expect(completed).toContain('getting-started');
  });

  test('paused pill on route change, resume restores the step', async ({ page, request }) => {
    const flowId = await createFreshFlow(request, authToken);
    await navigateWithAuth(page, authToken, `${BASE_URL}/#/main/home`, flowId);

    await page.locator('.tutorial-banner .banner-cta').click();
    await expectStep(page, 'Welcome to Flowfile!');

    await page.evaluate(() => {
      window.location.hash = '#/main/home';
    });
    await expect(page.locator('.tutorial-paused-pill')).toBeVisible();
    await expect(page.locator('.tutorial-overlay')).toHaveCount(0);
    await expect(page.locator('.tutorial-blocker')).toHaveCount(0);

    await page.locator('.tutorial-paused-pill .pill-btn--primary').click();
    await expectStep(page, 'Welcome to Flowfile!');
    await expect(page.locator('.tutorial-paused-pill')).toHaveCount(0);

    // Exit ends the tour and removes everything immediately
    await page.locator('.tutorial-tooltip .skip-btn').click();
    await expect(page.locator('.tutorial-overlay')).toHaveCount(0);
    await expect(page.locator('.tutorial-tooltip')).toHaveCount(0);
  });
});
