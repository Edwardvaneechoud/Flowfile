import { test, expect } from "@playwright/test";

import {
  API_URL,
  authHeaders,
  closeFlow,
  createFlow,
  getAuthToken,
  nodeIdsByType,
  readNodeSettings,
} from "./helpers/api";
import { dragPaletteNode, minimizePalette, openFlow } from "./helpers/canvas";

/**
 * The Write Data drawer of a new node.
 *
 * A new node starts on the folder "." and its drawer swaps in a real folder once core answers
 * `files/default_path/`. Closing the drawer saves what a never-configured node shows, and core
 * resolves a saved "." against its own working directory, so the save has to wait for that answer.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578)
 *   2. Frontend: `npm run dev:web` (port 8080) or `npm run preview:web` (4173)
 *   3. Run: `npx playwright test tests/output-drawer.spec.ts`
 */

const DEFAULT_PATH = "**/files/default_path/**";

test("closing a new Write Data drawer before its folder arrives saves that folder", async ({
  page,
  request,
}) => {
  const token = await getAuthToken(request);
  const name = `output-drawer-${Date.now()}`;
  const flowId = await createFlow(request, token, name);
  try {
    await openFlow(page, token, flowId, name);
    await dragPaletteNode(page, "write", "output", 420, 260);
    await minimizePalette(page);
    const outputId = (await nodeIdsByType(request, token, flowId)).output[0];

    let answer = () => {};
    const held = new Promise<void>((resolve) => (answer = resolve));
    await page.route(DEFAULT_PATH, async (route) => {
      await held;
      await route.continue();
    });
    const asked = page.waitForRequest(DEFAULT_PATH);
    await page.locator(`.vue-flow__node[data-id="${outputId}"]`).dispatchEvent("dblclick");
    await asked;

    await page.locator(".vue-flow__pane").click({ position: { x: 200, y: 300 } });
    answer();
    await expect(page.locator(".node-settings-drawer")).toHaveCount(0);

    const folder = await request.get(`${API_URL}/files/default_path/`, {
      headers: authHeaders(token),
    });
    await expect
      .poll(async () => (await readNodeSettings(request, token, flowId, outputId))?.is_setup)
      .toBe(true);
    const saved = (await readNodeSettings(request, token, flowId, outputId)).output_settings;
    expect(saved.directory).toBe(await folder.json());
  } finally {
    await closeFlow(request, token, flowId);
  }
});
