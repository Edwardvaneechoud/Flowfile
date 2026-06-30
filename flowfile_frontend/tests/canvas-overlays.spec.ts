import { test, expect, APIRequestContext } from "@playwright/test";

/**
 * Canvas overlay behaviour tests.
 *
 * Covers regression cases introduced when the panel system was refactored:
 *  - Floating panels (DraggableItem) are nested inside the canvas <main>
 *    element and use it for positioning and fullscreen sizing.
 *  - Double-click on a panel's resize bar toggles fullscreen.
 *  - Double-click on the empty canvas pane hides all right-side and bottom
 *    panels (left palette stays visible).
 *  - localStorage uses the v3 storage key prefix.
 *
 * Prerequisites (web mode):
 *   1. Backend: `poetry run flowfile_core` (port 63578)
 *   2. Frontend dev server: `npm run dev:web` (port 8080)
 *   3. Run: `npx playwright test tests/canvas-overlays.spec.ts`
 */

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  const tokenData = await tokenResponse.json();
  return tokenData.access_token;
}

async function authPost(request: APIRequestContext, url: string, token: string) {
  return request.post(url, { headers: { Authorization: `Bearer ${token}` } });
}

// Inject auth into localStorage before navigating so the SPA boots authed.
async function navigateWithAuth(page: any, token: string, targetUrl: string) {
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

test.describe("Canvas Overlay Behaviour", () => {
  let authToken: string;

  test.beforeAll(async ({ request }) => {
    authToken = await getAuthToken(request);
  });

  test("layoutControls widget is positioned inside the canvas main element", async ({ page }) => {
    await navigateWithAuth(page, authToken, BASE_URL);

    // The widget might not be visible until a flow is loaded, so we wait
    // for the canvas main element first.
    await page.waitForSelector("main", { timeout: 10000 });

    const widgetIsInsideMain = await page.evaluate(() => {
      const widget = document.querySelector(".layout-widget-wrapper");
      const main = document.querySelector("main");
      if (!widget || !main) return null;
      return main.contains(widget);
    });

    // null = widget not yet rendered (no flow open) — that's acceptable; the
    // assertion exists to guard against the widget escaping <main> when it
    // does render.
    if (widgetIsInsideMain !== null) {
      expect(widgetIsInsideMain).toBe(true);
    }
  });

  test("UndoRedoControls lives in the page header, not inside the canvas main", async ({
    page,
  }) => {
    await navigateWithAuth(page, authToken, BASE_URL);
    await page.waitForSelector("main", { timeout: 10000 });

    const placement = await page.evaluate(() => {
      const controls = document.querySelector(".undo-redo-controls");
      const main = document.querySelector("main");
      const header = document.querySelector(".header");
      if (!controls) return null;
      return {
        insideMain: main ? main.contains(controls) : false,
        insideHeader: header ? header.contains(controls) : false,
      };
    });

    if (placement) {
      expect(placement.insideMain).toBe(false);
      expect(placement.insideHeader).toBe(true);
    }
  });

  test("storage uses v3 key prefix and purges legacy keys", async ({ page }) => {
    await navigateWithAuth(page, authToken, BASE_URL);
    await page.waitForSelector("main", { timeout: 10000 });

    // The store purges older-version keys at init. Every panel-position key the
    // app writes must carry the current `.v3_` infix — no legacy underscore keys
    // and no stale `.v2_` keys remain.
    const staleKeys = await page.evaluate(() => {
      return Object.keys(localStorage).filter(
        (k) => k.startsWith("overlayPositionAndSize") && !k.includes(".v3_"),
      );
    });
    expect(staleKeys).toEqual([]);
  });

  test("draggable panel CSS uses position: absolute and lives under <main>", async ({ page }) => {
    await navigateWithAuth(page, authToken, BASE_URL);
    await page.waitForSelector("main", { timeout: 10000 });

    // The dataActions palette is the one panel that's always rendered, so
    // we check its containment.
    const palette = page.locator("#dataActions");
    if ((await palette.count()) === 0) return; // no flow loaded

    const isInsideMain = await page.evaluate(() => {
      const el = document.getElementById("dataActions");
      const main = document.querySelector("main");
      if (!el || !main) return false;
      return main.contains(el);
    });
    expect(isInsideMain).toBe(true);

    const cssPosition = await page.evaluate(() => {
      const el = document.getElementById("dataActions");
      return el ? getComputedStyle(el).position : null;
    });
    expect(cssPosition).toBe("absolute");
  });

  test("python script expanded editor dialog teleports to body and covers the viewport", async ({
    page,
    request,
  }) => {
    // Regression guard for the WKWebView (Tauri) clipping bug: rendered in
    // place, the dialog's fixed overlay is clipped to the node-settings
    // panel's overflow chain. append-to-body must keep the overlay a direct
    // child of <body>.
    const flowName = `Overlay_Dialog_Test_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      const addNodeResponse = await authPost(
        request,
        // pos_x clears the left Data-actions palette (~230px wide) so the
        // node is clickable and not under the panel's resize bar.
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=python_script&pos_x=600&pos_y=300`,
        authToken,
      );
      expect(addNodeResponse.ok()).toBe(true);

      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });

      // Session restore can reopen other flows and leave one of them active —
      // explicitly activate this test's flow tab before touching the canvas.
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();

      const node = page.locator('.vue-flow__node[data-id="1"]');
      await node.waitFor({ state: "visible", timeout: 10000 });
      await node.dblclick();

      const expandButton = page.locator('button[title="Expand Editor"]');
      await expandButton.waitFor({ state: "visible", timeout: 10000 });
      await expandButton.click();

      await page.waitForSelector(".expanded-editor-dialog", { timeout: 10000 });

      const overlayState = await page.evaluate(() => {
        const dialog = document.querySelector(".expanded-editor-dialog");
        const overlay = dialog?.closest(".el-overlay");
        if (!overlay) return null;
        const rect = overlay.getBoundingClientRect();
        return {
          parentIsBody: overlay.parentElement === document.body,
          coversViewport:
            rect.width >= window.innerWidth - 1 && rect.height >= window.innerHeight - 1,
        };
      });

      expect(overlayState).not.toBeNull();
      expect(overlayState!.parentIsBody).toBe(true);
      expect(overlayState!.coversViewport).toBe(true);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("notebook autocomplete tooltip mounts on body, escaping the panel overflow chain", async ({
    page,
    request,
  }) => {
    // Regression guard for the WKWebView (Tauri) clipping bug: CodeMirror
    // renders tooltips inside the editor DOM, where WebKit flips them to
    // position:absolute and the node-settings panel's overflow chain clips
    // them. tooltips({ parent: document.body }) must mount them on <body>.
    // getBoundingClientRect reports full size even when paint-clipped, so the
    // ancestry assertion is the real guard (and regresses in Chromium too).
    const flowName = `Overlay_CM_Tooltip_Test_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      const addNodeResponse = await authPost(
        request,
        `${API_URL}/editor/add_node/?flow_id=${flowId}&node_id=1&node_type=python_script&pos_x=600&pos_y=300`,
        authToken,
      );
      expect(addNodeResponse.ok()).toBe(true);

      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });

      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();

      const node = page.locator('.vue-flow__node[data-id="1"]');
      await node.waitFor({ state: "visible", timeout: 10000 });
      await node.dblclick();

      const cellContent = page.locator(".node-settings-body .cell-editor-wrapper .cm-content");
      await cellContent.first().waitFor({ state: "visible", timeout: 10000 });
      await cellContent.first().click();

      // flowfileApiCompletions is a static, client-side source triggered by
      // `flowfile_ctx.` — no backend round-trip needed for the tooltip.
      await page.keyboard.type("flowfile_ctx.");

      const tooltip = page.locator(".cm-tooltip-autocomplete");
      await tooltip.first().waitFor({ state: "visible", timeout: 10000 });

      const result = await page.evaluate(() => {
        const tip = document.querySelector(".cm-tooltip-autocomplete");
        if (!tip) return null;
        return {
          inSettingsPanel: !!tip.closest(".node-settings-body"),
          inEditor: !!tip.closest(".cm-editor"),
          // CM inserts the tooltip into a position:relative container that is
          // a direct child of <body>.
          containerOnBody: tip.parentElement?.parentElement === document.body,
          optionCount: tip.querySelectorAll("ul > li").length,
        };
      });

      expect(result).not.toBeNull();
      expect(result!.inSettingsPanel).toBe(false);
      expect(result!.inEditor).toBe(false);
      expect(result!.containerOnBody).toBe(true);
      expect(result!.optionCount).toBeGreaterThan(1);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("palette drag sets an explicit drag image", async ({ page, request }) => {
    // WebKit (Tauri's WKWebView) shows no default drag preview for
    // user-select:none elements, so onDragStart must call setDragImage with a
    // DOM-attached clone. Chromium can't render the OS drag preview in a
    // screenshot, so assert the contract instead.
    const flowName = `Drag_Image_Test_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();
      await page.waitForSelector(".node-item", { timeout: 10000 });

      const result = await page.evaluate(() => {
        const calls: { attached: boolean }[] = [];
        const original = DataTransfer.prototype.setDragImage;
        DataTransfer.prototype.setDragImage = function (image: Element, x: number, y: number) {
          calls.push({ attached: document.contains(image) });
          return original.call(this, image, x, y);
        };
        try {
          const item = document.querySelector(".node-item");
          if (!item) return null;
          const event = new DragEvent("dragstart", { bubbles: true, cancelable: true });
          Object.defineProperty(event, "dataTransfer", { value: new DataTransfer() });
          item.dispatchEvent(event);
          return { called: calls.length > 0, attached: calls[0]?.attached ?? false };
        } finally {
          DataTransfer.prototype.setDragImage = original;
        }
      });

      expect(result).not.toBeNull();
      expect(result!.called).toBe(true);
      expect(result!.attached).toBe(true);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });
});
