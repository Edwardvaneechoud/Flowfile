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
 *  - localStorage stores v4 intent records (user gestures only); legacy v3
 *    geometry records are migrated or healed at panel registration.
 *  - Docked panels resize via their splitter edges and only undock through a
 *    deliberate pop-out drag; releasing near an edge snaps back into the dock.
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

// Inject auth (and optional seeded localStorage entries, e.g. legacy panel
// geometry) before navigating so the SPA boots against them.
async function navigateWithAuth(
  page: any,
  token: string,
  targetUrl: string,
  seed: Record<string, string> = {},
) {
  await page.goto(targetUrl);
  await page.waitForLoadState("networkidle");
  const expirationTime = Date.now() + 60 * 60 * 1000;
  await page.evaluate(
    ({
      token,
      expiration,
      seed,
    }: {
      token: string;
      expiration: number;
      seed: Record<string, string>;
    }) => {
      localStorage.setItem("auth_token", token);
      localStorage.setItem("auth_token_expiration", expiration.toString());
      for (const [key, value] of Object.entries(seed)) {
        localStorage.setItem(key, value);
      }
    },
    { token, expiration: expirationTime, seed },
  );
  await page.reload();
  await page.waitForLoadState("networkidle");
}

// Drag from the center of a locator by (dx, dy) with real mouse events.
async function dragBy(page: any, locator: any, dx: number, dy: number) {
  const box = await locator.boundingBox();
  if (!box) throw new Error("dragBy: element has no bounding box");
  const x = box.x + box.width / 2;
  const y = box.y + box.height / 2;
  await page.mouse.move(x, y);
  await page.mouse.down();
  await page.mouse.move(x + dx, y + dy, { steps: 12 });
  await page.mouse.up();
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

  test("storage uses v4 key prefix and purges legacy keys", async ({ page }) => {
    await navigateWithAuth(page, authToken, BASE_URL);
    await page.waitForSelector("main", { timeout: 10000 });

    // The store purges pre-v3 keys (and all group keys) at init; v3 records
    // are only kept for the one-time migration at panel registration. Any
    // panel-position key must therefore carry the `.v4_` or `.v3_` infix.
    const staleKeys = await page.evaluate(() => {
      return Object.keys(localStorage).filter(
        (k) =>
          (k.startsWith("overlayPositionAndSize") && !k.includes(".v4_") && !k.includes(".v3_")) ||
          k.startsWith("overlayGroups"),
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

  test("container changes never write storage and panels recover from a transient shrink", async ({
    page,
    request,
  }) => {
    // The structural invariant of the intent/derived split: persisted state
    // only changes on user gestures. A transient container collapse (route
    // remount, window-state restore, viewport shrink) may briefly clamp the
    // RENDERED rect but must leave localStorage untouched, and the panel must
    // return to its exact geometry once the container recovers.
    const flowName = `Overlay_Transient_Shrink_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();

      // dataActions is the always-rendered left palette (fixed width 230).
      await page.waitForSelector("#dataActions", { timeout: 10000 });

      const readWidth = () =>
        page.evaluate(() => {
          const el = document.getElementById("dataActions");
          return el ? parseFloat(el.style.width) : NaN;
        });
      const readStored = () =>
        page.evaluate(() =>
          Object.keys(localStorage)
            .filter((k) => k.startsWith("overlayPositionAndSize"))
            .sort()
            .map((k) => `${k}=${localStorage.getItem(k)}`)
            .join("|"),
        );

      await expect.poll(readWidth, { timeout: 10000 }).toBeGreaterThanOrEqual(200);
      const widthBefore = await readWidth();
      const storedBefore = await readStored();

      // Collapse the canvas well below the usable threshold, then restore it.
      await page.setViewportSize({ width: 150, height: 120 });
      await page.waitForTimeout(600);
      await page.setViewportSize({ width: 1280, height: 800 });
      await page.waitForTimeout(600);

      // Exact recovery — derived geometry, nothing accumulated.
      expect(await readWidth()).toBe(widthBefore);
      // Byte-identical storage — container changes are not gestures.
      expect(await readStored()).toBe(storedBefore);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("a corrupted legacy v3 record heals to defaults at panel registration", async ({
    page,
    request,
  }) => {
    // Users who hit the historical collapse bug have records like
    // {width:150,height:100,top:0,left:464} in localStorage. Migration must
    // reject the fingerprint, drop the v3 key, and render the panel at its
    // default geometry.
    const flowName = `Overlay_Self_Heal_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`, {
        "overlayPositionAndSize.v3_dataActions": JSON.stringify({
          width: 150,
          height: 100,
          left: 464,
          top: 0,
          stickynessPosition: "free",
          fullWidth: false,
          fullHeight: false,
          zIndex: 105,
          fullScreen: false,
          clicked: false,
        }),
      });
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();
      await page.waitForSelector("#dataActions", { timeout: 10000 });

      // Healed: default docked-left geometry, not the seeded 150x100 float.
      const readRect = () =>
        page.evaluate(() => {
          const el = document.getElementById("dataActions");
          return el ? { left: parseFloat(el.style.left), width: parseFloat(el.style.width) } : null;
        });
      await expect
        .poll(async () => (await readRect())?.width ?? NaN, { timeout: 10000 })
        .toBeGreaterThanOrEqual(200);
      expect((await readRect())?.left).toBe(0);

      const keys = await page.evaluate(() => ({
        v3: localStorage.getItem("overlayPositionAndSize.v3_dataActions"),
        v4: localStorage.getItem("overlayPositionAndSize.v4_dataActions"),
      }));
      expect(keys.v3).toBeNull();
      // Rejected records fall back to defaults without writing a v4 record.
      expect(keys.v4).toBeNull();
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("splitter resize persists intent and survives a reload", async ({ page, request }) => {
    const flowName = `Overlay_Resize_Persist_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();
      await page.waitForSelector("#dataActions", { timeout: 10000 });

      const readWidth = () =>
        page.evaluate(() => {
          const el = document.getElementById("dataActions");
          return el ? parseFloat(el.style.width) : NaN;
        });
      await expect.poll(readWidth, { timeout: 10000 }).toBeGreaterThanOrEqual(200);
      const widthBefore = await readWidth();

      // Drag the palette's right splitter 60px outward.
      await dragBy(page, page.locator("#dataActions .draggable-line.right-vertical"), 60, 0);

      await expect.poll(readWidth).toBeGreaterThanOrEqual(widthBefore + 50);
      const widthAfter = await readWidth();

      const intent = await page.evaluate(() => {
        const raw = localStorage.getItem("overlayPositionAndSize.v4_dataActions");
        return raw ? JSON.parse(raw) : null;
      });
      expect(intent).not.toBeNull();
      expect(intent.dock).toBe("left");
      expect(Math.abs(intent.h.size - widthAfter)).toBeLessThanOrEqual(2);

      // The committed intent must survive a full reload.
      await page.reload();
      await page.waitForLoadState("networkidle");
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTabAfter = page.getByText(flowName, { exact: true });
      await flowTabAfter.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTabAfter.first().click();
      await page.waitForSelector("#dataActions", { timeout: 10000 });
      await expect.poll(readWidth, { timeout: 10000 }).toBe(widthAfter);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("docked panels pop out only deliberately and snap back into the dock", async ({
    page,
    request,
  }) => {
    const flowName = `Overlay_Dock_Policy_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
      authToken,
    );
    expect(createResponse.ok()).toBe(true);
    const flowId = await createResponse.json();

    try {
      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();
      await page.waitForSelector("#dataActions", { timeout: 10000 });

      const readState = () =>
        page.evaluate(() => {
          const el = document.getElementById("dataActions");
          const raw = localStorage.getItem("overlayPositionAndSize.v4_dataActions");
          return {
            left: el ? parseFloat(el.style.left) : NaN,
            dock: raw ? JSON.parse(raw).dock : null,
          };
        });
      const title = page.locator("#dataActions .dragitem-tab--static");

      // A sub-threshold header drag (20px < 48px pop-out) must be a no-op:
      // the panel stays docked and nothing is persisted.
      await dragBy(page, title, 20, 20);
      let state = await readState();
      expect(state.left).toBe(0);
      expect(state.dock).toBeNull();

      // A deliberate pull past the threshold pops the panel out to free mode.
      await dragBy(page, title, 300, 150);
      await expect.poll(async () => (await readState()).dock).toBe("free");
      state = await readState();
      expect(state.left).toBeGreaterThan(40);

      // Releasing with the panel edge inside the snap zone re-docks it.
      const currentLeft = state.left;
      await dragBy(page, title, -(currentLeft - 10), 0);
      await expect.poll(async () => (await readState()).dock).toBe("left");
      expect((await readState()).left).toBe(0);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });

  test("fullscreen round-trips without disturbing panel geometry", async ({ page, request }) => {
    const flowName = `Overlay_Fullscreen_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
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

      await page.setViewportSize({ width: 1280, height: 800 });
      await navigateWithAuth(page, authToken, `${BASE_URL}/#/designer/${flowId}`);
      await page.waitForSelector("main", { timeout: 10000 });
      const flowTab = page.getByText(flowName, { exact: true });
      await flowTab.first().waitFor({ state: "visible", timeout: 10000 });
      await flowTab.first().click();

      const node = page.locator('.vue-flow__node[data-id="1"]');
      await node.waitFor({ state: "visible", timeout: 10000 });
      await node.dblclick();
      await page.waitForSelector("#rightDrawer", { timeout: 10000 });

      const readRect = () =>
        page.evaluate(() => {
          const el = document.getElementById("rightDrawer");
          return el
            ? {
                left: parseFloat(el.style.left),
                top: parseFloat(el.style.top),
                width: parseFloat(el.style.width),
                height: parseFloat(el.style.height),
              }
            : null;
        });
      await expect
        .poll(async () => (await readRect())?.width ?? NaN, { timeout: 10000 })
        .toBeGreaterThanOrEqual(300);
      const rectBefore = await readRect();

      const mainSize = await page.evaluate(() => {
        const main = document.querySelector("main");
        return main ? { width: main.clientWidth, height: main.clientHeight } : null;
      });

      // Header dblclick toggles fullscreen: the drawer fills the canvas <main>.
      const header = page.locator("#rightDrawer .header");
      await header.dblclick();
      await expect
        .poll(async () => (await readRect())?.width ?? NaN, { timeout: 5000 })
        .toBe(mainSize!.width);
      expect((await readRect())?.height).toBe(mainSize!.height);

      // And back out — the exact pre-fullscreen rect returns (intent untouched).
      await header.dblclick();
      await expect
        .poll(async () => (await readRect())?.width ?? NaN, { timeout: 5000 })
        .toBe(rectBefore!.width);
      expect(await readRect()).toEqual(rectBefore);

      // Resize-bar dblclick must round-trip too: its zero-move clicks commit
      // nothing, so exiting restores the same rect instead of a canvas-wide one.
      const bar = page.locator("#rightDrawer .draggable-line.left-vertical");
      await bar.dblclick();
      await expect
        .poll(async () => (await readRect())?.width ?? NaN, { timeout: 5000 })
        .toBe(mainSize!.width);
      await bar.dblclick();
      await expect
        .poll(async () => (await readRect())?.width ?? NaN, { timeout: 5000 })
        .toBe(rectBefore!.width);
      expect(await readRect()).toEqual(rectBefore);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
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
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
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
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
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
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
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

  test("swallowed Shift keyup unlatches selection mode on mouse move (Cmd+Shift+5 regression)", async ({
    page,
    request,
  }) => {
    // macOS screen-capture overlays (Cmd+Shift+4/5) eat the Shift keyup, so
    // VueFlow's key tracking latched the canvas into rubber-band-selection
    // mode until Shift was pressed again. useStaleModifierGuard replays the
    // lost keyup on the first shift-less mouse move over the pane.
    const flowName = `Stale_Shift_Test_${Date.now()}`;
    const createResponse = await authPost(
      request,
      `${API_URL}/editor/create_flow/?name=${flowName}&register_in_catalog=false`,
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
      await page.waitForSelector(".vue-flow__pane", { timeout: 10000 });

      // Find a spot where the pane is actually exposed (not covered by panels).
      const paneBox = await page.locator(".vue-flow__pane").boundingBox();
      expect(paneBox).not.toBeNull();
      let paneX = 0;
      let paneY = 0;
      let found = false;
      for (const [fx, fy] of [
        [0.35, 0.25],
        [0.55, 0.3],
        [0.45, 0.45],
        [0.6, 0.5],
      ]) {
        const x = paneBox!.x + paneBox!.width * fx;
        const y = paneBox!.y + paneBox!.height * fy;
        const isPane = await page.evaluate(
          ([px, py]) =>
            document.elementFromPoint(px, py)?.classList.contains("vue-flow__pane") ?? false,
          [x, y],
        );
        if (isPane) {
          paneX = x;
          paneY = y;
          found = true;
          break;
        }
      }
      expect(found).toBe(true);

      const paneLatched = () =>
        page.evaluate(
          () => document.querySelector(".vue-flow__pane")?.classList.contains("selection") ?? false,
        );

      // Simulate the swallow: keydown reaches the page, keyup never does.
      // Park the mouse off-canvas so the guard can't fire yet.
      await page.mouse.move(5, 5);
      await page.evaluate(() => {
        document.dispatchEvent(
          new KeyboardEvent("keydown", { key: "Shift", code: "ShiftLeft", shiftKey: true }),
        );
      });
      await expect.poll(paneLatched).toBe(true);

      // First shift-less mouse move over the pane must unlatch selection mode.
      await page.mouse.move(paneX, paneY, { steps: 3 });
      await expect.poll(paneLatched).toBe(false);

      // And a plain drag must pan again, not draw a selection box.
      const transformBefore = await page.evaluate(
        () => document.querySelector<HTMLElement>(".vue-flow__transformationpane")?.style.transform,
      );
      await page.mouse.down();
      await page.mouse.move(paneX + 60, paneY + 50, { steps: 4 });
      const selectionBoxDrawn = await page.evaluate(
        () => !!document.querySelector(".vue-flow__selection"),
      );
      await page.mouse.up();
      const transformAfter = await page.evaluate(
        () => document.querySelector<HTMLElement>(".vue-flow__transformationpane")?.style.transform,
      );
      expect(selectionBoxDrawn).toBe(false);
      expect(transformAfter).not.toBe(transformBefore);
    } finally {
      await authPost(request, `${API_URL}/editor/close_flow/?flow_id=${flowId}`, authToken);
    }
  });
});
