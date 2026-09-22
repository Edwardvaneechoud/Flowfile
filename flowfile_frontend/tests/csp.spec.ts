// Desktop CSP parity: the Vite dev/preview servers send the Tauri CSP from
// tauri.conf.json (vite.config.mjs), so anything the packaged app would block
// shows up here as a `securitypolicyviolation` instead of at release time.
// The chart test mounts graphic-walker, whose bundle injects a stylesheet
// <link> into its shadow root — the CDN URL it used to point at is exactly
// what the desktop build blocked; leafletCssLocal() now rewrites it.
//
// Prerequisites (same as web-flow.spec.ts — no webServer block in the config):
// 1. Backend: poetry run flowfile_core (port 63578)
// 2. Worker: poetry run flowfile_worker (port 63579) — the chart needs data
//    to mount graphic-walker; without a worker that test skips, not passes.
// 3. Frontend: npm run dev:web (or the preview server, TEST_URL=http://localhost:4173)
// 4. Run: npx playwright test tests/csp.spec.ts
import { test, expect, APIRequestContext, Page } from "@playwright/test";
import { readFileSync } from "node:fs";
import path from "node:path";

const BASE_URL = process.env.TEST_URL || "http://localhost:8080";
const API_URL = process.env.API_URL || "http://localhost:63578";
const CHART_NAME = "e2e_csp_parity_chart";

const DESKTOP_CSP: string = JSON.parse(
  readFileSync(path.resolve(__dirname, "../src-tauri/tauri.conf.json"), "utf-8"),
).app.security.csp;
// The only dev-server addition to the desktop policy (Vite's HMR websocket).
const DEV_ONLY_SOURCES = "ws://localhost:* ws://127.0.0.1:* ";

async function getAuthToken(request: APIRequestContext): Promise<string> {
  const tokenResponse = await request.post(`${API_URL}/auth/token`);
  if (!tokenResponse.ok()) {
    throw new Error(`Failed to get auth token: ${tokenResponse.status()}`);
  }
  return (await tokenResponse.json()).access_token;
}

function authHeaders(token: string) {
  return { Authorization: `Bearer ${token}` };
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

// Violations are recorded in the page (the event is composed, so a blocked
// <link> inside graphic-walker's shadow root still reaches the document) and
// mirrored from the console, which is where Chromium reports them too.
async function collectCspViolations(page: Page): Promise<() => Promise<string[]>> {
  const fromConsole: string[] = [];
  page.on("console", (msg) => {
    if (msg.text().includes("Content Security Policy")) fromConsole.push(msg.text());
  });
  await page.addInitScript(() => {
    const store: string[] = [];
    (window as any).__cspViolations = store;
    document.addEventListener("securitypolicyviolation", (e) => {
      store.push(`${e.violatedDirective} blocked ${e.blockedURI}`);
    });
  });
  return async () => {
    const fromPage: string[] = await page.evaluate(() => (window as any).__cspViolations ?? []);
    return [...fromPage, ...fromConsole];
  };
}

test.describe("Desktop CSP parity", () => {
  test("the frontend server sends the Tauri CSP verbatim", async ({ request }) => {
    const response = await request.get(`${BASE_URL}/`);
    const header = response.headers()["content-security-policy"];
    expect(header, "no Content-Security-Policy header — see server.headers in vite.config.mjs").toBeDefined();
    expect(header.replace(DEV_ONLY_SOURCES, "")).toBe(DESKTOP_CSP);
  });

  test("designer and catalog load without CSP violations", async ({ page, request }) => {
    const token = await getAuthToken(request);
    const violations = await collectCspViolations(page);

    await navigateWithAuth(page, token, `${BASE_URL}/#/main/designer`);
    await expect(page.locator(".vue-flow").first()).toBeVisible({ timeout: 15000 });
    await page.goto(`${BASE_URL}/#/main/catalog?tab=visuals`);
    await page.waitForLoadState("networkidle");

    expect(await violations()).toEqual([]);
  });

  test("opening a chart mounts graphic-walker without CSP violations", async ({ page, request }) => {
    const token = await getAuthToken(request);
    const created = await request.post(`${API_URL}/catalog/visualizations`, {
      headers: authHeaders(token),
      data: { name: CHART_NAME, spec: [], source_type: "sql", sql_query: "SELECT 1 AS x" },
    });
    expect(created.ok(), `create visualization: ${created.status()}`).toBeTruthy();
    const vizId: number = (await created.json()).id;

    try {
      const fields = await request.post(`${API_URL}/catalog/visualizations/${vizId}/fields`, {
        headers: authHeaders(token),
      });
      test.skip(
        !fields.ok(),
        `chart fields unavailable (${fields.status()}) — flowfile_worker is not running, so graphic-walker cannot mount`,
      );

      const violations = await collectCspViolations(page);
      await navigateWithAuth(page, token, `${BASE_URL}/#/main/catalog?tab=visuals`);
      await page.getByText(CHART_NAME, { exact: true }).first().click();

      // graphic-walker renders inside a shadow root; its stylesheet <link> is the
      // element the desktop CSP used to block. Wait for it to settle either way.
      const linkHandle = await page.waitForFunction(
        () => {
          for (const host of document.querySelectorAll("*")) {
            const link = host.shadowRoot?.querySelector<HTMLLinkElement>("link[rel=stylesheet]");
            if (link && (link.sheet || (window as any).__cspViolations.length)) return link;
          }
          return null;
        },
        null,
        { timeout: 30000 },
      );
      const link = await linkHandle.evaluate((el) => ({
        href: (el as HTMLLinkElement).href,
        loaded: !!(el as HTMLLinkElement).sheet,
      }));

      expect(await violations()).toEqual([]);
      expect(new URL(link.href).origin).toBe(new URL(BASE_URL).origin);
      expect(link.loaded).toBe(true);
    } finally {
      await request.delete(`${API_URL}/catalog/visualizations/${vizId}`, { headers: authHeaders(token) });
    }
  });
});
