import { test, expect, APIRequestContext, Page } from "@playwright/test";

import { API_URL, BASE_URL } from "./helpers/api";

/**
 * Two pages of one browser follow each other's sign-out through localStorage: a sign-out in one page, or a
 * session the server refuses there, sends the other page to the sign-in page too, on the address it is served
 * under (`/ui/` from the pip server), with a sentence that says why.
 *
 * Prerequisites (web mode, a server that signs people in with a password):
 *   1. `FLOWFILE_MODE=docker` on the pip server (`flowfile run ui`, the editor under /ui/), or core with the
 *      Vite server; FLOWFILE_ADMIN_USER and FLOWFILE_ADMIN_PASSWORD set to an account of that server
 *   2. Run: `TEST_URL=http://127.0.0.1:<port>/ui API_URL=http://127.0.0.1:<port> npx playwright test
 *      tests/sign-out-tabs.spec.ts`
 */

const USERNAME = process.env.FLOWFILE_ADMIN_USER;
const PASSWORD = process.env.FLOWFILE_ADMIN_PASSWORD;

interface Session {
  access_token: string;
  refresh_token: string;
}

async function signIn(request: APIRequestContext): Promise<Session> {
  const response = await request.post(`${API_URL}/auth/token`, {
    multipart: { username: USERNAME ?? "", password: PASSWORD ?? "" },
  });
  expect(response.ok()).toBe(true);
  const session: Session = await response.json();
  // A new server's administrator is asked to change the password first, which would cover the page.
  const headers = { Authorization: `Bearer ${session.access_token}` };
  const me = await (await request.get(`${API_URL}/auth/users/me`, { headers })).json();
  if (me.must_change_password) {
    const cleared = await request.put(`${API_URL}/auth/users/${me.id}`, {
      headers,
      data: { must_change_password: false },
    });
    expect(cleared.ok()).toBe(true);
  }
  return session;
}

/** Keep `session` in this page's localStorage once, before its first load; a reload keeps what the page left. */
async function seed(page: Page, session: Session) {
  await page.addInitScript(
    ({ session, expiration }) => {
      if (sessionStorage.getItem("e2e_seeded")) return;
      sessionStorage.setItem("e2e_seeded", "1");
      localStorage.setItem("auth_token", session.access_token);
      localStorage.setItem("auth_token_expiration", String(expiration));
      localStorage.setItem("auth_refresh_token", session.refresh_token);
      localStorage.setItem("auth_username", "e2e");
    },
    { session, expiration: Date.now() + 60 * 60 * 1000 },
  );
}

async function openDesigner(page: Page) {
  await page.goto(`${BASE_URL}/#/main/designer`);
  await page.waitForLoadState("load");
  await expect(page.locator('[data-testid="sidebar-search"]')).toBeVisible({ timeout: 30_000 });
}

/** The page is on the sign-in page of the address it was served under, saying the sign-in ended. */
async function expectSentToSignIn(page: Page) {
  await expect(page).toHaveURL(new RegExp(`^${BASE_URL.replace(/[.?]/g, "\\$&")}/?#/login`), {
    timeout: 15_000,
  });
  await expect(page.locator('[data-testid="login-ended"]')).toBeVisible({ timeout: 15_000 });
  await expect(page.locator("form #username")).toBeVisible();
}

test.describe("Signing out in one page of two", () => {
  test.skip(!USERNAME || !PASSWORD, "needs FLOWFILE_ADMIN_USER and FLOWFILE_ADMIN_PASSWORD");

  test("a sign-out in one page sends the other page to the sign-in page", async ({
    context,
    request,
  }) => {
    const session = await signIn(request);
    const pageA = await context.newPage();
    const pageB = await context.newPage();
    await seed(pageA, session);
    await seed(pageB, session);
    await openDesigner(pageA);
    await openDesigner(pageB);

    await pageA.locator('.footer-btn-wrapper[data-tooltip="Help & more"] button').click();
    await pageA.locator(".sidebar-more-item", { hasText: "Sign out" }).click();

    await expect(pageA).toHaveURL(/#\/login/);
    await expectSentToSignIn(pageB);
    expect(new URL(pageB.url()).pathname).toBe(new URL(BASE_URL + "/").pathname);
  });

  test("a session the server refuses in one page sends both pages to the sign-in page", async ({
    context,
    request,
  }) => {
    const session = await signIn(request);
    const pageA = await context.newPage();
    const pageB = await context.newPage();
    await seed(pageB, session);
    await openDesigner(pageB);

    // A token the server refuses and a refresh token it will not renew, so the first request ends the session.
    await seed(pageA, { access_token: "not-a-token", refresh_token: "not-a-refresh-token" });
    await pageA.goto(`${BASE_URL}/#/main/designer`);

    await expectSentToSignIn(pageA);
    await expectSentToSignIn(pageB);
    for (const page of [pageA, pageB]) {
      expect(new URL(page.url()).pathname).toBe(new URL(BASE_URL + "/").pathname);
      expect(await page.evaluate(() => localStorage.getItem("auth_token"))).toBeNull();
    }
  });
});
