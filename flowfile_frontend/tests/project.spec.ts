import { test, expect } from "@playwright/test";
import fs from "fs";
import os from "os";
import path from "path";

import { API_URL, authHeaders, getAuthToken } from "./helpers/api";
import { login } from "./helpers/canvas";

/**
 * The Project page saves a version and then reads clean, with files Flowfile did not write (a
 * README, `.DS_Store`) in the folder. Opening a project replaces the active project of the core it
 * reaches, so the spec skips unless API_URL and TEST_URL both name a disposable core and preview,
 * as the E2E workflow starts them.
 */

const RUN_ID = process.env.E2E_RUN_ID || Date.now().toString(36);
const SECRET = `PROJECT_E2E_${RUN_ID.toUpperCase()}`;
const MESSAGE = `Add secret ${RUN_ID}`;

test.describe("Project page", () => {
  test.skip(
    !process.env.API_URL || !process.env.TEST_URL,
    "set API_URL and TEST_URL to a disposable stack; the spec replaces the active project",
  );

  let token = "";
  let root = "";

  test.beforeAll(async ({ request }) => {
    token = await getAuthToken(request);
    root = fs.mkdtempSync(path.join(os.tmpdir(), "ff-project-e2e-"));
    const init = await request.post(`${API_URL}/project/init`, {
      headers: authHeaders(token),
      data: { folder_path: root, name: `E2E ${RUN_ID}` },
    });
    expect(init.status()).toBe(201);
    fs.writeFileSync(path.join(root, "README.md"), "notes Flowfile did not write\n");
    fs.writeFileSync(path.join(root, ".DS_Store"), "");
    const secret = await request.post(`${API_URL}/project/secrets`, {
      headers: authHeaders(token),
      data: { secrets: [{ name: SECRET, value: "e2e" }] },
    });
    expect(secret.ok()).toBe(true);
  });

  test.afterAll(async ({ request }) => {
    if (token) {
      await request.delete(`${API_URL}/secrets/secrets/${encodeURIComponent(SECRET)}`, {
        headers: authHeaders(token),
      });
      await request.post(`${API_URL}/project/close`, { headers: authHeaders(token) });
    }
    if (root) fs.rmSync(root, { recursive: true, force: true });
  });

  test("saves a version and reads clean afterwards", async ({ page, request }) => {
    await login(page, token, "#/main/project");
    const pill = page.locator(".status-pill");
    await expect(pill).toHaveText("Unsaved changes");

    await page.locator(".save-version-form textarea").fill(MESSAGE);
    await page.locator(".save-version-form").getByRole("button", { name: "Save version" }).click();

    await expect(page.locator(".tl-message", { hasText: MESSAGE })).toBeVisible();
    await expect(pill).toHaveText("All saved");

    const active = await request.get(`${API_URL}/project/active`, { headers: authHeaders(token) });
    expect((await active.json()).dirty).toBe(false);
  });
});
