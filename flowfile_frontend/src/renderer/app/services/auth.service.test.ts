// @vitest-environment happy-dom
// A session handed over in the URL fragment (`#token=<jwt>`) on the web build.
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createRouter, createWebHashHistory, type RouterHistory } from "vue-router";

const base64url = (value: string): string => {
  const bytes = new TextEncoder().encode(value);
  return btoa(String.fromCharCode(...bytes))
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
};

const jwt = (claims: Record<string, unknown>): string =>
  [
    base64url(JSON.stringify({ alg: "HS256", typ: "JWT" })),
    base64url(JSON.stringify(claims)),
    "sig",
  ].join(".");

const inAnHour = () => Math.floor(Date.now() / 1000) + 3600;

function fakeLocalStorage() {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => void store.set(key, value),
    removeItem: (key: string) => void store.delete(key),
    clear: () => store.clear(),
  };
}

const load = async () => {
  vi.resetModules();
  return (await import("./auth.service")).authService;
};

const openAt = (hash: string) => window.history.replaceState(null, "", `/editor${hash}`);

const view = { render: () => null };

// The app's router imports the service, so the service is constructed before the hash history.
const startApp = async () => {
  const authService = await load();
  const history = createWebHashHistory();
  const router = createRouter({
    history,
    routes: [
      { path: "/", redirect: "/designer" },
      { path: "/designer", component: view },
      { path: "/:pathMatch(.*)*", redirect: "/designer" },
    ],
  });
  return { authService, history, router };
};

describe("auth service: token in the URL fragment", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeLocalStorage());
    openAt("");
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    openAt("");
  });

  it("stores the session from the token's claims and removes the fragment", async () => {
    const exp = inAnHour();
    const token = jwt({ sub: "zoë", exp, type: "access" });
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(authService.isAuthenticated()).toBe(true);
    expect(await authService.getToken()).toBe(token);
    expect(localStorage.getItem("auth_token")).toBe(token);
    expect(localStorage.getItem("auth_token_expiration")).toBe(String(exp * 1000));
    expect(localStorage.getItem("auth_username")).toBe("zoë");
    expect(window.location.hash).toBe("");
    expect(window.location.pathname).toBe("/editor");
  });

  it("replaces a stored session and drops its refresh token", async () => {
    localStorage.setItem("auth_token", "earlier");
    localStorage.setItem("auth_token_expiration", String(Date.now() + 3_600_000));
    localStorage.setItem("auth_username", "someone-else");
    localStorage.setItem("auth_refresh_token", "earlier-refresh");
    const token = jwt({ sub: "alice", exp: inAnHour() });
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBe(token);
    expect(localStorage.getItem("auth_username")).toBe("alice");
    expect(localStorage.getItem("auth_refresh_token")).toBeNull();
  });

  it("changes nothing without a fragment", async () => {
    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
  });

  it("reads only the token key from the fragment", async () => {
    openAt("#/designer");

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(window.location.hash).toBe("#/designer");
  });

  it.each([
    ["not a JWT", "abc"],
    ["a payload that is not JSON", `x.${base64url("not json")}.sig`],
    ["no subject", jwt({ exp: inAnHour() })],
    ["no expiry", jwt({ sub: "alice" })],
    ["an expired token", jwt({ sub: "alice", exp: Math.floor(Date.now() / 1000) - 60 })],
    ["an empty token", ""],
  ])("ignores %s and leaves the sign-in page to show", async (_label, token) => {
    openAt(`#token=${token}`);

    const authService = await load();

    expect(await authService.initialize()).toBe(false);
    expect(authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(localStorage.getItem("auth_username")).toBeNull();
    expect(window.location.hash).toBe("");
  });
});

describe("auth service: token in the URL fragment, with the hash router", () => {
  let history: RouterHistory | undefined;

  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeLocalStorage());
    openAt("");
  });

  afterEach(() => {
    history?.destroy();
    history = undefined;
    vi.unstubAllGlobals();
    openAt("");
  });

  it("signs in on a fresh load and leaves no token in the route or history", async () => {
    const token = jwt({ sub: "alice", exp: inAnHour() });
    openAt(`#token=${token}`);

    const app = await startApp();
    history = app.history;

    expect(await app.authService.initialize()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBe(token);
    expect(localStorage.getItem("auth_username")).toBe("alice");
    expect(app.history.location).toBe("/");
    expect(window.location.hash).toBe("#/");
    expect(JSON.stringify(window.history.state)).not.toContain(token);

    await app.router.push(app.history.location);
    expect(app.router.currentRoute.value.fullPath).toBe("/designer");
    expect(app.router.currentRoute.value.redirectedFrom?.fullPath).toBe("/");
    expect(window.location.href).not.toContain(token);
  });

  it("ignores a malformed token and leaves no trace of it", async () => {
    openAt("#token=not-a-jwt");

    const app = await startApp();
    history = app.history;

    expect(await app.authService.initialize()).toBe(false);
    expect(app.authService.requiresLogin()).toBe(true);
    expect(localStorage.getItem("auth_token")).toBeNull();
    expect(app.history.location).toBe("/");

    await app.router.push(app.history.location);
    expect(app.router.currentRoute.value.redirectedFrom?.fullPath).toBe("/");
    expect(window.location.href).not.toContain("not-a-jwt");
  });
});
