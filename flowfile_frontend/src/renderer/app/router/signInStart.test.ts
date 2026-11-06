// @vitest-environment happy-dom
// `#/login?hub_signin=1` (the hub's Continue) through the app's own router, booted as main.ts boots it.
import type { AxiosAdapter, InternalAxiosRequestConfig } from "axios";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const OPEN_URL = "https://hub.example.com:8443/open/12";

const inAnHour = () => Math.floor(Date.now() / 1000) + 3600;

/** Answers what core would for a docker server joined to a hub, and records each path asked. */
function server() {
  const paths: string[] = [];
  const adapter: AxiosAdapter = async (config: InternalAxiosRequestConfig) => {
    const path = config.url ?? "";
    paths.push(path);
    const answer = (data: unknown) => ({
      data,
      status: 200,
      statusText: "OK",
      headers: {},
      config,
    });
    if (path === "/health/status") return answer({ setup_required: false, mode: "docker" });
    if (path === "/auth/sign_in_method") {
      return answer({ method: "hub", url: OPEN_URL, hub_url: "https://hub.example.com:8443" });
    }
    if (path === "/auth/users/me") return answer({ username: "alice" });
    return answer({});
  };
  return { adapter, paths };
}

function fakeStorage() {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => store.get(key) ?? null,
    setItem: (key: string, value: string) => void store.set(key, value),
    removeItem: (key: string) => void store.delete(key),
    clear: () => store.clear(),
  };
}

const flush = async () => {
  for (let i = 0; i < 20; i++) await new Promise((resolve) => setTimeout(resolve, 0));
};

let unmount: (() => void) | null = null;

/** Boot as main.ts does: setup status, the auth store, the router, mount, then the redirect for a tab not signed in. */
const boot = async (hash: string) => {
  window.history.replaceState(null, "", `/${hash}`);
  vi.resetModules();
  const axios = (await import("axios")).default;
  const backend = server();
  axios.defaults.adapter = backend.adapter;
  const { createApp, h } = await import("vue");
  const { createPinia } = await import("pinia");
  const { RouterView } = await import("vue-router");
  const authService = (await import("../services/auth.service")).default;
  const setupService = (await import("../services/setup.service")).default;
  const { useAuthStore } = await import("../stores/auth-store");
  const { default: router, redirectToLogin } = await import("./index");

  const pinia = createPinia();
  // Renders the sign-in page only; the editor's layout needs i18n and the rest of main.ts.
  const app = createApp({
    render: () =>
      h(RouterView, null, {
        default: ({ Component, route }: { Component: unknown; route: { name?: unknown } }) =>
          route.name === "login" && Component ? h(Component as never) : null,
      }),
  });
  app.use(pinia);
  authService.setModeFromBackend((await setupService.getSetupStatus()).mode);
  const authenticated = await useAuthStore(pinia).initialize();
  app.use(router);
  const host = document.createElement("div");
  document.body.appendChild(host);
  app.mount(host);
  if (!authenticated && !authService.isInDesktopMode()) await redirectToLogin();
  await router.isReady();
  await flush();
  unmount = () => {
    app.unmount();
    host.remove();
  };
  return { router, paths: backend.paths };
};

// The first import of the router transforms the editor's layout, which takes seconds.
describe("the sign-in start through the app's router", { timeout: 30_000 }, () => {
  let replace: ReturnType<typeof vi.fn<(url: string | URL) => void>>;

  beforeEach(() => {
    vi.stubGlobal("localStorage", fakeStorage());
    vi.stubGlobal("sessionStorage", fakeStorage());
    replace = vi.fn<(url: string | URL) => void>();
    vi.spyOn(window.location, "replace").mockImplementation(replace);
  });

  afterEach(() => {
    unmount?.();
    unmount = null;
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    window.history.replaceState(null, "", "/");
  });

  it("sends an unsigned tab at `#/login?hub_signin=1` on to the hub's Open at once, with its state", async () => {
    const { router } = await boot("#/login?hub_signin=1");

    expect(replace).toHaveBeenCalledTimes(1);
    const url = new URL(replace.mock.calls[0][0] as string);
    expect(url.origin + url.pathname).toBe(OPEN_URL);
    expect(url.searchParams.get("state")).toBe(sessionStorage.getItem("hub_signin_state"));
    expect(router.currentRoute.value.fullPath).toBe("/login");
  });

  it("starts nothing for an unsigned tab at a plain `#/login`", async () => {
    const { router, paths } = await boot("#/login");

    expect(paths).toContain("/auth/sign_in_method");
    expect(replace).not.toHaveBeenCalled();
    expect(sessionStorage.getItem("hub_signin_state")).toBeNull();
    expect(router.currentRoute.value.name).toBe("login");
  });

  it("leaves a signed-in tab at `#/login?hub_signin=1` signed in and starts nothing", async () => {
    localStorage.setItem("auth_token", "a.b.c");
    localStorage.setItem("auth_token_expiration", String(inAnHour() * 1000));
    localStorage.setItem("auth_username", "alice");

    const { router } = await boot("#/login?hub_signin=1");

    expect(replace).not.toHaveBeenCalled();
    expect(sessionStorage.getItem("hub_signin_state")).toBeNull();
    expect(router.currentRoute.value.name).toBe("home");
    expect(localStorage.getItem("auth_token")).toBe("a.b.c");
  });
});
