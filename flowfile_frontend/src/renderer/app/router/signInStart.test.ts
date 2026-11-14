// @vitest-environment happy-dom
// Where the app's router leaves a tab at boot, on a memory history: nobody signed in reaches the sign-in page only once
// the first navigation settles, and a tab already there keeps its address, which `#/login?hub_signin=1` depends on.
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({ isAuthenticated: vi.fn() }));

vi.mock("vue-router", async (importOriginal) => {
  const actual = await importOriginal<typeof import("vue-router")>();
  return { ...actual, createWebHashHistory: actual.createMemoryHistory };
});
vi.mock("element-plus", () => ({ ElMessage: {} }));
vi.mock("../layouts/AppLayout.vue", () => ({ default: { render: () => null } }));
vi.mock("../views/LoginView/LoginView.vue", () => ({ default: { render: () => null } }));
vi.mock("../views/HomeView/HomeView.vue", () => ({ default: { render: () => null } }));
vi.mock("../stores/auth-store", () => ({ useAuthStore: () => ({ user: {} }) }));
vi.mock("../services/setup.service", () => ({
  default: { getSetupStatus: async () => ({ mode: "docker", setup_required: false }) },
}));
vi.mock("../services/auth.service", () => ({
  default: {
    isAuthenticated: mocks.isAuthenticated,
    isInDesktopMode: () => false,
    setModeFromBackend: () => undefined,
  },
}));

/** A new router on its first navigation, which main.ts does not wait for before it redirects. */
const boot = async (path: string) => {
  vi.resetModules();
  const { default: router, redirectToLogin } = await import("./index");
  router.addRoute({
    path: "/elsewhere",
    component: { render: () => null },
    meta: { requiresAuth: false },
  });
  router.addRoute({
    path: "/private",
    component: { render: () => null },
    meta: { requiresAuth: true },
  });
  void router.push(path);
  await redirectToLogin();
  return router.currentRoute.value;
};

beforeEach(() => {
  mocks.isAuthenticated.mockReset().mockReturnValue(false);
});

describe("redirectToLogin", () => {
  it("leaves an unsigned tab at the sign-in page its address, query and all", async () => {
    expect((await boot("/login?hub_signin=1")).fullPath).toBe("/login?hub_signin=1");
  });

  it("sends an unsigned tab anywhere else to the sign-in page", async () => {
    expect((await boot("/elsewhere")).name).toBe("login");
  });

  it("leaves a signed-in tab at the sign-in start, which may sign in someone else through the hub", async () => {
    mocks.isAuthenticated.mockReturnValue(true);

    expect((await boot("/login?hub_signin=1")).fullPath).toBe("/login?hub_signin=1");
  });

  it("sends a signed-in tab home once a password server drops the sign-in start's flag", async () => {
    mocks.isAuthenticated.mockReturnValue(true);
    await boot("/login?hub_signin=1");
    const { default: router } = await import("./index");

    await router.replace({ path: "/login", query: {} });

    expect(router.currentRoute.value.name).toBe("home");
  });

  it("sends an unsigned tab at a page that needs a sign-in to the sign-in page, which returns there", async () => {
    const route = await boot("/private?runId=17");

    expect(route.name).toBe("login");
    expect(route.query.redirect).toBe("/private?runId=17");
  });

  it("names no page to return to for the app's plain entry", async () => {
    expect((await boot("/")).fullPath).toBe("/login");
  });

  it("keeps a signed-in tab off a plain sign-in page", async () => {
    mocks.isAuthenticated.mockReturnValue(true);

    expect((await boot("/login")).name).toBe("home");
  });
});
