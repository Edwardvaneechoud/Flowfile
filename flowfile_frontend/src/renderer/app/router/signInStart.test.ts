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
  default: {
    getSetupStatus: async () => ({ mode: "docker", setup_required: false }),
    computes: () => true,
    cookieSession: () => false,
  },
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

  it("keeps a signed-in tab off the sign-in page, so `#/login?hub_signin=1` starts nothing", async () => {
    mocks.isAuthenticated.mockReturnValue(true);

    expect((await boot("/login?hub_signin=1")).name).toBe("home");
  });
});
