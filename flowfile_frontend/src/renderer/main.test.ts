// @vitest-environment happy-dom
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  initialize: vi.fn(),
  getCurrentUser: vi.fn(),
  setModeFromBackend: vi.fn(),
  mount: vi.fn(),
  use: vi.fn(),
  router: { push: vi.fn() },
}));

vi.mock("vue", async (importOriginal) => {
  const vue = await importOriginal<typeof import("vue")>();
  return {
    ...vue,
    createApp: () => ({ directive: vi.fn(), use: mocks.use, mount: mocks.mount }),
  };
});
vi.mock("./app/App.vue", () => ({ default: {} }));
vi.mock("./app/router", () => ({ default: mocks.router }));
vi.mock("./app/i18n", () => ({ default: {} }));
vi.mock("element-plus", () => ({ default: {}, messageConfig: {} }));
vi.mock("./app/services/axios.config", () => ({}));
vi.mock("./app/stores/telemetry-store", () => ({ useTelemetryStore: () => ({ $reset: vi.fn() }) }));
vi.mock("./app/stores/theme-store", () => ({ useThemeStore: () => ({ initialize: vi.fn() }) }));
vi.mock("./app/services/setup.service", () => ({
  default: { getSetupStatus: async () => ({ mode: "electron", setup_required: false }) },
}));
vi.mock("./app/services/auth.service", () => ({
  default: {
    initialize: mocks.initialize,
    getCurrentUser: mocks.getCurrentUser,
    setModeFromBackend: mocks.setModeFromBackend,
    isInDesktopMode: () => true,
  },
}));

const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
});

describe("startup navigation", () => {
  it.each([true, false])(
    "loads the user's role before routing and mounting (admin: %s)",
    async (isAdmin) => {
      const token = deferred<boolean>();
      const profile = deferred<{ username: string; is_admin: boolean }>();
      mocks.initialize.mockReturnValue(token.promise);
      mocks.getCurrentUser.mockReturnValue(profile.promise);
      const { default: stores } = await import("./app/stores");
      const { useAuthStore } = await import("./app/stores/auth-store");
      const { computeTabs } = await import("./app/views/ComputeView/computeTabs");
      const auth = useAuthStore(stores);
      let visibleAtMount: string[] = [];
      mocks.mount.mockImplementation(() => {
        visibleAtMount = computeTabs
          .filter((tab) => !tab.requiresAdmin || auth.isAdmin)
          .map((tab) => tab.key);
      });

      await import("./main");
      await vi.waitFor(() => expect(mocks.initialize).toHaveBeenCalledOnce());
      expect(mocks.setModeFromBackend).toHaveBeenCalledWith("electron");
      expect(mocks.use).not.toHaveBeenCalledWith(mocks.router);
      expect(mocks.mount).not.toHaveBeenCalled();

      token.resolve(true);
      await vi.waitFor(() => expect(mocks.getCurrentUser).toHaveBeenCalledOnce());
      expect(mocks.mount).not.toHaveBeenCalled();
      expect(mocks.use).not.toHaveBeenCalledWith(mocks.router);

      profile.resolve({ username: "startup-user", is_admin: isAdmin });
      await vi.waitFor(() => expect(mocks.mount).toHaveBeenCalledWith("#app"));
      expect(mocks.use).toHaveBeenCalledWith(mocks.router);
      expect(auth.isAdmin).toBe(isAdmin);
      expect(visibleAtMount.includes("performance")).toBe(isAdmin);
      expect(visibleAtMount.includes("backends")).toBe(isAdmin);
      expect(visibleAtMount).toContain("kernels");
    },
  );
});
