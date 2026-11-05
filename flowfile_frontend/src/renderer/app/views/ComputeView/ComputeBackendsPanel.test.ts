// @vitest-environment happy-dom
// The Backends list as an administrator sees it beside a hub: it loads without waiting for the hub connection, a
// row the hub filled carries its chip and no Remove, and only a server that joined its hub loads through Sync.
import { createApp, h, nextTick, type App, type FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { deferred } from "../../__fixtures__/promises";
import type { ComputeBackend } from "../../api/computeBackends.api";
import type { HubConnection } from "../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({ connection: vi.fn(), list: vi.fn(), sync: vi.fn() }));

vi.mock("../../api/hubConnection.api", () => ({
  getHubConnection: mocks.connection,
  connectHub: vi.fn(),
  disconnectHub: vi.fn(),
  syncHubComputeNodes: mocks.sync,
}));
vi.mock("../../api/computeBackends.api", () => ({
  listComputeBackends: mocks.list,
  addComputeBackend: vi.fn(),
  removeComputeBackend: vi.fn(),
  handshakeComputeBackend: vi.fn(),
}));
vi.mock("../../stores/auth-store", () => ({ useAuthStore: () => ({ isAdmin: true }) }));
vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
  ElMessageBox: { confirm: vi.fn() },
}));
vi.mock("./ExchangeStoreCard.vue", () => ({ default: { render: () => null } }));
vi.mock("../../components/settings/WorkerPoolCard.vue", () => ({
  default: { render: () => null },
}));

import ComputeBackendsPanel from "./ComputeBackendsPanel.vue";

const element = (tag: string): FunctionalComponent => {
  const stub: FunctionalComponent = (_, { attrs, slots }) => h(tag, attrs, slots.default?.());
  stub.inheritAttrs = false;
  return stub;
};

const stubs: Record<string, FunctionalComponent> = {
  "el-collapse-transition": (_, { slots }) => slots.default?.(),
  "el-button": element("button"),
  "el-input": element("input"),
  "el-checkbox": element("input"),
};

const none: HubConnection = {
  connected: false,
  hubUrl: null,
  deviceName: "ana-laptop",
  expiresAt: null,
  orgName: null,
  hubVersion: null,
  reachable: false,
  error: null,
  account: null,
  joined: false,
  organisationStorage: null,
};
const desktop: HubConnection = {
  ...none,
  connected: true,
  hubUrl: "https://hub.acme.test:8443",
  orgName: "Acme Data",
  reachable: true,
  account: "ana",
};
const server: HubConnection = { ...desktop, joined: true };

const row = (id: string, source: ComputeBackend["source"]): ComputeBackend => ({
  id,
  url: `https://${id.replace("worker:", "")}.example.test:63579`,
  shares_core_disk: false,
  verdict: null,
  source,
});

const fakeLocalStorage = () => {
  const map = new Map<string, string>();
  return {
    getItem: (k: string) => map.get(k) ?? null,
    setItem: (k: string, v: string) => void map.set(k, v),
    removeItem: (k: string) => void map.delete(k),
    clear: () => map.clear(),
  } as unknown as Storage;
};

const settle = async () => {
  for (let i = 0; i < 10; i++) await Promise.resolve();
  await nextTick();
};

let app: App | null = null;
let host: HTMLElement;

const mount = async () => {
  host = document.createElement("div");
  document.body.appendChild(host);
  app = createApp(ComputeBackendsPanel);
  app.use(createPinia());
  for (const [name, stub] of Object.entries(stubs)) app.component(name, stub);
  app.mount(host);
  await settle();
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);
const remove = (id: string) => host.querySelector(`[aria-label="Remove ${id}"]`);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeLocalStorage());
  mocks.connection.mockReset();
  mocks.list.mockReset();
  mocks.sync.mockReset();
});

afterEach(() => {
  app?.unmount();
  app = null;
  host?.remove();
  vi.unstubAllGlobals();
});

describe("ComputeBackendsPanel", () => {
  it("lists the configured workers before the hub connection has answered", async () => {
    const connection = deferred<HubConnection>();
    mocks.connection.mockReturnValue(connection.promise);
    mocks.list.mockResolvedValue([row("worker:own", "local")]);

    await mount();

    expect(find("backend-row-worker:own")).not.toBeNull();
    expect(mocks.sync).not.toHaveBeenCalled();
    connection.resolve(none);
    await settle();
    expect(mocks.list).toHaveBeenCalledTimes(1);
    expect(find("backends-hub")).toBeNull();
  });

  it("gives a connected desktop's hub rows the chip and no Remove, and loads them as any worker", async () => {
    mocks.connection.mockResolvedValue(desktop);
    mocks.list.mockResolvedValue([row("worker:team", "hub"), row("worker:own", "local")]);

    await mount();

    expect(find("backend-from-hub-worker:team")?.textContent).toBe("From your hub");
    expect(remove("worker:team")).toBeNull();
    expect(find("backend-from-hub-worker:own")).toBeNull();
    expect(remove("worker:own")).not.toBeNull();
    expect(mocks.sync).not.toHaveBeenCalled();
    expect(find("backends-hub")).not.toBeNull();
  });

  it("loads a joined server's list through Sync once the connection says it joined", async () => {
    mocks.connection.mockResolvedValue(server);
    mocks.list.mockResolvedValue([]);
    mocks.sync.mockResolvedValue({ backends: [row("worker:team", "hub")], skipped: [] });

    await mount();

    expect(mocks.list).toHaveBeenCalledTimes(1);
    expect(mocks.sync).toHaveBeenCalledTimes(1);
    expect(find("backend-row-worker:team")).not.toBeNull();
    expect(remove("worker:team")).toBeNull();
  });
});
