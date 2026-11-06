// @vitest-environment happy-dom
// The Backends list as an administrator sees it beside a hub: it loads without waiting for the hub connection, a
// row the hub filled carries its chip and no Remove, and only a server that joined its hub loads through Sync.
import type { FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { desktop, none, server } from "../../__fixtures__/hubConnection";
import { element, fakeStorage, mountIn, settle } from "../../__fixtures__/mount";
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

const stubs: Record<string, FunctionalComponent> = {
  "el-collapse-transition": (_, { slots }) => slots.default?.(),
  "el-button": element("button"),
  "el-input": element("input"),
  "el-checkbox": element("input"),
};

const row = (id: string, source: ComputeBackend["source"]): ComputeBackend => ({
  id,
  url: `https://${id.replace("worker:", "")}.example.test:63579`,
  shares_core_disk: false,
  verdict: null,
  source,
});

let host: HTMLElement;

const mount = async () => {
  host = await mountIn(ComputeBackendsPanel, { pinia: createPinia(), stubs });
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);
const remove = (id: string) => host.querySelector(`[aria-label="Remove ${id}"]`);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  mocks.connection.mockReset();
  mocks.list.mockReset();
  mocks.sync.mockReset();
});

afterEach(() => {
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
