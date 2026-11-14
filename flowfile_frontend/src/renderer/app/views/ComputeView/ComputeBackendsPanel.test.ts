// @vitest-environment happy-dom
// The Backends list as an administrator sees it: a row a remote fills carries its chip and no Remove.
import type { FunctionalComponent } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { element, fakeStorage, mountIn } from "../../__fixtures__/mount";
import type { ComputeBackend } from "../../api/computeBackends.api";

const mocks = vi.hoisted(() => ({ list: vi.fn() }));

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

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);
const remove = (id: string) => host.querySelector(`[aria-label="Remove ${id}"]`);

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  mocks.list.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("ComputeBackendsPanel", () => {
  it("gives a row a remote fills the chip and no Remove, from one listing", async () => {
    mocks.list.mockResolvedValue([row("worker:team", "remote"), row("worker:own", "local")]);

    host = await mountIn(ComputeBackendsPanel, { pinia: createPinia(), stubs });

    expect(find("backend-managed-worker:team")?.textContent).toBe("Managed elsewhere");
    expect(remove("worker:team")).toBeNull();
    expect(find("backend-managed-worker:own")).toBeNull();
    expect(remove("worker:own")).not.toBeNull();
    expect(mocks.list).toHaveBeenCalledTimes(1);
  });
});
