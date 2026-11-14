// @vitest-environment happy-dom
// The kernels page points at the Backends tab only for an administrator, the one reader who can open it.
import { ref } from "vue";
import { describe, expect, it, vi } from "vitest";
import { mountIn } from "../../__fixtures__/mount";

const auth = vi.hoisted(() => ({ isAdmin: false }));

vi.mock("../../stores/auth-store", () => ({ useAuthStore: () => auth }));
vi.mock("element-plus", () => ({ ElMessage: { error: vi.fn(), success: vi.fn() } }));
vi.mock("./useKernelManager", () => ({
  useKernelManager: () => ({
    kernels: ref([]),
    isLoading: ref(false),
    errorMessage: ref(null),
    dockerStatus: ref(null),
    memoryStats: ref({}),
    flavourInfo: ref(null),
    createKernel: vi.fn(),
    updateKernel: vi.fn(),
    startKernel: vi.fn(),
    stopKernel: vi.fn(),
    deleteKernel: vi.fn(),
    isActionInProgress: () => false,
    checkDockerStatus: vi.fn(),
  }),
}));
vi.mock("./CreateKernelForm.vue", () => ({ default: { render: () => null } }));
vi.mock("./KernelStatusSidebar.vue", () => ({ default: { render: () => null } }));
vi.mock("./KernelDetailsModal.vue", () => ({ default: { render: () => null } }));

import KernelManagerView from "./KernelManagerView.vue";

const description = async (isAdmin: boolean): Promise<string> => {
  auth.isAdmin = isAdmin;
  const host = await mountIn(KernelManagerView);
  return (host.querySelector(".page-description")?.textContent ?? "").replace(/\s+/g, " ").trim();
};

describe("KernelManagerView", () => {
  it("points an administrator at the Backends tab", async () => {
    expect(await description(true)).toMatch(/default worker \(see the Backends tab\)\.$/);
  });

  it("names no tab a member cannot open", async () => {
    const text = await description(false);
    expect(text).not.toContain("Backends");
    expect(text).toMatch(/run on Flowfile's default worker\.$/);
  });
});
