// @vitest-environment happy-dom
// The kernels page points at the Backends tab only for an administrator, the one reader who can open it.
import { h, ref } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { mountIn } from "../../__fixtures__/mount";

const auth = vi.hoisted(() => ({ isAdmin: false }));
const nav = vi.hoisted(() => ({
  route: { query: {} as Record<string, string> },
  replace: vi.fn(),
  kernels: [] as Array<{ id: string }>,
}));

vi.mock("vue-router", () => ({
  useRoute: () => nav.route,
  useRouter: () => ({ replace: nav.replace }),
}));
vi.mock("../../stores/auth-store", () => ({ useAuthStore: () => auth }));
vi.mock("element-plus", () => ({ ElMessage: { error: vi.fn(), success: vi.fn() } }));
vi.mock("./useKernelManager", () => ({
  useKernelManager: () => ({
    kernels: ref(nav.kernels),
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
vi.mock("./KernelDetailsModal.vue", () => ({
  default: {
    props: ["kernel"],
    emits: ["close"],
    setup:
      (props: { kernel: { id: string } }, { emit }: { emit: (event: "close") => void }) =>
      () =>
        h("div", { class: "details-modal", onClick: () => emit("close") }, props.kernel.id),
  },
}));

import KernelManagerView from "./KernelManagerView.vue";

const description = async (isAdmin: boolean): Promise<string> => {
  auth.isAdmin = isAdmin;
  const host = await mountIn(KernelManagerView);
  return (host.querySelector(".page-description")?.textContent ?? "").replace(/\s+/g, " ").trim();
};

describe("KernelManagerView", () => {
  beforeEach(() => {
    nav.route.query = {};
    nav.replace.mockReset();
    nav.kernels.splice(0);
  });

  it("points an administrator at the Backends tab", async () => {
    expect(await description(true)).toMatch(/default worker \(see the Backends tab\)\.$/);
  });

  it("names no tab a member cannot open", async () => {
    const text = await description(false);
    expect(text).not.toContain("Backends");
    expect(text).toMatch(/run on Flowfile's default worker\.$/);
  });

  it("opens the kernel a ?kernelId= link names and drops the parameter once it closes", async () => {
    nav.kernels.push({ id: "ml-kernel" });
    nav.route.query = { tab: "kernels", kernelId: "ml-kernel" };
    const host = await mountIn(KernelManagerView);
    const modal = host.querySelector<HTMLElement>(".details-modal");
    expect(modal?.textContent).toBe("ml-kernel");
    expect(nav.replace).not.toHaveBeenCalled();
    modal?.click();
    expect(nav.replace).toHaveBeenCalledWith({ query: { tab: "kernels" } });
  });
});
