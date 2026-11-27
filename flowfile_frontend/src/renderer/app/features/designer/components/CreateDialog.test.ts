// @vitest-environment happy-dom
// The Create dialog's file tab: a flow made on a server is a file in no catalog, on a desktop it is registered too.
import { h, type FunctionalComponent } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { element, mountIn, settle } from "../../../__fixtures__/mount";

const mocks = vi.hoisted(() => ({
  createFlow: vi.fn(),
  success: vi.fn(),
  error: vi.fn(),
  multiUser: false,
  ownCatalog: true,
}));

vi.mock("../../../composables/useMultiUser", async () => {
  const { ref } = await import("vue");
  return { useMultiUser: () => ({ isMultiUser: ref(mocks.multiUser) }) };
});
vi.mock("../../../composables/useOwnCatalog", async () => {
  const { ref } = await import("vue");
  return {
    useOwnCatalog: () => ({
      ownCatalogOffered: ref(mocks.ownCatalog),
      refreshOwnCatalog: async () => mocks.ownCatalog,
    }),
  };
});
vi.mock("../../../components/nodes/nodeLogic", () => ({ createFlow: mocks.createFlow }));
vi.mock("../../../api/file.api", () => ({
  getCatalogFlowsDirectory: async () => "/data/user/flows",
}));
vi.mock("../../../composables/useRecentFlows", () => ({ basenameNoExt: (path: string) => path }));
vi.mock("element-plus", () => ({ ElMessage: { error: mocks.error, success: mocks.success } }));
vi.mock("../../../components/common/FileBrowser/fileBrowser.vue", async () => {
  const { h: el } = await import("vue");
  return {
    default: {
      emits: ["create-file"],
      setup(_: unknown, { emit }: { emit: (event: string, path: string) => void }) {
        return () =>
          el("button", {
            "data-testid": "create-here",
            onClick: () => emit("create-file", "/data/user/workspaces/7/churn.yaml"),
          });
      },
    },
  };
});
vi.mock("./CatalogRegisterRow.vue", async () => {
  const { h: el } = await import("vue");
  return { default: { render: () => el("div", { "data-testid": "register-row" }) } };
});
vi.mock("./CatalogNamespacePicker.vue", () => ({ default: { render: () => null } }));

import CreateDialog from "./CreateDialog.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-dialog": (_, { slots }) => h("div", [slots.default?.(), slots.footer?.()]),
  "el-button": element("button"),
  "el-input": element("input"),
};

const createInTheFileTab = async () => {
  const host = await mountIn(CreateDialog, { props: { visible: true }, stubs });
  (host.querySelector('[data-testid="create-here"]') as HTMLElement).click();
  await settle();
  return host;
};

beforeEach(() => {
  mocks.createFlow.mockReset().mockResolvedValue(12);
  mocks.success.mockReset();
  mocks.error.mockReset();
  mocks.multiUser = false;
  mocks.ownCatalog = true;
});

describe("CreateDialog's file tab", () => {
  it("on a server makes the flow as a file and registers it nowhere", async () => {
    mocks.multiUser = true;
    const host = await createInTheFileTab();

    expect(host.querySelector('[data-testid="register-row"]')).toBeNull();
    expect(host.querySelector('[data-testid="workspace-file-note"]')).not.toBeNull();
    expect(mocks.createFlow).toHaveBeenCalledWith(
      "/data/user/workspaces/7/churn.yaml",
      null,
      null,
      false,
    );
    expect(mocks.success).toHaveBeenCalledWith("Flow created");
  });

  it("on a desktop offers the catalog and registers the flow there", async () => {
    const host = await createInTheFileTab();

    expect(host.querySelector('[data-testid="register-row"]')).not.toBeNull();
    expect(host.querySelector('[data-testid="workspace-file-note"]')).toBeNull();
    expect(mocks.createFlow).toHaveBeenCalledWith(
      "/data/user/workspaces/7/churn.yaml",
      null,
      null,
      true,
    );
  });
});
