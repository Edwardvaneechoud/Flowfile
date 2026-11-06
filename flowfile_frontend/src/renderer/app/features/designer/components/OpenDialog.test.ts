// @vitest-environment happy-dom
// The Open dialog's Hub tab: there only while connected to a hub, and then the hub's namespaces, a namespace's
// flows and the picked flow's versions, the newest preselected, opened through core.
import { h, type FunctionalComponent, type SetupContext } from "vue";
import { createPinia } from "pinia";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { desktop, none } from "../../../__fixtures__/hubConnection";
import { element, fakeStorage, mountIn, settle, text } from "../../../__fixtures__/mount";
import { httpError } from "../../../__fixtures__/promises";
import type { HubConnection } from "../../../api/hubConnection.api";

const mocks = vi.hoisted(() => ({
  getHubConnection: vi.fn(),
  getFlows: vi.fn(),
  listHubFlows: vi.fn(),
  listHubFlowVersions: vi.fn(),
  openFromHub: vi.fn(),
  confirm: vi.fn(),
  success: vi.fn(),
}));

vi.mock("../../../api/hubConnection.api", () => ({ getHubConnection: mocks.getHubConnection }));
vi.mock("../../../api", () => ({ CatalogApi: { getFlows: mocks.getFlows } }));
// The real hubFlows.api for refusalOf, over a stand-in for axios so nothing reads tokens at import.
vi.mock("../../../services/axios.config", () => ({ default: {} }));
vi.mock("../../../api/hubFlows.api", async () => ({
  ...(await vi.importActual<object>("../../../api/hubFlows.api")),
  listHubFlows: mocks.listHubFlows,
  listHubFlowVersions: mocks.listHubFlowVersions,
  openFromHub: mocks.openFromHub,
}));
vi.mock("element-plus", () => ({
  ElMessage: { error: vi.fn(), success: mocks.success },
  ElMessageBox: { confirm: mocks.confirm },
}));
vi.mock("../../../components/common/FileBrowser/fileBrowser.vue", () => ({
  default: { render: () => null },
}));
// The picker over the catalog tree, reduced to one hub namespace whose tree id is the negative of the hub's.
vi.mock("./CatalogNamespacePicker.vue", async () => {
  const { h: el } = await import("vue");
  return {
    default: {
      props: { modelValue: { type: Number, default: null }, hub: Boolean },
      emits: ["update:modelValue"],
      setup(props: { hub: boolean }, { emit, expose }: SetupContext) {
        expose({
          hubIdOf: (id: number | null) => (id === null ? null : -id),
          getNamespacePath: () => null,
          reload: () => undefined,
        });
        return () =>
          el("button", {
            "data-testid": props.hub ? "pick-hub-namespace" : "pick-namespace",
            onClick: () => emit("update:modelValue", -20),
          });
      },
    },
  };
});

import OpenDialog from "./OpenDialog.vue";

const stubs: Record<string, FunctionalComponent> = {
  "el-dialog": (_, { slots }) => h("div", [slots.default?.(), slots.footer?.()]),
  "el-button": element("button"),
  "el-alert": (_, { attrs }) => h("div", attrs, String(attrs.title ?? "")),
};

const orders = {
  flow_uuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
  name: "orders",
  latest_version: 2,
  author_name: null,
};
const versions = [
  {
    version: 2,
    author_id: 7,
    author_name: null,
    published_at: "2026-11-01T12:01:00Z",
    message: "theirs",
  },
  {
    version: 1,
    author_id: 1,
    author_name: null,
    published_at: "2026-11-01T12:00:00Z",
    message: null,
  },
];

let host: HTMLElement;
let opened: unknown[] = [];

const mount = async (connection: HubConnection) => {
  mocks.getHubConnection.mockResolvedValue(connection);
  opened = [];
  host = await mountIn(OpenDialog, {
    props: { visible: true, onOpenFlow: (payload: unknown) => opened.push(payload) },
    pinia: createPinia(),
    stubs,
  });
  const { useHubConnectionStore } = await import("../../../stores/hub-connection-store");
  await useHubConnectionStore().load();
  await settle();
};

const find = (testId: string) => host.querySelector(`[data-testid="${testId}"]`);
const click = async (el: Element | null) => {
  (el as HTMLElement).click();
  await settle();
};

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  mocks.getHubConnection.mockReset();
  mocks.getFlows.mockReset().mockResolvedValue([]);
  mocks.listHubFlows.mockReset().mockResolvedValue([orders]);
  mocks.listHubFlowVersions.mockReset().mockResolvedValue(versions);
  mocks.openFromHub.mockReset().mockResolvedValue({ flow_path: "/flows/hub/orders.yaml" });
  mocks.confirm.mockReset().mockResolvedValue("confirm");
  mocks.success.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("OpenDialog's Hub tab", () => {
  it("is not there without a hub, so the dialog offers the file system and the catalog as before", async () => {
    await mount(none);

    expect(find("open-hub-tab")).toBeNull();
    expect(find("open-hub-panel")).toBeNull();
    expect(host.querySelectorAll(".sidebar-btn")).toHaveLength(2);
  });

  it("shows the file system when the remembered tab is the Hub and no hub is connected", async () => {
    localStorage.setItem("openDialog_mode", "hub");

    await mount(none);

    expect(host.querySelector(".sidebar-btn.active")?.textContent).toContain("File System");
    expect(find("open-hub-panel")).toBeNull();
  });

  it("lists a hub namespace's flows by the hub's id, then the picked flow's versions with the newest preselected", async () => {
    await mount(desktop);
    await click(find("open-hub-tab"));
    expect(find("open-hub-panel")).not.toBeNull();

    await click(find("pick-hub-namespace"));

    expect(mocks.listHubFlows).toHaveBeenCalledWith(20);
    const flows = host.querySelectorAll('[data-testid="hub-open-flows"] .flow-row');
    expect(flows).toHaveLength(1);
    expect(text(flows[0])).toContain("orders");
    expect(text(flows[0])).toContain("v2");

    await click(flows[0]);

    expect(mocks.listHubFlowVersions).toHaveBeenCalledWith(orders.flow_uuid);
    const rows = host.querySelectorAll('[data-testid="hub-open-versions"] .flow-row');
    expect(rows).toHaveLength(2);
    expect(rows[0].className).toContain("selected");
    expect(text(rows[0])).toContain("v2");
    expect(text(rows[0])).toContain("user #7");
    expect(text(rows[0])).toContain("theirs");
  });

  it("names the authors of the flow's newest version and of each version when the hub sends their names", async () => {
    mocks.listHubFlows.mockResolvedValue([{ ...orders, author_name: "Cal Jones" }]);
    mocks.listHubFlowVersions.mockResolvedValue([
      { ...versions[0], author_name: "Cal Jones" },
      { ...versions[1], author_name: "Ada Lovelace" },
    ]);
    await mount(desktop);
    await click(find("open-hub-tab"));
    await click(find("pick-hub-namespace"));

    const flow = host.querySelector('[data-testid="hub-open-flows"] .flow-row');
    expect(text(flow)).toContain("Cal Jones");
    await click(flow);

    const rows = host.querySelectorAll('[data-testid="hub-open-versions"] .flow-row');
    expect(text(rows[0])).toContain("Cal Jones");
    expect(text(rows[1])).toContain("Ada Lovelace");
    expect(text(rows[0])).not.toContain("user #");
  });

  it("opens the picked version through core and hands the flow's path on", async () => {
    await mount(desktop);
    await click(find("open-hub-tab"));
    await click(find("pick-hub-namespace"));
    await click(host.querySelector('[data-testid="hub-open-flows"] .flow-row'));
    await click(host.querySelectorAll('[data-testid="hub-open-versions"] .flow-row')[1]);

    await click(find("hub-open"));

    expect(mocks.openFromHub).toHaveBeenCalledWith({
      flow_uuid: orders.flow_uuid,
      version: 1,
      replace: false,
    });
    expect(opened).toEqual([
      { message: "Flow opened from hub", flowPath: "/flows/hub/orders.yaml" },
    ]);
    expect(mocks.success).toHaveBeenCalledWith("Flow opened from hub");
  });

  it("asks before replacing a local copy and opens again with replace", async () => {
    mocks.openFromHub
      .mockRejectedValueOnce(
        httpError(409, { kind: "local_copy_exists", flow_path: "/flows/hub/orders.yaml" }),
      )
      .mockResolvedValueOnce({ flow_path: "/flows/hub/orders.yaml" });
    await mount(desktop);
    await click(find("open-hub-tab"));
    await click(find("pick-hub-namespace"));
    await click(host.querySelector('[data-testid="hub-open-flows"] .flow-row'));

    await click(find("hub-open"));

    expect(mocks.confirm).toHaveBeenCalledOnce();
    expect(mocks.openFromHub).toHaveBeenLastCalledWith({
      flow_uuid: orders.flow_uuid,
      version: 2,
      replace: true,
    });
    expect(opened).toHaveLength(1);
  });

  it("shows the hub's refusal in its words and keeps the dialog open", async () => {
    mocks.openFromHub.mockRejectedValue(
      httpError(403, { kind: "refused", status: 403, detail: "no read access" }),
    );
    await mount(desktop);
    await click(find("open-hub-tab"));
    await click(find("pick-hub-namespace"));
    await click(host.querySelector('[data-testid="hub-open-flows"] .flow-row'));

    await click(find("hub-open"));

    expect(text(find("hub-open-error"))).toBe("The hub refused the request (403): no read access");
    expect(mocks.success).not.toHaveBeenCalled();
    expect(opened).toEqual([]);
  });
});
