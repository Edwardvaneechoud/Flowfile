// @vitest-environment happy-dom
// The Open dialog's Catalog tab with a hub's flow beside this machine's: a local flow opens by its path, a hub flow's
// newest version opens through core as the person's copy.
import { h, type FunctionalComponent, type SetupContext } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fakeStorage, element, mountIn, settle } from "../../../__fixtures__/mount";
import { httpError } from "../../../__fixtures__/promises";

const mocks = vi.hoisted(() => ({
  getFlows: vi.fn(),
  openFromHub: vi.fn(),
  confirm: vi.fn(),
  success: vi.fn(),
  error: vi.fn(),
}));

vi.mock("../../../api", () => ({ CatalogApi: { getFlows: mocks.getFlows } }));
// The real hubFlows.api for refusalOf, over a stand-in for axios so nothing reads tokens at import.
vi.mock("../../../services/axios.config", () => ({ default: {} }));
vi.mock("../../../api/hubFlows.api", async () => ({
  ...(await vi.importActual<object>("../../../api/hubFlows.api")),
  openFromHub: mocks.openFromHub,
}));
vi.mock("element-plus", () => ({
  ElMessage: { error: mocks.error, success: mocks.success },
  ElMessageBox: { confirm: mocks.confirm },
}));
vi.mock("../../../components/common/FileBrowser/fileBrowser.vue", () => ({
  default: { render: () => null },
}));
// The picker over the catalog tree, reduced to one namespace a click selects.
vi.mock("./CatalogNamespacePicker.vue", async () => {
  const { h: el } = await import("vue");
  return {
    default: {
      props: { modelValue: { type: Number, default: null } },
      emits: ["update:modelValue"],
      setup(_: unknown, { emit, expose }: SetupContext) {
        expose({ getNamespacePath: () => "main.default", reload: () => undefined });
        return () =>
          el("button", {
            "data-testid": "pick-namespace",
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
};

const flowRow = { file_exists: true, last_run_at: null, last_run_success: null, namespace_id: -20 };
const local = { ...flowRow, id: 3, name: "daily", flow_path: "/flows/daily.yaml" };
const orders = {
  ...flowRow,
  id: -5,
  name: "orders",
  flow_path: "hub://flows/aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
  source: "hub",
  flow_uuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
  latest_version: 2,
};

let host: HTMLElement;
let opened: unknown[] = [];

const openRow = async (name: string) => {
  opened = [];
  host = await mountIn(OpenDialog, {
    props: { visible: true, onOpenFlow: (payload: unknown) => opened.push(payload) },
    stubs,
  });
  (host.querySelector('[data-testid="pick-namespace"]') as HTMLElement).click();
  await settle();
  const row = [...host.querySelectorAll(".flow-row")].find((r) => r.textContent?.includes(name));
  row?.dispatchEvent(new MouseEvent("dblclick"));
  await settle();
};

beforeEach(() => {
  vi.stubGlobal("localStorage", fakeStorage());
  mocks.getFlows.mockReset().mockResolvedValue([local, orders]);
  mocks.openFromHub.mockReset().mockResolvedValue({ flow_path: "/flows/hub/orders.yaml" });
  mocks.confirm.mockReset().mockResolvedValue("confirm");
  mocks.success.mockReset();
  mocks.error.mockReset();
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("OpenDialog's Catalog tab", () => {
  it("lists a hub namespace's flows and opens a local flow by its path", async () => {
    await openRow("daily");

    expect(mocks.getFlows).toHaveBeenCalledWith(-20);
    expect(host.querySelectorAll(".flow-row")).toHaveLength(2);
    expect(mocks.openFromHub).not.toHaveBeenCalled();
    expect(opened).toEqual([
      {
        message: "Flow opened from catalog",
        flowPath: "/flows/daily.yaml",
        flowName: "daily",
        catalogRef: "main.default.daily",
      },
    ]);
  });

  it("opens a hub flow's newest version through core and hands the flow's path on", async () => {
    await openRow("orders");

    expect(mocks.openFromHub).toHaveBeenCalledWith({
      flow_uuid: orders.flow_uuid,
      version: 2,
      replace: false,
    });
    expect(opened).toEqual([
      { message: "Flow opened from hub", flowPath: "/flows/hub/orders.yaml" },
    ]);
    expect(mocks.success).toHaveBeenCalledWith("Flow opened from hub");
  });

  const openOverEditedCopy = async () => {
    mocks.openFromHub
      .mockRejectedValueOnce(
        httpError(409, { kind: "local_copy_exists", flow_path: "/flows/hub/orders.yaml" }),
      )
      .mockResolvedValueOnce({ flow_path: "/flows/hub/orders.yaml" });
    await openRow("orders");
  };

  it("offers a copy with unpublished edits and opens it without asking the hub again", async () => {
    await openOverEditedCopy();

    expect(mocks.confirm).toHaveBeenCalledOnce();
    const [question, title, options] = mocks.confirm.mock.calls[0];
    expect(question).toContain("replace it with v2 from the hub");
    expect(title).toBe("Your copy has unpublished edits");
    expect(options).toMatchObject({
      confirmButtonText: "Open my copy",
      cancelButtonText: "Replace with v2",
      distinguishCancelAndClose: true,
    });
    expect(mocks.openFromHub).toHaveBeenCalledOnce();
    expect(opened).toEqual([
      { message: "Opened your copy of the flow", flowPath: "/flows/hub/orders.yaml" },
    ]);
  });

  it("replaces the copy with the newest version when asked to", async () => {
    mocks.confirm.mockRejectedValue("cancel");

    await openOverEditedCopy();

    expect(mocks.openFromHub).toHaveBeenLastCalledWith({
      flow_uuid: orders.flow_uuid,
      version: 2,
      replace: true,
    });
    expect(opened).toEqual([
      { message: "Flow opened from hub", flowPath: "/flows/hub/orders.yaml" },
    ]);
  });

  it("opens nothing when the question is closed", async () => {
    mocks.confirm.mockRejectedValue("close");

    await openOverEditedCopy();

    expect(mocks.openFromHub).toHaveBeenCalledOnce();
    expect(opened).toEqual([]);
    expect(mocks.error).not.toHaveBeenCalled();
  });

  it("shows the hub's refusal in its words and opens nothing", async () => {
    mocks.openFromHub.mockRejectedValue(
      httpError(403, { kind: "refused", status: 403, detail: "no read access" }),
    );

    await openRow("orders");

    expect(mocks.error).toHaveBeenCalledWith("The hub refused the request (403): no read access");
    expect(mocks.success).not.toHaveBeenCalled();
    expect(opened).toEqual([]);
  });

  it("opens nothing for a hub flow without a published version", async () => {
    mocks.getFlows.mockResolvedValue([{ ...orders, latest_version: null }]);

    await openRow("orders");

    expect(mocks.openFromHub).not.toHaveBeenCalled();
    expect(mocks.error).toHaveBeenCalledWith("This flow has no published version yet.");
    expect(opened).toEqual([]);
  });
});
