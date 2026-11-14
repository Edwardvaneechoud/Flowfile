// @vitest-environment happy-dom
// The Publish to hub dialog for a flow already on the hub: the next version by default, or a new flow in a schema.
import { h, type FunctionalComponent, type SetupContext } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { element, mountIn, settle, text } from "../../../__fixtures__/mount";
import { httpError } from "../../../__fixtures__/promises";

const mocks = vi.hoisted(() => ({
  publishToHub: vi.fn(),
  publishCheck: vi.fn(),
  openFromHub: vi.fn(),
}));

// The real hubFlows.api for refusalOf, over a stand-in for axios so nothing reads tokens at import.
vi.mock("../../../services/axios.config", () => ({ default: {} }));
vi.mock("../../../api/hubFlows.api", async () => ({
  ...(await vi.importActual<object>("../../../api/hubFlows.api")),
  publishToHub: mocks.publishToHub,
  publishCheck: mocks.publishCheck,
  openFromHub: mocks.openFromHub,
}));
vi.mock("element-plus", () => ({
  ElMessage: { success: vi.fn(), error: vi.fn() },
  ElMessageBox: { confirm: vi.fn() },
}));
// The hub's schema picker, reduced to one schema a click selects (hub rows carry negated ids).
vi.mock("./CatalogNamespacePicker.vue", async () => {
  const { h: el } = await import("vue");
  return {
    default: {
      props: { modelValue: { type: Number, default: null }, hub: Boolean },
      emits: ["update:modelValue"],
      setup(_: unknown, { emit, expose }: SetupContext) {
        expose({ hubIdOf: (id: number | null) => (id !== null && id < 0 ? -id : null) });
        return () =>
          el("button", {
            "data-testid": "pick-schema",
            onClick: () => emit("update:modelValue", -3),
          });
      },
    },
  };
});

import HubPublishDialog from "./HubPublishDialog.vue";

const elInput: FunctionalComponent = (_, { attrs }) =>
  h("input", {
    "data-testid": attrs["data-testid"],
    value: attrs.modelValue,
    onInput: (event: Event) =>
      (attrs["onUpdate:modelValue"] as (value: string) => void)(
        (event.target as HTMLInputElement).value,
      ),
  });
elInput.inheritAttrs = false;

const stubs: Record<string, FunctionalComponent> = {
  "el-dialog": (_, { slots }) => h("div", [slots.default?.(), slots.footer?.()]),
  "el-button": element("button"),
  "el-alert": element("div"),
  "el-input": elInput,
};

const origin = {
  hub_url: "https://hub.example.test",
  flow_uuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
  version: 4,
};

let host: HTMLElement;
let published: unknown[] = [];

const open = async () => {
  published = [];
  host = await mountIn(HubPublishDialog, {
    props: {
      visible: true,
      flowId: 42,
      hubOrigin: origin,
      flowName: "Sales report",
      onPublished: (result: unknown) => published.push(result),
    },
    stubs,
  });
};

const byTestId = (id: string) => host.querySelector(`[data-testid="${id}"]`) as HTMLElement | null;

const click = async (id: string) => {
  byTestId(id)?.click();
  await settle();
};

beforeEach(() => {
  mocks.publishToHub.mockReset().mockResolvedValue({ version: 5, name: "Sales report" });
  mocks.publishCheck.mockReset().mockResolvedValue([]);
  mocks.openFromHub.mockReset();
});

describe("HubPublishDialog for a flow already on the hub", () => {
  it("publishes the next version by default and offers a new flow instead", async () => {
    await open();

    expect(text(host)).toContain(
      "This flow is on the hub as version 4; this publish becomes the next one.",
    );
    expect(text(byTestId("hub-publish-as-new"))).toBe("Publish as a new flow instead");
    expect(byTestId("hub-publish-new-name")).toBeNull();
    expect(byTestId("pick-schema")).toBeNull();
    expect(text(byTestId("hub-publish"))).toBe("Publish");

    await click("hub-publish");

    expect(mocks.publishToHub).toHaveBeenCalledWith({
      flow_id: 42,
      namespace_id: null,
      message: undefined,
      overwrite_version: undefined,
    });
    expect(mocks.publishToHub.mock.calls[0][0]).not.toHaveProperty("as_new");
  });

  it("publishes a new flow in the schema picked, under the name typed", async () => {
    mocks.publishToHub.mockResolvedValue({ version: 1, name: "Sales by region", flow_id: 77 });
    await open();

    await click("hub-publish-as-new");

    const name = byTestId("hub-publish-new-name") as HTMLInputElement;
    expect(name.value).toBe("Sales report copy");
    expect(text(byTestId("hub-publish-new-note"))).toBe(
      "A new flow at version 1 in the schema you pick. This tab becomes the new flow; the original stays on the hub as it is, with its versions, schedules and run history.",
    );
    expect(text(host)).toContain("With none picked, the flow goes to the hub's default namespace.");
    expect(text(byTestId("hub-publish"))).toBe("Publish as new flow");

    name.value = "Sales by region";
    name.dispatchEvent(new Event("input"));
    await click("pick-schema");
    await click("hub-publish");

    expect(mocks.publishToHub).toHaveBeenCalledWith({
      flow_id: 42,
      namespace_id: 3,
      message: undefined,
      overwrite_version: undefined,
      as_new: true,
      name: "Sales by region",
    });
    expect(published).toEqual([{ version: 1, name: "Sales by region", flow_id: 77 }]);
    expect(byTestId("hub-published")?.getAttribute("title")).toBe("Published as version 1");
    expect(text(byTestId("hub-published-as-new"))).toBe(
      "Sales by region is a new flow on the hub at version 1. This tab is that flow now, so your next publish builds on it; the original is unchanged.",
    );
  });

  it("sends no schema when none is picked, so the hub uses its default", async () => {
    await open();
    await click("hub-publish-as-new");

    await click("hub-publish");

    expect(mocks.publishToHub.mock.calls[0][0]).toMatchObject({
      namespace_id: null,
      as_new: true,
      name: "Sales report copy",
    });
  });

  it("shows the hub's refusal and stays open on the new flow's form", async () => {
    mocks.publishToHub.mockRejectedValue(
      httpError(409, {
        kind: "refused",
        status: 409,
        detail: "a flow with this name already exists in this namespace",
      }),
    );
    await open();
    await click("hub-publish-as-new");

    await click("hub-publish");

    expect(byTestId("hub-publish-error")?.getAttribute("title")).toBe(
      "The hub refused the request (409): a flow with this name already exists in this namespace",
    );
    expect(byTestId("hub-published")).toBeNull();
    expect(byTestId("hub-publish-new-name")).not.toBeNull();
    expect(published).toEqual([]);
  });

  it("goes back to the next version", async () => {
    await open();
    await click("hub-publish-as-new");

    await click("hub-publish-next-version");

    expect(byTestId("hub-publish-new-name")).toBeNull();
    expect(text(byTestId("hub-publish"))).toBe("Publish");
    await click("hub-publish");
    expect(mocks.publishToHub.mock.calls[0][0]).toMatchObject({ namespace_id: null });
    expect(mocks.publishToHub.mock.calls[0][0]).not.toHaveProperty("as_new");
  });
});
