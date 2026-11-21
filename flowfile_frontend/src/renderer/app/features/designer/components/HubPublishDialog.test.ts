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
  mergeFromHub: vi.fn(),
  listMergeConflicts: vi.fn(),
  resolveMergeConflict: vi.fn(),
}));

// The real hubFlows.api for refusalOf, over a stand-in for axios so nothing reads tokens at import.
vi.mock("../../../services/axios.config", () => ({ default: {} }));
vi.mock("../../../api/hubFlows.api", async () => ({
  ...(await vi.importActual<object>("../../../api/hubFlows.api")),
  publishToHub: mocks.publishToHub,
  publishCheck: mocks.publishCheck,
  openFromHub: mocks.openFromHub,
  mergeFromHub: mocks.mergeFromHub,
  listMergeConflicts: mocks.listMergeConflicts,
  resolveMergeConflict: mocks.resolveMergeConflict,
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
      inheritAttrs: false,
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
let mergedEvents: unknown[] = [];

const open = async () => {
  published = [];
  mergedEvents = [];
  host = await mountIn(HubPublishDialog, {
    props: {
      visible: true,
      flowId: 42,
      hubOrigin: origin,
      flowName: "Sales report",
      onPublished: (result: unknown) => published.push(result),
      onMerged: (result: unknown) => mergedEvents.push(result),
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
  mocks.mergeFromHub.mockReset();
  mocks.listMergeConflicts.mockReset();
  mocks.resolveMergeConflict.mockReset();
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
    expect(text(host)).toContain("With none picked, the flow goes to the hub's default schema.");
    expect(text(host)).not.toContain("amespace");
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

describe("HubPublishDialog when the hub moved on", () => {
  const version = (n: number, message: string) => ({
    flow_uuid: origin.flow_uuid,
    version: n,
    author_id: 7,
    published_at: "2026-11-20T10:00:00Z",
    message,
  });
  const stale = () =>
    httpError(409, {
      kind: "stale_base",
      flow_uuid: origin.flow_uuid,
      base: version(4, "mine"),
      current: version(5, "theirs"),
    });

  it("merges the newest version in and then publishes on top of it", async () => {
    mocks.publishToHub.mockRejectedValueOnce(stale());
    mocks.mergeFromHub.mockResolvedValue({
      version: 5,
      applied: true,
      changes: [{ kind: "added", label: "step 7 filter", details: [] }],
      conflicts: [],
      warnings: [],
    });
    await open();
    await click("hub-publish");
    expect(text(byTestId("hub-merge"))).toBe("Merge v5 in");

    await click("hub-merge");
    expect(mocks.mergeFromHub).toHaveBeenCalledWith(42);
    expect(mergedEvents).toHaveLength(1);
    expect(byTestId("hub-merged")?.getAttribute("title")).toBe(
      "Merged v5: 1 step changed on your canvas. Publish to put both on the hub.",
    );
    expect(text(byTestId("hub-merged-changes"))).toBe("added step 7 filter");

    mocks.publishToHub.mockResolvedValueOnce({ version: 6, name: "Sales report" });
    await click("hub-publish");
    expect(mocks.publishToHub.mock.calls[1][0]).toMatchObject({ overwrite_version: undefined });
  });

  const row = {
    key: "step:2",
    kind: "both_changed",
    label: "step 2 sample",
    message: "Both sides changed step 2.",
    step_id: 2,
    ours_label: "version 5",
    theirs_label: "your copy",
    ours: ["settings.sample_size: 3 → 50"],
    theirs: ["settings.sample_size: 3 → 70"],
  };

  it("shows what each side did to a step both changed, and publishes once it is settled", async () => {
    mocks.publishToHub.mockRejectedValueOnce(stale());
    mocks.mergeFromHub.mockResolvedValue({
      version: 5,
      applied: true,
      changes: [],
      conflicts: [{ key: row.key, kind: row.kind, label: row.label, message: row.message }],
      warnings: [],
    });
    mocks.listMergeConflicts.mockResolvedValue([row]);
    mocks.resolveMergeConflict.mockResolvedValue([]);
    await open();
    await click("hub-publish");
    await click("hub-merge");

    expect(byTestId("hub-merged")?.getAttribute("title")).toContain("1 step both of you changed");
    const settleRow = text(byTestId("hub-settle-step:2"));
    expect(settleRow).toContain("Both sides changed step 2.");
    for (const piece of [
      "settings.sample_size: 3 → 70",
      "settings.sample_size: 3 → 50",
      "Keep your copy",
      "Keep version 5",
    ]) {
      expect(settleRow).toContain(piece);
    }
    expect(byTestId("hub-publish")).toBeNull();

    await click("hub-settle-ours-step:2");
    expect(mocks.resolveMergeConflict).toHaveBeenCalledWith(42, "step:2", "ours");
    expect(mergedEvents).toHaveLength(2);
    expect(byTestId("hub-merged")?.getAttribute("title")).toBe(
      "Every conflict is settled. Publish to put your copy on the hub.",
    );
    expect(byTestId("hub-publish")).not.toBeNull();
  });

  it("lists the conflicts still open when a publish is refused for them", async () => {
    mocks.publishToHub.mockRejectedValueOnce(
      httpError(409, {
        kind: "unresolved_conflicts",
        count: 1,
        message: "Settle the 1 merge conflict in this flow first.",
      }),
    );
    mocks.listMergeConflicts.mockResolvedValue([row]);
    await open();
    await click("hub-publish");
    expect(mocks.listMergeConflicts).toHaveBeenCalledWith(42);
    expect(byTestId("hub-settle-step:2")).not.toBeNull();
    expect(byTestId("hub-merged-done")).not.toBeNull();
  });
});
