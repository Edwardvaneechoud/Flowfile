// @vitest-environment happy-dom
// The panel of a flow published on the hub: its versions as the hub lists them, the newest picked, Open emitting the
// picked version, and the hub's refusal shown when the versions cannot be read.
import { defineComponent, h, ref } from "vue";
import { describe, expect, it, vi } from "vitest";
import { createPinia, setActivePinia } from "pinia";
import { flow, ns } from "../../__fixtures__/catalogTree";
import { useCatalogStore } from "../../stores/catalog-store";
import { mountIn, settle, text } from "../../__fixtures__/mount";

const mocks = vi.hoisted(() => ({ listHubFlowVersions: vi.fn() }));

vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../api/hubFlows.api", () => ({ listHubFlowVersions: mocks.listHubFlowVersions }));

import HubFlowDetailPanel from "./HubFlowDetailPanel.vue";

const hubFlow = flow({
  id: -20,
  name: "demo-flow",
  namespace_id: -11,
  flow_path: "hub://flows/u-1",
  source: "hub",
  flow_uuid: "u-1",
  latest_version: 2,
  author_name: "ana",
});

const versions = [
  {
    version: 2,
    author_id: 7,
    author_name: "ana",
    published_at: "2026-11-02T09:00:00Z",
    message: "fix",
  },
  {
    version: 1,
    author_id: 7,
    author_name: "ana",
    published_at: "2026-11-01T12:00:00Z",
    message: null,
  },
];

describe("HubFlowDetailPanel", () => {
  it("lists the versions newest first, picks the newest, and Open emits the picked one", async () => {
    mocks.listHubFlowVersions.mockResolvedValue(versions);
    const opened = vi.fn();
    const pinia = createPinia();
    setActivePinia(pinia);
    useCatalogStore().tree = [
      ns({
        id: -10,
        name: "main",
        place: "Acme Data",
        children: [ns({ id: -11, name: "default" })],
      }),
    ];
    const host = await mountIn(HubFlowDetailPanel, {
      pinia,
      props: { flow: hubFlow, onOpen: opened },
    });

    expect(mocks.listHubFlowVersions).toHaveBeenCalledWith("u-1");
    expect(text(host.querySelector("[data-testid='hub-flow-home']"))).toBe(
      "On Acme Data, in main.default",
    );
    const rows = Array.from(host.querySelectorAll("[data-testid='hub-flow-versions'] li"));
    expect(rows.map((row) => row.querySelector(".version-number")?.textContent)).toEqual([
      "v2",
      "v1",
    ]);
    expect(rows[0].classList.contains("selected")).toBe(true);
    expect(text(rows[0])).toContain("ana");

    (host.querySelector("[data-testid='hub-flow-open']") as HTMLButtonElement).click();
    expect(opened).toHaveBeenCalledWith(2);

    (rows[1] as HTMLElement).click();
    await settle();
    (host.querySelector("[data-testid='hub-flow-open']") as HTMLButtonElement).click();
    expect(opened).toHaveBeenLastCalledWith(1);
  });

  it("drops the versions of a flow that is no longer the one shown", async () => {
    const answers: Record<string, (listed: typeof versions) => void> = {};
    mocks.listHubFlowVersions.mockImplementation(
      (uuid: string) => new Promise((resolve) => (answers[uuid] = resolve)),
    );
    const shown = ref(hubFlow);
    const opened = vi.fn();
    const pinia = createPinia();
    setActivePinia(pinia);
    const host = await mountIn(
      defineComponent(() => () => h(HubFlowDetailPanel, { flow: shown.value, onOpen: opened })),
      { pinia },
    );

    shown.value = { ...hubFlow, id: -21, flow_uuid: "u-2" };
    await settle();
    answers["u-2"]([{ ...versions[1], version: 5 }]);
    await settle();
    answers["u-1"](versions);
    await settle();

    const rows = Array.from(host.querySelectorAll("[data-testid='hub-flow-versions'] li"));
    expect(rows.map((row) => row.querySelector(".version-number")?.textContent)).toEqual(["v5"]);
    (host.querySelector("[data-testid='hub-flow-open']") as HTMLButtonElement).click();
    expect(opened).toHaveBeenCalledWith(5);
  });

  it("shows the refusal when the versions cannot be read, and offers no Open", async () => {
    mocks.listHubFlowVersions.mockRejectedValue({
      response: { data: { detail: "Connect to a hub first (Compute → Hub)." } },
    });
    const pinia = createPinia();
    setActivePinia(pinia);
    const host = await mountIn(HubFlowDetailPanel, { pinia, props: { flow: hubFlow } });

    expect(text(host.querySelector("[data-testid='hub-flow-error']"))).toBe(
      "Connect to a hub first (Compute → Hub).",
    );
    expect(text(host.querySelector("[data-testid='hub-flow-home']"))).toBe("On the hub");
    expect(
      (host.querySelector("[data-testid='hub-flow-open']") as HTMLButtonElement).disabled,
    ).toBe(true);
  });
});
