// @vitest-environment happy-dom
// The panel of a flow published on the hub: its versions as the hub lists them, the newest picked, Open emitting the
// picked version, the hub's refusal shown when the versions cannot be read, and no Open where this core computes
// nothing (a hub's own page), since there is no designer to open the flow in.
import { describe, expect, it, vi } from "vitest";
import { flow } from "../../__fixtures__/catalogTree";
import { mountIn, settle, text } from "../../__fixtures__/mount";

const mocks = vi.hoisted(() => ({ listHubFlowVersions: vi.fn(), computes: vi.fn(() => true) }));

vi.mock("../../services/axios.config", () => ({ default: {} }));
vi.mock("../../api/hubFlows.api", () => ({ listHubFlowVersions: mocks.listHubFlowVersions }));
vi.mock("../../services/setup.service", () => ({ default: { computes: mocks.computes } }));

import HubFlowDetailPanel from "./HubFlowDetailPanel.vue";

const hubFlow = flow({
  id: -20,
  name: "demo-flow",
  namespace_id: -11,
  flow_path: "hub://flows/u-1",
  source: "hub",
  hub_id: 20,
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
    const host = await mountIn(HubFlowDetailPanel, {
      props: { flow: hubFlow, place: "Acme Data", schemaPath: "main.default", onOpen: opened },
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

  it("shows the refusal when the versions cannot be read, and offers no Open", async () => {
    mocks.listHubFlowVersions.mockRejectedValue({
      response: { data: { detail: "Connect to a hub first (Compute → Hub)." } },
    });
    const host = await mountIn(HubFlowDetailPanel, {
      props: { flow: hubFlow, place: null, schemaPath: "" },
    });

    expect(text(host.querySelector("[data-testid='hub-flow-error']"))).toBe(
      "Connect to a hub first (Compute → Hub).",
    );
    expect(text(host.querySelector("[data-testid='hub-flow-home']"))).toBe("On the hub");
    expect(
      (host.querySelector("[data-testid='hub-flow-open']") as HTMLButtonElement).disabled,
    ).toBe(true);
  });

  it("offers no Open where this core computes nothing: the versions show, the designer is not here", async () => {
    mocks.computes.mockReturnValueOnce(false);
    mocks.listHubFlowVersions.mockResolvedValue(versions);
    const host = await mountIn(HubFlowDetailPanel, {
      props: { flow: hubFlow, place: "Acme Data", schemaPath: "main.default" },
    });

    expect(host.querySelectorAll("[data-testid='hub-flow-versions'] li")).toHaveLength(2);
    expect(host.querySelector("[data-testid='hub-flow-open']")).toBeNull();
    expect(host.querySelector(".note")).toBeNull();
  });
});
