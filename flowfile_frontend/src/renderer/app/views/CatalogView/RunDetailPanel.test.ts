// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";

vi.mock("../../api/catalog.api", () => ({ CatalogApi: {} }));

import { mount, type Mounted } from "../../__fixtures__/mount";
import type { FlowRunDetail } from "../../types";
import RunDetailPanel from "./RunDetailPanel.vue";

const node = (node_id: number, success: boolean | null, error = "") => ({
  node_id,
  node_name: `node_${node_id}`,
  success,
  error,
  run_time_ms: 5,
  is_running: false,
});

const run = (fields: Partial<FlowRunDetail>): FlowRunDetail => ({
  id: 7,
  registration_id: 1,
  flow_name: "orders",
  flow_path: "/flows/orders.yaml",
  user_id: 1,
  started_at: "2026-11-10T10:00:00",
  ended_at: "2026-11-10T10:00:05",
  success: false,
  nodes_completed: 1,
  number_of_nodes: 3,
  duration_seconds: 5,
  run_type: "manual",
  schedule_id: null,
  has_snapshot: false,
  has_log: false,
  flow_snapshot: null,
  node_results_json: null,
  ...fields,
});

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

const status = () => mounted!.el.querySelector(".meta-card:nth-child(2) .meta-value")!;
const nodeBadges = () =>
  Array.from(mounted!.el.querySelectorAll(".results-table .status-badge")).map((badge) => ({
    label: badge.textContent!.trim(),
    classes: Array.from(badge.classList).filter((name) => name !== "status-badge"),
  }));

describe("RunDetailPanel", () => {
  it("shows a cancelled run as Cancelled in a neutral colour, with its stopped node", () => {
    mounted = mount(RunDetailPanel, {
      run: run({
        cancelled: true,
        node_results_json: JSON.stringify([node(1, true), { ...node(2, null), cancelled: true }]),
      }),
    });

    expect(status().textContent).toBe("Cancelled");
    expect(Array.from(status().classList)).toContain("text-cancelled");
    expect(Array.from(status().classList)).not.toContain("text-danger");
    expect(nodeBadges()).toEqual([
      { label: "OK", classes: ["success"] },
      { label: "Cancelled", classes: ["cancelled"] },
    ]);
    expect(mounted.text()).not.toContain("Failed");
  });

  it("shows a cancelled run that reported no node without a node table", () => {
    mounted = mount(RunDetailPanel, { run: run({ cancelled: true, nodes_completed: 0 }) });

    expect(status().textContent).toBe("Cancelled");
    expect(mounted.el.querySelector(".results-table")).toBeNull();
  });

  it("shows a failed run as Failed, also one recorded before the mark existed", () => {
    const nodes = JSON.stringify([node(1, true), node(2, false, "boom")]);
    for (const fields of [{ cancelled: false }, {}]) {
      mounted?.unmount();
      mounted = mount(RunDetailPanel, { run: run({ ...fields, node_results_json: nodes }) });

      expect(status().textContent).toBe("Failed");
      expect(Array.from(status().classList)).toContain("text-danger");
      expect(nodeBadges()[1]).toEqual({ label: "Failed", classes: ["failure"] });
    }
  });
});
