// @vitest-environment happy-dom
import { afterEach, describe, expect, it, vi } from "vitest";

import type { RunInformation } from "../../../types";

const state = vi.hoisted(() => ({ run: null as unknown }));

vi.mock("../../../stores/column-store", () => ({
  useNodeStore: () => ({ currentRunResult: state.run, flow_id: 1, nodeDescriptions: {} }),
}));
vi.mock("../../../stores/ai-store", () => ({ useAiStore: () => ({ isStreaming: false }) }));
vi.mock("../../../stores/editor-store", () => ({ useEditorStore: () => ({}) }));
vi.mock("../../../stores/drawer-store", () => ({ useDrawerStore: () => ({}) }));

import { mount, type Mounted } from "../../../__fixtures__/mount";
import Results from "./results.vue";

const node = (node_id: number, success: boolean | null, error = "") => ({
  node_id,
  node_name: `node_${node_id}`,
  start_timestamp: 1794338581,
  end_timestamp: 1794338582,
  success,
  error,
  run_time_ms: 1000,
  is_running: false,
});

const run = (fields: Partial<RunInformation>): RunInformation =>
  ({
    flow_id: 1,
    start_time: "2026-11-10T10:00:00",
    end_time: "2026-11-10T10:00:05",
    success: false,
    is_running: false,
    execution_mode: "Development",
    nodes_completed: 1,
    number_of_nodes: 3,
    node_step_result: [],
    run_type: "full_run",
    ...fields,
  }) as RunInformation;

let mounted: Mounted | undefined;
afterEach(() => mounted?.unmount());

const show = (info: RunInformation) => {
  mounted?.unmount();
  state.run = info;
  mounted = mount(Results);
};

const summary = () => mounted!.el.querySelector(".flow-summary")!;

describe("the designer's run results", () => {
  it("shows a cancelled run as Cancelled, muted, with the node it stopped", () => {
    show(
      run({
        cancelled: true,
        node_step_result: [node(1, true), node(2, null, "Cancelled")] as never,
      }),
    );

    expect(summary().textContent).toBe("Cancelled");
    expect(Array.from(summary().classList)).toContain("skipped");
    expect(Array.from(summary().classList)).not.toContain("failure");
    expect(mounted!.text()).toContain("Status: Cancelled");
    expect(mounted!.text()).not.toContain("Status: Running");
    expect(mounted!.text()).toContain("1 succeeded");
    expect(mounted!.text()).toContain("2 not run");
    expect(mounted!.text()).not.toMatch(/\d+ failed/);
  });

  it("shows a run that failed as Failed, also one cancelled after a node failed", () => {
    const nodes = [node(1, true), node(2, false, "boom"), node(3, null)] as never;
    for (const fields of [{ cancelled: false }, {}]) {
      show(run({ ...fields, node_step_result: nodes }));

      expect(summary().textContent).toBe("Failed");
      expect(Array.from(summary().classList)).toContain("failure");
      expect(mounted!.text()).toContain("1 failed");
    }
  });

  it("shows a node without an outcome as Running while the run runs", () => {
    show(
      run({
        success: null,
        is_running: true,
        node_step_result: [{ ...node(1, null), is_running: true }] as never,
      }),
    );

    expect(summary().textContent).toBe("Running");
    expect(mounted!.text()).toContain("Status: Running");
  });
});
