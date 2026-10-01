// Unit tests for applySettingsValidation: backend-owned validation entries are
// replaced per flow on every apply (fixed nodes revert to valid), while
// client-written entries (in-drawer config checks, no source marker) survive.

import { setActivePinia, createPinia } from "pinia";
import { beforeEach, describe, expect, it } from "vitest";
import type { FlowSettingsValidation, NodeResult, RunInformation } from "../types";

import { useResultsStore } from "./results-store";

const backendResult = (nodes: FlowSettingsValidation["nodes"]): FlowSettingsValidation => ({
  enabled: true,
  nodes,
});

const issue = (message: string, missing: string[] = ["a"]) => ({
  input_handle: "main" as const,
  missing_columns: missing,
  message,
  kind: "missing_columns" as const,
});

describe("applySettingsValidation", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  it("marks nodes from the backend response as invalid with joined messages", () => {
    const store = useResultsStore();
    store.applySettingsValidation(1, backendResult([
      { node_id: 3, issues: [issue("left is broken"), issue("right is broken")] },
    ]));

    const validation = store.getNodeValidation(1, 3);
    expect(validation.isValid).toBe(false);
    expect(validation.error).toBe("left is broken\nright is broken");
    expect(validation.validationTime).toBeGreaterThan(0);
  });

  it("clears backend entries absent from the next response", () => {
    const store = useResultsStore();
    store.applySettingsValidation(1, backendResult([{ node_id: 3, issues: [issue("broken")] }]));
    expect(store.getNodeValidation(1, 3).isValid).toBe(false);

    store.applySettingsValidation(1, backendResult([]));
    expect(store.getNodeValidation(1, 3).isValid).toBe(true);
  });

  it("clears backend entries when validation is disabled", () => {
    const store = useResultsStore();
    store.applySettingsValidation(1, backendResult([{ node_id: 3, issues: [issue("broken")] }]));

    store.applySettingsValidation(1, { enabled: false, nodes: [] });
    expect(store.getNodeValidation(1, 3).isValid).toBe(true);
  });

  it("preserves client-written entries across backend applies", () => {
    const store = useResultsStore();
    store.setNodeValidation(1, 7, { isValid: false, error: "Please select at least one field." });

    store.applySettingsValidation(1, backendResult([{ node_id: 3, issues: [issue("broken")] }]));

    expect(store.getNodeValidation(1, 7).error).toBe("Please select at least one field.");
    expect(store.getNodeValidation(1, 3).isValid).toBe(false);
  });

  it("lets a backend entry take over a client entry for the same node", () => {
    const store = useResultsStore();
    store.setNodeValidation(1, 3, { isValid: true, error: "" });

    store.applySettingsValidation(1, backendResult([{ node_id: 3, issues: [issue("broken")] }]));
    expect(store.getNodeValidation(1, 3).error).toBe("broken");
  });

  it("does not touch other flows", () => {
    const store = useResultsStore();
    store.applySettingsValidation(2, backendResult([{ node_id: 5, issues: [issue("other flow")] }]));

    store.applySettingsValidation(1, backendResult([]));
    expect(store.getNodeValidation(2, 5).isValid).toBe(false);
  });
});

describe("insertRunResult", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
  });

  const run = (flowId: number): RunInformation => ({
    flow_id: flowId,
    start_time: "t1",
    end_time: "t1",
    success: true,
    is_running: false,
    execution_mode: "Development",
    nodes_completed: 1,
    number_of_nodes: 1,
    node_step_result: [{ node_id: 3, success: true } as NodeResult],
    run_type: "full_run",
  });

  it("records a run that is not current without replacing the shown one", () => {
    const store = useResultsStore();
    store.insertRunResult(run(2));
    store.insertRunResult(run(1), false);
    expect(store.currentRunResult?.flow_id).toBe(2);
    expect(store.getRunResult(1)?.flow_id).toBe(1);
    expect(store.runNodeResults[1][3].success).toBe(true);
  });
});
