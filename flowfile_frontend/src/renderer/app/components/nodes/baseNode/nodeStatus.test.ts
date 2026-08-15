import { describe, it, expect } from "vitest";
import { deriveNodeStatus, nodeStatusTooltip } from "./nodeStatus";
import type { NodeResult, NodeValidation } from "../../../types/node.types";

function makeResult(overrides: Partial<NodeResult> = {}): NodeResult {
  return {
    node_id: 1,
    start_timestamp: 1000,
    end_timestamp: 1001,
    success: true,
    error: "",
    run_time_ms: 5,
    is_running: false,
    ...overrides,
  };
}

// The store hands out this shape for a node nothing has flagged.
function validation(overrides: Partial<NodeValidation> = {}): NodeValidation {
  return { isValid: true, error: "", validationTime: 0, ...overrides };
}

describe("deriveNodeStatus", () => {
  it("returns undefined when there is neither a result nor a validation", () => {
    expect(deriveNodeStatus(undefined, undefined)).toBeUndefined();
  });

  it("reports a running node regardless of its recorded success", () => {
    const status = deriveNodeStatus(makeResult({ is_running: true, success: false }), validation());
    expect(status?.statusIndicator).toBe("running");
    expect(status?.hasRun).toBe(false);
  });

  it("maps a plain successful run to success", () => {
    expect(deriveNodeStatus(makeResult(), validation())?.statusIndicator).toBe("success");
  });

  it("maps a failed run with no fresh validation to failure", () => {
    const status = deriveNodeStatus(makeResult({ success: false, error: "boom" }), validation());
    expect(status?.statusIndicator).toBe("failure");
    expect(status?.error).toBe("boom");
  });

  it("shows a warning when a success is invalidated by a newer validation", () => {
    const status = deriveNodeStatus(
      makeResult(),
      validation({ isValid: false, error: "missing column", validationTime: 2000 }),
    );
    expect(status?.statusIndicator).toBe("warning");
    expect(status?.error).toBe("missing column");
  });

  it("downgrades a failure to unknown once settings validate again after the run", () => {
    const status = deriveNodeStatus(
      makeResult({ success: false, error: "boom" }),
      validation({ validationTime: 2000 }),
    );
    expect(status?.statusIndicator).toBe("unknown");
  });

  it("keeps a success green when the invalidating validation predates the run", () => {
    const status = deriveNodeStatus(
      makeResult(),
      validation({ isValid: false, error: "stale", validationTime: 500 }),
    );
    expect(status?.statusIndicator).toBe("success");
  });

  // A null verdict means "no verdict yet" (cancelled run, result written before
  // the node finished) — it must not be painted red.
  it("maps a null verdict to unknown, not failure", () => {
    expect(
      deriveNodeStatus(makeResult({ success: undefined }), validation())?.statusIndicator,
    ).toBe("unknown");
    expect(
      deriveNodeStatus(makeResult({ success: null as unknown as undefined }), validation())
        ?.statusIndicator,
    ).toBe("unknown");
  });

  it("reports a gated-off node as skipped even though the backend marks it successful", () => {
    const status = deriveNodeStatus(makeResult({ skipped: true, run_time_ms: 0 }), validation());
    expect(status?.statusIndicator).toBe("skipped");
    expect(status?.hasRun).toBe(false);
  });

  it("prefers skipped over a stale validation warning", () => {
    const status = deriveNodeStatus(
      makeResult({ skipped: true }),
      validation({ isValid: false, error: "missing column", validationTime: 2000 }),
    );
    expect(status?.statusIndicator).toBe("skipped");
  });

  it("falls back to validation-only states for a node that never ran", () => {
    expect(deriveNodeStatus(undefined, validation())?.statusIndicator).toBe("unknown");
    expect(deriveNodeStatus(undefined, validation({ isValid: false }))?.statusIndicator).toBe(
      "warning",
    );
  });
});

describe("nodeStatusTooltip", () => {
  it("explains the skipped state", () => {
    expect(nodeStatusTooltip({ statusIndicator: "skipped", hasRun: false })).toBe(
      "Skipped (gated off) — condition not met",
    );
  });

  it("keeps the existing copy for the other states", () => {
    expect(nodeStatusTooltip({ statusIndicator: "success", hasRun: true })).toBe(
      "Operation successful",
    );
    expect(nodeStatusTooltip({ statusIndicator: "failure", error: "boom", hasRun: true })).toBe(
      "Operation failed: \nboom",
    );
    expect(nodeStatusTooltip(undefined)).toBe("Status unknown");
  });
});
