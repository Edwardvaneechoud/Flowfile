import { describe, expect, it } from "vitest";

import {
  NODE_OUTCOME_BADGE,
  NODE_OUTCOME_LABEL,
  RUN_OUTCOME_BADGE,
  RUN_OUTCOME_LABEL,
  RUN_OUTCOME_TEXT,
  nodeOutcome,
  runOutcome,
} from "./runStatus";

describe("runOutcome", () => {
  it("reads a run stopped on purpose as cancelled, never failed", () => {
    const outcome = runOutcome(false, true);
    expect(outcome).toBe("cancelled");
    expect(RUN_OUTCOME_LABEL[outcome]).toBe("Cancelled");
    expect(RUN_OUTCOME_BADGE[outcome]).not.toBe("failure");
    expect(RUN_OUTCOME_TEXT[outcome]).not.toBe("text-danger");
  });

  it("keeps a failed run failed, also a record from before the mark existed", () => {
    expect(runOutcome(false, false)).toBe("failed");
    expect(runOutcome(false)).toBe("failed");
    expect(runOutcome(false, null)).toBe("failed");
    expect(RUN_OUTCOME_LABEL.failed).toBe("Failed");
  });

  it("reads the mark only on an ended, unsuccessful run", () => {
    expect(runOutcome(true, true)).toBe("success");
    expect(runOutcome(null, true)).toBe("running");
  });
});

describe("nodeOutcome", () => {
  it("reads a node the engine marked as stopped by a cancel as cancelled", () => {
    const outcome = nodeOutcome({ success: null, cancelled: true });
    expect(NODE_OUTCOME_LABEL[outcome]).toBe("Cancelled");
    expect(NODE_OUTCOME_BADGE[outcome]).toBe("cancelled");
  });

  it("reads a node without an outcome and without the mark as running", () => {
    expect(nodeOutcome({ success: null })).toBe("running");
  });

  it("keeps finished, failed and gated-off nodes as they were", () => {
    expect(nodeOutcome({ success: true })).toBe("ok");
    expect(nodeOutcome({ success: false })).toBe("failed");
    expect(nodeOutcome({ success: true, skipped: true })).toBe("skipped");
  });
});
