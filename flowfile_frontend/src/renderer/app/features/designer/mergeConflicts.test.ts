import { describe, expect, it } from "vitest";
import {
  conflictsTitle,
  runRefusedMessage,
  settledNotice,
  unresolvedConflictsOf,
} from "./mergeConflicts";

const refusal = (status: number, detail: unknown) => ({ response: { status, data: { detail } } });

describe("mergeConflicts", () => {
  it("counts the conflicts left in the bar's heading", () => {
    expect(conflictsTitle(1)).toBe("1 merge conflict to settle before this flow can run");
    expect(conflictsTitle(3)).toBe("3 merge conflicts to settle before this flow can run");
  });

  it("sends a project pull's flow back to the project page once settled", () => {
    expect(settledNotice({ project: { state: "conclude" } })).toEqual({
      message:
        "Every conflict in this flow is settled. Go back to the project page to finish the merge.",
      toProject: true,
    });
    expect(settledNotice({ project: { state: "settle_rest", left: 2 } }).message).toContain(
      "2 items of the pull still to settle on the project page",
    );
    expect(settledNotice({ project: { state: "merged" } }).message).toMatch(/pull is applied/);
  });

  it("tells a hub flow to publish and any other flow that it runs again", () => {
    expect(settledNotice({ hub: true })).toEqual({
      message: "Every conflict is settled. Publish to put your copy on the hub.",
      toProject: false,
    });
    expect(settledNotice({}).message).toBe("Every conflict is settled: the flow can run again.");
  });

  it("recognises core's unresolved-conflicts refusal and nothing else", () => {
    const detail = {
      kind: "unresolved_conflicts",
      code: "UNRESOLVED_CONFLICTS",
      count: 2,
      message: "Settle the 2 merge conflicts in this flow first.",
    };
    const found = unresolvedConflictsOf(refusal(409, detail));
    expect(found).toEqual({ count: 2, message: detail.message });
    expect(runRefusedMessage(found!)).toBe(
      "Settle the 2 merge conflicts in this flow first. Pick a side for each in the merge conflicts bar above the canvas.",
    );
    expect(unresolvedConflictsOf(refusal(409, "Flow is already running"))).toBeNull();
    expect(unresolvedConflictsOf(refusal(422, detail))).toBeNull();
    expect(unresolvedConflictsOf(new Error("network"))).toBeNull();
  });
});
