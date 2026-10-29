import { describe, expect, it } from "vitest";

import type { NodeResult } from "../types";
import { computeErrorBanner } from "./computeErrorBanner";

const row = (node_id: number, fields: Partial<NodeResult>): NodeResult => ({
  node_id,
  start_timestamp: 0,
  end_timestamp: 0,
  error: "",
  run_time_ms: 0,
  is_running: false,
  success: true,
  ...fields,
});

describe("computeErrorBanner", () => {
  it("shows a refused run's reason once", () => {
    const reason = 'cannot run on backend "worker:gone": no such backend';
    expect(
      computeErrorBanner([
        row(1, { success: false, error: reason, compute_error: "Refusal" }),
        row(2, { success: false, error: reason, compute_error: "Refusal" }),
      ]),
    ).toEqual({ title: "This flow cannot run on its compute backend", messages: [reason] });
  });

  it("titles an unreachable and a mismatched backend", () => {
    expect(
      computeErrorBanner([
        row(1, {
          success: false,
          error: "connection refused",
          compute_error: "ComputeUnavailable",
        }),
      ])?.title,
    ).toBe("The compute backend could not be reached");
    expect(
      computeErrorBanner([
        row(1, { success: false, error: "polars differs", compute_error: "VersionMismatch" }),
      ])?.title,
    ).toBe("The compute backend does not match this server");
  });

  it("shows nothing for a node that failed on its own, another compute error or no failure", () => {
    expect(
      computeErrorBanner([row(1, { success: false, error: "no column a", compute_error: null })]),
    ).toBeNull();
    expect(
      computeErrorBanner([
        row(1, { success: false, error: "killed", compute_error: "TaskFailed" }),
      ]),
    ).toBeNull();
    expect(computeErrorBanner([row(1, {})])).toBeNull();
    expect(computeErrorBanner(undefined)).toBeNull();
  });
});
