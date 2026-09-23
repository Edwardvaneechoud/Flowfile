import { describe, it, expect } from "vitest";
import {
  canPartition,
  modeDescription,
  needsMergeKeys,
  trackChangesDisabledReason,
  type DeltaWriteMode,
} from "./deltaWriteModes";

describe("deltaWriteModes", () => {
  it("asks for key columns only on merge-style modes", () => {
    const merge: DeltaWriteMode[] = ["upsert", "update", "delete", "scd2"];
    const plain: DeltaWriteMode[] = ["overwrite", "error", "append"];
    expect(merge.every(needsMergeKeys)).toBe(true);
    expect(plain.some(needsMergeKeys)).toBe(false);
  });

  it("partitions only on modes that can create the table", () => {
    expect(canPartition("append")).toBe(true);
    expect(canPartition("upsert")).toBe(false);
  });

  it("explains why change tracking is off for overwrite, scd2 and virtual", () => {
    expect(trackChangesDisabledReason("overwrite")).toMatch(/^An overwrite replaces/);
    expect(trackChangesDisabledReason("scd2")).toMatch(/^SCD2 already keeps history/);
    expect(trackChangesDisabledReason("append", { virtual: true })).toBe(
      "Virtual tables have no change feed.",
    );
    expect(trackChangesDisabledReason("upsert")).toBeNull();
  });

  it("describes physical modes and nothing for virtual", () => {
    expect(modeDescription("delete")).toBe(
      "Remove rows from the target table that match the key columns in the source data.",
    );
    expect(modeDescription("virtual")).toBeNull();
  });
});
