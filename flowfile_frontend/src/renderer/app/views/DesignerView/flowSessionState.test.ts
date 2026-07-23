import { describe, expect, it } from "vitest";

import { resolveBootFlowId, resolveNextFlowAfterClose } from "./flowSessionState";

describe("resolveBootFlowId", () => {
  it("returns -1 when no flows are open", () => {
    expect(resolveBootFlowId([], -1)).toBe(-1);
  });

  it("returns -1 when no flows are open even with a stale persisted id", () => {
    expect(resolveBootFlowId([], 5)).toBe(-1);
  });

  it("keeps the persisted id when it is still open", () => {
    expect(resolveBootFlowId([10, 20, 30], 20)).toBe(20);
  });

  it("falls back to the first open flow when the persisted id is gone", () => {
    expect(resolveBootFlowId([10, 20], 5)).toBe(10);
  });

  it("falls back to the first open flow when nothing was persisted", () => {
    expect(resolveBootFlowId([10, 20], -1)).toBe(10);
  });
});

describe("resolveNextFlowAfterClose", () => {
  it("returns null when the current flow was not closed", () => {
    expect(resolveNextFlowAfterClose([10, 30], [20], 10)).toBe(null);
  });

  it("switches to the first remaining flow when the current one closed", () => {
    expect(resolveNextFlowAfterClose([30, 40], [10, 20], 20)).toBe(30);
  });

  it("returns -1 when the current flow closed and none remain", () => {
    expect(resolveNextFlowAfterClose([], [10], 10)).toBe(-1);
  });

  it("returns -1 on close-all", () => {
    expect(resolveNextFlowAfterClose([], [10, 20, 30], 20)).toBe(-1);
  });

  it("returns null when nothing was closed", () => {
    expect(resolveNextFlowAfterClose([10, 20], [], 10)).toBe(null);
  });
});
