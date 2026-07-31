import { describe, expect, it } from "vitest";

import { classifyStatsError, statsCacheKey } from "./statsRequest";

function axiosError(status: number, detail?: unknown) {
  return { response: { status, data: detail === undefined ? {} : { detail } } };
}

describe("statsCacheKey", () => {
  it("partitions by node, output handle and column", () => {
    expect(statsCacheKey(1, "main", "city")).toBe("1::main::city");
    expect(statsCacheKey(1, "main", "city")).not.toBe(statsCacheKey(1, "right", "city"));
  });
});

describe("classifyStatsError", () => {
  it("maps 409 to not-run and keeps the server reason verbatim", () => {
    expect(classifyStatsError(axiosError(409, "Node has no cached result. Run the flow first."))).
      toEqual({ kind: "not-run", detail: "Node has no cached result. Run the flow first." });
  });

  it("drops a non-string 409 detail so the panel falls back to its own copy", () => {
    expect(classifyStatsError(axiosError(409, { msg: "nope" }))).toEqual({
      kind: "not-run",
      detail: null,
    });
    expect(classifyStatsError(axiosError(409))).toEqual({ kind: "not-run", detail: null });
  });

  it("maps every other status to the retryable error kind", () => {
    expect(classifyStatsError(axiosError(422, "Could not compute column statistics"))).toEqual({
      kind: "error",
      detail: null,
    });
    expect(classifyStatsError(axiosError(404))).toEqual({ kind: "error", detail: null });
  });

  it("maps a response-less failure (network, abort) to the error kind", () => {
    expect(classifyStatsError(new Error("Network Error"))).toEqual({ kind: "error", detail: null });
    expect(classifyStatsError(undefined)).toEqual({ kind: "error", detail: null });
  });
});
