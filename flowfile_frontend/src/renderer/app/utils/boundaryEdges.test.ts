import { describe, expect, it } from "vitest";

import type { RunInformation } from "../types";
import { crossesBackends } from "./boundaryEdges";

const run = (boundaries?: [number, number][]) => ({ flow_id: 1, boundaries }) as RunInformation;

describe("crossesBackends", () => {
  it("marks an edge the run listed, by the string ids the canvas gives it", () => {
    const listed = run([
      [1, 2],
      [2, 3],
    ]);
    expect(crossesBackends(listed, "2", "3")).toBe(true);
  });

  it("does not mark the reversed pair or an edge the run did not list", () => {
    expect(crossesBackends(run([[1, 2]]), "2", "1")).toBe(false);
    expect(crossesBackends(run([[1, 2]]), "1", "3")).toBe(false);
  });

  it("marks nothing without a run, without the field or without a boundary", () => {
    expect(crossesBackends(null, "1", "2")).toBe(false);
    expect(crossesBackends(run(undefined), "1", "2")).toBe(false);
    expect(crossesBackends(run([]), "1", "2")).toBe(false);
  });
});
