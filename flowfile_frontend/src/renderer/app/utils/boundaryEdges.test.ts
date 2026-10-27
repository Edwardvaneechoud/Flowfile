import { describe, expect, it } from "vitest";

import type { RunInformation } from "../types";
import { crossesBackends, crossingNote, runNodeLabel } from "./boundaryEdges";

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

describe("runNodeLabel", () => {
  const withResults = {
    flow_id: 1,
    node_step_result: [{ node_id: 2, node_name: "filter" }],
  } as RunInformation;

  it("names the node by the run's result for it, from the string id the canvas gives", () => {
    expect(runNodeLabel(withResults, "2")).toBe("Node 2: filter");
  });

  it("falls back to the id when the run has no result or no name for the node", () => {
    expect(runNodeLabel(withResults, "3")).toBe("Node 3");
    expect(runNodeLabel(run([[1, 2]]), "1")).toBe("Node 1");
    expect(runNodeLabel(null, 4)).toBe("Node 4");
  });
});

describe("crossingNote", () => {
  const placed = {
    flow_id: 1,
    node_step_result: [
      { node_id: 4, backend_id: "worker" },
      { node_id: 6, backend_id: "worker:polars" },
    ],
  } as RunInformation;

  it("names both backends and says the output goes through the exchange store", () => {
    expect(crossingNote(placed, "4", "6")).toBe(
      'Node 4 runs on the default worker and node 6 on backend "worker:polars", so node 4\'s output is ' +
        "written to the exchange store for node 6 to read. That copy adds time to the run.",
    );
  });

  it("leaves the backends out when the run did not report both", () => {
    expect(crossingNote(placed, "4", "7")).toBe(
      "These nodes run on different backends, so node 4's output is written to the exchange store for " +
        "node 7 to read. That copy adds time to the run.",
    );
    expect(crossingNote(null, 1, 2)).toMatch(/^These nodes run on different backends/);
  });
});
