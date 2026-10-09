import { describe, expect, it } from "vitest";
import type { SelectionMessage } from "../../../lib/popoutWindow";
import type { NodeInput, VueFlowInput } from "../../types/flow.types";
import { nextNodeId, outputsForNode } from "./tableWindow";

const node = (id: number, output: number, output_names?: string[]): NodeInput =>
  ({ id, input: 1, output, output_names, multi: false, pos_x: 0, pos_y: 0 }) as unknown as NodeInput;

const flow: VueFlowInput = {
  node_edges: [],
  node_inputs: [node(1, 1), node(2, 2, ["Pass", "Fail"])],
  groups: [],
};

describe("outputsForNode", () => {
  it("derives the handles the canvas would show", () => {
    expect(outputsForNode(flow, 1)?.map((o) => o.id)).toEqual(["output-0"]);
    expect(outputsForNode(flow, 2)?.map((o) => [o.id, o.title])).toEqual([
      ["output-0", "Pass"],
      ["output-1", "Fail"],
    ]);
  });

  it("knows a node that is not on the canvas, and no canvas at all", () => {
    expect(outputsForNode(flow, 7)).toBeNull();
    expect(outputsForNode(flow, null)).toBeNull();
    expect(outputsForNode(null, 1)).toBeNull();
  });
});

describe("nextNodeId", () => {
  it("follows a previewed node and keeps the current one on a deselect", () => {
    const selection = (previewNodeId: number | null): SelectionMessage => ({
      type: "selection",
      previewNodeId,
      selectedNodeIds: [],
    });
    expect(nextNodeId(null, selection(3))).toBe(3);
    expect(nextNodeId(3, selection(5))).toBe(5);
    expect(nextNodeId(3, selection(null))).toBe(3);
    expect(nextNodeId(null, selection(null))).toBeNull();
  });
});
