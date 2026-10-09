import { describe, expect, it } from "vitest";
import type { SelectionMessage } from "../../../lib/popoutWindow";
import type { NodeInput, VueFlowInput } from "../../types/flow.types";
import { applySelection, outputsForNode } from "./tableWindow";

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

describe("applySelection", () => {
  const selection = (previewNodeId: number | null, previewToken = 1): SelectionMessage => ({
    type: "selection",
    previewNodeId,
    previewToken,
  });

  it("follows a previewed node and keeps the current one on a deselect", () => {
    expect(applySelection({ nodeId: null, token: null }, selection(3))).toEqual({
      preview: { nodeId: 3, token: 1 },
      refetch: false,
    });
    expect(applySelection({ nodeId: 3, token: 1 }, selection(5, 2))).toEqual({
      preview: { nodeId: 5, token: 2 },
      refetch: false,
    });
    expect(applySelection({ nodeId: 3, token: 1 }, selection(null, 0))).toEqual({
      preview: { nodeId: 3, token: 1 },
      refetch: false,
    });
    expect(applySelection({ nodeId: null, token: null }, selection(null, 0)).preview.nodeId).toBeNull();
  });

  it("reads the same node again only when the designer sent it again", () => {
    const shown = { nodeId: 3, token: 1 };
    expect(applySelection(shown, selection(3, 2))).toEqual({
      preview: { nodeId: 3, token: 2 },
      refetch: true,
    });
    expect(applySelection(shown, selection(3, 1)).refetch).toBe(false);
    // The URL gave the node: the first message after a mount or reload only adopts the count.
    expect(applySelection({ nodeId: 3, token: null }, selection(3, 5))).toEqual({
      preview: { nodeId: 3, token: 5 },
      refetch: false,
    });
  });
});
