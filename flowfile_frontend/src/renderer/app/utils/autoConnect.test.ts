// Unit tests for palette-drop auto-connect geometry.

import { describe, it, expect } from "vitest";
import { findAutoConnectMatch, preferUnusedOutputs, type AutoConnectNode } from "./autoConnect";

// 60x60 node at (100, 100): output anchor (160, 130), input anchor (100, 130).
const reader: AutoConnectNode = {
  id: "1",
  x: 100,
  y: 100,
  width: 60,
  height: 60,
  freeInputs: [],
  outputs: ["output-0"],
};

const transform = { inputHandle: "input-0", outputHandle: "output-0" };

describe("findAutoConnectMatch", () => {
  it("connects a transform dropped just right of a node to that node's output", () => {
    const match = findAutoConnectMatch({ x: 200, y: 100, ...transform }, [reader]);
    expect(match).toEqual({
      nodeId: "1",
      direction: "upstream",
      existingHandle: "output-0",
      newHandle: "input-0",
    });
  });

  it("ignores drops outside the radius", () => {
    expect(findAutoConnectMatch({ x: 400, y: 100, ...transform }, [reader])).toBeNull();
    expect(findAutoConnectMatch({ x: 200, y: 300, ...transform }, [reader])).toBeNull();
  });

  it("never connects a drop on top of or left of a node as its downstream", () => {
    expect(findAutoConnectMatch({ x: 100, y: 100, ...transform }, [reader])).toBeNull();
    expect(findAutoConnectMatch({ x: 60, y: 100, ...transform }, [reader])).toBeNull();
  });

  it("does nothing for a node without inputs dropped after a node", () => {
    const drop = { x: 200, y: 100, inputHandle: null, outputHandle: "output-0" };
    expect(findAutoConnectMatch(drop, [reader])).toBeNull();
  });

  it("connects a source dropped left of a node into its first free input", () => {
    const join: AutoConnectNode = {
      ...reader,
      id: "2",
      x: 300,
      freeInputs: ["input-1"],
    };
    const drop = { x: 220, y: 100, inputHandle: null, outputHandle: "output-0" };
    expect(findAutoConnectMatch(drop, [join])).toEqual({
      nodeId: "2",
      direction: "downstream",
      existingHandle: "input-1",
      newHandle: "output-0",
    });
  });

  it("skips downstream targets with no free input", () => {
    const filter: AutoConnectNode = { ...reader, id: "2", x: 300, freeInputs: [] };
    const drop = { x: 220, y: 100, inputHandle: null, outputHandle: "output-0" };
    expect(findAutoConnectMatch(drop, [filter])).toBeNull();
  });

  it("prefers the nearest candidate and upstream on a tie", () => {
    const left: AutoConnectNode = { ...reader, id: "L", x: 100 };
    const right: AutoConnectNode = { ...reader, id: "R", x: 340, freeInputs: ["input-0"] };
    // Exactly between: 60 from L's output (160) to the new input (220), 60 from
    // the new output (280) to R's input (340).
    const match = findAutoConnectMatch({ x: 220, y: 100, ...transform }, [left, right]);
    expect(match?.nodeId).toBe("L");
    expect(match?.direction).toBe("upstream");
    const closerToRight = findAutoConnectMatch({ x: 230, y: 100, ...transform }, [left, right]);
    expect(closerToRight?.nodeId).toBe("R");
  });

  it("uses the median node size to estimate the dropped node", () => {
    const tall: AutoConnectNode = { ...reader, id: "T", x: 300, height: 400, freeInputs: [] };
    // With reader's 60px height the new input anchor is y=130, in range of reader;
    // the tall node's median-skewing size must not pull the anchor far off.
    const match = findAutoConnectMatch({ x: 200, y: 100, ...transform }, [reader, tall]);
    expect(match?.nodeId).toBe("1");
  });

  it("uses the dragged node's own size when given", () => {
    // A 200-wide node at x=100 ends at x=300, 100px short of the join's input.
    const join: AutoConnectNode = { ...reader, id: "2", x: 400, freeInputs: ["input-0"] };
    const drop = {
      x: 100,
      y: 100,
      width: 200,
      height: 60,
      inputHandle: null,
      outputHandle: "output-0",
    };
    expect(findAutoConnectMatch(drop, [join])?.direction).toBe("downstream");
    // The 60px median estimate would leave it 240px short.
    expect(findAutoConnectMatch({ ...drop, width: undefined }, [join])).toBeNull();
  });

  it("returns null on an empty canvas", () => {
    expect(findAutoConnectMatch({ x: 0, y: 0, ...transform }, [])).toBeNull();
  });
});

describe("preferUnusedOutputs", () => {
  it("moves outputs that already have an edge behind the unused ones", () => {
    expect(preferUnusedOutputs(["output-0", "output-1"], new Set(["output-0"]))).toEqual([
      "output-1",
      "output-0",
    ]);
    expect(preferUnusedOutputs(["output-0"], new Set())).toEqual(["output-0"]);
  });
});
