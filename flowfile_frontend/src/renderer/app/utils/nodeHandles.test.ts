// Unit tests for canvas handle derivation — guards the drop/copy/import
// contract for both static and dynamic-handle (run_flow) nodes.

import { describe, it, expect } from "vitest";
import { Position } from "@vue-flow/core";
import {
  buildOutputHandles,
  buildDynamicInputHandles,
  buildDynamicOutputHandles,
  deriveHandles,
} from "./nodeHandles";

const staticTemplate = {
  input: 2,
  output: 1,
  multi: false,
};

describe("deriveHandles (static nodes)", () => {
  it("derives bare input handles from the template count", () => {
    const { inputs, outputs } = deriveHandles(staticTemplate);
    expect(inputs.map((h) => h.id)).toEqual(["input-0", "input-1"]);
    expect(inputs.every((h) => h.kind === undefined)).toBe(true);
    expect(outputs.map((h) => h.id)).toEqual(["output-0"]);
  });

  it("collapses multi nodes to a single input handle", () => {
    const { inputs } = deriveHandles({ input: 10, output: 1, multi: true });
    expect(inputs.map((h) => h.id)).toEqual(["input-0"]);
  });

  it("labels static multi-output handles with letters and names as titles", () => {
    const { outputs } = deriveHandles({ input: 1, output: 2, output_names: ["left", "right"] });
    expect(outputs.map((h) => h.label)).toEqual(["A", "B"]);
    expect(outputs.map((h) => h.title)).toEqual(["left", "right"]);
  });
});

describe("deriveHandles (dynamic nodes)", () => {
  it("gives a fresh run_flow no handles until a flow is picked", () => {
    const { inputs, outputs } = deriveHandles({
      input: 1,
      output: 1,
      dynamic_inputs: true,
    });
    expect(inputs).toEqual([]);
    expect(outputs).toEqual([]);
  });

  it("derives ids/labels/kind from instance input_names/output_names", () => {
    const { inputs, outputs } = deriveHandles({
      input: 1,
      output: 1,
      input_names: ["Parameters", "orders", "customers"],
      output_names: ["result", "errors"],
    });
    expect(inputs.map((h) => h.id)).toEqual(["input-0", "input-1", "input-2"]);
    expect(inputs.map((h) => h.kind)).toEqual(["parameter", "data", "data"]);
    expect(inputs[0].position).toBe(Position.Bottom);
    expect(inputs.slice(1).map((h) => h.label)).toEqual(["orders", "customers"]);
    expect(outputs.map((h) => h.id)).toEqual(["output-0", "output-1"]);
    expect(outputs.map((h) => h.label)).toEqual(["result", "errors"]);
  });

  it("omits the parameter handle when the subflow has no parameters", () => {
    const { inputs } = deriveHandles({
      input: 1,
      output: 1,
      input_names: ["", "orders"],
    });
    expect(inputs.map((h) => h.id)).toEqual(["input-1"]);
    expect(inputs[0].kind).toBe("data");
  });

  it("derives output count from output_names alone, not template.output", () => {
    const { outputs } = deriveHandles({
      input: 1,
      output: 1,
      dynamic_inputs: true,
      output_names: [],
    });
    expect(outputs).toEqual([]);
  });
});

describe("buildDynamicInputHandles", () => {
  it("marks only index 0 as the parameter handle, at the bottom", () => {
    const handles = buildDynamicInputHandles(["Parameters", "left"]);
    expect(handles[0].kind).toBe("parameter");
    expect(handles[0].position).toBe(Position.Bottom);
    expect(handles[0].title).toBe("Parameter data (optional)");
    expect(handles[1].kind).toBe("data");
    expect(handles[1].title).toBe("left");
  });
});

describe("buildDynamicOutputHandles", () => {
  it("uses the full output names as labels", () => {
    const handles = buildDynamicOutputHandles(["main"]);
    expect(handles).toHaveLength(1);
    expect(handles[0].id).toBe("output-0");
    expect(handles[0].label).toBe("main");
  });
});

describe("buildOutputHandles (regression)", () => {
  it("uses max(template output count, saved names length)", () => {
    expect(buildOutputHandles(2)).toHaveLength(2);
    expect(buildOutputHandles(2, ["a", "b", "c"])).toHaveLength(3);
    expect(buildOutputHandles(3, ["a"])).toHaveLength(3);
  });

  it("omits labels for single-output nodes", () => {
    const handles = buildOutputHandles(1, ["only"]);
    expect(handles[0].label).toBeUndefined();
    expect(handles[0].title).toBeUndefined();
  });
});
