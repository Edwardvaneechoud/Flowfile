// Unit tests for canvas handle derivation — guards the drop/copy/import
// contract for both static and dynamic-handle (run_flow) nodes.

import { describe, it, expect } from "vitest";
import { Position } from "@vue-flow/core";
import {
  buildInputHandles,
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
  it("derives letter-labelled input handles from the template count", () => {
    const { inputs, outputs } = deriveHandles(staticTemplate);
    expect(inputs.map((h) => h.id)).toEqual(["input-0", "input-1"]);
    expect(inputs.map((h) => h.label)).toEqual(["A", "B"]);
    expect(inputs.every((h) => h.kind === undefined)).toBe(true);
    expect(outputs.map((h) => h.id)).toEqual(["output-0"]);
  });

  it("collapses multi nodes to a single input handle", () => {
    const { inputs } = deriveHandles({ input: 10, output: 1, multi: true });
    expect(inputs.map((h) => h.id)).toEqual(["input-0"]);
  });

  it("labels static multi-output handles with initials and names as titles", () => {
    const { outputs } = deriveHandles({ input: 1, output: 2, output_names: ["left", "right"] });
    expect(outputs.map((h) => h.label)).toEqual(["L", "R"]);
    expect(outputs.map((h) => h.title)).toEqual(["left", "right"]);
  });

  it("falls back to positional letters when two output initials collide", () => {
    const { outputs } = deriveHandles({ input: 1, output: 2, output_names: ["train", "test"] });
    expect(outputs.map((h) => h.label)).toEqual(["A", "B"]);
    expect(outputs.map((h) => h.title)).toEqual(["train", "test"]);
  });
});

describe("deriveHandles (static input labels)", () => {
  it("letters multi-input handles from the declared names and titles them fully", () => {
    const { inputs } = deriveHandles({ input: 2, output: 1, input_labels: ["Left", "Right"] });
    expect(inputs.map((h) => h.id)).toEqual(["input-0", "input-1"]);
    expect(inputs.map((h) => h.label)).toEqual(["L", "R"]);
    expect(inputs.map((h) => h.title)).toEqual(["Left", "Right"]);
  });

  it("uses initials for a custom node's own port names", () => {
    const { inputs } = deriveHandles({
      input: 2,
      output: 1,
      input_labels: ["training", "scoring"],
    });
    expect(inputs.map((h) => h.label)).toEqual(["T", "S"]);
  });

  it("falls back to positional letters when two input initials collide", () => {
    const { inputs } = deriveHandles({
      input: 2,
      output: 1,
      input_labels: ["scoring", "sampling"],
    });
    expect(inputs.map((h) => h.label)).toEqual(["A", "B"]);
    expect(inputs.map((h) => h.title)).toEqual(["scoring", "sampling"]);
  });

  it("falls back to positional letters when a name starts with punctuation", () => {
    const { inputs } = deriveHandles({ input: 2, output: 1, input_labels: ["(left)", "right"] });
    expect(inputs.map((h) => h.label)).toEqual(["A", "B"]);
  });

  // Regression guard for the branch discriminator: input_labels must not be
  // mistaken for run_flow's input_names, which would turn index 0 into a
  // bottom-positioned parameter handle and silently drop an input.
  it("keeps a named static node on the static path", () => {
    const { inputs, outputs } = deriveHandles({
      input: 2,
      output: 1,
      input_labels: ["Left", "Right"],
    });
    expect(inputs.every((h) => h.position === Position.Left)).toBe(true);
    expect(inputs.every((h) => h.kind === undefined)).toBe(true);
    expect(outputs.map((h) => h.id)).toEqual(["output-0"]);
  });

  it("omits the label on a single-input node even when a name is declared", () => {
    const { inputs } = deriveHandles({ input: 1, output: 1, input_labels: ["only"] });
    expect(inputs).toHaveLength(1);
    expect(inputs[0].label).toBeUndefined();
    expect(inputs[0].title).toBe("only");
  });

  it("omits labels on multi nodes, which collapse to one handle", () => {
    const { inputs } = deriveHandles({
      input: 10,
      output: 1,
      multi: true,
      input_labels: ["a", "b"],
    });
    expect(inputs.map((h) => h.id)).toEqual(["input-0"]);
    expect(inputs[0].label).toBeUndefined();
  });

  it("letters every handle when fewer names than inputs are declared", () => {
    const { inputs } = deriveHandles({ input: 3, output: 1, input_labels: ["Left"] });
    expect(inputs.map((h) => h.label)).toEqual(["A", "B", "C"]);
    expect(inputs.map((h) => h.title)).toEqual(["Left", undefined, undefined]);
  });

  it("uses the same letter sequence for inputs and outputs at the same index", () => {
    const { inputs, outputs } = deriveHandles({ input: 3, output: 3 });
    expect(inputs.map((h) => h.label)).toEqual(outputs.map((h) => h.label));
  });
});

describe("buildInputHandles", () => {
  it("labels only when there is more than one handle", () => {
    expect(buildInputHandles(1, ["only"])[0].label).toBeUndefined();
    expect(buildInputHandles(2, ["a", "b"]).map((h) => h.label)).toEqual(["A", "B"]);
  });

  it("returns no handles for a zero-input node", () => {
    expect(buildInputHandles(0)).toEqual([]);
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

  // run_flow deliberately spells its ports out instead of using letters — the
  // names are per-instance and carry real meaning.
  it("leaves run_flow data handles labelled with the full subflow port name", () => {
    const { inputs } = deriveHandles({
      input: 1,
      output: 1,
      input_names: ["Parameters", "orders", "customers"],
    });
    expect(inputs.slice(1).map((h) => h.label)).toEqual(["orders", "customers"]);
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

describe("deriveHandles (control input pips)", () => {
  // Gate's control input renders as a bottom "parameter" pip; the lone data
  // handle then deliberately carries no letter chip (side count drops to 1).
  it("renders a marked input as a bottom parameter handle", () => {
    const { inputs } = deriveHandles({
      input: 2,
      output: 1,
      input_labels: ["Data (passes through)", "Control (bottom handle)"],
      control_input_indices: [1],
    });
    expect(inputs.map((h) => h.id)).toEqual(["input-0", "input-1"]);
    expect(inputs[0].position).toBe(Position.Left);
    expect(inputs[0].kind).toBeUndefined();
    expect(inputs[0].label).toBeUndefined();
    expect(inputs[1].position).toBe(Position.Bottom);
    expect(inputs[1].kind).toBe("parameter");
    expect(inputs[1].title).toBe("Control (bottom handle)");
  });

  it("gate with else output gets T/E letters on the output side", () => {
    const { outputs } = deriveHandles({
      input: 2,
      output: 1,
      output_names: ["then", "else"],
      control_input_indices: [1],
    });
    expect(outputs.map((h) => h.id)).toEqual(["output-0", "output-1"]);
    expect(outputs.map((h) => h.label)).toEqual(["T", "E"]);
    expect(outputs.map((h) => h.title)).toEqual(["then", "else"]);
  });
});

describe("buildOutputHandles (regression)", () => {
  it("uses max(template output count, saved names length)", () => {
    expect(buildOutputHandles(2)).toHaveLength(2);
    expect(buildOutputHandles(2, ["a", "b", "c"])).toHaveLength(3);
    expect(buildOutputHandles(3, ["a"])).toHaveLength(3);
  });

  // A split-mode filter declares output_names but keeps template output=1, so the
  // handle count has to come from the names or the second handle vanishes on reload.
  it("sizes off the names when a node declares more than its template output", () => {
    const handles = buildOutputHandles(1, ["pass", "fail"]);
    expect(handles.map((h) => h.id)).toEqual(["output-0", "output-1"]);
    expect(handles.map((h) => h.label)).toEqual(["P", "F"]);
    expect(handles.map((h) => h.title)).toEqual(["pass", "fail"]);
  });

  it("omits labels for single-output nodes", () => {
    const handles = buildOutputHandles(1, ["only"]);
    expect(handles[0].label).toBeUndefined();
    expect(handles[0].title).toBeUndefined();
  });
});
