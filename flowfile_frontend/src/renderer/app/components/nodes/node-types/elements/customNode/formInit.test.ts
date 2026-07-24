// Unit tests for the pure custom-node form helpers (no Vue/axios involved).
import { describe, expect, it } from "vitest";
import { initializeFormData, resolveOptionsLoading, schemaWantsGlobalArtifacts } from "./formInit";
import type { CustomNodeSchema, UIComponent } from "./interface";

const schemaWith = (components: Record<string, Partial<UIComponent>>): CustomNodeSchema =>
  ({
    node_name: "Test",
    node_category: "Custom",
    node_icon: "icon.png",
    number_of_inputs: 1,
    number_of_outputs: 1,
    node_type: "process",
    transform_type: "wide",
    settings_schema: {
      main: { component_type: "Section", components } as never,
    },
  }) as CustomNodeSchema;

describe("initializeFormData precedence", () => {
  it("prefers saved value over schema value over default", () => {
    const schema = schemaWith({
      a: {
        component_type: "TextInput",
        value: "from-schema",
        default: "fallback",
        input_type: "text",
      },
      b: { component_type: "TextInput", value: "kept", default: "fallback", input_type: "text" },
      c: { component_type: "TextInput", value: undefined, default: "fallback", input_type: "text" },
    });
    const saved = { main: { a: "from-saved" } };

    const data = initializeFormData(schema, saved);
    expect(data.main.a).toBe("from-saved");
    expect(data.main.b).toBe("kept");
    expect(data.main.c).toBe("fallback");
  });

  it("defaults array inputs with null default to []", () => {
    const schema = schemaWith({
      cols: { component_type: "MultiSelect", value: undefined, default: null, input_type: "array" },
      note: { component_type: "TextInput", value: undefined, default: null, input_type: "text" },
    });
    const data = initializeFormData(schema, null);
    expect(data.main.cols).toEqual([]);
    expect(data.main.note).toBeNull();
  });

  it("ignores saved keys the schema no longer defines", () => {
    const schema = schemaWith({
      a: { component_type: "TextInput", value: undefined, default: "d", input_type: "text" },
    });
    const data = initializeFormData(schema, { main: { gone: 1 }, dropped_section: { x: 2 } });
    expect(data).toEqual({ main: { a: "d" } });
  });
});

describe("schemaWantsGlobalArtifacts", () => {
  const artifactSchema = (scope?: string): CustomNodeSchema =>
    schemaWith({
      pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", ...(scope ? { scope } : {}) },
      } as never,
    });

  it("is true only for global/all artifact scopes", () => {
    expect(schemaWantsGlobalArtifacts(artifactSchema("global"))).toBe(true);
    expect(schemaWantsGlobalArtifacts(artifactSchema("all"))).toBe(true);
    expect(schemaWantsGlobalArtifacts(artifactSchema("upstream"))).toBe(false);
    expect(schemaWantsGlobalArtifacts(artifactSchema(undefined))).toBe(false);
  });

  it("is false for incoming-column and static selects", () => {
    const incoming = schemaWith({
      pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "IncomingColumns" },
      } as never,
      fixed: {
        component_type: "MultiSelect",
        input_type: "array",
        value: undefined,
        default: null,
        options: ["a", "b"],
      } as never,
    });
    expect(schemaWantsGlobalArtifacts(incoming)).toBe(false);
  });
});

describe("resolveOptionsLoading", () => {
  const component = (partial: Record<string, unknown>): UIComponent => partial as never;

  it("column-driven components follow columnsLoading", () => {
    expect(
      resolveOptionsLoading(component({ component_type: "ColumnSelector" }), true, false),
    ).toBe(true);
    expect(
      resolveOptionsLoading(component({ component_type: "ColumnActionInput" }), true, false),
    ).toBe(true);
    expect(
      resolveOptionsLoading(
        component({ component_type: "SingleSelect", options: { __type__: "IncomingColumns" } }),
        true,
        false,
      ),
    ).toBe(true);
  });

  it("artifact selects follow artifactsLoading; static lists never load", () => {
    expect(
      resolveOptionsLoading(
        component({ component_type: "MultiSelect", options: { __type__: "AvailableArtifacts" } }),
        false,
        true,
      ),
    ).toBe(true);
    expect(
      resolveOptionsLoading(
        component({ component_type: "SingleSelect", options: ["a"] }),
        true,
        true,
      ),
    ).toBe(false);
    expect(resolveOptionsLoading(component({ component_type: "TextInput" }), true, true)).toBe(
      false,
    );
  });
});
