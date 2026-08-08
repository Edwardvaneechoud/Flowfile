// Unit tests for the pure custom-node form helpers (no Vue/axios involved).
import { describe, expect, it } from "vitest";
import {
  coerceArtifactValue,
  coerceLegacyArtifactValues,
  initializeFormData,
  resolveOptionsLoading,
  schemaWantsGlobalArtifacts,
} from "./formInit";
import type { CustomNodeSchema, UIComponent } from "./interface";
import type { ArtifactSelectOption } from "./components/artifactFilter";

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

describe("legacy bare-name artifact values", () => {
  const opt = (value: string, key: string): ArtifactSelectOption => ({ value, label: value, key });
  const options = [
    opt("General.default::churn", "1::churn"),
    opt("General.models::churn", "7::churn"),
    opt("General.models::scaler", "7::scaler"),
    opt("plain", "::plain"),
  ];

  it("resolves an unambiguous bare name onto its qualified option", () => {
    expect(coerceArtifactValue("scaler", options)).toBe("General.models::scaler");
  });

  it("clears an ambiguous bare name instead of guessing", () => {
    expect(coerceArtifactValue("churn", options)).toBeNull();
  });

  it("leaves already-qualified, unknown and non-string values alone", () => {
    expect(coerceArtifactValue("General.models::churn", options)).toBe("General.models::churn");
    expect(coerceArtifactValue("gone", options)).toBe("gone");
    expect(coerceArtifactValue("plain", options)).toBe("plain");
    expect(coerceArtifactValue("", options)).toBe("");
    expect(coerceArtifactValue(7, options)).toBe(7);
  });

  it("maps multi-select arrays and drops the ambiguous entries", () => {
    expect(coerceArtifactValue(["scaler", "churn", "plain"], options)).toEqual([
      "General.models::scaler",
      "plain",
    ]);
  });

  it("only touches global-scope artifact selects", () => {
    const schema = schemaWith({
      global_pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", scope: "global" },
      } as never,
      upstream_pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", scope: "upstream" },
      } as never,
      all_pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", scope: "all" },
      } as never,
      text: { component_type: "TextInput", value: undefined, default: null, input_type: "text" },
    });
    const data = {
      main: {
        global_pick: "scaler",
        upstream_pick: "scaler",
        all_pick: "scaler",
        text: "scaler",
      },
    };
    coerceLegacyArtifactValues(schema, data, () => options);
    expect(data.main.global_pick).toBe("General.models::scaler");
    expect(data.main.upstream_pick).toBe("scaler");
    expect(data.main.all_pick).toBe("scaler");
    expect(data.main.text).toBe("scaler");
  });

  // A scope-"all" value may name an upstream artifact; a failed upstream fetch
  // leaves only catalog options, which must never repoint the saved selection.
  it("never repoints a scope-all selection when the upstream fetch came back empty", () => {
    const schema = schemaWith({
      pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", scope: "all" },
      } as never,
      ambiguous_pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", scope: "all" },
      } as never,
    });
    const data = { main: { pick: "scaler", ambiguous_pick: "churn" } };

    coerceLegacyArtifactValues(schema, data, () => options);

    expect(data.main.pick).toBe("scaler");
    expect(data.main.ambiguous_pick).toBe("churn");
  });

  it("skips components with no saved selection", () => {
    const schema = schemaWith({
      pick: {
        component_type: "SingleSelect",
        input_type: "text",
        value: undefined,
        default: null,
        options: { __type__: "AvailableArtifacts", scope: "global" },
      } as never,
    });
    const data = { main: { pick: null } };
    coerceLegacyArtifactValues(schema, data, () => options);
    expect(data.main.pick).toBeNull();
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
