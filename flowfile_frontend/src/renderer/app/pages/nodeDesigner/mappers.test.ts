// mappers: DesignerState -> frontend SettingsSchema (drift-gated against the
// shared roundtrip fixture, which the backend also asserts to_frontend_schema()
// against), plus legacy draft conversion and per-type defaults.
import { describe, expect, it } from "vitest";
import {
  defaultComponentState,
  designerStateToFrontendSchema,
  legacyDraftToDesignerState,
  toPascalCase,
  toSnakeCase,
} from "./mappers";
import type { DesignerState } from "./designerState";
import fixture from "./__fixtures__/designer_state_roundtrip.json";

const state = fixture as unknown as DesignerState;

describe("designerStateToFrontendSchema", () => {
  const schema = designerStateToFrontendSchema(state);

  it("emits one Section per designer section, keyed by name", () => {
    expect(Object.keys(schema)).toEqual(["main_config", "advanced"]);
    expect(schema.main_config.component_type).toBe("Section");
    expect(schema.main_config.title).toBe("Main config");
    expect(schema.main_config.description).toBe("Primary settings.");
  });

  it("keys components by name and carries component_type", () => {
    const comps = schema.main_config.components;
    expect(Object.keys(comps)).toEqual(["report_title", "include_totals", "mode", "group_cols"]);
    expect(comps.report_title.component_type).toBe("TextInput");
    expect(comps.include_totals.component_type).toBe("ToggleSwitch");
  });

  it("passes through labels, defaults and placeholders", () => {
    const text = schema.main_config.components.report_title;
    expect(text.label).toBe("Report title");
    expect(text.value).toBe("Q1");
    expect((text as { placeholder?: string }).placeholder).toBe("e.g. Q1");
  });

  it("renders static select options as a string array", () => {
    const select = schema.main_config.components.mode;
    expect((select as { options: unknown }).options).toEqual(["sum", "mean"]);
  });

  it("renders IncomingColumns options as the __type__ marker", () => {
    const multi = schema.advanced.components.extra_cols;
    expect((multi as { options: unknown }).options).toEqual({ __type__: "IncomingColumns" });
  });

  it("renders ColumnSelector required/multiple/data_types", () => {
    const col = schema.main_config.components.group_cols;
    expect((col as { required: boolean }).required).toBe(true);
    expect((col as { multiple: boolean }).multiple).toBe(true);
    expect((col as { data_types: unknown }).data_types).toBe("ALL");
  });
});

describe("defaultComponentState", () => {
  it("builds a ColumnActionInput with the default action set", () => {
    const comp = defaultComponentState("ColumnActionInput", "actions_1");
    expect(comp.component_type).toBe("ColumnActionInput");
    if (comp.component_type === "ColumnActionInput") {
      expect(comp.actions.map((a) => a.value)).toEqual(["sum", "mean", "min", "max"]);
      expect(comp.output_name_template).toBe("{column}_{action}");
    }
  });

  it("builds a SliderInput with sensible bounds", () => {
    const comp = defaultComponentState("SliderInput", "s");
    if (comp.component_type === "SliderInput") {
      expect(comp.min_value).toBe(0);
      expect(comp.max_value).toBe(100);
      expect(comp.step).toBe(1);
    }
  });
});

describe("legacyDraftToDesignerState", () => {
  it("converts a legacy draft, mapping requires_kernel -> environment", () => {
    const result = legacyDraftToDesignerState({
      nodeMetadata: {
        node_name: "Old Node",
        node_category: "Custom",
        title: "",
        intro: "",
        number_of_inputs: 2,
        number_of_outputs: 1,
        node_icon: "user-defined-icon.png",
        requires_kernel: true,
        kernel_id: "k1",
        output_names: ["main"],
      },
      sections: [
        {
          name: "opts",
          title: "Options",
          components: [
            {
              component_type: "TextInput",
              field_name: "greeting",
              label: "Greeting",
              default: "hi",
            },
            {
              component_type: "SingleSelect",
              field_name: "mode",
              label: "Mode",
              options_source: "static",
              options_string: "a, b, c",
            },
          ],
        },
      ],
      processCode: "def process(self, *inputs): return inputs[0]",
    });

    expect(result.node_name).toBe("Old Node");
    expect(result.class_name).toBe("OldNode");
    expect(result.number_of_inputs).toBe(2);
    expect(result.environment.kind).toBe("kernel");
    expect(result.environment.default_kernel_id).toBe("k1");
    expect(result.sections[0].name).toBe("opts");
    expect(result.sections[0].components[0].name).toBe("greeting");
    const select = result.sections[0].components[1];
    if (select.component_type === "SingleSelect") {
      expect(select.options.map((o) => o.value)).toEqual(["a", "b", "c"]);
    }
  });
});

describe("naming helpers", () => {
  it("toSnakeCase normalizes labels to python identifiers", () => {
    expect(toSnakeCase("My Field Name")).toBe("my_field_name");
    expect(toSnakeCase("someCamelCase")).toBe("some_camel_case");
  });

  it("toPascalCase builds a class name", () => {
    expect(toPascalCase("my cool node")).toBe("MyCoolNode");
    expect(toPascalCase("already_snake")).toBe("AlreadySnake");
  });
});
