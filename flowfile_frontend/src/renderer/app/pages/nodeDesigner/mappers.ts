// DesignerState <-> CustomNodeForm schema, plus legacy store-shape converters.
//
// `designerStateToFrontendSchema` mirrors the backend to_frontend_schema() output
// (shared/node_designer/custom_node.py) so the live preview updates without a
// backend round trip. `mappers.test.ts` asserts it against the shared drift fixture.
import type {
  ColumnActionInputState,
  ColumnSelectorState,
  ComponentState,
  ComponentType,
  DataTypesSpec,
  DesignerState,
  NumericInputState,
  SecretSelectorState,
  SectionState,
  SelectState,
  SliderInputState,
  TextInputState,
  ToggleSwitchState,
} from "./designerState";
import { newDesignerState, newEnvironmentState } from "./designerState";
import type {
  AvailableArtifactsMarker,
  ColumnActionInputComponent,
  ColumnSelectorComponent,
  MultiSelectComponent,
  NumericInputComponent,
  SecretSelectorComponent,
  SectionComponent,
  SettingsSchema,
  SingleSelectComponent,
  SliderInputComponent,
  TextInputComponent,
  ToggleSwitchComponent,
  UIComponent,
} from "../../components/nodes/node-types/elements/customNode/interface";
import type { DesignerComponent, DesignerSection, NodeMetadata } from "./types";

// --- naming helpers (shared with the deleted useCodeGeneration) ---

export function toSnakeCase(str: string): string {
  return str
    .replace(/\s+/g, "_")
    .replace(/([a-z])([A-Z])/g, "$1_$2")
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "");
}

export function toPascalCase(str: string): string {
  return str
    .split(/[\s_-]+/)
    .filter(Boolean)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join("");
}

// --- DesignerState -> frontend SettingsSchema (renderer shape) ---

const OPTIONS_INPUT_TYPE: Record<ComponentType, string> = {
  TextInput: "text",
  NumericInput: "number",
  SliderInput: "number",
  ToggleSwitch: "boolean",
  SingleSelect: "text",
  MultiSelect: "array",
  ColumnSelector: "array",
  SecretSelector: "secret",
  ColumnActionInput: "array",
};

type SelectOptions = SingleSelectComponent["options"];

function optionsFromSelect(comp: SelectState): SelectOptions {
  if (comp.options_source === "incoming_columns") return { __type__: "IncomingColumns" };
  if (comp.options_source === "available_artifacts") {
    // Omit-when-default: byte-identical to the backend serializer (wire contract).
    const marker: AvailableArtifactsMarker = { __type__: "AvailableArtifacts" };
    if (comp.artifact_scope && comp.artifact_scope !== "upstream") marker.scope = comp.artifact_scope;
    const filters = [...new Set(comp.artifact_type_filter ?? [])].sort();
    if (filters.length) marker.type_filter = filters;
    return marker;
  }
  return comp.options.map((o) => o.value);
}

function componentToFrontend(comp: ComponentState): UIComponent {
  const inputType = OPTIONS_INPUT_TYPE[comp.component_type];
  const base = {
    label: comp.label ?? comp.name,
    input_type: inputType,
  };

  switch (comp.component_type) {
    case "TextInput":
      return {
        ...base,
        component_type: "TextInput",
        value: comp.default ?? null,
        default: comp.default ?? null,
        placeholder: comp.placeholder ?? undefined,
      } as TextInputComponent;
    case "NumericInput":
      return {
        ...base,
        component_type: "NumericInput",
        value: comp.default ?? null,
        default: comp.default ?? null,
        min_value: comp.min_value ?? undefined,
        max_value: comp.max_value ?? undefined,
      } as NumericInputComponent;
    case "SliderInput":
      return {
        ...base,
        component_type: "SliderInput",
        value: comp.default ?? null,
        default: comp.default ?? null,
        min_value: comp.min_value,
        max_value: comp.max_value,
        step: comp.step,
      } as SliderInputComponent;
    case "ToggleSwitch":
      return {
        ...base,
        component_type: "ToggleSwitch",
        value: comp.default,
        default: comp.default,
      } as ToggleSwitchComponent;
    case "SingleSelect":
      return {
        ...base,
        component_type: "SingleSelect",
        value: comp.default ?? null,
        default: comp.default ?? null,
        options: optionsFromSelect(comp),
      } as SingleSelectComponent;
    case "MultiSelect":
      return {
        ...base,
        component_type: "MultiSelect",
        value: comp.default ?? [],
        default: comp.default ?? [],
        options: optionsFromSelect(comp),
      } as MultiSelectComponent;
    case "ColumnSelector":
      return {
        ...base,
        component_type: "ColumnSelector",
        value: comp.multiple ? [] : null,
        default: comp.multiple ? [] : null,
        required: comp.required,
        multiple: comp.multiple,
        data_types: comp.data_types,
      } as ColumnSelectorComponent;
    case "SecretSelector":
      return {
        ...base,
        component_type: "SecretSelector",
        value: null,
        default: null,
        required: comp.required,
        description: comp.description ?? undefined,
        options: { __type__: "AvailableSecrets" },
        name_prefix: comp.name_prefix ?? undefined,
      } as SecretSelectorComponent;
    case "ColumnActionInput":
      return {
        ...base,
        component_type: "ColumnActionInput",
        value: { rows: [], group_by_columns: [], order_by_column: null },
        default: { rows: [], group_by_columns: [], order_by_column: null },
        actions: comp.actions.map((a) => ({ value: a.value, label: a.label ?? a.value })),
        output_name_template: comp.output_name_template,
        show_group_by: comp.show_group_by,
        show_order_by: comp.show_order_by,
        data_types: comp.data_types,
      } as ColumnActionInputComponent;
  }
}

export function designerStateToFrontendSchema(state: DesignerState): SettingsSchema {
  const schema: SettingsSchema = {};
  for (const section of state.sections) {
    const components: Record<string, UIComponent> = {};
    for (const comp of section.components) {
      components[comp.name] = componentToFrontend(comp);
    }
    const sectionComp: SectionComponent = {
      component_type: "Section",
      title: section.title ?? undefined,
      description: section.description ?? undefined,
      hidden: section.hidden || undefined,
      visible_when: section.visible_when ?? undefined,
      layout: section.layout ?? "vertical",
      components,
    };
    schema[section.name] = sectionComp;
  }
  return schema;
}

// --- per-type default ComponentState (was NodeDesigner.vue::handleDrop) ---

export function defaultComponentState(type: ComponentType, name: string): ComponentState {
  const label = name;
  switch (type) {
    case "TextInput":
      return { component_type: "TextInput", name, label, default: "", placeholder: null };
    case "NumericInput":
      return {
        component_type: "NumericInput",
        name,
        label,
        default: 0,
        min_value: null,
        max_value: null,
      };
    case "SliderInput":
      return {
        component_type: "SliderInput",
        name,
        label,
        default: null,
        min_value: 0,
        max_value: 100,
        step: 1,
      };
    case "ToggleSwitch":
      return { component_type: "ToggleSwitch", name, label, default: false, description: null };
    case "SingleSelect":
      return {
        component_type: "SingleSelect",
        name,
        label,
        options_source: "static",
        options: [],
        artifact_scope: "upstream",
        artifact_type_filter: [],
        default: null,
      };
    case "MultiSelect":
      return {
        component_type: "MultiSelect",
        name,
        label,
        options_source: "static",
        options: [],
        artifact_scope: "upstream",
        artifact_type_filter: [],
        default: [],
      };
    case "ColumnSelector":
      return {
        component_type: "ColumnSelector",
        name,
        label,
        required: false,
        multiple: false,
        data_types: "ALL",
      };
    case "SecretSelector":
      return {
        component_type: "SecretSelector",
        name,
        label,
        required: false,
        description: null,
        name_prefix: null,
      };
    case "ColumnActionInput":
      return {
        component_type: "ColumnActionInput",
        name,
        label,
        actions: [
          { value: "sum", label: "sum" },
          { value: "mean", label: "mean" },
          { value: "min", label: "min" },
          { value: "max", label: "max" },
        ],
        output_name_template: "{column}_{action}",
        show_group_by: false,
        show_order_by: false,
        data_types: "ALL",
      };
  }
}

// --- legacy store-shape (DesignerComponent/Section/NodeMetadata) -> DesignerState ---
// Used for the one-time draft migration and defensive fallbacks.

function parseCsvOptions(raw: string | undefined): { value: string; label: string }[] {
  if (!raw) return [];
  return raw
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean)
    .map((v) => ({ value: v, label: v }));
}

function normalizeDataTypes(raw: unknown): DataTypesSpec {
  if (raw === undefined || raw === null || raw === "ALL") return "ALL";
  if (Array.isArray(raw)) return raw as string[];
  if (typeof raw === "string") return [raw];
  return "ALL";
}

function legacyComponentToState(comp: DesignerComponent): ComponentState {
  const name = toSnakeCase(comp.field_name || "field");
  const label = comp.label ?? name;
  const type = comp.component_type as ComponentType;
  switch (type) {
    case "TextInput":
      return {
        component_type: "TextInput",
        name,
        label,
        default: (comp.default as string) ?? "",
        placeholder: comp.placeholder ?? null,
      } as TextInputState;
    case "NumericInput":
      return {
        component_type: "NumericInput",
        name,
        label,
        default: (comp.default as number) ?? null,
        min_value: comp.min_value ?? null,
        max_value: comp.max_value ?? null,
      } as NumericInputState;
    case "SliderInput":
      return {
        component_type: "SliderInput",
        name,
        label,
        default: null,
        min_value: comp.min_value ?? 0,
        max_value: comp.max_value ?? 100,
        step: comp.step ?? 1,
      } as SliderInputState;
    case "ToggleSwitch":
      return {
        component_type: "ToggleSwitch",
        name,
        label,
        default: Boolean(comp.default),
        description: comp.description ?? null,
      } as ToggleSwitchState;
    case "SingleSelect":
    case "MultiSelect":
      return {
        component_type: type,
        name,
        label,
        options_source: (comp.options_source as SelectState["options_source"]) ?? "static",
        options: parseCsvOptions(comp.options_string),
        artifact_scope: "upstream",
        artifact_type_filter: [],
        default: type === "MultiSelect" ? [] : null,
      } as SelectState;
    case "ColumnSelector":
      return {
        component_type: "ColumnSelector",
        name,
        label,
        required: Boolean(comp.required),
        multiple: Boolean(comp.multiple),
        data_types: normalizeDataTypes(comp.data_types),
      } as ColumnSelectorState;
    case "SecretSelector":
      return {
        component_type: "SecretSelector",
        name,
        label,
        required: Boolean(comp.required),
        description: comp.description ?? null,
        name_prefix: comp.name_prefix ?? null,
      } as SecretSelectorState;
    case "ColumnActionInput":
      return {
        component_type: "ColumnActionInput",
        name,
        label,
        actions: parseCsvOptions(comp.actions_string),
        output_name_template: comp.output_name_template ?? "{column}_{action}",
        show_group_by: Boolean(comp.show_group_by),
        show_order_by: Boolean(comp.show_order_by),
        data_types: normalizeDataTypes(comp.data_types),
      } as ColumnActionInputState;
    default:
      return {
        component_type: "TextInput",
        name,
        label,
        default: "",
        placeholder: null,
      } as TextInputState;
  }
}

function legacySectionToState(section: DesignerSection): SectionState {
  return {
    name: toSnakeCase(section.name || section.title || "section"),
    title: section.title ?? null,
    description: null,
    hidden: false,
    layout: "vertical",
    components: (section.components ?? []).map(legacyComponentToState),
  };
}

export interface LegacyDraft {
  nodeMetadata: NodeMetadata;
  sections: DesignerSection[];
  processCode: string;
}

export function legacyDraftToDesignerState(draft: LegacyDraft): DesignerState {
  const state = newDesignerState();
  const meta = draft.nodeMetadata ?? ({} as NodeMetadata);
  state.class_name = toPascalCase(meta.node_name || "MyCustomNode") || "MyCustomNode";
  state.settings_class_name = `${state.class_name}Settings`;
  state.node_name = meta.node_name ?? "";
  state.node_category = meta.node_category ?? "Custom";
  state.node_icon = meta.node_icon ?? "user-defined-icon.png";
  state.title = meta.title ?? "";
  state.intro = meta.intro ?? "";
  state.number_of_inputs = meta.number_of_inputs ?? 1;
  state.number_of_outputs = meta.number_of_outputs ?? 1;
  state.output_names = meta.output_names ?? ["main"];
  state.environment = newEnvironmentState();
  if (meta.requires_kernel) {
    state.environment.kind = "kernel";
    state.environment.default_kernel_id = meta.kernel_id ?? null;
  }
  state.sections = (draft.sections ?? []).map(legacySectionToState);
  state.process_code = draft.processCode ?? "";
  return state;
}
