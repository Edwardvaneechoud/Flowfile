// Hand-maintained TS mirror of the backend DesignerState wire contract
// (flowfile_core/flowfile_core/flowfile/node_designer/state.py). Keep 1:1:
// the AST parser produces this shape and the canonical code generator consumes it.
// Discriminated on `component_type`.

export type ComponentType =
  | "TextInput"
  | "NumericInput"
  | "SliderInput"
  | "ToggleSwitch"
  | "SingleSelect"
  | "MultiSelect"
  | "ColumnSelector"
  | "SecretSelector"
  | "ColumnActionInput";

export type OptionsSource = "static" | "incoming_columns" | "available_artifacts";

/** "ALL" or an explicit list of polars dtype group names. */
export type DataTypesSpec = "ALL" | string[];

export interface SelectOption {
  value: string;
  label?: string | null;
}

interface ComponentBase {
  name: string;
  label?: string | null;
}

export interface TextInputState extends ComponentBase {
  component_type: "TextInput";
  default?: string | null;
  placeholder?: string | null;
}

export interface NumericInputState extends ComponentBase {
  component_type: "NumericInput";
  default?: number | null;
  min_value?: number | null;
  max_value?: number | null;
}

export interface SliderInputState extends ComponentBase {
  component_type: "SliderInput";
  default?: number | null;
  min_value: number;
  max_value: number;
  step: number;
}

export interface ToggleSwitchState extends ComponentBase {
  component_type: "ToggleSwitch";
  default: boolean;
  description?: string | null;
}

export interface SelectState extends ComponentBase {
  component_type: "SingleSelect" | "MultiSelect";
  options_source: OptionsSource;
  options: SelectOption[];
  // available_artifacts only: lineage vs catalog source + dotted-type globs.
  artifact_scope: "upstream" | "global" | "all";
  artifact_type_filter: string[];
  // list for MultiSelect, scalar for SingleSelect
  default?: unknown;
}

export interface ColumnSelectorState extends ComponentBase {
  component_type: "ColumnSelector";
  required: boolean;
  multiple: boolean;
  data_types: DataTypesSpec;
}

export interface SecretSelectorState extends ComponentBase {
  component_type: "SecretSelector";
  required: boolean;
  description?: string | null;
  name_prefix?: string | null;
}

export interface ColumnActionInputState extends ComponentBase {
  component_type: "ColumnActionInput";
  actions: SelectOption[];
  output_name_template: string;
  show_group_by: boolean;
  show_order_by: boolean;
  data_types: DataTypesSpec;
}

export type ComponentState =
  | TextInputState
  | NumericInputState
  | SliderInputState
  | ToggleSwitchState
  | SelectState
  | ColumnSelectorState
  | SecretSelectorState
  | ColumnActionInputState;

export type SectionLayout = "vertical" | "horizontal";

/** Show the section only while the referenced toggle field equals `equals`. */
export interface VisibleWhenState {
  field: string; // dotted "<section>.<toggle>" reference
  equals?: boolean; // defaults to true
}

export interface SectionState {
  name: string;
  title?: string | null;
  description?: string | null;
  hidden: boolean;
  visible_when?: VisibleWhenState | null;
  layout: SectionLayout;
  components: ComponentState[]; // ordered
}

export type EnvironmentKind = "local" | "kernel";

export interface EnvironmentState {
  kind: EnvironmentKind;
  dependencies: string[]; // pip requirement strings, kernel only
  default_kernel_id?: string | null; // legacy kernel_id carry-over, deprecated
}

/** Column-oriented sample data for one input port. */
export interface ExampleInput {
  data: Record<string, unknown[]>;
}

/** A declared published artifact (name + optional dotted type hint). */
export interface PublishState {
  name: string;
  type: string | null;
}

export interface DesignerState {
  schema_version: 1;
  class_name: string;
  settings_class_name: string;
  node_name: string;
  node_category: string;
  node_group: string;
  node_icon: string;
  title: string;
  intro: string;
  author: string;
  version: string;
  tags: string[];
  number_of_inputs: number;
  number_of_outputs: number;
  output_names: string[];
  environment: EnvironmentState;
  publishes: PublishState[];
  sections: SectionState[];
  process_code: string; // full `def process(...)` source, dedented, verbatim
  predict_schema_code: string; // full `def predict_output_schema(...)` source; "" = no hook
  example_inputs: ExampleInput[] | null;
  example_settings: Record<string, Record<string, unknown>> | null;
  extra_imports: string[];
  module_extra: string[];
  class_extra: string[];
}

export type ParseIssueSeverity = "error" | "warning";

export interface ParseIssue {
  code: string;
  message: string;
  line?: number | null;
  severity: ParseIssueSeverity;
}

export type ParseMode = "designer" | "code_only";

export interface ParseResult {
  mode: ParseMode;
  designer_state: DesignerState | null; // null iff code_only
  issues: ParseIssue[];
}

export function newEnvironmentState(): EnvironmentState {
  return { kind: "local", dependencies: [], default_kernel_id: null };
}

/** A blank DesignerState with one seeded "settings" section, matching the store defaults. */
export function newDesignerState(): DesignerState {
  return {
    schema_version: 1,
    class_name: "MyCustomNode",
    settings_class_name: "MyCustomNodeSettings",
    node_name: "",
    node_category: "Custom",
    node_group: "custom",
    node_icon: "user-defined-icon.png",
    title: "",
    intro: "",
    author: "",
    version: "",
    tags: [],
    number_of_inputs: 1,
    number_of_outputs: 1,
    output_names: ["main"],
    environment: newEnvironmentState(),
    publishes: [],
    sections: [
      {
        name: "settings",
        title: "Settings",
        description: null,
        hidden: false,
        layout: "vertical",
        components: [],
      },
    ],
    process_code: "",
    predict_schema_code: "",
    example_inputs: null,
    example_settings: null,
    extra_imports: [],
    module_extra: [],
    class_extra: [],
  };
}
