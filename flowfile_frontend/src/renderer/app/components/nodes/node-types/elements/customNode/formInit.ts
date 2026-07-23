// Pure helpers for the custom-node settings form (no Vue/axios imports so they
// stay unit-testable in node-env vitest).
import type { CustomNodeSchema, SectionComponent, UIComponent } from "./interface";

// Inner values are `any`: the child inputs each declare their own modelValue type
// and the drawer has always fed them untyped saved-settings values.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type CustomNodeFormData = Record<string, Record<string, any>>;

// Builds the initial form data: saved value wins, then the schema's current
// value, then the component default (arrays default to []).
export function initializeFormData(
  schemaData: CustomNodeSchema,
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  savedSettings: any,
): CustomNodeFormData {
  const data: CustomNodeFormData = {};

  for (const sectionKey in schemaData.settings_schema) {
    data[sectionKey] = {};
    const section: SectionComponent = schemaData.settings_schema[sectionKey];
    for (const componentKey in section.components) {
      const component = section.components[componentKey];

      const savedValue = savedSettings?.[sectionKey]?.[componentKey];

      if (savedValue !== undefined) {
        data[sectionKey][componentKey] = savedValue;
      } else if (component.value !== undefined) {
        data[sectionKey][componentKey] = component.value;
      } else {
        let defaultValue = component.default ?? null;
        if (component.input_type === "array" && defaultValue === null) {
          defaultValue = [];
        }
        data[sectionKey][componentKey] = defaultValue;
      }
    }
  }
  return data;
}

// True when any select in the schema opts into the global (catalog) artifact source.
export function schemaWantsGlobalArtifacts(schemaData: CustomNodeSchema): boolean {
  for (const sectionKey in schemaData.settings_schema) {
    const section = schemaData.settings_schema[sectionKey];
    for (const componentKey in section.components) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const options = (section.components[componentKey] as any).options;
      if (
        options?.__type__ === "AvailableArtifacts" &&
        (options.scope === "global" || options.scope === "all")
      )
        return true;
    }
  }
  return false;
}

// Which hydration flag gates a component's option list: incoming-column selects
// follow columnsLoading, artifact selects follow artifactsLoading, static lists none.
export function resolveOptionsLoading(
  component: UIComponent,
  columnsLoading: boolean,
  artifactsLoading: boolean,
): boolean {
  if (
    component.component_type === "ColumnSelector" ||
    component.component_type === "ColumnActionInput"
  ) {
    return columnsLoading;
  }
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const options = (component as any).options;
  if (options?.__type__ === "IncomingColumns") return columnsLoading;
  if (options?.__type__ === "AvailableArtifacts") return artifactsLoading;
  return false;
}
