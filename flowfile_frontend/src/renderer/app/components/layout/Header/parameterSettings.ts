// Pure helpers for the typed flow-parameter editor (unit-tested in node env).
import type { FlowParameter, FlowParamType } from "../../../types/flow.types";

export const FLOW_PARAM_TYPES: FlowParamType[] = [
  "string",
  "integer",
  "float",
  "boolean",
  "enum",
];

// Backfill fields for parameters saved before types existed.
export function normalizeParameter(param: FlowParameter): FlowParameter {
  return {
    ...param,
    type: param.type ?? "string",
    enum_values: param.enum_values ?? [],
  };
}

// Coerce a default value into something valid for the parameter type:
// invalid int/float → "", boolean → "true"/"false", enum default not in the
// allowed values → first value (or "" when there are none).
export function coerceDefaultForType(
  value: string,
  type: FlowParamType,
  enumValues: string[] = [],
): string {
  switch (type) {
    case "integer": {
      const num = Number(value);
      return value.trim() !== "" && Number.isInteger(num) ? String(num) : "";
    }
    case "float": {
      const num = Number(value);
      return value.trim() !== "" && Number.isFinite(num) ? String(num) : "";
    }
    case "boolean":
      return value === "true" ? "true" : "false";
    case "enum":
      return enumValues.includes(value) ? value : (enumValues[0] ?? "");
    default:
      return value;
  }
}

// Comma-separated text → trimmed, deduped enum values.
export function enumValuesFromText(text: string): string[] {
  const seen = new Set<string>();
  const values: string[] = [];
  for (const raw of text.split(",")) {
    const value = raw.trim();
    if (value && !seen.has(value)) {
      seen.add(value);
      values.push(value);
    }
  }
  return values;
}

export function enumValuesToText(values: string[] | null | undefined): string {
  return (values ?? []).join(", ");
}

export interface ParameterIssue {
  index: number;
  message: string;
}

// Rows the backend would reject: enum parameters without values and duplicate
// (non-empty) names. While any exist the editor holds the settings POST and
// shows the message inline on the offending row.
export function validateParameters(params: FlowParameter[]): ParameterIssue[] {
  const issues: ParameterIssue[] = [];
  const seen = new Set<string>();
  params.forEach((param, index) => {
    if ((param.type ?? "string") === "enum" && (param.enum_values ?? []).length === 0) {
      issues.push({ index, message: "Enum parameters need at least one value" });
    }
    const name = param.name.trim();
    if (name) {
      if (seen.has(name)) {
        issues.push({ index, message: `Duplicate parameter name "${name}"` });
      }
      seen.add(name);
    }
  });
  return issues;
}
