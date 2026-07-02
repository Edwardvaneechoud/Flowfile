// Pure helpers for flow-parameter tokens in the formula/filter expression editor.
// The syntax mirrors the backend resolver (`parameter_resolver._PARAM_PATTERN`):
// a reference is `${name}` where name = [a-zA-Z_][a-zA-Z0-9_]*. Kept dependency-free
// and unit-tested so highlighting, autocomplete, hover and the sidebar share one
// definition (see paramTokens.test.ts).
import type { FlowParameter, FlowParamType } from "../../../types/flow.types";

export const PARAM_NAME_RE = "[a-zA-Z_][a-zA-Z0-9_]*";

// The first alternative absorbs a matching wrapping quote pair ("${p}" / '${p}')
// so a quoted string-param reads as ONE param token rather than a string literal.
export function paramRegex(): RegExp {
  return new RegExp(`(["'])\\$\\{(${PARAM_NAME_RE})\\}\\1|\\$\\{(${PARAM_NAME_RE})\\}`, "g");
}

export interface ParamSpan {
  start: number; // offset of first char of the token (the quote, if quoted)
  end: number; // offset just past the last char (the closing quote, if quoted)
  name: string; // captured parameter name
  quoted: boolean; // whether the token includes wrapping quotes
}

// All param tokens in `text`; offsets are relative to `text` (callers add a base).
export function findParamSpans(text: string): ParamSpan[] {
  const re = paramRegex();
  const spans: ParamSpan[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const quoted = m[2] !== undefined;
    spans.push({
      start: m.index,
      end: m.index + m[0].length,
      name: quoted ? m[2] : m[3],
      quoted,
    });
  }
  return spans;
}

// "bare" -> always `${name}` (paired with the backend expression-literal renderer,
// which quotes strings itself). "quoted" -> `"${name}"` for string/enum, bare for
// numeric/boolean; a fallback for when the backend rendering is unavailable.
export type ParamInsertVariant = "bare" | "quoted";
export const PARAM_INSERT_VARIANT: ParamInsertVariant = "bare";

export function paramInsertText(
  param: Pick<FlowParameter, "name" | "type">,
  variant: ParamInsertVariant = PARAM_INSERT_VARIANT,
): string {
  if (variant === "bare") return `\${${param.name}}`;
  const type: FlowParamType = param.type ?? "string";
  const needsQuotes = type === "string" || type === "enum";
  return needsQuotes ? `"\${${param.name}}"` : `\${${param.name}}`;
}

export function isKnownParam(name: string, known: ReadonlySet<string>): boolean {
  return known.has(name);
}

// Short badge label for the pill / completion detail.
export function shortType(type?: FlowParamType): string {
  switch (type ?? "string") {
    case "integer":
      return "int";
    case "float":
      return "float";
    case "boolean":
      return "bool";
    case "enum":
      return "enum";
    default:
      return "str";
  }
}
