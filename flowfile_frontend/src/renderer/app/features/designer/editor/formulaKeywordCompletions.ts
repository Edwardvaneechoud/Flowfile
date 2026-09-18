import type { Completion, CompletionContext, CompletionResult } from "@codemirror/autocomplete";
import { isInsideStringOrComment } from "./formulaText";

const keywords: Completion[] = [
  {
    label: "if",
    type: "keyword",
    detail: "Start a condition",
    info: 'Choose a value based on a condition. Example: if [amount] > 100 then "high" else "low" endif',
  },
  {
    label: "then",
    type: "keyword",
    detail: "Value when true",
    info: "Follow an if or elseif condition with then and the value to return when it is true.",
  },
  {
    label: "elseif",
    type: "keyword",
    detail: "Another condition",
    info: 'Test another condition when earlier conditions are false. Example: if [score] > 90 then "A" elseif [score] > 80 then "B" else "C" endif',
  },
  {
    label: "else",
    type: "keyword",
    detail: "Fallback value",
    info: "Return this value when none of the preceding if or elseif conditions are true. Example: else 0 endif",
  },
  {
    label: "endif",
    type: "keyword",
    detail: "End a condition",
    info: "Close an if expression after its else value. Each nested if needs its own endif.",
  },
  {
    label: "and",
    type: "keyword",
    detail: "Both conditions",
    info: "True when both conditions are true. Example: [amount] > 100 and [quantity] > 1",
  },
  {
    label: "or",
    type: "keyword",
    detail: "Either condition",
    info: 'True when at least one condition is true. Example: [city] = "Paris" or [city] = "London"',
  },
];

export function formulaKeywordCompletions(context: CompletionContext): CompletionResult | null {
  const line = context.state.doc.lineAt(context.pos);
  const offset = context.pos - line.from;
  if (isInsideStringOrComment(line.text, offset)) return null;

  // Column and parameter names have their own completion sources, including
  // names containing spaces. Do not mix language keywords into those lists.
  const before = line.text.slice(0, offset);
  if (/\[[^\]]*$/.test(before) || /\$\{[^}]*$/.test(before) || /\$\w*$/.test(before)) return null;

  const word = context.matchBefore(/[A-Za-z_][A-Za-z0-9_]*/);
  if (!word && !context.explicit) return null;
  const prefix = word?.text.toLowerCase() ?? "";
  return {
    from: word?.from ?? context.pos,
    options: keywords.filter((keyword) => keyword.label.startsWith(prefix)),
  };
}
