import { CompletionContext } from "@codemirror/autocomplete";
import { EditorState } from "@codemirror/state";
import { describe, expect, it } from "vitest";
import { formulaKeywordCompletions } from "./formulaKeywordCompletions";

function complete(doc: string, explicit = false, pos = doc.length) {
  return formulaKeywordCompletions(
    new CompletionContext(EditorState.create({ doc }), pos, explicit),
  );
}

describe("formula keyword completions", () => {
  it("offers all conditional keywords and logical operators on explicit completion", () => {
    expect(complete("", true)?.options.map((option) => option.label)).toEqual([
      "if",
      "then",
      "elseif",
      "else",
      "endif",
      "and",
      "or",
    ]);
    expect(complete("")).toBeNull();
  });

  it("matches prefixes regardless of case and replaces only the current word", () => {
    const result = complete("if [a] > 0 then 1 EL");
    expect(result?.from).toBe(18);
    expect(result?.options.map((option) => option.label)).toEqual(["elseif", "else"]);
    expect(complete("[a] > 0 an")?.options[0].label).toBe("and");
    expect(complete("[a] > 0\nor")?.options[0].label).toBe("or");
  });

  it.each(['"if', "'else", "// and", "[or", "[column and", "${if", "$or"])(
    "does not offer keywords inside strings, comments or references: %s",
    (doc) => expect(complete(doc, true)).toBeNull(),
  );

  it("resumes suggestions after a closed string or reference", () => {
    expect(complete('"text" or')?.options[0].label).toBe("or");
    expect(complete("[amount] an")?.options[0].label).toBe("and");
    expect(complete("${amount} an")?.options[0].label).toBe("and");
  });

  it("provides documentation and inserts plain keywords without function parentheses", () => {
    for (const option of complete("", true)!.options) {
      expect(option.type).toBe("keyword");
      expect(option.info).toBeTruthy();
      expect(option.detail).toBeTruthy();
      // CodeMirror's default apply inserts the label verbatim.
      expect(option.apply).toBeUndefined();
    }
  });
});
