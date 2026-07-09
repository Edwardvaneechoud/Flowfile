// Lint the process() BODY editor for code that escapes the method.
//
// In designer mode the Code tab edits only the body of process(), indented one
// level under the read-only `def process(...):` header (the store composes them
// back and codegen nests the whole thing in the class). So a `def`/`class`
// written at column 0 falls OUT of the method and becomes a class member — a
// helper defined that way needs `self` and must be called as `self.name(...)`,
// which isn't obvious. We detect this structurally rather than by regex:
// reconstruct the real method (`def process(...):` + body) — valid Python that
// Lezer parses cleanly, so strings/comments never false-positive — then flag
// every top-level sibling of `process`, i.e. code that fell out of the body.
//
// Code-only mode (whole-file editing) is excluded by the caller via `isEnabled`,
// not by inspecting the text — that's the reliable signal (store.codeOnly).
import { linter, type Diagnostic } from "@codemirror/lint";
import type { EditorView } from "@codemirror/view";
import type { Extension } from "@codemirror/state";
import { pythonLanguage } from "@codemirror/lang-python";
import type { SyntaxNode } from "@lezer/common";

const PREFIX = "def process(self, *inputs):\n";

export interface BodyLintIssue {
  from: number;
  to: number;
  message: string;
  severity: "warning" | "info";
}

function firstParamIsSelf(fnNode: SyntaxNode, src: string): boolean {
  const params = fnNode.getChild("ParamList");
  const first = params?.getChild("VariableName") ?? null;
  return first !== null && src.slice(first.from, first.to) === "self";
}

// A decorated def/class is wrapped by Lezer in a DecoratedStatement node; unwrap
// to the inner FunctionDefinition/ClassDefinition so @staticmethod/@property/…
// helpers are still detected (the decorator line stays the underline anchor).
function definitionNode(node: SyntaxNode): SyntaxNode | null {
  if (node.name === "FunctionDefinition" || node.name === "ClassDefinition") return node;
  if (node.name === "DecoratedStatement") {
    return node.getChild("FunctionDefinition") ?? node.getChild("ClassDefinition");
  }
  return null;
}

export function lintProcessBody(body: string): BodyLintIssue[] {
  if (!body.trim()) return [];

  const src = PREFIX + body;
  const tree = pythonLanguage.parser.parse(src);
  const issues: BodyLintIssue[] = [];

  let sawProcess = false;
  for (let node: SyntaxNode | null = tree.topNode.firstChild; node; node = node.nextSibling) {
    const def = definitionNode(node);
    if (!sawProcess) {
      // The reconstructed method itself; anything after it escaped the body.
      if (def?.name === "FunctionDefinition") sawProcess = true;
      continue;
    }
    if (!def) continue; // only defs/classes; ignore Lezer error-recovery noise
    const isClass = def.name === "ClassDefinition";

    // Anchor the underline at the escaping statement (the @decorator line when present).
    const from = Math.max(0, node.from - PREFIX.length);
    const nl = body.indexOf("\n", from);
    const to = nl === -1 ? body.length : nl;

    let message: string;
    let severity: "warning" | "info";
    if (isClass) {
      severity = "warning";
      message =
        "This class is defined outside process(), so it becomes a class member. " +
        "Indent it to nest it inside process, or switch to Code-only editing for module-level definitions.";
    } else if (firstParamIsSelf(def, src)) {
      // Valid code — a column-0 def(self) is a real method; just a heads-up on how to call it.
      severity = "info";
      message =
        "This is a class method (defined outside process()). Call it with self.<name>(...), " +
        "or indent it to nest it inside process.";
    } else {
      severity = "warning";
      message =
        "This function is defined outside process(), so it becomes a class method. Add 'self' as its " +
        "first parameter (e.g. def my_function(self):) and call it with self.<name>(...) — or indent it " +
        "to nest it inside process, or switch to Code-only editing for a module-level function.";
    }
    issues.push({ from, to: Math.max(from + 1, to), message, severity });
  }
  return issues;
}

// CodeMirror extension for the editable body editor. `isEnabled` gates it off in
// code-only (whole-file) mode, where module-level defs are valid.
export function makeProcessBodyLinter(isEnabled: () => boolean): Extension {
  return linter((view: EditorView): Diagnostic[] => {
    if (!isEnabled()) return [];
    return lintProcessBody(view.state.doc.toString()).map((issue) => ({
      from: issue.from,
      to: issue.to,
      severity: issue.severity,
      message: issue.message,
      source: "node-designer",
    }));
  });
}
