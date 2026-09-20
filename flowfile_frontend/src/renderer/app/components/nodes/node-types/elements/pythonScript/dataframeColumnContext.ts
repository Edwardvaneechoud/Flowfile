// Column positions come off the syntax tree: an unfinished string is still a `String` node.
import { ensureSyntaxTree, syntaxTree, syntaxTreeAvailable } from "@codemirror/language";
import type { EditorState, Text } from "@codemirror/state";
import type { SyntaxNode } from "@lezer/common";

/** Methods whose string arguments name columns of the receiver's pre-call schema. */
export const COLUMN_POSITION_METHODS: ReadonlySet<string> = new Set([
  "select",
  "with_columns",
  "filter",
  "sort",
  "group_by",
  "agg",
  "drop",
  "unique",
  "drop_nulls",
  "explode",
  "partition_by",
]);

/** Column-reference functions, written bare or as `pl.<name>`. */
export const COLUMN_REF_FUNCTIONS: ReadonlySet<string> = new Set([
  "col",
  "exclude",
  "sum",
  "mean",
  "min",
  "max",
  "first",
  "last",
  "median",
  "n_unique",
  "std",
  "var",
]);

/** Module aliases under which the column-reference functions are recognised. */
export const POLARS_MODULE_NAMES: ReadonlySet<string> = new Set(["pl"]);

export type ColumnReceiver = { kind: "expr"; node: SyntaxNode } | { kind: "bare-col" };

export interface ColumnContext {
  receiver: ColumnReceiver;
  method: string;
  stringFrom: number;
  stringTo: number;
  contentFrom: number;
  contentTo: number;
  quote: '"' | "'";
  prefix: string;
}

// Wrappers lezer puts between a string and the ArgList holding it. The
// *Comprehension* variants are what an unclosed `[`/`{` parses as.
const STRING_WRAPPERS = new Set([
  "ArrayExpression",
  "ArrayComprehensionExpression",
  "TupleExpression",
  "ParenthesizedExpression",
  "SetExpression",
]);

const DICT_WRAPPERS = new Set([
  "DictionaryExpression",
  "DictionaryComprehensionExpression",
  "SetComprehensionExpression",
]);

const IGNORED_SIBLINGS = new Set(["Comment", "⚠"]);

const MAX_COL_CLIMB = 8;

interface CalleeInfo {
  method: string;
  object: SyntaxNode | null;
}

function calleeInfo(call: SyntaxNode, doc: Text): CalleeInfo | null {
  const callee = call.firstChild;
  if (!callee) return null;
  if (callee.name === "VariableName") {
    return { method: doc.sliceString(callee.from, callee.to), object: null };
  }
  if (callee.name === "MemberExpression") {
    const property = callee.getChild("PropertyName");
    const object = callee.firstChild;
    if (!property || !object || object === property) return null;
    return { method: doc.sliceString(property.from, property.to), object };
  }
  return null;
}

function isColumnRefCallee(info: CalleeInfo, doc: Text): boolean {
  if (!COLUMN_REF_FUNCTIONS.has(info.method)) return false;
  if (info.object === null) return true;
  return (
    info.object.name === "VariableName" &&
    POLARS_MODULE_NAMES.has(doc.sliceString(info.object.from, info.object.to))
  );
}

/** The receiver of `<expr>.rename({…})` when this dict/set wrapper is that call's argument. */
function renameReceiver(wrapper: SyntaxNode, doc: Text): SyntaxNode | null {
  const argList = wrapper.parent;
  if (!argList || argList.name !== "ArgList") return null;
  const call = argList.parent;
  if (!call || call.name !== "CallExpression") return null;
  const info = calleeInfo(call, doc);
  return info?.object && info.method === "rename" ? info.object : null;
}

function isDictKeyPosition(node: SyntaxNode): boolean {
  let sibling = node.prevSibling;
  while (sibling && IGNORED_SIBLINGS.has(sibling.name)) sibling = sibling.prevSibling;
  return sibling !== null && (sibling.name === "{" || sibling.name === ",");
}

function isStatementBoundary(node: SyntaxNode): boolean {
  return node.name === "Script" || node.name === "Body" || node.name.endsWith("Statement");
}

/**
 * Walk outward from a `pl.col(...)`-style call to the frame method that encloses it,
 * so the column reference borrows that receiver's schema.
 */
function enclosingColumnCall(
  colCall: SyntaxNode,
  doc: Text,
): { method: string; object: SyntaxNode } | null {
  let levels = 0;
  for (let node = colCall.parent; node && levels < MAX_COL_CLIMB; node = node.parent) {
    if (isStatementBoundary(node)) return null;
    if (node.name !== "ArgList") continue;
    levels += 1;
    const call = node.parent;
    if (!call || call.name !== "CallExpression") continue;
    const info = calleeInfo(call, doc);
    if (info?.object && COLUMN_POSITION_METHODS.has(info.method)) {
      return { method: info.method, object: info.object };
    }
  }
  return null;
}

export function resolveColumnContext(state: EditorState, pos: number): ColumnContext | null {
  const tree = syntaxTreeAvailable(state, pos)
    ? syntaxTree(state)
    : (ensureSyntaxTree(state, pos, 50) ?? syntaxTree(state));
  const node = tree.resolveInner(pos, -1);
  if (node.name !== "String") return null;

  const text = state.doc.sliceString(node.from, node.to);
  const quote: '"' | "'" | null = text[0] === '"' ? '"' : text[0] === "'" ? "'" : null;
  if (quote === null) return null;
  if (text.startsWith(quote.repeat(3))) return null;

  const unterminated = node.lastChild?.name === "⚠";
  const closed = !unterminated && text.length > 1 && text.endsWith(quote);
  const contentFrom = node.from + 1;
  const contentTo = closed ? node.to - 1 : pos;
  if (pos < contentFrom || pos > contentTo) return null;

  const result = (receiver: ColumnReceiver, method: string): ColumnContext => ({
    receiver,
    method,
    stringFrom: node.from,
    stringTo: node.to,
    contentFrom,
    contentTo,
    quote,
    prefix: state.doc.sliceString(contentFrom, pos),
  });

  let parent = node.parent;
  while (parent && parent.name === "ContinuedString") parent = parent.parent;
  if (!parent) return null;

  // closeBrackets turns `rename({` + quote into `{""}`, which lezer reads as a set.
  const setWrapper = parent.name === "SetExpression";
  if (DICT_WRAPPERS.has(parent.name) || setWrapper) {
    const receiver = isDictKeyPosition(node) ? renameReceiver(parent, state.doc) : null;
    if (receiver) return result({ kind: "expr", node: receiver }, "rename");
    // A set is a plain argument elsewhere (`select({"a"})`), so only a dict stops here.
    if (!setWrapper) return null;
  }

  while (parent && STRING_WRAPPERS.has(parent.name)) parent = parent.parent;
  if (!parent) return null;

  if (parent.name === "MemberExpression") {
    const object = parent.firstChild;
    if (!object || object === node) return null;
    return result({ kind: "expr", node: object }, "__getitem__");
  }

  if (parent.name !== "ArgList") return null;
  const call = parent.parent;
  if (!call || call.name !== "CallExpression") return null;
  const info = calleeInfo(call, state.doc);
  if (!info) return null;

  if (isColumnRefCallee(info, state.doc)) {
    const enclosing = enclosingColumnCall(call, state.doc);
    if (enclosing) return result({ kind: "expr", node: enclosing.object }, enclosing.method);
    return result({ kind: "bare-col" }, info.method);
  }

  if (info.object && COLUMN_POSITION_METHODS.has(info.method)) {
    return result({ kind: "expr", node: info.object }, info.method);
  }
  return null;
}
