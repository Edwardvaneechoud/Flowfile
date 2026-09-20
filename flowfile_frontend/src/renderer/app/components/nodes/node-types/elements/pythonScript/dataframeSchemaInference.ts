// Source-only inference: only transforms that cannot produce a wrong schema carry through.
import { pythonLanguage } from "@codemirror/lang-python";
import type { SyntaxNode, Tree } from "@lezer/common";
import { POLARS_MODULE_NAMES } from "./dataframeColumnContext";
import {
  catalogRefKey,
  type CatalogRefLiteral,
  type FrameKind,
  type SchemaColumn,
} from "./dataframeSchemaTypes";

export interface CellSource {
  id: string | null;
  code: string;
}

export interface SchemaSources {
  /** Names the kernel reported after the last run. */
  runtime(name: string): { columns: SchemaColumn[]; kind: FrameKind } | null;
  /** `undefined` means the reference has never been resolved; `null` means unresolvable. */
  catalogRef(key: string): SchemaColumn[] | null | undefined;
  input(name: string): SchemaColumn[] | null;
  isCellOutdated(cellId: string | null): boolean;
}

export interface InferredSchema {
  columns: SchemaColumn[];
  kind: FrameKind;
  provenance: "source" | "runtime";
  sourceLabel: string;
  outdated: boolean;
  grouped?: boolean;
}

export interface InferResult {
  schema: InferredSchema | null;
  pendingRefs: CatalogRefLiteral[];
}

const MAX_DEPTH = 32;
const PARSE_CACHE_LIMIT = 200;

const PUNCTUATION = new Set(["(", ")", "[", "]", "{", "}", ",", ":", "Comment", "⚠"]);
const CONTEXT_NAMES = new Set(["flowfile_ctx", "flowfile"]);
const INPUT_METHODS = new Set(["read_input", "read_first"]);
const ROW_ONLY_METHODS = new Set([
  "filter",
  "sort",
  "head",
  "tail",
  "limit",
  "slice",
  "unique",
  "drop_nulls",
  "reverse",
  "clone",
  "cache",
  "rechunk",
]);
const FRAME_CONSTRUCTORS = new Map<string, FrameKind>([
  ["DataFrame", "DataFrame"],
  ["LazyFrame", "LazyFrame"],
  ["from_dict", "DataFrame"],
]);

const parseCache = new Map<string, Tree>();

function parseCode(code: string): Tree {
  const cached = parseCache.get(code);
  if (cached) {
    parseCache.delete(code);
    parseCache.set(code, cached);
    return cached;
  }
  const tree = pythonLanguage.parser.parse(code);
  parseCache.set(code, tree);
  if (parseCache.size > PARSE_CACHE_LIMIT) {
    const oldest = parseCache.keys().next();
    if (!oldest.done) parseCache.delete(oldest.value);
  }
  return tree;
}

export function resolveExpressionText(node: SyntaxNode, code: string): string {
  return code.slice(node.from, node.to);
}

function stringValue(node: SyntaxNode, code: string): string | null {
  if (node.name !== "String") return null;
  const raw = code.slice(node.from, node.to);
  const quote = raw[0];
  if (quote !== '"' && quote !== "'") return null;
  if (raw.startsWith(quote.repeat(3))) return null;
  if (raw.length < 2 || !raw.endsWith(quote)) return null;
  return raw.slice(1, -1).replace(/\\(.)/g, (_, char: string) => {
    if (char === "n") return "\n";
    if (char === "t") return "\t";
    return char;
  });
}

interface CalleeInfo {
  method: string;
  object: SyntaxNode | null;
}

function calleeOf(call: SyntaxNode, code: string): CalleeInfo | null {
  const callee = call.firstChild;
  if (!callee) return null;
  if (callee.name === "VariableName")
    return { method: resolveExpressionText(callee, code), object: null };
  if (callee.name === "MemberExpression") {
    const property = callee.getChild("PropertyName");
    const object = callee.firstChild;
    if (!property || !object || object === property) return null;
    return { method: resolveExpressionText(property, code), object };
  }
  return null;
}

interface CallArg {
  keyword: string | null;
  value: SyntaxNode;
}

function callArgs(call: SyntaxNode, code: string): CallArg[] {
  const list = call.getChild("ArgList");
  if (!list) return [];
  const args: CallArg[] = [];
  let child = list.firstChild;
  while (child) {
    if (PUNCTUATION.has(child.name)) {
      child = child.nextSibling;
      continue;
    }
    if (child.name === "VariableName" && child.nextSibling?.name === "AssignOp") {
      const value = child.nextSibling.nextSibling;
      if (!value) break;
      args.push({ keyword: resolveExpressionText(child, code), value });
      child = value.nextSibling;
      continue;
    }
    args.push({ keyword: null, value: child });
    child = child.nextSibling;
  }
  return args;
}

function firstExpressionChild(node: SyntaxNode): SyntaxNode | null {
  for (let child = node.firstChild; child; child = child.nextSibling) {
    if (!PUNCTUATION.has(child.name)) return child;
  }
  return null;
}

function literalNameList(node: SyntaxNode, code: string): string[] | null {
  if (node.name === "String") {
    const value = stringValue(node, code);
    return value === null ? null : [value];
  }
  if (node.name !== "ArrayExpression" && node.name !== "TupleExpression") return null;
  const names: string[] = [];
  for (let child = node.firstChild; child; child = child.nextSibling) {
    if (PUNCTUATION.has(child.name)) continue;
    const value = stringValue(child, code);
    if (value === null) return null;
    names.push(value);
  }
  return names;
}

function isPolarsModule(node: SyntaxNode | null, code: string): boolean {
  return (
    node !== null &&
    node.name === "VariableName" &&
    POLARS_MODULE_NAMES.has(resolveExpressionText(node, code))
  );
}

/** `pl.col("a")` / `col("a")`, optionally wrapped in a single `.alias("x")`. */
function columnRefArg(node: SyntaxNode, code: string): { source: string; alias?: string } | null {
  if (node.name !== "CallExpression") return null;
  const info = calleeOf(node, code);
  if (!info) return null;
  const args = callArgs(node, code);
  if (args.length !== 1 || args[0].keyword !== null) return null;

  if (info.method === "alias") {
    if (!info.object) return null;
    const inner = columnRefArg(info.object, code);
    if (!inner || inner.alias !== undefined) return null;
    const alias = stringValue(args[0].value, code);
    return alias === null ? null : { source: inner.source, alias };
  }
  if (info.method !== "col") return null;
  if (info.object !== null && !isPolarsModule(info.object, code)) return null;
  const name = stringValue(args[0].value, code);
  return name === null ? null : { source: name };
}

type RootDescriptor =
  | { kind: "input"; name: string | null }
  | { kind: "catalog"; ref: CatalogRefLiteral | null }
  | { kind: "literal"; columns: SchemaColumn[] | null; frameKind: FrameKind };

function catalogName(node: SyntaxNode | null, code: string): string | null {
  if (!node || node.name !== "CallExpression") return null;
  const info = calleeOf(node, code);
  if (!info || info.method !== "get_catalog") return null;
  const args = callArgs(node, code);
  if (args.length !== 1 || args[0].keyword !== null) return null;
  return stringValue(args[0].value, code);
}

function schemaChain(
  node: SyntaxNode | null,
  code: string,
): { schema: string; catalog?: string } | null {
  if (!node || node.name !== "CallExpression") return null;
  const info = calleeOf(node, code);
  if (!info || info.method !== "get_schema") return null;
  const args = callArgs(node, code);
  if (args.length !== 1 || args[0].keyword !== null) return null;
  const schema = stringValue(args[0].value, code);
  if (schema === null) return null;
  const catalog = catalogName(info.object, code);
  return catalog === null ? { schema } : { schema, catalog };
}

function tableRefFromCall(
  call: SyntaxNode,
  code: string,
  chain: { schema: string; catalog?: string },
): CatalogRefLiteral | null {
  const args = callArgs(call, code);
  if (args.length !== 1 || args[0].keyword !== null) return null;
  const table = stringValue(args[0].value, code);
  return table === null ? null : { table, ...chain };
}

function catalogTableArgs(call: SyntaxNode, code: string): CatalogRefLiteral | null {
  let table: string | null = null;
  let schema: string | undefined;
  let namespaceId: number | undefined;
  for (const arg of callArgs(call, code)) {
    if (arg.keyword === null) {
      if (table !== null) return null;
      table = stringValue(arg.value, code);
      if (table === null) return null;
    } else if (arg.keyword === "schema") {
      const value = stringValue(arg.value, code);
      if (value === null) return null;
      schema = value;
    } else if (arg.keyword === "namespace_id") {
      if (arg.value.name !== "Number") return null;
      const parsed = Number(resolveExpressionText(arg.value, code));
      if (!Number.isFinite(parsed)) return null;
      namespaceId = parsed;
    }
  }
  if (table === null) return null;
  const ref: CatalogRefLiteral = { table };
  if (schema !== undefined) ref.schema = schema;
  if (namespaceId !== undefined) ref.namespaceId = namespaceId;
  return ref;
}

function describeRoot(call: SyntaxNode, code: string): RootDescriptor | null {
  const info = calleeOf(call, code);
  if (!info) return null;
  const onContext =
    info.object === null ||
    (info.object.name === "VariableName" &&
      CONTEXT_NAMES.has(resolveExpressionText(info.object, code)));

  if (onContext && INPUT_METHODS.has(info.method)) {
    const args = callArgs(call, code);
    if (args.length === 0) return { kind: "input", name: "main" };
    if (args.length === 1 && args[0].keyword === null) {
      return { kind: "input", name: stringValue(args[0].value, code) };
    }
    return { kind: "input", name: null };
  }

  if (onContext && info.method === "read_catalog_table") {
    return { kind: "catalog", ref: catalogTableArgs(call, code) };
  }

  if (info.method === "read_table") {
    const chain = schemaChain(info.object, code);
    if (!chain) return null;
    return { kind: "catalog", ref: tableRefFromCall(call, code, chain) };
  }

  if (info.method === "read" && info.object?.name === "CallExpression") {
    const inner = calleeOf(info.object, code);
    if (!inner || inner.method !== "get_table_ref") return null;
    const chain = schemaChain(inner.object, code);
    if (!chain) return null;
    if (callArgs(call, code).length !== 0) return null;
    return { kind: "catalog", ref: tableRefFromCall(info.object, code, chain) };
  }

  const frameKind = FRAME_CONSTRUCTORS.get(info.method);
  if (frameKind && isPolarsModule(info.object, code)) {
    const args = callArgs(call, code);
    const dict = args.length >= 1 && args[0].keyword === null ? args[0].value : null;
    if (!dict || dict.name !== "DictionaryExpression") {
      return { kind: "literal", columns: null, frameKind };
    }
    const columns: SchemaColumn[] = [];
    let child = dict.firstChild;
    while (child) {
      if (child.name === "{" || child.name === "}" || child.name === ",") {
        child = child.nextSibling;
        continue;
      }
      const name = stringValue(child, code);
      const separator = child.nextSibling;
      const valueNode = separator?.nextSibling;
      if (name === null || separator?.name !== ":" || !valueNode) {
        return { kind: "literal", columns: null, frameKind };
      }
      columns.push({ name, dtype: "" });
      child = valueNode.nextSibling;
    }
    return { kind: "literal", columns, frameKind };
  }
  return null;
}

interface Assignment {
  name: string;
  value: SyntaxNode;
  code: string;
  cellId: string | null;
  index: number;
}

function assignParts(statement: SyntaxNode): { name: SyntaxNode; value: SyntaxNode } | null {
  let child = statement.firstChild;
  if (!child || child.name !== "VariableName") return null;
  const name = child;
  child = child.nextSibling;
  if (child?.name === "TypeDef") child = child.nextSibling;
  if (!child || child.name !== "AssignOp") return null;
  const value = child.nextSibling;
  if (!value || value.nextSibling) return null;
  return { name, value };
}

function collectAssignments(
  root: SyntaxNode,
  code: string,
  cellId: string | null,
  cutoff: number | null,
  into: Assignment[],
): void {
  for (let child = root.firstChild; child; child = child.nextSibling) {
    if (child.name !== "AssignStatement") continue;
    if (cutoff !== null && child.to > cutoff) continue;
    const parts = assignParts(child);
    if (!parts) continue;
    into.push({
      name: resolveExpressionText(parts.name, code),
      value: parts.value,
      code,
      cellId,
      index: into.length,
    });
  }
}

interface InferContext {
  assignments: Assignment[];
  sources: SchemaSources;
  pending: Map<string, CatalogRefLiteral>;
}

function lastAssignmentBefore(
  assignments: Assignment[],
  name: string,
  atIndex: number,
): Assignment | null {
  for (let i = Math.min(atIndex, assignments.length) - 1; i >= 0; i -= 1) {
    if (assignments[i].name === name) return assignments[i];
  }
  return null;
}

function selectColumns(
  call: SyntaxNode,
  code: string,
  base: InferredSchema,
): SchemaColumn[] | null {
  const byName = new Map(base.columns.map((column) => [column.name, column]));
  const args = callArgs(call, code);
  if (args.length === 0) return null;
  const picked: SchemaColumn[] = [];
  for (const arg of args) {
    if (arg.keyword !== null) {
      const ref = columnRefArg(arg.value, code);
      if (!ref || ref.alias !== undefined) return null;
      const column = byName.get(ref.source);
      if (!column) return null;
      picked.push({ name: arg.keyword, dtype: column.dtype });
      continue;
    }
    const names = literalNameList(arg.value, code);
    if (names) {
      for (const name of names) {
        const column = byName.get(name);
        if (!column) return null;
        picked.push(column);
      }
      continue;
    }
    const ref = columnRefArg(arg.value, code);
    if (!ref) return null;
    const column = byName.get(ref.source);
    if (!column) return null;
    picked.push(ref.alias === undefined ? column : { name: ref.alias, dtype: column.dtype });
  }
  return picked;
}

function renameColumns(
  call: SyntaxNode,
  code: string,
  base: InferredSchema,
): SchemaColumn[] | null {
  const args = callArgs(call, code);
  if (args.length !== 1 || args[0].keyword !== null) return null;
  const dict = args[0].value;
  if (dict.name !== "DictionaryExpression") return null;
  const mapping = new Map<string, string>();
  const known = new Set(base.columns.map((column) => column.name));
  let child = dict.firstChild;
  while (child) {
    if (child.name === "{" || child.name === "}" || child.name === ",") {
      child = child.nextSibling;
      continue;
    }
    const key = stringValue(child, code);
    const separator = child.nextSibling;
    const valueNode = separator?.nextSibling;
    if (key === null || separator?.name !== ":" || !valueNode) return null;
    const value = stringValue(valueNode, code);
    if (value === null || !known.has(key)) return null;
    mapping.set(key, value);
    child = valueNode.nextSibling;
  }
  if (mapping.size === 0) return null;
  return base.columns.map((column) =>
    mapping.has(column.name) ? { name: mapping.get(column.name)!, dtype: column.dtype } : column,
  );
}

function dropColumns(call: SyntaxNode, code: string, base: InferredSchema): SchemaColumn[] | null {
  const args = callArgs(call, code);
  if (args.length === 0) return null;
  const known = new Set(base.columns.map((column) => column.name));
  const removed = new Set<string>();
  for (const arg of args) {
    if (arg.keyword !== null) return null;
    const names = literalNameList(arg.value, code);
    if (!names) return null;
    for (const name of names) {
      if (!known.has(name)) return null;
      removed.add(name);
    }
  }
  return base.columns.filter((column) => !removed.has(column.name));
}

function applyMethod(
  method: string,
  call: SyntaxNode,
  code: string,
  base: InferredSchema,
): InferredSchema | null {
  if (ROW_ONLY_METHODS.has(method)) return base;
  if (method === "collect") return { ...base, kind: "DataFrame" };
  if (method === "lazy") return { ...base, kind: "LazyFrame" };
  if (method === "group_by") return { ...base, grouped: true };
  if (method === "select") {
    const columns = selectColumns(call, code, base);
    return columns ? { ...base, columns, grouped: false } : null;
  }
  if (method === "rename") {
    const columns = renameColumns(call, code, base);
    return columns ? { ...base, columns } : null;
  }
  if (method === "drop") {
    const columns = dropColumns(call, code, base);
    return columns ? { ...base, columns } : null;
  }
  return null;
}

function schemaFromRoot(root: RootDescriptor, ctx: InferContext): InferredSchema | null {
  if (root.kind === "input") {
    if (root.name === null) return null;
    const columns = ctx.sources.input(root.name);
    if (!columns) return null;
    return {
      columns,
      kind: "LazyFrame",
      provenance: "source",
      sourceLabel: `${root.name} (input)`,
      outdated: false,
    };
  }
  if (root.kind === "catalog") {
    if (!root.ref) return null;
    const key = catalogRefKey(root.ref);
    const columns = ctx.sources.catalogRef(key);
    if (columns === undefined) {
      ctx.pending.set(key, root.ref);
      return null;
    }
    if (columns === null) return null;
    return {
      columns,
      kind: "LazyFrame",
      provenance: "source",
      sourceLabel: `${root.ref.table} (catalog)`,
      outdated: false,
    };
  }
  if (root.columns === null) return null;
  return {
    columns: root.columns,
    kind: root.frameKind,
    provenance: "source",
    sourceLabel: "literal",
    outdated: false,
  };
}

function runtimeSchema(
  name: string,
  runtime: { columns: SchemaColumn[]; kind: FrameKind },
  outdated: boolean,
): InferredSchema {
  return {
    columns: runtime.columns,
    kind: runtime.kind,
    provenance: "runtime",
    sourceLabel: outdated ? `${name} (last run, outdated)` : `${name} (last run)`,
    outdated,
  };
}

/** A literal frame gives names but no dtypes, so the kernel's entry for the same columns wins. */
function runtimeRefinesLiteral(
  resolved: InferredSchema,
  runtime: { columns: SchemaColumn[]; kind: FrameKind },
): boolean {
  if (resolved.columns.length === 0) return false;
  if (!resolved.columns.every((column) => column.dtype === "")) return false;
  if (runtime.columns.length !== resolved.columns.length) return false;
  const names = new Set(resolved.columns.map((column) => column.name));
  return runtime.columns.every((column) => names.has(column.name));
}

function inferExpr(
  node: SyntaxNode,
  code: string,
  atIndex: number,
  ctx: InferContext,
  depth: number,
): InferredSchema | null {
  if (depth > MAX_DEPTH) return null;

  if (node.name === "ParenthesizedExpression") {
    const inner = firstExpressionChild(node);
    return inner ? inferExpr(inner, code, atIndex, ctx, depth + 1) : null;
  }

  if (node.name === "VariableName") {
    const name = resolveExpressionText(node, code);
    const assignment = lastAssignmentBefore(ctx.assignments, name, atIndex);
    const runtime = ctx.sources.runtime(name);
    const outdated = assignment ? ctx.sources.isCellOutdated(assignment.cellId) : false;
    if (assignment) {
      const resolved = inferExpr(
        assignment.value,
        assignment.code,
        assignment.index,
        ctx,
        depth + 1,
      );
      if (resolved) {
        if (runtime && runtimeRefinesLiteral(resolved, runtime)) {
          return runtimeSchema(name, runtime, outdated);
        }
        return { ...resolved, provenance: "source", sourceLabel: name, outdated: false };
      }
    }
    if (!runtime) return null;
    return runtimeSchema(name, runtime, outdated);
  }

  if (node.name !== "CallExpression") return null;

  const root = describeRoot(node, code);
  if (root) return schemaFromRoot(root, ctx);

  const info = calleeOf(node, code);
  if (!info?.object) return null;
  const base = inferExpr(info.object, code, atIndex, ctx, depth + 1);
  if (!base) return null;
  return applyMethod(info.method, node, code, base);
}

function topNodeOf(node: SyntaxNode): SyntaxNode {
  let root = node;
  while (root.parent) root = root.parent;
  return root;
}

export function inferSchema(
  receiver: SyntaxNode,
  currentCode: string,
  pos: number,
  cells: CellSource[],
  currentCellId: string | null,
  sources: SchemaSources,
): InferResult {
  const assignments: Assignment[] = [];
  for (const cell of cells) {
    collectAssignments(parseCode(cell.code).topNode, cell.code, cell.id, null, assignments);
  }
  collectAssignments(
    topNodeOf(receiver),
    currentCode,
    currentCellId,
    Math.min(receiver.from, pos),
    assignments,
  );

  const ctx: InferContext = { assignments, sources, pending: new Map() };
  const schema = inferExpr(receiver, currentCode, assignments.length, ctx, 0);
  return { schema, pendingRefs: Array.from(ctx.pending.values()) };
}

/** Every literal catalog root form in one cell, for pre-resolution. */
export function scanCatalogRefs(code: string): CatalogRefLiteral[] {
  const refs = new Map<string, CatalogRefLiteral>();
  parseCode(code).iterate({
    enter(nodeRef) {
      if (nodeRef.name !== "CallExpression") return;
      const root = describeRoot(nodeRef.node, code);
      if (root?.kind === "catalog" && root.ref) refs.set(catalogRefKey(root.ref), root.ref);
    },
  });
  return Array.from(refs.values());
}
