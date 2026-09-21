// dtype + source ride in `detail`: `withoutInfo` strips `info`. Only a first catalog ref awaits.
import type {
  Completion,
  CompletionContext,
  CompletionResult,
  CompletionSource,
} from "@codemirror/autocomplete";
import { getCatalogRefColumns, resolveCatalogRefs } from "../../../../notebook/catalogRefResolver";
import { cellRuntime } from "../../../../notebook/notebookRuntimeState";
import { getSchemas } from "../../../../notebook/useDataframeSchemas";
import { resolveColumnContext, type ColumnContext } from "./dataframeColumnContext";
import { inferSchema, type InferredSchema, type SchemaSources } from "./dataframeSchemaInference";
import type { CatalogRefLiteral, SchemaColumn } from "./dataframeSchemaTypes";
import type { UpstreamColumn } from "./useUpstreamColumns";

export interface DataframeCompletionContext {
  ownerId: string;
  cellId: string | null;
  surface: "node" | "catalog";
  getPriorCells: () => { id: string; code: string }[];
  getUpstreamColumns: () => UpstreamColumn[];
}

interface Attempt {
  result: CompletionResult | null;
  pending: CatalogRefLiteral[];
}

const VALID_DOUBLE = /^[^"\\]*$/;
const VALID_SINGLE = /^[^'\\]*$/;

export function createDataframeColumnCompletions(
  getCtx: () => DataframeCompletionContext,
): CompletionSource {
  return (context) => {
    const column = resolveColumnContext(context.state, context.pos);
    if (!column) return null;
    const ctx = getCtx();
    const first = attempt(column, ctx, context);
    if (first.pending.length === 0) return first.result;
    return resolveCatalogRefs(first.pending).then(() =>
      context.aborted ? null : attempt(column, ctx, context).result,
    );
  };
}

function attempt(
  column: ColumnContext,
  ctx: DataframeCompletionContext,
  context: CompletionContext,
): Attempt {
  if (column.receiver.kind === "bare-col") {
    return { result: toResult(column, fallbackOptions(ctx, column.quote)), pending: [] };
  }
  const { schema, pendingRefs } = inferSchema(
    column.receiver.node,
    context.state.doc.toString(),
    context.pos,
    ctx.getPriorCells(),
    ctx.cellId,
    buildSources(ctx),
  );
  const subscript = column.method === "__getitem__";
  if (!schema) {
    if (pendingRefs.length > 0) return { result: null, pending: pendingRefs };
    // A subscript on an unknown receiver may not even be a frame, so it never falls back.
    if (subscript) return { result: null, pending: [] };
    return { result: toResult(column, fallbackOptions(ctx, column.quote)), pending: [] };
  }
  if (subscript && schema.kind !== "DataFrame") return { result: null, pending: [] };
  return { result: toResult(column, schemaOptions(schema, column.quote)), pending: [] };
}

function buildSources(ctx: DataframeCompletionContext): SchemaSources {
  return {
    runtime(name) {
      const frame = getSchemas(ctx.ownerId)?.frames.get(name);
      if (!frame || frame.state !== "ready") return null;
      return { columns: frame.columns, kind: frame.kind };
    },
    catalogRef: (key) => getCatalogRefColumns(key),
    input(name) {
      const columns = ctx
        .getUpstreamColumns()
        .filter((column) => column.source_input === name)
        .map((column) => ({ name: column.name, dtype: column.data_type }));
      return columns.length > 0 ? columns : null;
    },
    isCellOutdated: (cellId) => isCellOutdated(ctx, cellId),
  };
}

function isCellOutdated(ctx: DataframeCompletionContext, cellId: string | null): boolean {
  const id = cellId ?? ctx.cellId;
  if (!id) return false;
  const runtime = cellRuntime(ctx.ownerId, id);
  if (!runtime || runtime.submittedRevision === null) return true;
  if (runtime.staleReason) return true;
  return runtime.sourceRevision !== runtime.submittedRevision;
}

function schemaOptions(schema: InferredSchema, quote: '"' | "'"): Completion[] {
  return schema.columns.map((column: SchemaColumn) =>
    toOption(column.name, column.dtype, schema.sourceLabel, quote),
  );
}

/** Node inputs, one row per input so two same-named columns keep their own dtype. */
function fallbackOptions(ctx: DataframeCompletionContext, quote: '"' | "'"): Completion[] {
  if (ctx.surface === "catalog") return [];
  return ctx
    .getUpstreamColumns()
    .map((column) =>
      toOption(column.name, column.data_type, `input ${column.source_input}`, quote),
    );
}

function toOption(name: string, dtype: string, sourceLabel: string, quote: '"' | "'"): Completion {
  const option: Completion = {
    label: name,
    type: "property",
    detail: dtype ? `${dtype} · ${sourceLabel}` : `· ${sourceLabel}`,
    boost: 6,
  };
  const escaped = escapeForQuote(name, quote);
  if (escaped !== null) option.apply = escaped;
  return option;
}

function escapeForQuote(name: string, quote: '"' | "'"): string | null {
  if (!name.includes(quote) && !name.includes("\\")) return null;
  return name.split("\\").join("\\\\").split(quote).join(`\\${quote}`);
}

function toResult(column: ColumnContext, options: Completion[]): CompletionResult | null {
  if (options.length === 0) return null;
  return {
    from: column.contentFrom,
    to: column.contentTo,
    options,
    validFor: column.quote === '"' ? VALID_DOUBLE : VALID_SINGLE,
  };
}
