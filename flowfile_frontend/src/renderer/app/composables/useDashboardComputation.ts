import { computed, type ComputedRef, type Ref } from "vue";
import { CatalogApi } from "../api/catalog.api";
import { useGraphicWalkerCompute } from "./useGraphicWalkerCompute";
import type { DashboardFilter, DashboardFilterKind, DashboardTile } from "../types";

type SemanticType = "quantitative" | "nominal" | "ordinal" | "temporal";

const isSemantic = (s: unknown): s is SemanticType =>
  s === "quantitative" || s === "nominal" || s === "ordinal" || s === "temporal";

interface VisFilter {
  fid: string;
  rule: Record<string, unknown>;
}

export const buildFilterStep = (
  filters: DashboardFilter[],
  tileFields: TileField[] | null,
): { type: "filter"; filters: VisFilter[] } | null => {
  const visFilters: VisFilter[] = [];
  for (const f of filters) {
    const rule = filterToRule(
      f,
      tileFields?.find((x) => x.fid === f.field_name),
    );
    if (rule) visFilters.push({ fid: f.field_name, rule });
  }
  if (!visFilters.length) return null;
  return { type: "filter", filters: visFilters };
};

const toNumber = (v: unknown): number =>
  typeof v === "number" ? v : typeof v === "string" && v.trim() !== "" ? Number(v) : NaN;

/** Categorical selections are stored as strings (the option list is rendered
 * text) and the worker's ``is_in`` does no casting: strings against a numeric
 * column raise, so a quantitative field gets numbers instead. Entries that do
 * not parse are dropped rather than sent. */
const categoricalValues = (selected: unknown[], field?: TileField): unknown[] =>
  field?.semanticType === "quantitative"
    ? selected.map(toNumber).filter(Number.isFinite)
    : selected;

const filterToRule = (f: DashboardFilter, field?: TileField): Record<string, unknown> | null => {
  if (f.kind === "categorical") {
    const selected = (f.state.selected as unknown[]) ?? [];
    if (!Array.isArray(selected) || !selected.length) return null;
    const value = categoricalValues(selected, field);
    if (!value.length) return null;
    return { type: "one of", value };
  }
  if (f.kind === "numeric_range") {
    const min = f.state.min as number | null | undefined;
    const max = f.state.max as number | null | undefined;
    if (min == null && max == null) return null;
    return { type: "range", value: [min ?? null, max ?? null] };
  }
  if (f.kind === "date_range") {
    const start = f.state.start as string | null | undefined;
    const end = f.state.end as string | null | undefined;
    if (!start && !end) return null;
    return {
      type: "temporal range",
      value: [start ? Date.parse(start) : null, end ? Date.parse(end) : null],
    };
  }
  return null;
};

export type TileDatasourceResolver = (tileId: string) => number | null;

/** One column of a tile's data; ``semanticType`` absent or unknown means unclassified. */
export interface TileField {
  fid: string;
  semanticType?: string;
}

/** A filter reaches beyond its own table when scoped "all" or legacy-untied. */
export const isCrossSourceFilter = (f: DashboardFilter): boolean =>
  f.scope === "all" || f.datasource_id == null;

const COMPATIBLE_SEMANTIC_TYPES: Record<DashboardFilterKind, SemanticType[]> = {
  categorical: ["nominal", "ordinal", "quantitative"],
  numeric_range: ["quantitative"],
  date_range: ["temporal"],
};

/** The worker applies filter rules without casting, so a same-named column of
 * another type errors (string values in a date ``is_in``, epoch-ms bounds on a
 * string). Categorical reaches quantitative columns because its values are
 * coerced to numbers on the way out. Unclassified columns pass. */
const kindFitsField = (kind: DashboardFilterKind, field: TileField): boolean =>
  !isSemantic(field.semanticType) || COMPATIBLE_SEMANTIC_TYPES[kind].includes(field.semanticType);

export const filterFitsField = (f: DashboardFilter, field: TileField): boolean =>
  kindFitsField(f.kind, field);

const SEMANTIC_LABEL: Record<SemanticType, string> = {
  quantitative: "number",
  nominal: "text",
  ordinal: "text",
  temporal: "date",
};

const KIND_LABEL: Record<DashboardFilterKind, string> = {
  categorical: "categorical",
  numeric_range: "range",
  date_range: "date range",
};

export type FieldFit =
  | { status: "unknown" | "fits" | "missing" }
  | { status: "type"; reason: string };

/** Explain, for the filter dialog, whether a cross-source filter of ``kind``
 * on ``fieldName`` reaches a tile whose data has ``tileFields``. */
export const describeFieldFit = (
  kind: DashboardFilterKind,
  fieldName: string,
  tileFields: TileField[] | null,
): FieldFit => {
  if (!tileFields) return { status: "unknown" };
  const field = tileFields.find((x) => x.fid === fieldName);
  if (!field) return { status: "missing" };
  if (kindFitsField(kind, field)) return { status: "fits" };
  const needs = Array.from(new Set(COMPATIBLE_SEMANTIC_TYPES[kind].map((s) => SEMANTIC_LABEL[s])));
  const here = SEMANTIC_LABEL[field.semanticType as SemanticType];
  return {
    status: "type",
    reason: `${fieldName} is a ${here} column here; a ${KIND_LABEL[kind]} filter needs ${needs.join(" or ")}`,
  };
};

/** Decide which dashboard filters apply to a given tile.
 *
 * Two gates:
 *   1. Source gate — a datasource-scoped filter needs the tile's underlying
 *      CatalogTable to match. A cross-source filter (scope "all", or the
 *      legacy untied null ``datasource_id``) instead needs the tile's data to
 *      have a column named ``field_name`` whose type fits the filter kind;
 *      ``tileFields`` null means the tile's columns are unknown and the gate
 *      is skipped.
 *   2. Target gate — ``target='all'`` matches every tile that passed
 *      the source gate; ``target='tiles'`` matches only the listed ids.
 */
export const filtersTargetingTile = (
  filters: DashboardFilter[],
  tileId: string,
  tileDatasource?: TileDatasourceResolver,
  tileFields?: TileField[] | null,
): DashboardFilter[] =>
  filters.filter((f) => {
    if (isCrossSourceFilter(f)) {
      if (tileFields) {
        const field = tileFields.find((x) => x.fid === f.field_name);
        if (!field || !filterFitsField(f, field)) return false;
      }
    } else {
      const tds = tileDatasource ? tileDatasource(tileId) : null;
      if (tds !== f.datasource_id) return false;
    }
    return f.target === "all" || (f.target === "tiles" && f.target_tile_ids.includes(tileId));
  });

export interface UseDashboardComputationOptions {
  tile: Ref<DashboardTile> | ComputedRef<DashboardTile>;
  filters: Ref<DashboardFilter[]> | ComputedRef<DashboardFilter[]>;
  tileDatasource?: TileDatasourceResolver;
  /** Columns of the tile's data; null while unknown (still loading). */
  tileFields?: () => TileField[] | null;
  onMissing?: () => void;
}

/** Wraps useGraphicWalkerCompute. Each GW IDataQueryPayload gets a
 * dashboard filter step prepended (when there are active filters that
 * target this tile) before being forwarded to the saved-viz compute API. */
export function useDashboardComputation(opts: UseDashboardComputationOptions) {
  const effectiveFilters = computed(() =>
    filtersTargetingTile(
      opts.filters.value,
      opts.tile.value.id,
      opts.tileDatasource,
      opts.tileFields?.() ?? null,
    ),
  );

  const fetcher = async (payload: any): Promise<{ rows: any[]; error: string | null }> => {
    const vizId = opts.tile.value.viz_id;
    if (vizId == null) return { rows: [], error: null };
    const filterStep = buildFilterStep(effectiveFilters.value, opts.tileFields?.() ?? null);
    const finalPayload =
      filterStep && payload?.workflow
        ? { ...payload, workflow: [filterStep, ...payload.workflow] }
        : payload;
    try {
      const resp = await CatalogApi.computeSavedVisualization(vizId, { payload: finalPayload });
      return { rows: resp.rows, error: resp.error ?? null };
    } catch (err: any) {
      if (err?.response?.status === 404) {
        opts.onMissing?.();
        return { rows: [], error: "Visualization not found" };
      }
      throw err;
    }
  };

  const { computation, lastError } = useGraphicWalkerCompute(
    fetcher,
    `dashboard-tile-${opts.tile.value.id}`,
  );

  return { computation, lastError, effectiveFilters };
}
