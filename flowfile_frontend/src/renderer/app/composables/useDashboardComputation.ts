import { computed, type ComputedRef, type Ref } from "vue";
import { CatalogApi } from "../api/catalog.api";
import { useGraphicWalkerCompute } from "./useGraphicWalkerCompute";
import type {
  DashboardFilter,
  DashboardFilterKind,
  DashboardTile,
  VisualizationComputeResponse,
} from "../types";
import { resolveDateRange } from "../views/DashboardsView/dateRange";

type SemanticType = "quantitative" | "nominal" | "ordinal" | "temporal";

const isSemantic = (s: unknown): s is SemanticType =>
  s === "quantitative" || s === "nominal" || s === "ordinal" || s === "temporal";

export interface VisFilter {
  fid: string;
  rule: Record<string, unknown>;
}

/** A chart's own filter as its spec holds it. */
interface ChartFilter {
  fid: string;
  rule?: Record<string, unknown> | null;
  computed?: boolean;
  enableAgg?: boolean;
}

interface ChartWithFilters {
  encodings?: { filters?: ChartFilter[] };
}

/**
 * Take a chart's row filters out of its spec, for a dashboard tile.
 *
 * Graphic Walker draws an input for every filter a chart holds, above the chart. A filter on a
 * stored column is the first step of the chart's query, so it applies just the same when the tile
 * sends it with the dashboard's own filters and the chart no longer holds it. A filter on a
 * computed or an aggregated field runs later in the query and stays where it is.
 */
export const splitChartFilters = <T extends ChartWithFilters>(
  chart: T,
): { chart: T; filters: VisFilter[] } => {
  const all = chart.encodings?.filters ?? [];
  const kept = all.filter((f) => f.computed || f.enableAgg);
  if (kept.length === all.length) return { chart, filters: [] };
  const filters = all
    .filter((f) => !f.computed && !f.enableAgg && f.rule != null)
    .map((f) => ({ fid: f.fid, rule: f.rule as Record<string, unknown> }));
  return { chart: { ...chart, encodings: { ...chart.encodings, filters: kept } }, filters };
};

export const buildFilterStep = (
  filters: DashboardFilter[],
  tileFields: TileField[] | null,
  chartFilters: VisFilter[] = [],
): { type: "filter"; filters: VisFilter[] } | null => {
  const visFilters: VisFilter[] = [...chartFilters];
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

/** A workflow payload with the filter step run first; a payload without a workflow is left alone. */
export const withFilterStep = <P extends { workflow?: unknown[] }>(
  payload: P,
  step: ReturnType<typeof buildFilterStep>,
): P =>
  step && payload?.workflow ? { ...payload, workflow: [step, ...payload.workflow] } : payload;

const toNumber = (v: unknown): number =>
  typeof v === "number" ? v : typeof v === "string" && v.trim() !== "" ? Number(v) : NaN;

/** Categorical selections are stored as strings (the option list is rendered
 * text) and the worker's ``is_in`` does no casting, so a quantitative column
 * needs numbers. Entries that do not parse are dropped rather than sent. */
const categoricalValues = (selected: unknown[], field?: TileField): unknown[] =>
  field?.semanticType === "quantitative"
    ? selected.map(toNumber).filter(Number.isFinite)
    : selected;

const filterToRule = (f: DashboardFilter, field?: TileField): Record<string, unknown> | null => {
  if (f.kind === "categorical") {
    const stored = (f.state.selected as unknown[]) ?? [];
    if (!Array.isArray(stored) || !stored.length) return null;
    const selected = f.multiple === false ? stored.slice(0, 1) : stored;
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
    const range = resolveDateRange(f.state);
    if (!range) return null;
    return {
      type: "temporal range",
      value: [
        range.start ? Date.parse(range.start) : null,
        range.end ? Date.parse(range.end) : null,
      ],
    };
  }
  return null;
};

/** Where a tile's rows come from: a catalog table read directly, or a saved chart's source. */
export type TileSource = { kind: "table"; tableId: number } | { kind: "viz"; vizId: number };

/** A KPI or table tile reads ``table_id`` when set, else its chart; a chart tile its chart.
 * A filter tile is a control and reads no data. */
export const tileSourceOf = (tile: DashboardTile): TileSource | null => {
  if (tile.type === "filter") return null;
  if ((tile.type === "kpi" || tile.type === "table") && tile.table_id != null) {
    return { kind: "table", tableId: tile.table_id };
  }
  return tile.viz_id != null ? { kind: "viz", vizId: tile.viz_id } : null;
};

export const sameSource = (a: TileSource | null, b: TileSource | null): boolean =>
  JSON.stringify(a) === JSON.stringify(b);

/** What a tile says when its source is gone. */
export const missingSourceText = (source: TileSource): string =>
  source.kind === "table"
    ? `Table #${source.tableId} no longer exists.`
    : `Visualization #${source.vizId} no longer exists.`;

/** Run one Graphic Walker workflow payload against a source on the worker. A table goes through
 * the ad-hoc route, which checks the caller may read that table; a chart through its own route. */
export const computeOnSource = (
  source: TileSource,
  payload: Record<string, unknown>,
  maxRows?: number,
): Promise<VisualizationComputeResponse> =>
  source.kind === "table"
    ? CatalogApi.computeAdHocVisualization(
        { source_type: "table", table_id: source.tableId },
        payload,
        maxRows,
      )
    : CatalogApi.computeSavedVisualization(source.vizId, { payload, maxRows });

/**
 * Which source a tile reads, as filters compare it: `table:<id>` for a catalog table (read
 * directly or through a chart on it), `viz:<id>` for a chart with no table (a SQL chart).
 */
export type DatasourceKey = string;

export const tableSourceKey = (tableId: number): DatasourceKey => `table:${tableId}`;
export const vizSourceKey = (vizId: number): DatasourceKey => `viz:${vizId}`;

/** The source a filter is bound to, or null for a legacy untied filter. */
export const filterSourceOf = (f: DashboardFilter): TileSource | null => {
  if (f.datasource_id != null) return { kind: "table", tableId: f.datasource_id };
  if (f.datasource_viz_id != null) return { kind: "viz", vizId: f.datasource_viz_id };
  return null;
};

export const filterSourceKey = (f: DashboardFilter): DatasourceKey | null => {
  const source = filterSourceOf(f);
  if (source == null) return null;
  return source.kind === "table" ? tableSourceKey(source.tableId) : vizSourceKey(source.vizId);
};

export type TileDatasourceResolver = (tileId: string) => DatasourceKey | null;

/** One column of a tile's data; ``semanticType`` absent or unknown means unclassified. */
export interface TileField {
  fid: string;
  semanticType?: string;
}

/** A filter reaches beyond its own source when scoped "all" or legacy-untied. */
export const isCrossSourceFilter = (f: DashboardFilter): boolean =>
  f.scope === "all" || filterSourceKey(f) == null;

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
 *   1. Source gate — a datasource-scoped filter needs the tile's source key
 *      (its CatalogTable, or for a SQL chart the chart) to match. A
 *      cross-source filter (scope "all", or a legacy untied one) instead needs the tile's data to
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
      if (tds !== filterSourceKey(f)) return false;
    }
    return f.target === "all" || (f.target === "tiles" && f.target_tile_ids.includes(tileId));
  });

export interface UseDashboardComputationOptions {
  tile: Ref<DashboardTile> | ComputedRef<DashboardTile>;
  filters: Ref<DashboardFilter[]> | ComputedRef<DashboardFilter[]>;
  tileDatasource?: TileDatasourceResolver;
  /** Columns of the tile's data; null while unknown (still loading). */
  tileFields?: () => TileField[] | null;
  /** Row filters taken out of the tile's chart (``splitChartFilters``). */
  chartFilters?: () => VisFilter[];
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
    const source = tileSourceOf(opts.tile.value);
    if (source == null) return { rows: [], error: null };
    const filterStep = buildFilterStep(
      effectiveFilters.value,
      opts.tileFields?.() ?? null,
      opts.chartFilters?.() ?? [],
    );
    try {
      const resp = await computeOnSource(source, withFilterStep(payload, filterStep));
      return { rows: resp.rows, error: resp.error ?? null };
    } catch (err: any) {
      if (err?.response?.status === 404) {
        opts.onMissing?.();
        return {
          rows: [],
          error: source.kind === "table" ? "Table not found" : "Visualization not found",
        };
      }
      throw err;
    }
  };

  const { computation, lastError } = useGraphicWalkerCompute(
    fetcher,
    `dashboard-tile-${opts.tile.value.id}`,
  );

  /** One query exactly as the tile sends it, without touching the tile's ``lastError``. */
  const query = fetcher;

  return { computation, lastError, effectiveFilters, query };
}
