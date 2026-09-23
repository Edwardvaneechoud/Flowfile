import { differenceInCalendarDays, subDays } from "date-fns";
import {
  filtersTargetingTile,
  type TileDatasourceResolver,
  type TileField,
} from "../../composables/useDashboardComputation";
import type { DashboardFilter, DashboardKpi, KpiAgg, KpiValueSize } from "../../types";

/** Key the single aggregate row carries its value under. */
const KPI_VALUE_KEY = "kpi_value";

/** 48-column grid → four KPI tiles per row. */
export const KPI_DEFAULT_SIZE = { w: 12, h: 3 } as const;

export const DEFAULT_KPI: DashboardKpi = {
  field: null,
  agg: "sum",
  label: null,
  prefix: null,
  suffix: null,
  decimals: null,
  compact: true,
  comparison: "none",
  target: null,
  higher_is_better: true,
  value_size: "auto",
};

export const KPI_AGG_OPTIONS: { label: string; value: KpiAgg }[] = [
  { label: "Sum", value: "sum" },
  { label: "Average", value: "mean" },
  { label: "Median", value: "median" },
  { label: "Min", value: "min" },
  { label: "Max", value: "max" },
  { label: "Count", value: "count" },
  { label: "Distinct count", value: "distinctCount" },
];

export const KPI_VALUE_SIZE_OPTIONS: { label: string; value: KpiValueSize }[] = [
  { label: "Auto", value: "auto" },
  { label: "Small", value: "sm" },
  { label: "Medium", value: "md" },
  { label: "Large", value: "lg" },
  { label: "Extra large", value: "xl" },
];

const VALUE_SIZE_PX: Record<Exclude<KpiValueSize, "auto">, number> = {
  sm: 20,
  md: 32,
  lg: 48,
  xl: 72,
};

/** Fixed font size in px, or null for "auto" (fit to the tile). */
export const valueSizePx = (size: KpiValueSize | null | undefined): number | null =>
  !size || size === "auto" ? null : VALUE_SIZE_PX[size];

const NUMERIC_AGGS: KpiAgg[] = ["sum", "mean", "median", "min", "max"];

/** Numeric aggregates need a quantitative column (gated on semanticType, since
 * low-cardinality integers are classified as dimensions); counts take any. */
export const fieldEligible = (agg: KpiAgg, semanticType?: string): boolean =>
  !NUMERIC_AGGS.includes(agg) || semanticType === "quantitative";

/** Only a plain row count works without a field. */
export const kpiNeedsField = (agg: KpiAgg): boolean => agg !== "count";

export const isKpiComplete = (kpi: DashboardKpi | null | undefined): kpi is DashboardKpi =>
  !!kpi && (!kpiNeedsField(kpi.agg) || !!kpi.field);

const measureFor = (kpi: DashboardKpi): Record<string, unknown> => {
  if (kpi.agg === "count") {
    return { field: kpi.field ?? "*", agg: "count", asFieldKey: KPI_VALUE_KEY };
  }
  if (kpi.agg === "distinctCount") {
    // SQL COUNT(DISTINCT) skips nulls; polars-gw's distinctCount (n_unique) counts them.
    const quoted = (kpi.field ?? "").replace(/"/g, '""');
    return { agg: "expr", expression: `COUNT(DISTINCT "${quoted}")`, asFieldKey: KPI_VALUE_KEY };
  }
  return { field: kpi.field, agg: kpi.agg, asFieldKey: KPI_VALUE_KEY };
};

/** A one-row, empty-groupBy aggregate for the saved-viz compute route. The
 * dashboard composable prepends the filter step; ``limit`` caps the raw rows
 * polars-gw returns when the field is missing, so the guard stays cheap. */
export const buildKpiPayload = (kpi: DashboardKpi): Record<string, unknown> | null => {
  if (!isKpiComplete(kpi)) return null;
  return {
    workflow: [
      { type: "view", query: [{ op: "aggregate", groupBy: [], measures: [measureFor(kpi)] }] },
    ],
    limit: 2,
  };
};

type KpiReadResult = { value: number | null } | { error: string };

export const readKpiValue = (
  rows: Record<string, unknown>[],
  fieldLabel: string,
): KpiReadResult => {
  if (rows.length !== 1 || !(KPI_VALUE_KEY in rows[0])) {
    return { error: `Field '${fieldLabel}' not found in the source` };
  }
  const raw = rows[0][KPI_VALUE_KEY];
  if (raw == null) return { value: null };
  const value = typeof raw === "number" ? raw : Number(raw);
  return Number.isFinite(value) ? { value } : { error: "The aggregate did not return a number" };
};

interface DateRangeState {
  start: string;
  end: string;
}

const dateRangeOf = (state: Record<string, unknown> | undefined): DateRangeState | null => {
  const start = state?.start;
  const end = state?.end;
  return typeof start === "string" && start && typeof end === "string" && end
    ? { start, end }
    : null;
};

/** The date_range filter a previous-period comparison shifts: the first one
 * that targets the tile and has both bounds set. */
export const pickComparisonFilter = (
  filters: DashboardFilter[],
  tileId: string,
  tileDatasource?: TileDatasourceResolver,
  tileFields?: TileField[] | null,
): DashboardFilter | null =>
  filtersTargetingTile(filters, tileId, tileDatasource, tileFields).find(
    (f) => f.kind === "date_range" && dateRangeOf(f.state) != null,
  ) ?? null;

/** The first date_range filter that targets the tile, whether or not its
 * bounds are set (the settings dialog explains what is still missing). */
export const findDateRangeFilter = (
  filters: DashboardFilter[],
  tileId: string,
  tileDatasource?: TileDatasourceResolver,
  tileFields?: TileField[] | null,
): DashboardFilter | null =>
  filtersTargetingTile(filters, tileId, tileDatasource, tileFields).find(
    (f) => f.kind === "date_range",
  ) ?? null;

const rangeLengthDays = (range: DateRangeState): number =>
  differenceInCalendarDays(new Date(range.end), new Date(range.start)) + 1;

/** The window of the same length ending the day before ``start`` (no overlap). */
export const shiftDateRangeBack = (state: Record<string, unknown>): DateRangeState | null => {
  const range = dateRangeOf(state);
  if (!range) return null;
  const n = rangeLengthDays(range);
  return {
    start: subDays(new Date(range.start), n).toISOString(),
    end: subDays(new Date(range.end), n).toISOString(),
  };
};

export const previousPeriodLabel = (state: Record<string, unknown>): string | null => {
  const range = dateRangeOf(state);
  if (!range) return null;
  const n = rangeLengthDays(range);
  return n === 1 ? "vs previous day" : `vs previous ${n} days`;
};

/** Same filters with only the comparison filter's window shifted back; targeting
 * ignores state, so the shifted copy reaches exactly the same tiles. */
export const previousPeriodFilters = (
  filters: DashboardFilter[],
  comparisonFilterId: string | null | undefined,
): DashboardFilter[] => {
  if (!comparisonFilterId) return filters;
  return filters.map((f) => {
    if (f.id !== comparisonFilterId) return f;
    const shifted = shiftDateRangeBack(f.state);
    return shifted ? { ...f, state: { ...f.state, ...shifted } } : f;
  });
};

export interface KpiDelta {
  abs: number;
  /** null when the reference is 0. */
  pct: number | null;
  direction: "up" | "down" | "flat";
  tone: "good" | "bad" | "neutral";
}

export const computeDelta = (
  value: number | null,
  reference: number | null | undefined,
  higherIsBetter = true,
): KpiDelta | null => {
  if (value == null || reference == null) return null;
  const abs = value - reference;
  const pct = reference === 0 ? null : abs / Math.abs(reference);
  const direction = abs > 0 ? "up" : abs < 0 ? "down" : "flat";
  const tone =
    direction === "flat" ? "neutral" : (direction === "up") === higherIsBetter ? "good" : "bad";
  return { abs, pct, direction, tone };
};

type KpiFormat = Pick<DashboardKpi, "compact" | "decimals" | "prefix" | "suffix">;

/** Locale number with the prefix placed after the sign ("-$1.2K", not "$-1.2K"). */
export const formatKpiValue = (
  value: number | null | undefined,
  kpi: KpiFormat,
  locale?: string,
  signDisplay: "auto" | "exceptZero" = "auto",
): string => {
  if (value == null || !Number.isFinite(value)) return "—";
  const decimals = kpi.decimals ?? null;
  const options: Intl.NumberFormatOptions =
    (kpi.compact ?? true)
      ? { notation: "compact", maximumFractionDigits: decimals ?? 1 }
      : { maximumFractionDigits: decimals ?? 2, minimumFractionDigits: decimals ?? 0 };
  const parts = new Intl.NumberFormat(locale, { ...options, signDisplay }).formatToParts(value);
  const isSign = (p: Intl.NumberFormatPart) => p.type === "minusSign" || p.type === "plusSign";
  const sign = parts
    .filter(isSign)
    .map((p) => p.value)
    .join("");
  const body = parts
    .filter((p) => !isSign(p))
    .map((p) => p.value)
    .join("");
  return `${sign}${kpi.prefix ?? ""}${body}${kpi.suffix ?? ""}`;
};

export const formatDeltaAbs = (abs: number, kpi: KpiFormat, locale?: string): string =>
  formatKpiValue(abs, kpi, locale, "exceptZero");

export const formatDeltaPct = (pct: number, locale?: string): string =>
  new Intl.NumberFormat(locale, {
    style: "percent",
    maximumFractionDigits: 1,
    signDisplay: "exceptZero",
  }).format(pct);

const AGG_PHRASE: Record<KpiAgg, string> = {
  sum: "Sum of",
  mean: "Average",
  median: "Median",
  min: "Min",
  max: "Max",
  count: "Count of",
  distinctCount: "Distinct count of",
};

export const autoKpiLabel = (kpi: Pick<DashboardKpi, "agg" | "field">): string => {
  if (kpi.agg === "count" && !kpi.field) return "Row count";
  if (!kpi.field) return KPI_AGG_OPTIONS.find((o) => o.value === kpi.agg)?.label ?? "KPI";
  return `${AGG_PHRASE[kpi.agg]} ${kpi.field}`;
};
