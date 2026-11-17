import { differenceInCalendarDays, subDays } from "date-fns";
import {
  filtersTargetingTile,
  type TileDatasourceResolver,
  type TileField,
} from "../../composables/useDashboardComputation";
import type {
  DashboardFilter,
  DashboardKpi,
  DashboardKpiTrend,
  KpiAgg,
  KpiTrendGrain,
  KpiValueSize,
} from "../../types";
import { resolveDateRange } from "./dateRange";

/** Key the single aggregate row carries its value under. */
const KPI_VALUE_KEY = "kpi_value";
const KPI_SPAN_START_KEY = "kpi_span_start";
const KPI_SPAN_END_KEY = "kpi_span_end";
const KPI_BUCKET_KEY = "kpi_bucket";

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
  trend: null,
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
 * polars-gw returns when the field is missing, so the guard stays cheap.
 * ``withSpan`` also reads the first and last date of the trend column, which an
 * "auto" trend grain is picked from (``readKpiSpan``). */
export const buildKpiPayload = (
  kpi: DashboardKpi,
  { withSpan = false }: { withSpan?: boolean } = {},
): Record<string, unknown> | null => {
  if (!isKpiComplete(kpi)) return null;
  const measures = [measureFor(kpi)];
  const trendField = kpi.trend?.field;
  if (withSpan && trendField) {
    measures.push(
      { field: trendField, agg: "min", asFieldKey: KPI_SPAN_START_KEY },
      { field: trendField, agg: "max", asFieldKey: KPI_SPAN_END_KEY },
    );
  }
  return {
    workflow: [{ type: "view", query: [{ op: "aggregate", groupBy: [], measures }] }],
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

// A named window counts like a fixed one: it has both bounds once resolved.
const dateRangeOf = (
  state: Record<string, unknown> | undefined,
  now?: Date,
): DateRangeState | null => {
  const range = resolveDateRange(state, now);
  return range?.start && range.end ? { start: range.start, end: range.end } : null;
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
export const shiftDateRangeBack = (
  state: Record<string, unknown>,
  now?: Date,
): DateRangeState | null => {
  const range = dateRangeOf(state, now);
  if (!range) return null;
  const n = rangeLengthDays(range);
  return {
    start: subDays(new Date(range.start), n).toISOString(),
    end: subDays(new Date(range.end), n).toISOString(),
  };
};

export const previousPeriodLabel = (state: Record<string, unknown>, now?: Date): string | null => {
  const range = dateRangeOf(state, now);
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
    if (!shifted) return f;
    // The shifted window is a fixed one, so a preset the filter named no longer applies.
    const state: Record<string, unknown> = { ...f.state, ...shifted };
    delete state.preset;
    return { ...f, state };
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

/** The most recent buckets a trend draws: two months of days, a year of weeks, five years of months. */
export const KPI_TREND_MAX_BUCKETS = 60;

export type TrendGrain = Exclude<KpiTrendGrain, "auto">;

export const KPI_TREND_GRAIN_OPTIONS: { label: string; value: KpiTrendGrain }[] = [
  { label: "Auto", value: "auto" },
  { label: "Day", value: "day" },
  { label: "Week", value: "week" },
  { label: "Month", value: "month" },
  { label: "Quarter", value: "quarter" },
  { label: "Year", value: "year" },
];

export const kpiTrendOf = (kpi: DashboardKpi | null | undefined): DashboardKpiTrend | null =>
  kpi?.trend?.field ? kpi.trend : null;

/** True when the headline query must also read the trend column's span. */
export const trendNeedsSpan = (kpi: DashboardKpi | null | undefined): boolean =>
  kpiTrendOf(kpi)?.grain === "auto";

/** A date bucket as polars-gw sends it ("2024-03-01", "2024-03-01 00:00:00.000000", an
 * offset after it) to the local calendar time it names; null when it is no date. */
export const parseBucket = (raw: unknown): number | null => {
  if (typeof raw === "number") return Number.isFinite(raw) ? raw : null;
  if (typeof raw !== "string") return null;
  const m = /^(-?\d{4,})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2}))?)?/.exec(raw.trim());
  if (!m) return null;
  const [, y, mo, d, h = "0", mi = "0", sec = "0"] = m;
  const date = new Date(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), Number(sec));
  const t = date.getTime();
  return Number.isFinite(t) ? t : null;
};

export interface KpiSpan {
  start: number;
  end: number;
}

/** The trend column's first and last date from the headline row, or null when unknown. */
export const readKpiSpan = (rows: Record<string, unknown>[] | null | undefined): KpiSpan | null => {
  const row = rows?.length === 1 ? rows[0] : null;
  if (!row) return null;
  const start = parseBucket(row[KPI_SPAN_START_KEY]);
  const end = parseBucket(row[KPI_SPAN_END_KEY]);
  return start != null && end != null && end >= start ? { start, end } : null;
};

const DAY_MS = 86_400_000;
// The upper span, in days, each grain covers within KPI_TREND_MAX_BUCKETS buckets.
const AUTO_GRAIN_LIMITS: [TrendGrain, number][] = [
  ["day", KPI_TREND_MAX_BUCKETS],
  ["week", KPI_TREND_MAX_BUCKETS * 7],
  ["month", KPI_TREND_MAX_BUCKETS * 30.44],
  ["quarter", KPI_TREND_MAX_BUCKETS * 91.31],
];

/** The finest grain whose buckets over the span fit in ``KPI_TREND_MAX_BUCKETS``. */
export const autoTrendGrain = (span: KpiSpan | null): TrendGrain => {
  if (!span) return "month";
  const days = (span.end - span.start) / DAY_MS;
  return AUTO_GRAIN_LIMITS.find(([, limit]) => days < limit)?.[0] ?? "year";
};

export const resolveTrendGrain = (grain: KpiTrendGrain, span: KpiSpan | null): TrendGrain =>
  grain === "auto" ? autoTrendGrain(span) : grain;

/** The KPI's measure per ``grain`` bucket of the trend column, newest first, on the worker:
 * polars-gw truncates the date (Graphic Walker's ``dateTimeDrill`` transform) and aggregates.
 * The composable prepends the filter step the headline gets. One bucket over the cap leaves
 * room for the null-date bucket, which sorts first. */
export const buildKpiTrendPayload = (
  kpi: DashboardKpi,
  grain: TrendGrain,
): Record<string, unknown> | null => {
  const trend = kpiTrendOf(kpi);
  if (!trend || !isKpiComplete(kpi)) return null;
  return {
    workflow: [
      {
        type: "transform",
        transform: [
          {
            key: KPI_BUCKET_KEY,
            expression: {
              op: "dateTimeDrill",
              as: KPI_BUCKET_KEY,
              params: [
                { type: "field", value: trend.field },
                { type: "value", value: grain },
              ],
            },
          },
        ],
      },
      {
        type: "view",
        query: [{ op: "aggregate", groupBy: [KPI_BUCKET_KEY], measures: [measureFor(kpi)] }],
      },
      { type: "sort", by: [KPI_BUCKET_KEY], sort: "descending" },
    ],
    limit: KPI_TREND_MAX_BUCKETS + 1,
  };
};

export interface TrendPoint {
  /** Local time the bucket starts at, in ms. */
  t: number;
  value: number;
}

/** The trend rows as points, oldest first, at most ``KPI_TREND_MAX_BUCKETS``: a bucket without
 * a date or a value is left out. Null when the rows are not a trend (the column was missing). */
export const readKpiTrend = (
  rows: Record<string, unknown>[] | null | undefined,
): TrendPoint[] | null => {
  if (!rows) return null;
  if (rows.length && !(KPI_BUCKET_KEY in rows[0] && KPI_VALUE_KEY in rows[0])) return null;
  const points: TrendPoint[] = [];
  for (const row of rows) {
    const t = parseBucket(row[KPI_BUCKET_KEY]);
    const raw = row[KPI_VALUE_KEY];
    const value = typeof raw === "number" ? raw : raw == null ? NaN : Number(raw);
    if (t != null && Number.isFinite(value)) points.push({ t, value });
  }
  points.sort((a, b) => a.t - b.t);
  return points.slice(-KPI_TREND_MAX_BUCKETS);
};

const GRAIN_NOUN: Record<TrendGrain, string> = {
  day: "day",
  week: "week",
  month: "month",
  quarter: "quarter",
  year: "year",
};

/** A bucket as a reader names it: "3 Mar 2024", "Week of 4 Mar 2024", "Mar 2024", "Q1 2024", "2024". */
export const formatTrendBucket = (t: number, grain: TrendGrain, locale?: string): string => {
  const d = new Date(t);
  const day = () =>
    d.toLocaleDateString(locale, { day: "numeric", month: "short", year: "numeric" });
  switch (grain) {
    case "day":
      return day();
    case "week":
      return `Week of ${day()}`;
    case "month":
      return d.toLocaleDateString(locale, { month: "short", year: "numeric" });
    case "quarter":
      return `Q${Math.floor(d.getMonth() / 3) + 1} ${d.getFullYear()}`;
    case "year":
      return String(d.getFullYear());
  }
};

/** The line's text alternative: what it shows, the span and the direction, first to last bucket. */
export const trendDescription = (
  points: TrendPoint[],
  grain: TrendGrain,
  label: string,
  locale?: string,
): string => {
  if (points.length < 2) return `${label} by ${GRAIN_NOUN[grain]}`;
  const first = points[0];
  const last = points[points.length - 1];
  const span = `${formatTrendBucket(first.t, grain, locale)} to ${formatTrendBucket(last.t, grain, locale)}`;
  const delta = computeDelta(last.value, first.value);
  let direction = "flat";
  if (delta && delta.direction !== "flat") {
    const word = delta.direction === "up" ? "up" : "down";
    direction =
      delta.pct != null
        ? `${word} ${formatDeltaPct(Math.abs(delta.pct), locale).replace(/^\+/, "")}`
        : word;
  }
  return `${label} by ${GRAIN_NOUN[grain]}, ${span}: ${direction}`;
};
