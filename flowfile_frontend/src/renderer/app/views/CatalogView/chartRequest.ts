import type {
  DraggableFieldState,
  IAnalyticType,
  IChart,
  IMutField,
  IViewField,
  TerseComputedField,
  TerseFieldRef,
  TerseFilter,
  TerseSpec,
  TerseSpecSchemaUrl,
} from "@kanaries/graphic-walker/interfaces";
import type { normalize } from "@kanaries/graphic-walker/models/normalize";
import type {
  expandTerse,
  parseShorthand,
  projectTerse,
  terseComputedFid,
} from "@kanaries/graphic-walker/models/terse";

const CHART_MARKS = ["bar", "line", "area", "point", "arc", "rect", "boxplot", "table"] as const;
const CHART_AGGREGATES = ["sum", "mean", "median", "min", "max", "count", "distinctCount"] as const;
const CHART_TIME_UNITS = ["year", "quarter", "month", "week", "day", "hour"] as const;
const CHART_STACKS = ["stack", "none", "normalize"] as const;

export type ChartMark = (typeof CHART_MARKS)[number];
export type ChartAggregate = (typeof CHART_AGGREGATES)[number];
export type ChartTimeUnit = (typeof CHART_TIME_UNITS)[number];

/** A column on a channel: grouped by when it has no `aggregate`, aggregated when it has one. */
export interface FieldRef {
  /** Exact column name; `"*"` only with `aggregate: "count"` (the row count). */
  field: string;
  aggregate?: ChartAggregate;
  timeUnit?: ChartTimeUnit;
  /** Number of equal-width bins. */
  bin?: number;
}

export type ChartFilterValue = string | number | boolean;

/** A row filter; `dateRange` bounds are inclusive days. */
export type ChartFilter =
  | { field: string; oneOf: ChartFilterValue[] }
  | { field: string; notIn: ChartFilterValue[] }
  | { field: string; range: [number | null, number | null] }
  | { field: string; dateRange: [string | null, string | null] };

/** The chart request core and the browser exchange (the plan's contract). */
export interface ChartRequest {
  mark: ChartMark;
  x?: FieldRef | FieldRef[];
  y?: FieldRef | FieldRef[];
  color?: FieldRef;
  /** Scatter only. */
  size?: FieldRef;
  /** Pie only, required there. */
  theta?: FieldRef;
  filters?: ChartFilter[];
  /** Default true; false plots raw rows. */
  aggregate?: boolean;
  /** Bar and area only. */
  stack?: (typeof CHART_STACKS)[number];
  /** Orders the category axis by the measure. */
  sort?: "ascending" | "descending";
  /** Keeps N after the sort. */
  limit?: number;
}

/** The Graphic Walker functions the conversion runs, passed in so this module never loads the package. */
export interface GraphicWalkerFunctions {
  normalize: typeof normalize;
  expandTerse: typeof expandTerse;
  projectTerse: typeof projectTerse;
  parseShorthand: typeof parseShorthand;
  terseComputedFid: typeof terseComputedFid;
}

export interface ChartContext {
  /** The dataset's fields as the builder's store holds them. */
  fields: IMutField[];
  /** The chart on screen, or null for an empty builder. */
  current: IChart | null;
  gw: GraphicWalkerFunctions;
}

export type ChartFromRequestResult = { ok: true; chart: IChart } | { ok: false; error: string };

export type DescribeChartResult =
  | { ok: true; request: ChartRequest }
  | { ok: false; unsupported: string[] };

const TERSE_SCHEMA_URL: typeof TerseSpecSchemaUrl =
  "https://graphic-walker.kanaries.net/tersespec_v1.json";
const COUNT_FID = "gw_count_fid";
const MEA_KEY_FID = "gw_mea_key_fid";
const MEA_VAL_FID = "gw_mea_val_fid";
const PAINT_FID = "gw_paint_fid";
const SYNTHETIC_FIDS = [COUNT_FID, MEA_KEY_FID, MEA_VAL_FID];
const DAY_END_MS = 86_399_999;
const MIN_BINS = 2;
const MAX_BINS = 50;

type RefChannel = "x" | "y" | "color" | "size" | "theta";
const REF_CHANNELS: RefChannel[] = ["x", "y", "color", "size", "theta"];
const ENCODING_OF: Record<RefChannel, "columns" | "rows" | "color" | "size" | "theta"> = {
  x: "columns",
  y: "rows",
  color: "color",
  size: "size",
  theta: "theta",
};
const OTHER_CHANNELS: Record<string, string> = {
  opacity: "a field on opacity",
  shape: "a field on shape",
  text: "a text label",
  details: "a detail field",
  radius: "a field on radius",
  longitude: "a map",
  latitude: "a map",
  geoId: "a map",
};

class ChartRequestError extends Error {}

function fail(message: string): never {
  throw new ChartRequestError(message);
}

const asList = <T>(value: T | T[] | null | undefined): T[] =>
  value == null ? [] : Array.isArray(value) ? value : [value];

const includes = <T extends string>(list: readonly T[], value: unknown): value is T =>
  typeof value === "string" && (list as readonly string[]).includes(value);

/** A copy without the keys the request sets, so `expandTerse` never sees them twice. */
const without = <T extends object>(value: T | undefined, keys: string[]): Partial<T> =>
  Object.fromEntries(
    Object.entries(value ?? {}).filter(([key]) => !keys.includes(key)),
  ) as Partial<T>;

const errorText = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

/**
 * Turn a chart request into a complete Graphic Walker chart for the store's `applyChart`.
 * The current chart's name, layout, config, synthetic field names and user-made computed
 * fields carry over, since `applyChart` would otherwise replace them. Any Graphic Walker
 * warning is treated as a failure.
 */
export function chartFromRequest(request: ChartRequest, ctx: ChartContext): ChartFromRequestResult {
  try {
    return { ok: true, chart: buildChart(request, ctx) };
  } catch (error) {
    if (error instanceof ChartRequestError) return { ok: false, error: error.message };
    return { ok: false, error: `The chart could not be built: ${errorText(error)}` };
  }
}

interface PlannedRef {
  channel: RefChannel;
  measure: boolean;
  terse: TerseFieldRef;
}

function buildChart(request: ChartRequest, ctx: ChartContext): IChart {
  const { gw, current } = ctx;
  if (!includes(CHART_MARKS, request.mark)) {
    fail(`"${String(request.mark)}" is not a chart type this can draw.`);
  }
  const mark = request.mark;
  const aggregated = request.aggregate !== false;
  if (mark === "table" && !aggregated) {
    fail("A table shows aggregated values; give each number an aggregate.");
  }
  const columns = new Map(ctx.fields.map((f) => [f.fid, f]));
  const roles = new Map<string, IAnalyticType>();
  const setRole = (column: string, role: IAnalyticType) => {
    const previous = roles.get(column);
    if (previous && previous !== role) {
      fail(`"${column}" is both grouped and aggregated. Use "bin" on one, or drop one.`);
    }
    roles.set(column, role);
  };
  const computed = new Map<string, TerseComputedField>();

  const planRef = (channel: RefChannel, ref: FieldRef): PlannedRef => {
    if (!ref || typeof ref.field !== "string") fail(`${channel} needs a column.`);
    const { field, timeUnit, bin } = ref;
    const aggregate = aggregated ? ref.aggregate : undefined;
    if (aggregate !== undefined && !includes(CHART_AGGREGATES, aggregate)) {
      fail(`"${String(aggregate)}" is not an aggregate this can draw.`);
    }
    if (timeUnit !== undefined && !includes(CHART_TIME_UNITS, timeUnit)) {
      fail(
        `"${String(timeUnit)}" is not a date grain; use year, quarter, month, week, day or hour.`,
      );
    }
    if (field === "*") {
      if (!aggregated) fail("A row count needs an aggregated chart.");
      if (aggregate !== "count" || timeUnit !== undefined || bin !== undefined) {
        fail('"*" means row count: use {"field":"*","aggregate":"count"}.');
      }
      return { channel, measure: true, terse: `fid:${COUNT_FID}` };
    }
    const column = columns.get(field);
    if (!column) fail(`"${field}" is not a column of this table.`);
    const kinds = [aggregate !== undefined, timeUnit !== undefined, bin !== undefined];
    if (kinds.filter(Boolean).length > 1) {
      fail(`"${field}" can take one of aggregate, timeUnit or bin, not several.`);
    }
    if (bin !== undefined) {
      if (!Number.isInteger(bin) || bin < MIN_BINS || bin > MAX_BINS) {
        fail(`bin needs a whole number of bins between ${MIN_BINS} and ${MAX_BINS}.`);
      }
      const name = `bin${bin}(${field})`;
      if (columns.has(name)) fail(`A column is already called "${name}"; it cannot be binned.`);
      computed.set(name, { name, bin: { field: `fid:${field}`, count: bin } });
      return { channel, measure: false, terse: `fid:${gw.terseComputedFid(name)}` };
    }
    if (timeUnit !== undefined) {
      setRole(field, "dimension");
      return { channel, measure: false, terse: { field: `fid:${field}`, timeUnit } };
    }
    if (aggregate !== undefined) {
      setRole(field, "measure");
      return { channel, measure: true, terse: { field: `fid:${field}`, aggregate } };
    }
    const measure =
      !aggregated &&
      (channel === "x" || channel === "y" || channel === "size") &&
      column.semanticType === "quantitative";
    setRole(field, measure ? "measure" : "dimension");
    return { channel, measure, terse: `fid:${field}` };
  };

  const planned = REF_CHANNELS.flatMap((channel) =>
    asList(request[channel]).map((ref) => planRef(channel, ref)),
  );
  let xRefs = planned.filter((p) => p.channel === "x");
  let yRefs = planned.filter((p) => p.channel === "y");
  if (mark === "table") {
    const both = [...yRefs, ...xRefs];
    yRefs = both.filter((p) => !p.measure);
    xRefs = both.filter((p) => p.measure);
  }

  if (request.sort != null) {
    if (request.sort !== "ascending" && request.sort !== "descending") {
      fail('sort takes "ascending" or "descending".');
    }
    const axis =
      yRefs.length > 0 && !yRefs.some((p) => p.measure)
        ? yRefs
        : xRefs.length > 0 && !xRefs.some((p) => p.measure)
          ? xRefs
          : fail("sort needs a category axis to order.");
    const last = axis[axis.length - 1];
    const base = typeof last.terse === "string" ? { field: last.terse } : last.terse;
    last.terse = { ...base, sort: request.sort };
  }

  const terse: TerseSpec = { $schema: TERSE_SCHEMA_URL, mark, aggregate: aggregated };
  if (current?.name) terse.name = current.name;
  const single = (refs: PlannedRef[]) =>
    refs.length === 1 ? refs[0].terse : refs.map((p) => p.terse);
  if (xRefs.length) terse.x = single(xRefs);
  if (yRefs.length) terse.y = single(yRefs);
  for (const channel of ["color", "size", "theta"] as const) {
    const ref = planned.find((p) => p.channel === channel);
    if (ref) terse[channel] = ref.terse;
  }
  if (computed.size) terse.computed = [...computed.values()];
  if (request.filters?.length) terse.filters = request.filters.map((f) => terseFilter(f, columns));
  if (request.limit != null) {
    if (!Number.isInteger(request.limit) || request.limit < 1) {
      fail("limit needs a whole number of 1 or more.");
    }
    terse.limit = request.limit;
  }

  terse.config = {
    ...without(current?.config, ["geoms", "defaultAggregated", "limit"]),
    coordSystem: "generic",
  };
  terse.layout = without(current?.layout, ["stack"]);
  if (request.stack != null && (mark === "bar" || mark === "area")) {
    if (!includes(CHART_STACKS, request.stack)) fail('stack takes "stack", "none" or "normalize".');
    terse.layout.stack = request.stack;
  }

  const meta = datasetFieldList(ctx.fields, current, roles);
  const warnings: string[] = [];
  const chart = gw.normalize(
    gw.expandTerse(terse, meta, (message) => warnings.push(message)),
    meta,
  );
  if (warnings.length) fail(`The chart could not be built: ${warnings.join(" ")}`);
  return carryOver(chart, current);
}

function terseFilter(filter: ChartFilter, columns: Map<string, IMutField>): TerseFilter {
  const field = filter?.field;
  if (field === "*") fail("Filters apply to columns, not to the row count.");
  if (!columns.has(field)) fail(`The filter's "${field}" is not a column of this table.`);
  const keys = ["oneOf", "notIn", "range", "dateRange"].filter((k) => k in filter);
  if (keys.length !== 1) {
    fail(`The filter on "${field}" needs exactly one of oneOf, notIn, range or dateRange.`);
  }
  const ref = `fid:${field}`;
  if ("oneOf" in filter || "notIn" in filter) {
    const values = "oneOf" in filter ? filter.oneOf : filter.notIn;
    if (!Array.isArray(values) || values.length === 0 || values.some((v) => v === null)) {
      fail(`The filter on "${field}" needs at least one value; null is not matched.`);
    }
    return "oneOf" in filter
      ? { field: ref, oneOf: [...values] }
      : { field: ref, notIn: [...values] };
  }
  if ("range" in filter) {
    const [min, max] = Array.isArray(filter.range) ? filter.range : [null, null];
    const bound = (v: unknown) => v === null || (typeof v === "number" && Number.isFinite(v));
    if ((min === null && max === null) || !bound(min) || !bound(max)) {
      fail(`The range on "${field}" needs [min, max] numbers with at least one bound.`);
    }
    return { field: ref, range: [min, max] };
  }
  const [start, end] = Array.isArray(filter.dateRange) ? filter.dateRange : [null, null];
  if (start === null && end === null) fail(`The date range on "${field}" needs a start or an end.`);
  const startMs = start === null ? null : utcDay(start, field);
  const endMs = end === null ? null : utcDay(end, field) + DAY_END_MS;
  return { field: ref, timeRange: [startMs, endMs] };
}

function utcDay(value: string, field: string): number {
  const match = typeof value === "string" ? /^(\d{4})-(\d{2})-(\d{2})$/.exec(value) : null;
  const ms = match ? Date.UTC(Number(match[1]), Number(match[2]) - 1, Number(match[3])) : NaN;
  if (Number.isNaN(ms) || new Date(ms).toISOString().slice(0, 10) !== value) {
    fail(`The date range on "${field}" takes "YYYY-MM-DD" or null.`);
  }
  return ms;
}

/** The store's fields, overlaid with the current chart's names and roles, then the request's roles. */
function datasetFieldList(
  fields: IMutField[],
  current: IChart | null,
  roles: Map<string, IAnalyticType>,
): IMutField[] {
  const pool = new Map<string, IViewField>();
  for (const f of current ? poolFields(current) : []) if (!pool.has(f.fid)) pool.set(f.fid, f);
  return fields.map((field) => {
    const own = pool.get(field.fid);
    const isOwn = own && !own.computed;
    return {
      ...field,
      name: isOwn && own.name ? own.name : field.name,
      basename: (isOwn && own.basename) || field.basename || field.name,
      analyticType: roles.get(field.fid) ?? (isOwn ? own.analyticType : field.analyticType),
    };
  });
}

const poolFields = (chart: IChart): IViewField[] => [
  ...chart.encodings.dimensions,
  ...chart.encodings.measures,
];

const isUserComputed = (field: IViewField) =>
  Boolean(field.computed || field.expression) && !SYNTHETIC_FIDS.includes(field.fid);

function carryOver(chart: IChart, current: IChart | null): IChart {
  if (!current) return chart;
  const currentPool = poolFields(current);
  const syntheticNames = new Map(
    currentPool
      .filter((f) => SYNTHETIC_FIDS.includes(f.fid) && typeof f.name === "string")
      .map((f) => [f.fid, f.name]),
  );
  const rename = <T extends IViewField>(f: T): T =>
    syntheticNames.has(f.fid) ? { ...f, name: syntheticNames.get(f.fid)! } : f;
  const encodings = Object.fromEntries(
    Object.entries(chart.encodings).map(([key, list]) => [key, (list as IViewField[]).map(rename)]),
  ) as unknown as DraggableFieldState;

  const present = poolFields({ ...chart, encodings });
  const taken = new Set(present.flatMap((f) => [`fid:${f.fid}`, `name:${f.name}`]));
  const missing = currentPool.filter(
    (f) => isUserComputed(f) && !taken.has(`fid:${f.fid}`) && !taken.has(`name:${f.name}`),
  );
  encodings.dimensions = syntheticLast(
    [...encodings.dimensions, ...missing.filter((f) => f.analyticType === "dimension")],
    [PAINT_FID, MEA_KEY_FID],
  );
  encodings.measures = syntheticLast(
    [...encodings.measures, ...missing.filter((f) => f.analyticType === "measure")],
    [COUNT_FID, MEA_VAL_FID],
  );
  return { ...chart, visId: current.visId, encodings };
}

/** Graphic Walker keeps these fields at the end of their pool (`lintExtraFields`). */
function syntheticLast(fields: IViewField[], lastFids: string[]): IViewField[] {
  const rest = fields.filter((f) => !lastFids.includes(f.fid));
  return rest.concat(lastFids.flatMap((fid) => fields.filter((f) => f.fid === fid)));
}

/**
 * Describe a chart as a chart request for a follow-up prompt, or list the parts a request
 * cannot express, each as a short phrase a user can read.
 */
export function describeChart(
  chart: IChart,
  ctx: Pick<ChartContext, "fields" | "gw">,
): DescribeChartResult {
  const unsupported = new Set<string>();
  const warnings: string[] = [];
  let spec: TerseSpec;
  try {
    spec = ctx.gw.projectTerse(chart, (message) => warnings.push(message));
  } catch {
    return { ok: false, unsupported: ["a part of this chart"] };
  }
  warnings.forEach((w) => unsupported.add(warningPhrase(w)));

  const stored = chart.config.geoms[0];
  const mark = drawnMark(chart);
  if (!includes(CHART_MARKS, mark)) {
    unsupported.add(
      stored === "auto" || !stored ? "the automatic chart type" : `the "${stored}" chart type`,
    );
  }
  if (chart.config.coordSystem === "geographic") unsupported.add("a map");
  for (const [channel, phrase] of Object.entries(OTHER_CHANNELS)) {
    const fields = chart.encodings[channel as keyof DraggableFieldState];
    if (fields && fields.length > 0) unsupported.add(phrase);
  }
  for (const filter of chart.encodings.filters) {
    if (!filter.rule) continue;
    if (filter.enableAgg) unsupported.add("a filter on an aggregated value");
    if (filter.rule.type === "temporal range" && filter.rule.offset) {
      unsupported.add("a date filter with a time zone offset");
    }
  }
  for (const def of spec.computed ?? []) {
    if (def.expr !== undefined) unsupported.add("a formula field");
    if (def.log !== undefined) unsupported.add("a log field");
  }
  if (unsupported.size) return { ok: false, unsupported: [...unsupported] };

  const aggregated = chart.config.defaultAggregated !== false;
  const pool = poolFields(chart);
  const byFid = new Map(pool.map((f) => [f.fid, f]));
  const byName = new Map<string, IViewField>();
  for (const f of pool)
    if (typeof f.name === "string" && !byName.has(f.name)) byName.set(f.name, f);
  const datasetFids = new Set(ctx.fields.map((f) => f.fid));
  const columnOf = (ref: string): string | null => {
    const field = ref.startsWith("fid:") ? byFid.get(ref.slice(4)) : byName.get(ref);
    return field && !field.computed && !field.expression && datasetFids.has(field.fid)
      ? field.fid
      : null;
  };
  const bins = new Map((spec.computed ?? []).filter((d) => d.bin).map((d) => [d.name, d.bin!]));

  const describeRef = (ref: TerseFieldRef, shelf: IViewField): FieldRef | null => {
    if (shelf.fid === COUNT_FID) return { field: "*", aggregate: "count" };
    if (typeof ref !== "string" && ref.timeUnit) {
      const column = columnOf(ref.field);
      if (!includes(CHART_TIME_UNITS, ref.timeUnit)) {
        unsupported.add(`a date grouped by ${ref.timeUnit}`);
      } else if (!column) {
        unsupported.add("a computed field");
      } else if (shelf.analyticType === "measure") {
        unsupported.add("an aggregated date");
      } else {
        return { field: column, timeUnit: ref.timeUnit };
      }
      return null;
    }
    const parsed =
      typeof ref === "string"
        ? ctx.gw.parseShorthand(ref)
        : { field: ref.field, aggregate: ref.aggregate };
    const bin = (shelf.name && bins.get(shelf.name)) || bins.get(parsed.field);
    if (bin) {
      const base = columnOf(bin.field);
      if (base) return { field: base, bin: bin.count ?? 10 };
      unsupported.add("a computed field");
      return null;
    }
    const column = columnOf(parsed.field);
    if (!column) {
      unsupported.add("a computed field");
      return null;
    }
    if (!aggregated || shelf.analyticType !== "measure" || !parsed.aggregate)
      return { field: column };
    if (!includes(CHART_AGGREGATES, parsed.aggregate)) {
      unsupported.add(`a ${parsed.aggregate} aggregate`);
      return null;
    }
    return { field: column, aggregate: parsed.aggregate };
  };

  const request: ChartRequest = { mark: mark as ChartMark };
  for (const channel of REF_CHANNELS) {
    const shelf = chart.encodings[ENCODING_OF[channel]];
    const refs = asList(spec[channel]);
    if (refs.length !== shelf.length) {
      unsupported.add("a part of this chart");
      continue;
    }
    const described = refs.map((ref, i) => describeRef(ref, shelf[i]));
    if (described.length === 0 || described.some((d) => d === null)) continue;
    const list = described as FieldRef[];
    if (channel === "x" || channel === "y") request[channel] = list.length === 1 ? list[0] : list;
    else request[channel] = list[0];
  }

  const ruled = chart.encodings.filters.filter((f) => f.rule);
  const terseFilters = spec.filters ?? [];
  if (terseFilters.length !== ruled.length) unsupported.add("a part of this chart");
  const filters: ChartFilter[] = [];
  for (const filter of terseFilters) {
    const column = columnOf(filter.field);
    if (!column) {
      unsupported.add("a filter on a computed field");
      continue;
    }
    if ("oneOf" in filter) filters.push({ field: column, oneOf: [...filter.oneOf] });
    else if ("notIn" in filter) filters.push({ field: column, notIn: [...filter.notIn] });
    else if ("range" in filter) filters.push({ field: column, range: [...filter.range] });
    else
      filters.push({
        field: column,
        dateRange: [isoDay(filter.timeRange[0]), isoDay(filter.timeRange[1])],
      });
  }
  if (filters.length) request.filters = filters;

  if (!aggregated) request.aggregate = false;
  const stack = chart.layout.stack;
  if ((mark === "bar" || mark === "area") && stack !== "stack") {
    if (includes(CHART_STACKS, stack)) request.stack = stack;
    else unsupported.add(`the "${stack}" stack`);
  }
  const sort = effectiveSort(chart);
  if (sort) request.sort = sort;
  if (chart.config.limit !== undefined && chart.config.limit > 0)
    request.limit = chart.config.limit;
  if (unsupported.size === 0) {
    const kinds = new Map(ctx.fields.map((f) => [f.fid, columnKind(f)]));
    coreRefusals(request, kinds).forEach((phrase) => unsupported.add(phrase));
    // Core reads a scatter of plain numbers with no `aggregate` as raw rows; this one groups them.
    const plainNumbers = (refs: FieldRef[]) =>
      refs.length > 0 && refs.every((r) => isPlain(r) && kinds.get(r.field) === "number");
    if (
      mark === "point" &&
      aggregated &&
      plainNumbers(asList(request.x)) &&
      plainNumbers(asList(request.y))
    ) {
      request.aggregate = true;
    }
  }

  return unsupported.size ? { ok: false, unsupported: [...unsupported] } : { ok: true, request };
}

/**
 * The mark Graphic Walker draws for a chart: its own mark, or for `auto` the one the worksheet
 * renderer picks (`renderer/native/adapter.ts::chartToWorksheet`, mirrored because the package
 * root does not export it; a unit test pins the two together). Null for `auto` on another
 * renderer or with nothing on rows and columns, where nothing names a mark.
 */
export function drawnMark(chart: IChart): string | null {
  const stored = chart.config.geoms[0] ?? "auto";
  if (stored !== "auto") return stored;
  const renderer = chart.layout.renderer;
  if (renderer !== undefined && renderer !== "native") return null;
  const aggregated = chart.config.defaultAggregated;
  // Without aggregation the worksheet leaves aggregate expressions out of every shelf.
  const drawn = (fields: IViewField[]) =>
    aggregated ? fields : fields.filter((f) => f.aggName !== "expr");
  const isMeasure = (f: IViewField) => f.analyticType === "measure";
  const rows = drawn(chart.encodings.rows);
  const columns = drawn(chart.encodings.columns);
  if (rows.length === 0 && columns.length === 0) return null;
  const rowMeasures = rows.some(isMeasure);
  const columnMeasures = columns.some(isMeasure);
  if (rowMeasures && columnMeasures) return "point";
  if (rowMeasures || columnMeasures) {
    const other = rowMeasures ? columns : rows;
    const terminal = other.length > 0 && !other.some(isMeasure) ? other[other.length - 1] : null;
    const timeAxis =
      terminal?.semanticType === "temporal" && (!terminal.computed || Boolean(terminal.timeUnit));
    return timeAxis ? "line" : aggregated ? "bar" : "tick";
  }
  const color = drawn(chart.encodings.color)[0];
  if (drawn(chart.encodings.text)[0]) return "text";
  return color && isMeasure(color) ? "rect" : "point";
}

/** A column as the browser can tell it apart; core's six types collapse into these three. */
export type ColumnKind = "number" | "date" | "text";

const columnKind = (field: IMutField): ColumnKind =>
  field.semanticType === "quantitative"
    ? "number"
    : field.semanticType === "temporal"
      ? "date"
      : "text";

const isPlain = (ref: FieldRef) =>
  ref.aggregate === undefined && ref.bin === undefined && ref.timeUnit === undefined;

const NUMBER_AGGREGATES: readonly string[] = ["sum", "mean", "median", "min", "max"];

/**
 * The parts of a described request that core's `chart_request.check_chart` would reject or
 * rewrite, so a follow-up never sends a `current_chart` core drops. Mirrors its rules on what the
 * browser can tell apart: number, date or text, not integer from decimal. Exported for the shared
 * cases file both suites read (`flowfile_core/tests/ai/chart_request_cases.json`).
 */
export function coreRefusals(request: ChartRequest, kinds: Map<string, ColumnKind>): string[] {
  const found = new Set<string>();
  const { mark } = request;
  const aggregated = request.aggregate !== false;
  const x = asList(request.x);
  const y = asList(request.y);
  const axes = { x, y } as const;
  const others = [request.color, request.size, request.theta].filter(
    (r): r is FieldRef => r !== undefined,
  );
  const all = [...x, ...y, ...others];
  const kindOf = (ref: FieldRef) => (ref.field === "*" ? undefined : kinds.get(ref.field));
  const isMeasure = (key: string, ref: FieldRef) =>
    ref.aggregate !== undefined ||
    (!aggregated &&
      (key === "x" || key === "y" || key === "size") &&
      isPlain(ref) &&
      kindOf(ref) === "number");

  if (all.length === 0) found.add("a chart with no fields");
  for (const ref of all) {
    const kind = kindOf(ref);
    if (ref.aggregate && NUMBER_AGGREGATES.includes(ref.aggregate) && kind !== "number") {
      found.add(`a ${ref.aggregate} of a column that is not a number`);
    }
    if (ref.timeUnit !== undefined && kind !== "date")
      found.add("a date grain on a column that is not a date");
    if (ref.timeUnit !== undefined && ref.aggregate !== undefined)
      found.add("a date grain with an aggregate");
    const binDrawable =
      kind === "number" &&
      ref.aggregate === undefined &&
      ref.timeUnit === undefined &&
      ref.bin !== undefined &&
      ref.bin >= MIN_BINS &&
      ref.bin <= MAX_BINS;
    if (ref.bin !== undefined && !binDrawable) {
      found.add(
        `bins core cannot draw (a number column, ${MIN_BINS} to ${MAX_BINS} bins, no aggregate)`,
      );
    }
  }
  if (request.size && mark !== "point") found.add("a field on size");
  if (request.theta && mark !== "arc") found.add("a field on angle");

  if (mark === "bar" || mark === "line" || mark === "area") {
    const onX = x.some((r) => isMeasure("x", r));
    const onY = y.some((r) => isMeasure("y", r));
    if (onX && onY && aggregated) found.add(`a ${mark} chart with numbers on both axes`);
    if (!onX && !onY) found.add(`a ${mark} chart without a number axis`);
  }
  if (mark !== "table") {
    for (const [key, refs] of Object.entries(axes)) {
      if (refs.filter((r) => isMeasure(key, r)).length > 1)
        found.add("several measures on one axis");
    }
  }
  if (aggregated) {
    const plain = new Set(all.filter((r) => isPlain(r) && r.field !== "*").map((r) => r.field));
    if (all.some((r) => r.aggregate !== undefined && r.field !== "*" && plain.has(r.field))) {
      found.add("a column both grouped and aggregated");
    }
  }
  if (mark === "arc") {
    const { theta, color } = request;
    if (x.length || y.length || !theta?.aggregate || !color || color.aggregate) {
      found.add("a pie that is not one angle and one category");
    }
    if (request.sort || request.limit) found.add("a sorted or limited pie");
  }
  if (mark === "rect") {
    const shaped =
      x.length > 0 &&
      y.length > 0 &&
      [...x, ...y].every((r) => !r.aggregate) &&
      request.color?.aggregate;
    if (!shaped) found.add("a heatmap that is not two categories and an aggregated colour");
  }
  if (mark === "point" && aggregated) {
    const grouped = [...x, ...y, request.color, request.size].some((r) => r && !r.aggregate);
    if (!grouped) found.add("a scatter that draws a single point");
  }
  if (mark === "boxplot") {
    const valueAxis = (refs: FieldRef[]) =>
      refs.length === 1 && isPlain(refs[0]) && kindOf(refs[0]) === "number";
    const categoryAxis = (key: string, refs: FieldRef[]) =>
      refs.length > 0 && refs.every((r) => r.field !== "*" && !isMeasure(key, r));
    const color = request.color;
    const colorOk = !color || (color.field !== "*" && kindOf(color) !== "number");
    const shaped = (valueAxis(y) && categoryAxis("x", x)) || (valueAxis(x) && categoryAxis("y", y));
    if (aggregated) found.add("a box plot of aggregated values");
    else if (!shaped || !colorOk) found.add("a box plot that is not categories against one number");
  }
  if (mark === "table" && !aggregated) found.add("a table of raw rows");
  if (!aggregated && mark !== "boxplot" && mark !== "table" && all.some((r) => r.aggregate)) {
    found.add("a row count in a chart of raw rows");
  }
  if (mark !== "arc") {
    if (request.sort) {
      const key = (["x", "y"] as const).find(
        (k) => axes[k].length > 0 && !axes[k].some((r) => isMeasure(k, r)),
      );
      const last = key ? axes[key][axes[key].length - 1] : undefined;
      if (!last) found.add("a sort with no category axis");
      else if (
        last.timeUnit !== undefined ||
        last.bin !== undefined ||
        kindOf(last) === "date" ||
        (isPlain(last) && kindOf(last) === "number")
      ) {
        found.add("a sort on a time or number axis");
      }
    }
    if (request.limit && aggregated) {
      if (!request.sort) found.add("a limit without a sort");
      const keyed: [string, FieldRef | undefined][] = [
        ...x.map((r): [string, FieldRef] => ["x", r]),
        ...y.map((r): [string, FieldRef] => ["y", r]),
        ["color", request.color],
        ["size", request.size],
      ];
      const dimensions = keyed.filter(([k, r]) => r && !isMeasure(k, r));
      if (dimensions.length > 1) found.add("a limit over several categories");
    }
  }
  for (const filter of request.filters ?? []) {
    const kind = kinds.get(filter.field);
    if ("oneOf" in filter || "notIn" in filter) {
      const values = "oneOf" in filter ? filter.oneOf : filter.notIn;
      if (kind === "date") found.add("a value filter on a date column");
      else if (values.length === 0 || values.some((v) => v === null)) {
        found.add("a value filter on no values or on empty values");
      }
    } else if ("range" in filter) {
      const [low, high] = filter.range;
      if (kind !== "number") found.add("a range filter on a column that is not a number");
      else if ((low === null && high === null) || (low !== null && high !== null && low > high)) {
        found.add("a range filter without a valid range");
      }
    } else {
      const [start, end] = filter.dateRange;
      if (kind !== "date") found.add("a date filter on a column that is not a date");
      else if (
        (start === null && end === null) ||
        (start !== null && end !== null && start > end)
      ) {
        found.add("a date filter without a valid range");
      }
    }
  }
  return [...found];
}

const isoDay = (ms: number | null): string | null =>
  ms === null ? null : new Date(ms).toISOString().slice(0, 10);

/** The order Graphic Walker reads: the last field of the axis that holds no measure (`utils/index.ts::getSort`). */
function effectiveSort(chart: IChart): "ascending" | "descending" | undefined {
  const { rows, columns } = chart.encodings;
  const axis =
    rows.length && !rows.some((f) => f.analyticType === "measure")
      ? rows
      : columns.length && !columns.some((f) => f.analyticType === "measure")
        ? columns
        : [];
  const sort = axis[axis.length - 1]?.sort;
  return sort === "ascending" || sort === "descending" ? sort : undefined;
}

function warningPhrase(warning: string): string {
  if (warning.includes("synthetic field")) return "measure names or values";
  if (warning.includes("(dateTimeFeature)")) return "a date part field";
  if (warning.includes("(paint)")) return "painted marks";
  if (warning.includes("(expr)")) return "a formula field";
  if (/^Filter on .*\(regexp\)/.test(warning)) return "a pattern filter";
  if (warning.startsWith("Display-only timeUnit"))
    return "a date shown by a unit it is not grouped by";
  const channel = /^Channel '(\w+)' has \d+ fields/.exec(warning);
  if (channel) return `several fields on ${channel[1]}`;
  if (warning.includes("cannot be expressed")) return "a computed field";
  return "a part of this chart";
}
