import type { IChart, IRow } from "@kanaries/graphic-walker/interfaces";

/**
 * How Graphic Walker 0.6 colours a nominal field, and the one way a host can steer it.
 *
 * Both renderers it draws a chart with build the colour domain from the values in the data the
 * chart got: the worksheet renderer sorts them (`worksheet/keys.js::compareValue`) and gives the
 * i-th value the i-th colour of the theme's category range, cycling after its last; Vega-Lite does
 * the same with its own sort. Neither takes a value-to-colour map. The worksheet renderer does read
 * the chart's `layout.colorPalette` as the category range on every render (an array is taken as
 * the colours themselves), so a range ordered like the domain it will build gives each value the
 * colour the host picked, without a new query. Vega-Lite takes an explicit `{domain, range}`
 * through the `scales.color` prop.
 */

/** A host's colours for a colour field's values, in the order given; null keeps Graphic Walker's. */
export type CategoryColors = (field: string, domain: unknown[], dark: boolean) => string[] | null;

const isRange = (value: unknown): value is number[] =>
  Array.isArray(value) && value.length > 0 && value.every((v) => typeof v === "number");

const typed = (value: unknown): [string, unknown] => {
  if (value === null) return ["null", null];
  if (value === undefined) return ["undefined", null];
  if (typeof value === "number" && !Number.isFinite(value)) return ["number", String(value)];
  if (value instanceof Date) return ["time", value.getTime()];
  if (isRange(value)) return ["range", value.join("|")];
  return [typeof value, value];
};

/** The identity Graphic Walker gives a value: 1 and 1.0 are one value, 1 and "1" are two. */
export const colorValueKey = (value: unknown): string => JSON.stringify(typed(value));

const rangeNumber = new Intl.NumberFormat("en", { maximumFractionDigits: 2 });

const displayValue = (value: unknown): string => {
  if (value == null) return "(null)";
  if (isRange(value)) return value.map((v) => rangeNumber.format(v)).join(" – ");
  if (value instanceof Date) {
    return Number.isNaN(value.getTime())
      ? "(invalid date)"
      : value.toISOString().replace(/T00:00:00\.000Z$/, "");
  }
  return String(value);
};

/** Graphic Walker's worksheet order of two values (`worksheet/keys.js::compareValue`). */
export const compareColorValues = (a: unknown, b: unknown): number => {
  if (typeof a === "number" && typeof b === "number") return a - b;
  if (a instanceof Date && b instanceof Date) return a.getTime() - b.getTime();
  if (isRange(a) && isRange(b)) return a[0] - b[0] || (a[1] ?? 0) - (b[1] ?? 0);
  return (
    displayValue(a).localeCompare(displayValue(b), undefined, { numeric: true }) ||
    colorValueKey(a).localeCompare(colorValueKey(b))
  );
};

/** The values of a colour field once each, in the order the worksheet renderer lists them. */
export const colorDomain = (values: Iterable<unknown>): unknown[] => {
  const unique = new Map<string, unknown>();
  for (const value of values) {
    const key = colorValueKey(value);
    if (!unique.has(key)) unique.set(key, value);
  }
  return [...unique.values()].sort(compareColorValues);
};

/**
 * The field a chart colours by value, when the host may colour it: a nominal or ordinal
 * dimension, drawn by the worksheet or Vega-Lite renderer, on a chart that picked no palette.
 */
export const categoryColorField = (chart: IChart | null | undefined): string | null => {
  const field = chart?.encodings?.color?.[0];
  if (!field || field.analyticType !== "dimension") return null;
  if (field.semanticType !== "nominal" && field.semanticType !== "ordinal") return null;
  if (chart.layout?.colorPalette) return null;
  if (chart.config?.coordSystem === "geographic" || chart.config?.geoms?.[0] === "table") {
    return null;
  }
  const renderer = chart.layout?.renderer ?? "native";
  return renderer === "native" || renderer === "vega-lite" ? field.fid : null;
};

/** One query the worksheet sent and what came back. */
export interface ColorQueryResult {
  /** The query carried a row limit: only such queries draw marks when the chart has one. */
  limited: boolean;
  rows: IRow[];
}

/**
 * The domain the worksheet renderer builds from one round of queries. It takes the values from
 * the queries that draw marks: when the chart has a row limit those are exactly the limited ones,
 * since the queries that only rank members are sent without it. A row without the field comes
 * from a ranking query and holds no colour.
 */
export const worksheetColorDomain = (results: ColorQueryResult[], field: string): unknown[] => {
  const drawing = results.some((r) => r.limited) ? results.filter((r) => r.limited) : results;
  const values: unknown[] = [];
  for (const { rows } of drawing) {
    for (const row of rows) {
      if (Object.prototype.hasOwnProperty.call(row, field)) values.push(row[field]);
    }
  }
  return colorDomain(values);
};

/**
 * Follows the queries one worksheet sends to tell the domain it will draw. A round is the set of
 * queries one plan sends; the first query after ``newRound`` (the chart changed) starts the next
 * one, and an answer to a query of an older round tells nothing.
 */
export function createWorksheetColorTracker() {
  let round = 0;
  let batch = { round: -1, results: new Map<string, ColorQueryResult>() };
  return {
    newRound(): void {
      round += 1;
    },
    /** Notes a query as sent; the function it returns takes the rows and answers the domain so
     * far, or null once a newer round started. */
    sent(payload: any, field: string): (rows: IRow[]) => unknown[] | null {
      if (batch.round !== round) batch = { round, results: new Map() };
      const mine = batch;
      const key = JSON.stringify(payload ?? null);
      return (rows) => {
        if (batch !== mine) return null;
        mine.results.set(key, { limited: payload?.limit != null, rows });
        return worksheetColorDomain([...mine.results.values()], field);
      };
    },
  };
}

/** The chart with ``colors`` as its category range, for the worksheet renderer. */
export const withCategoryRange = (chart: IChart, colors: string[] | null): IChart =>
  colors ? { ...chart, layout: { ...chart.layout, colorPalette: colors as any } } : chart;
