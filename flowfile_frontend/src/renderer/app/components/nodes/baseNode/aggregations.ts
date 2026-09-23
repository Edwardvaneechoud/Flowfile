import type { AggOption } from "./nodeInput";

/** The aggregations the Group By and Pivot nodes offer, with their display labels. */
export const AGGREGATE_OPTIONS: readonly { value: AggOption; label: string }[] = [
  { value: "count", label: "Count" },
  { value: "sum", label: "Sum" },
  { value: "mean", label: "Mean" },
  { value: "median", label: "Median" },
  { value: "min", label: "Min" },
  { value: "max", label: "Max" },
  { value: "n_unique", label: "N unique" },
  { value: "first", label: "First" },
  { value: "last", label: "Last" },
  { value: "concat", label: "Concat" },
];

export const aggLabel = (agg: string): string =>
  agg === "groupby"
    ? "Group by"
    : (AGGREGATE_OPTIONS.find((option) => option.value === agg)?.label ?? agg);
