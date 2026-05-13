import type { DashboardFilterKind } from "../types";

const NUMERIC_RE = /^(int|uint|float|decimal)/i;
const TEMPORAL_RE = /^(date|datetime|time|duration|timestamp)/i;

export const isNumericDtype = (dtype: string | null | undefined): boolean =>
  !!dtype && NUMERIC_RE.test(dtype.trim());

export const isTemporalDtype = (dtype: string | null | undefined): boolean =>
  !!dtype && TEMPORAL_RE.test(dtype.trim());

export const dtypeToDefaultFilterKind = (
  dtype: string | null | undefined,
): DashboardFilterKind => {
  if (isNumericDtype(dtype)) return "numeric_range";
  if (isTemporalDtype(dtype)) return "date_range";
  return "categorical";
};
