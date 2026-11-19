import {
  endOfDay,
  endOfMonth,
  endOfQuarter,
  endOfYear,
  startOfDay,
  startOfMonth,
  startOfQuarter,
  startOfYear,
  subDays,
  subMonths,
} from "date-fns";

/**
 * What a date range filter's state stands for.
 *
 * The state is either a fixed window, `{ start, end }` as ISO strings, or a window named relative
 * to today, `{ preset }`. A named window is worked out when a query is built, so a dashboard saved
 * with "Last 30 days" still shows the last 30 days next month. Days are the viewer's local days.
 */

export type DatePreset =
  | "today"
  | "yesterday"
  | "last_7_days"
  | "last_30_days"
  | "last_90_days"
  | "this_month"
  | "last_month"
  | "this_quarter"
  | "this_year"
  | "year_to_date";

export interface ResolvedDateRange {
  start: string | null;
  end: string | null;
}

const window = (start: Date, end: Date): ResolvedDateRange => ({
  start: start.toISOString(),
  end: end.toISOString(),
});

const lastDays = (n: number) => (now: Date) =>
  window(startOfDay(subDays(now, n - 1)), endOfDay(now));

const PRESET_WINDOWS: Record<DatePreset, (now: Date) => ResolvedDateRange> = {
  today: (now) => window(startOfDay(now), endOfDay(now)),
  yesterday: (now) => window(startOfDay(subDays(now, 1)), endOfDay(subDays(now, 1))),
  last_7_days: lastDays(7),
  last_30_days: lastDays(30),
  last_90_days: lastDays(90),
  this_month: (now) => window(startOfMonth(now), endOfMonth(now)),
  last_month: (now) => window(startOfMonth(subMonths(now, 1)), endOfMonth(subMonths(now, 1))),
  this_quarter: (now) => window(startOfQuarter(now), endOfQuarter(now)),
  this_year: (now) => window(startOfYear(now), endOfYear(now)),
  year_to_date: (now) => window(startOfYear(now), endOfDay(now)),
};

export const DATE_PRESETS: { value: DatePreset; label: string }[] = [
  { value: "today", label: "Today" },
  { value: "yesterday", label: "Yesterday" },
  { value: "last_7_days", label: "Last 7 days" },
  { value: "last_30_days", label: "Last 30 days" },
  { value: "last_90_days", label: "Last 90 days" },
  { value: "this_month", label: "This month" },
  { value: "last_month", label: "Last month" },
  { value: "this_quarter", label: "This quarter" },
  { value: "this_year", label: "This year" },
  { value: "year_to_date", label: "Year to date" },
];

// Own keys only: a state can come from a URL, and "toString" is in every object.
export const isDatePreset = (value: unknown): value is DatePreset =>
  typeof value === "string" && Object.prototype.hasOwnProperty.call(PRESET_WINDOWS, value);

/** The preset a state names, or null for a fixed or an unset window. */
export const presetOf = (state: Record<string, unknown> | undefined): DatePreset | null =>
  isDatePreset(state?.preset) ? state.preset : null;

const bound = (value: unknown): string | null =>
  typeof value === "string" && value ? value : null;

/** The window a state stands for at `now`; null when it sets neither bound. */
export const resolveDateRange = (
  state: Record<string, unknown> | undefined,
  now: Date = new Date(),
): ResolvedDateRange | null => {
  const preset = presetOf(state);
  if (preset) return PRESET_WINDOWS[preset](now);
  const start = bound(state?.start);
  const end = bound(state?.end);
  return start || end ? { start, end } : null;
};
