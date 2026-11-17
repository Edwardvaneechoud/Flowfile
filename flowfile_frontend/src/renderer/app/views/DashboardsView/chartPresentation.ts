import { format as numberFormat } from "d3-format";
import { utcFormat } from "d3-time-format";

import type { IChart } from "@kanaries/graphic-walker/interfaces";
import type { DashboardChartFormat } from "../../types";

export interface FormatPreset {
  id: string;
  label: string;
  /** The d3 format string; null leaves the chart's own. */
  format: string | null;
}

export const CUSTOM_FORMAT = "custom";

/** d3-format strings an analyst recognises; `~s` writes billions as G, as d3 does. */
export const NUMBER_FORMAT_PRESETS: FormatPreset[] = [
  { id: "auto", label: "Automatic", format: null },
  { id: "thousands", label: "Thousands separators", format: "," },
  { id: "whole", label: "Whole numbers", format: ",.0f" },
  { id: "decimals2", label: "Two decimals", format: ",.2f" },
  { id: "compact", label: "Compact (1.2M)", format: ".3~s" },
  { id: "percent", label: "Percent (0.12 → 12%)", format: ".1%" },
];

/** d3-time-format strings, written in UTC as Graphic Walker hands times over. */
export const DATE_FORMAT_PRESETS: FormatPreset[] = [
  { id: "auto", label: "Automatic", format: null },
  { id: "iso", label: "ISO date (2024-03-31)", format: "%Y-%m-%d" },
  { id: "dmy", label: "Day month year (31 Mar 2024)", format: "%d %b %Y" },
  { id: "my", label: "Month year (Mar 2024)", format: "%b %Y" },
  { id: "year", label: "Year (2024)", format: "%Y" },
];

export const SAMPLE_NUMBER = 1234567.891;
export const SAMPLE_SHARE = 0.1234;
export const SAMPLE_DATE = new Date(Date.UTC(2024, 2, 31, 14, 30));

/** The preset whose string this is, else `custom`; null or blank is the automatic preset. */
export const presetIdFor = (format: string | null | undefined, presets: FormatPreset[]): string => {
  const value = format?.trim() || null;
  return presets.find((p) => p.format === value)?.id ?? CUSTOM_FORMAT;
};

/** Why d3 cannot read this number format, or null when it can (blank counts as automatic). */
export const numberFormatError = (format: string | null | undefined): string | null => {
  const value = format?.trim();
  if (!value) return null;
  try {
    numberFormat(value);
    return null;
  } catch {
    return `"${value}" is not a d3 number format, e.g. ",.2f" or ".3~s".`;
  }
};

/** A date format with no `%` directive would write the same text for every date. */
export const timeFormatError = (format: string | null | undefined): string | null => {
  const value = format?.trim();
  if (!value) return null;
  return /%[a-zA-Z]/.test(value)
    ? null
    : `"${value}" has no d3 directive such as %Y or %b, so every date would read the same.`;
};

/** A value written with a number format, or null when the format is blank or unreadable. */
export const formatSampleNumber = (format: string | null | undefined, value: number) =>
  numberFormatError(format) || !format?.trim() ? null : numberFormat(format.trim())(value);

export const formatSampleDate = (format: string | null | undefined, value: Date) =>
  timeFormatError(format) || !format?.trim() ? null : utcFormat(format.trim())(value);

const clean = (value: string | null | undefined): string | null => value?.trim() || null;

/** The overrides as they are saved: trimmed, blanks as null, and null when nothing is set. */
export const normalizeChartFormat = (
  overrides: DashboardChartFormat | null | undefined,
): DashboardChartFormat | null => {
  if (!overrides) return null;
  const next: DashboardChartFormat = {
    number_format: clean(overrides.number_format),
    time_format: clean(overrides.time_format),
    x_title: clean(overrides.x_title),
    y_title: clean(overrides.y_title),
  };
  return Object.values(next).some((v) => v != null) ? next : null;
};

/** The last field of a shelf is the one Graphic Walker draws as that shelf's axis. */
const withAxisTitle = <T extends { name?: string }>(shelf: T[], title: string | null): T[] => {
  if (!title || shelf.length === 0) return shelf;
  const last = shelf.length - 1;
  return shelf.map((field, i) => (i === last ? { ...field, name: title } : field));
};

/**
 * The chart copy a tile draws: the tile's number and date formats as the chart's
 * `layout.format`, and its axis titles as the display name of the field ending the columns
 * (horizontal) and rows (vertical) shelves. A format d3 cannot read is left out, so the tile keeps
 * drawing. Without overrides the same chart object comes back, untouched.
 */
export const applyTilePresentation = (
  chart: IChart,
  overrides: DashboardChartFormat | null | undefined,
): IChart => {
  const settings = normalizeChartFormat(overrides);
  if (!settings) return chart;
  const number = numberFormatError(settings.number_format) ? null : settings.number_format;
  const time = timeFormatError(settings.time_format) ? null : settings.time_format;
  const { x_title: xTitle, y_title: yTitle } = settings;
  if (!number && !time && !xTitle && !yTitle) return chart;
  const format = { ...(chart.layout?.format ?? {}) };
  if (number) format.numberFormat = number;
  if (time) format.timeFormat = time;
  return {
    ...chart,
    layout: { ...chart.layout, format },
    encodings: {
      ...chart.encodings,
      columns: withAxisTitle(chart.encodings.columns, xTitle ?? null),
      rows: withAxisTitle(chart.encodings.rows, yTitle ?? null),
    },
  };
};
