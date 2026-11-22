import type { GWGlobalConfig } from "@kanaries/graphic-walker/vis/theme";

/**
 * The chart theme every Graphic Walker surface gets, so a chart looks the same in the builder and
 * on a dashboard. Canvas marks cannot read CSS variables, so the values mirror the tokens in
 * `styles/base/_variables.css` by hand. The categorical slots are a fixed order checked for
 * colour-vision separation on both surfaces (#ffffff and #1a1a2e); a new series takes the next
 * slot and the order is never reshuffled.
 */

const FONT =
  '"Roboto", "Source Sans Pro", -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif';

export const CATEGORY_LIGHT = [
  "#2a78d6",
  "#eb6834",
  "#1baf7a",
  "#eda100",
  "#e87ba4",
  "#008300",
  "#6250d6",
  "#e34948",
];

export const CATEGORY_DARK = [
  "#3987e5",
  "#d95926",
  "#199e70",
  "#c98500",
  "#d55181",
  "#0ca30c",
  "#9085e9",
  "#e66767",
];

// One hue, light to dark, for magnitude.
const RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"];

interface Ink {
  primary: string;
  title: string;
  label: string;
  grid: string;
  rule: string;
  frame: string;
}

const LIGHT_INK: Ink = {
  primary: "#1a1a2e",
  title: "#4a5568",
  label: "#718096",
  grid: "#edf2f7",
  rule: "#cbd5e0",
  frame: "#e2e8f0",
};

const DARK_INK: Ink = {
  primary: "#f1f5f9",
  title: "#cbd5e1",
  label: "#94a3b8",
  grid: "#1e293b",
  rule: "#475569",
  frame: "#334155",
};

const themeFor = (category: string[], ramp: string[], ink: Ink) => {
  const mark = category[0];
  return {
    background: "transparent",
    font: FONT,
    range: { category, ramp, heatmap: ramp },
    area: { fill: mark },
    bar: { fill: mark, cornerRadiusEnd: 2 },
    circle: { fill: mark },
    line: { stroke: mark, strokeWidth: 2 },
    point: { stroke: mark },
    rect: { fill: mark },
    tick: { fill: mark },
    arc: { fill: mark },
    boxplot: { fill: mark, ticks: { fill: ink.rule }, rule: { color: ink.rule } },
    errorbar: { stroke: mark },
    errorband: { fill: mark },
    text: { color: ink.primary },
    view: { stroke: ink.frame },
    axis: {
      labelColor: ink.label,
      titleColor: ink.title,
      labelFontSize: 11,
      titleFontSize: 11,
      titleFontWeight: 500,
      gridColor: ink.grid,
      domainColor: ink.rule,
      tickColor: ink.rule,
    },
    legend: {
      labelColor: ink.title,
      titleColor: ink.title,
      labelFontSize: 11,
      titleFontSize: 11,
      titleFontWeight: 500,
    },
    header: {
      labelColor: ink.primary,
      titleColor: ink.title,
      labelFontSize: 11,
      titleFontSize: 11,
      titleFontWeight: 500,
    },
  };
};

export const flowfileVizTheme = {
  light: themeFor(CATEGORY_LIGHT, RAMP, LIGHT_INK),
  dark: themeFor(CATEGORY_DARK, [...RAMP].reverse(), DARK_INK),
} as GWGlobalConfig;
