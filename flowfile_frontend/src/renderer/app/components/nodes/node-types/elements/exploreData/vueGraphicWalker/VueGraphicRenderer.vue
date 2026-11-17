<script setup lang="ts">
import { onMounted, onUnmounted, ref, toRaw, watch } from "vue";

import type {
  IChart,
  IFieldInfos,
  IGWProps,
  IRow,
  IUIThemeConfig,
} from "@kanaries/graphic-walker/interfaces";
import {
  categoryColorField,
  colorDomain,
  createWorksheetColorTracker,
  withCategoryRange,
  type CategoryColors,
} from "./chartColors";
import { flowfileVizTheme } from "./vizTheme";

/**
 * A read-only chart that fills its box.
 *
 * Graphic Walker draws a chart with the renderer its spec names. Its worksheet renderer ("native",
 * the default) lives inside `GraphicRenderer` and takes its height from the box that component is
 * given, so the box is measured here and handed over. A chart on another renderer, a map or a
 * table goes through `PureRenderer`, which draws only the chart and fills its parent by itself.
 *
 * With `categoryColors` the host picks the colour of each value of a nominal colour field (see
 * `chartColors.ts`): the worksheet's queries are watched for the values it will draw and the
 * matching range is in place before it gets the rows; Vega-Lite gets an explicit scale.
 */
interface VueGRProps {
  chart: IChart;
  appearance?: IGWProps["appearance"];
  computation: (payload: any) => Promise<IRow[]>;
  categoryColors?: CategoryColors;
}

const props = defineProps<VueGRProps>();

const wrapper = ref<HTMLElement | null>(null);
const container = ref<HTMLElement | null>(null);
const isLoading = ref(true);
const loadError = ref<string | null>(null);

let reactRootInstance: any = null;
let React: any = null;
let GraphicRenderer: any = null;
let PureRenderer: any = null;
let handle: { current: any } | null = null;
let resizeObserver: ResizeObserver | null = null;
let resizeTimer: ReturnType<typeof setTimeout> | null = null;
let size = { width: 0, height: 0 };

// Graphic Walker refetches when these change identity, so one copy is kept per chart prop.
let chartRaw: IChart | null = null;
let chartList: IChart[] = [];
let fieldList: unknown[] = [];

/**
 * The first opaque background behind the chart. Graphic Walker paints its own surface, which in
 * the dark theme is darker than a tile; a chart that sets no background gets the tile's instead.
 */
const surfaceColour = (): string | undefined => {
  for (let el: HTMLElement | null = wrapper.value; el; el = el.parentElement) {
    const colour = getComputedStyle(el).backgroundColor;
    const alpha = colour.startsWith("rgba(") ? Number(colour.slice(5, -1).split(",")[3]) : 1;
    if (colour !== "transparent" && alpha > 0.9) return colour;
  }
  return undefined;
};

// Graphic Walker's own shell around the chart: its stock colours with the tile's background.
let stockTheme: IUIThemeConfig | undefined;
let shellTheme: IUIThemeConfig | undefined;

// Graphic Walker's theme parser reads hex, not the rgb() a computed style gives.
const toHex = (colour: string): string | undefined => {
  const channels = colour.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/);
  if (!channels) return undefined;
  return `#${channels
    .slice(1, 4)
    .map((n) => Number(n).toString(16).padStart(2, "0"))
    .join("")}`;
};

// The colour field the host colours, the domain the worksheet last drew it with and its range.
let colorField: string | null = null;
let drawnDomain: unknown[] | null = null;
let colorRange: string[] | null = null;

const isDark = (): boolean =>
  props.appearance === "dark" ||
  (props.appearance === "media" && !!window.matchMedia?.("(prefers-color-scheme: dark)").matches);

/** Works the range out again from the last domain; true when it changed. */
const refreshRange = (): boolean => {
  const next =
    colorField && drawnDomain && props.categoryColors
      ? props.categoryColors(colorField, drawnDomain, isDark())
      : null;
  if (JSON.stringify(next) === JSON.stringify(colorRange)) return false;
  colorRange = next;
  return true;
};

const syncChart = () => {
  const raw = toRaw(props.chart);
  const field = categoryColorField(raw);
  if (field !== colorField) {
    colorField = field;
    drawnDomain = null;
    colorRange = null;
  }
  const surface = surfaceColour();
  const background = raw.layout?.background || surface;
  chartRaw = background ? { ...raw, layout: { ...raw.layout, background } } : raw;
  if (drawnByWorksheet(chartRaw)) chartRaw = withCategoryRange(chartRaw, colorRange);
  const shell = surface ? toHex(surface) : undefined;
  shellTheme =
    shell && stockTheme
      ? {
          light: { ...stockTheme.light, background: shell },
          dark: { ...stockTheme.dark, background: shell },
        }
      : undefined;
  chartList = [chartRaw];
  fieldList = [...(chartRaw.encodings?.dimensions ?? []), ...(chartRaw.encodings?.measures ?? [])];
};

/** The same split Graphic Walker's own `PureRenderer` makes. */
const drawnByWorksheet = (chart: IChart): boolean => {
  const dedicated =
    chart.config?.coordSystem === "geographic" || chart.config?.geoms?.[0] === "table";
  return !dedicated && (chart.layout?.renderer ?? "native") === "native";
};

// The worksheet draws once its last query is answered and its theme follows the chart a render
// later, so a new range is given a moment to land before the rows go back.
const rangeLanded = () =>
  new Promise<void>((resolve) => {
    let done = false;
    const finish = () => {
      if (done) return;
      done = true;
      resolve();
    };
    requestAnimationFrame(() => requestAnimationFrame(finish));
    setTimeout(finish, 100);
  });

let tracker = createWorksheetColorTracker();
let trackedFor: VueGRProps["computation"] | null = null;
let tracked: VueGRProps["computation"] | null = null;

const trackColors = (compute: VueGRProps["computation"]): VueGRProps["computation"] => {
  const own = createWorksheetColorTracker();
  tracker = own;
  return async (payload) => {
    const watched = colorField && chartRaw && drawnByWorksheet(chartRaw);
    const record = watched ? own.sent(payload, colorField!) : null;
    const rows = await compute(payload);
    const domain = record?.(rows);
    if (domain && tracker === own) {
      drawnDomain = domain;
      if (refreshRange()) {
        syncChart();
        renderReact();
        await rangeLanded();
      }
    }
    return rows;
  };
};

// One function per computation prop, since Graphic Walker queries again when it changes.
const graphicWalkerComputation = (): VueGRProps["computation"] => {
  if (!props.categoryColors) return props.computation;
  if (trackedFor !== props.computation || !tracked) {
    trackedFor = props.computation;
    tracked = trackColors(props.computation);
  }
  return tracked;
};

/** Vega-Lite's colour scale for the host's colours, built from the values it was given. */
const vegaScales = () => {
  const field = colorField;
  const colorsOf = props.categoryColors;
  if (!field || !colorsOf) return undefined;
  return {
    color: (info: IFieldInfos) => {
      if (info.semanticType !== "nominal" && info.semanticType !== "ordinal") return undefined;
      const domain = colorDomain(info.values);
      const range = colorsOf(field, domain, info.theme === "dark");
      return range ? { domain: domain as string[], range } : undefined;
    },
  };
};

const renderReact = () => {
  if (!reactRootInstance || !React || !chartRaw) return;
  const shared = {
    ref: handle,
    computation: graphicWalkerComputation(),
    appearance: props.appearance || "light",
    vizThemeConfig: flowfileVizTheme,
    uiTheme: shellTheme,
    overrideSize: { mode: "full", ...size },
  };
  if (drawnByWorksheet(chartRaw)) {
    reactRootInstance.render(
      React.createElement(GraphicRenderer, {
        ...shared,
        chart: chartList,
        fields: fieldList,
        containerStyle: { width: `${size.width}px`, height: `${size.height}px` },
      }),
    );
    return;
  }
  reactRootInstance.render(
    React.createElement(PureRenderer, {
      ...shared,
      type: "remote",
      name: chartRaw.name,
      visualState: chartRaw.encodings,
      visualConfig: chartRaw.config,
      visualLayout: chartRaw.layout,
      scales: vegaScales(),
    }),
  );
};

const measure = (): boolean => {
  if (!wrapper.value) return false;
  const rect = wrapper.value.getBoundingClientRect();
  const next = {
    width: Math.max(1, Math.floor(rect.width)),
    height: Math.max(1, Math.floor(rect.height)),
  };
  if (Math.abs(next.width - size.width) < 2 && Math.abs(next.height - size.height) < 2)
    return false;
  size = next;
  return true;
};

const scheduleRender = () => {
  if (resizeTimer) clearTimeout(resizeTimer);
  resizeTimer = setTimeout(() => {
    resizeTimer = null;
    renderReact();
  }, 80);
};

onMounted(async () => {
  if (!container.value || !wrapper.value) {
    loadError.value = "Container not found";
    isLoading.value = false;
    return;
  }
  try {
    const [reactModule, reactDomModule, gwModule] = await Promise.all([
      import("react"),
      import("react-dom/client"),
      import("@kanaries/graphic-walker"),
    ]);
    // The component may have unmounted while the chunks loaded.
    if (!container.value || !wrapper.value) return;
    React = reactModule.default;
    GraphicRenderer = gwModule.GraphicRenderer;
    PureRenderer = gwModule.PureRenderer;
    stockTheme = gwModule.zincTheme;
    handle = React.createRef();
    measure();
    syncChart();
    reactRootInstance = reactDomModule.createRoot(container.value);
    renderReact();
    isLoading.value = false;

    resizeObserver = new ResizeObserver(() => {
      if (measure()) scheduleRender();
    });
    resizeObserver.observe(wrapper.value);
  } catch (e) {
    console.error("[VueGR] Error mounting the chart:", e);
    loadError.value = e instanceof Error ? e.message : "Failed to load";
    isLoading.value = false;
  }
});

watch(
  () => props.chart,
  () => {
    tracker.newRound();
    syncChart();
    if (refreshRange()) syncChart();
    renderReact();
  },
  { deep: true },
);

// A theme switch changes the surface behind the chart, so the chart is read again.
watch(
  () => props.appearance,
  () => {
    refreshRange();
    syncChart();
    renderReact();
  },
);

// The host's colours changed (a value was given one, or its stored ones were replaced).
watch(
  () => props.categoryColors,
  () => {
    if (!colorField || !chartRaw) return;
    const changed = refreshRange();
    if (changed) syncChart();
    if (changed || !drawnByWorksheet(chartRaw)) renderReact();
  },
);

watch(() => props.computation, renderReact);

onUnmounted(() => {
  if (resizeTimer) clearTimeout(resizeTimer);
  resizeObserver?.disconnect();
  resizeObserver = null;
  if (reactRootInstance) {
    reactRootInstance.unmount();
    reactRootInstance = null;
  }
});

/** The drawn chart as a PNG data URL, or null while nothing is drawn. */
const exportImage = async (): Promise<string | null> => {
  const current = handle?.current;
  if (!current) return null;
  try {
    const image =
      typeof current.exportChart === "function"
        ? (await current.exportChart("data-url"))?.charts?.[0]?.data
        : (await current.getCanvasData?.())?.[0];
    return typeof image === "string" && image.startsWith("data:image/") ? image : null;
  } catch {
    // Not drawn yet, or the renderer has no picture to give.
    return null;
  }
};

defineExpose({ exportImage });
</script>

<template>
  <div ref="wrapper" class="gr-wrapper">
    <div v-if="isLoading" class="loading">Loading chart…</div>
    <div v-else-if="loadError" class="error">{{ loadError }}</div>
    <div v-show="!isLoading && !loadError" ref="container" class="gr-container"></div>
  </div>
</template>

<style scoped>
.gr-wrapper {
  width: 100%;
  height: 100%;
  position: relative;
}
.gr-container {
  width: 100%;
  height: 100%;
}
.loading,
.error {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--el-text-color-secondary, #909399);
  font-size: 12px;
}
.error {
  color: var(--el-color-danger, #f56c6c);
}
</style>
