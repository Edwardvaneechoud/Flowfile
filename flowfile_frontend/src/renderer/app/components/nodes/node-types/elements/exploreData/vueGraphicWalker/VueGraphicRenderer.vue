<script setup lang="ts">
import { onMounted, onUnmounted, ref, toRaw, watch } from "vue";

import type { IChart, IGWProps, IRow, IUIThemeConfig } from "@kanaries/graphic-walker/interfaces";
import { flowfileVizTheme } from "./vizTheme";

/**
 * A read-only chart that fills its box.
 *
 * Graphic Walker draws a chart with the renderer its spec names. Its worksheet renderer ("native",
 * the default) lives inside `GraphicRenderer` and takes its height from the box that component is
 * given, so the box is measured here and handed over. A chart on another renderer, a map or a
 * table goes through `PureRenderer`, which draws only the chart and fills its parent by itself.
 */
interface VueGRProps {
  chart: IChart;
  appearance?: IGWProps["appearance"];
  computation: (payload: any) => Promise<IRow[]>;
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

const syncChart = () => {
  const raw = toRaw(props.chart);
  const surface = surfaceColour();
  const background = raw.layout?.background || surface;
  chartRaw = background ? { ...raw, layout: { ...raw.layout, background } } : raw;
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

const renderReact = () => {
  if (!reactRootInstance || !React || !chartRaw) return;
  const shared = {
    ref: handle,
    computation: props.computation,
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
    syncChart();
    renderReact();
  },
  { deep: true },
);

// A theme switch changes the surface behind the chart, so the chart is read again.
watch(
  () => props.appearance,
  () => {
    syncChart();
    renderReact();
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
