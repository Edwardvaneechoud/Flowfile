<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, defineExpose, toRaw } from "vue";

// Types only - these don't add to bundle size
import type {
  IRow,
  IMutField,
  IChart,
  IGWProps,
  IGWHandler,
} from "@kanaries/graphic-walker/interfaces";
import { ISegmentKey } from "@kanaries/graphic-walker/interfaces";
import type { VizSpecStore } from "@kanaries/graphic-walker/store/visualSpecStore";

interface VueGWProps {
  data?: IRow[];
  fields?: IMutField[];
  specList?: IChart[];
  appearance?: IGWProps["appearance"];
  themeKey?: IGWProps["themeKey"];
  /** Which segment tab to show initially: "data" or "vis" (default: "vis") */
  defaultTab?: "data" | "vis";
  /**
   * Server-side compute callback. When provided, every chart aggregation
   * GraphicWalker performs is forwarded here as an IDataQueryPayload —
   * this is the polars-gw walk() pattern. The caller should POST the
   * payload to a backend endpoint (e.g. /catalog/visualizations/compute)
   * and return the resulting rows.
   *
   * If both ``data`` and ``computation`` are provided, ``computation``
   * wins and ``data`` is ignored — GW will not need it.
   */
  computation?: (payload: any) => Promise<IRow[]>;
}

const props = defineProps<VueGWProps>();

const container = ref<HTMLElement | null>(null);
const isLoading = ref(true);
const loadError = ref<string | null>(null);

let reactRootInstance: any = null;
let React: any = null;
let ReactDOMClient: any = null;
let GraphicWalker: any = null;

const internalStoreRef = ref<{ current: VizSpecStore | null }>({ current: null });

// Handle to GraphicWalker's forwarded ref. Holds the IGWHandlerInsider methods
// (exportChart, exportChartList, renderStatus, …). Allocated lazily after the
// React import so we can use React.createRef from the dynamically loaded module.
let gwHandleRef: { current: IGWHandler | null } | null = null;

const dummyComputation = async (): Promise<IRow[]> => {
  console.warn(
    "Dummy computation function called. This should not happen when providing local data.",
  );
  return [];
};

const getReactProps = () => {
  const chartSpecArray = props.specList ? toRaw(props.specList) : [];
  const usingComputation = typeof props.computation === "function";

  const reactProps: Record<string, any> = {
    fields: props.fields ? toRaw(props.fields) : undefined,
    appearance: props.appearance || "light",
    themeKey: props.themeKey,
    storeRef: internalStoreRef.value,
    hideProfiling: true,
    ...(chartSpecArray.length > 0 && { chart: chartSpecArray }),
  };

  if (usingComputation) {
    // Server-side compute path: GW fires one fetch per aggregation. We must
    // not pass ``data`` — GW would otherwise short-circuit and aggregate in
    // the browser.
    reactProps.computation = props.computation;
  } else {
    reactProps.data = props.data ? toRaw(props.data) : undefined;
    reactProps.computation = dummyComputation;
  }

  Object.keys(reactProps).forEach((key) => {
    if (reactProps[key] === undefined) {
      delete reactProps[key];
    }
  });

  return reactProps;
};

onMounted(async () => {
  if (!container.value) {
    console.error("[VueGW] Container element not found for mounting.");
    loadError.value = "Container not found";
    isLoading.value = false;
    return;
  }

  try {
    // Dynamic imports - only loaded when component mounts
    const [reactModule, reactDomModule, gwModule] = await Promise.all([
      import("react"),
      import("react-dom/client"),
      import("@kanaries/graphic-walker"),
    ]);

    React = reactModule.default;
    ReactDOMClient = reactDomModule;
    GraphicWalker = gwModule.GraphicWalker;

    gwHandleRef = React.createRef();
    reactRootInstance = ReactDOMClient.createRoot(container.value);
    const componentProps = getReactProps();
    reactRootInstance.render(
      React.createElement(GraphicWalker, { ...componentProps, ref: gwHandleRef }),
    );
    isLoading.value = false;

    if (props.defaultTab) {
      const tab = props.defaultTab;
      const checkStore = setInterval(() => {
        const store = internalStoreRef.value?.current;
        if (store && typeof store.setSegmentKey === "function") {
          store.setSegmentKey(tab as ISegmentKey);
          clearInterval(checkStore);
        }
      }, 50);
      setTimeout(() => clearInterval(checkStore), 3000);
    }
  } catch (e) {
    console.error("[VueGW] Error mounting GraphicWalker:", e);
    loadError.value = e instanceof Error ? e.message : "Failed to load";
    isLoading.value = false;
  }
});

onUnmounted(() => {
  if (reactRootInstance) {
    reactRootInstance.unmount();
    reactRootInstance = null;
  }
});

/**
 * Capture the currently visible chart as a base64 PNG data URL via
 * GraphicWalker's exportChart('data-url'). Returns null when the chart
 * isn't ready (no fields, render error, or unmounted). The catalog uses
 * this as a static thumbnail so list views don't need to remount GW.
 */
const exportImage = async (): Promise<string | null> => {
  const handle = gwHandleRef?.current;
  if (!handle || typeof handle.exportChart !== "function") {
    return null;
  }
  try {
    const result = await handle.exportChart("data-url");
    const first = result?.charts?.[0]?.data;
    return typeof first === "string" && first.startsWith("data:image/") ? first : null;
  } catch (error) {
    console.error("[VueGW] exportImage failed:", error);
    return null;
  }
};

const exportCode = async (): Promise<IChart[] | null> => {
  const storeInstance = internalStoreRef.value?.current;
  if (!storeInstance) {
    console.error(
      "[VueGW] Cannot export code: Store instance is not available.",
      internalStoreRef.value,
    );
    return null;
  }
  if (typeof storeInstance.exportCode !== "function") {
    console.error(
      "[VueGW] Cannot export code: 'exportCode' method not found on store instance.",
      storeInstance,
    );
    return null;
  }
  try {
    const result = await storeInstance.exportCode();
    return result ?? [];
  } catch (error) {
    console.error("[VueGW] Error during exportCode execution:", error);
    return null;
  }
};

defineExpose({
  exportCode,
  exportImage,
});
</script>

<template>
  <div class="gw-wrapper">
    <div v-if="isLoading" class="loading">Loading visualization...</div>
    <div v-else-if="loadError" class="error">{{ loadError }}</div>
    <div v-show="!isLoading && !loadError" ref="container"></div>
  </div>
</template>

<style scoped>
.gw-wrapper {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  /* Was `min-height: 500px` — that floor pushed the wrapper past its
     constraint container on tighter layouts (small dialogs, narrow viewports),
     causing graphic-walker's internal flex-1 chart pane to mis-size and the
     parent scroll layer to take the chart's overflow instead of GW's own
     `.overflow-auto`. Dropping the floor lets GW lay out at the size we give
     it, and its built-in scroll handles oversized charts internally. */
  min-height: 0;
}

.gw-wrapper > div {
  width: 100%;
  /* React mount must be a deterministic flex item, not `height: 100%`.
     With `height: 100%`, GW's `flex-1` Tailwind root only resolves to a
     definite height when the percentage chain propagates cleanly through
     every ancestor — under .viz-scroll-area's flex layout that fails on
     some renders, so GW root sizes to its content (encoding controls +
     2900px canvas) and overflows past .gw-wrapper (default overflow:
     visible) until it hits .viz-scroll-area's clip. With `flex: 1 1 0`
     plus `overflow: hidden` here, GW's chart area is forced into a
     definite box, engaging its native `.overflow-auto` for chart scroll. */
  flex: 1 1 0;
  display: flex;
  flex-direction: column;
  min-width: 0;
  min-height: 0;
  overflow: hidden;
}

/* GW renders its React tree into a shadow root that lives TWO div levels
   below our React mount (React-rendered intermediate wrapper, then the
   shadow-host div). Those intermediates have no Vue data-v attribute and
   no height/flex constraints, so without :deep() rules they size to GW's
   natural content height (~1495px) instead of inheriting our React mount's
   constrained box. That in turn leaves GW's `.App.h-full` inside the shadow
   root resolving to an indefinite parent, so GW's own `flex-1 min-h-0`
   chart pane never gets a definite height and its native `.overflow-auto`
   never engages — the chart silently overflows and is clipped at the React
   mount. Force both intermediates to inherit the height. */
.gw-wrapper > div :deep(> div),
.gw-wrapper > div :deep(> div > div) {
  height: 100%;
  width: 100%;
  min-height: 0;
  min-width: 0;
}

.loading,
.error {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--el-text-color-secondary, #909399);
}

.error {
  color: var(--el-color-danger, #f56c6c);
}
</style>
