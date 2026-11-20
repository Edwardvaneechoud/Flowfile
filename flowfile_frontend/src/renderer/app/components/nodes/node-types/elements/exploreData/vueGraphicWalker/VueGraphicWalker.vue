<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, defineEmits, defineExpose, toRaw } from "vue";

import type {
  IRow,
  IMutField,
  IChart,
  IGWProps,
  IGWHandler,
} from "@kanaries/graphic-walker/interfaces";
import type { VizSpecStore } from "@kanaries/graphic-walker/store/visualSpecStore";
import type { GraphicWalkerFunctions } from "@/views/CatalogView/chartRequest";
import { graphicWalkerToolkit } from "@/views/CatalogView/graphicWalkerFunctions";
import { toPlainJson } from "@/utils/structuredClone";
import { flowfileVizTheme } from "./vizTheme";

interface VueGWProps {
  data?: IRow[];
  fields?: IMutField[];
  specList?: IChart[];
  appearance?: IGWProps["appearance"];
  /** Initial tab: "data" or "vis" (default "vis"). */
  defaultTab?: "data" | "vis";
  /** Server-side compute callback; when set, GW aggregates through this instead of in-browser `data`. */
  computation?: (payload: any) => Promise<IRow[]>;
  /** Hide GW's chart-tab bar so the surface is a single chart. */
  hideChartNav?: boolean;
  /** Opt-in: emit "specChanged" when the user edits the chart spec (default false, zero cost when off). */
  watchSpec?: boolean;
}

const props = defineProps<VueGWProps>();

const emit = defineEmits<{ (e: "specChanged"): void }>();

const container = ref<HTMLElement | null>(null);
const isLoading = ref(true);
const loadError = ref<string | null>(null);

let reactRootInstance: any = null;
let React: any = null;
let ReactDOMClient: any = null;
let GraphicWalker: any = null;
let gwFunctions: GraphicWalkerFunctions | null = null;

const internalStoreRef = ref<{ current: VizSpecStore | null }>({ current: null });

let gwHandleRef: { current: IGWHandler | null } | null = null;

const SPEC_POLL_MS = 2000;

let lastEmittedSpec: string | null = null;
let disposeRenderStatus: (() => void) | null = null;
let specArmTimer: ReturnType<typeof setInterval> | null = null;
let specArmBailout: ReturnType<typeof setTimeout> | null = null;
let specPollTimer: ReturnType<typeof setInterval> | null = null;

const maybeEmitSpecChange = () => {
  try {
    const charts = internalStoreRef.value?.current?.exportCode();
    if (!charts) {
      return;
    }
    const spec = JSON.stringify(charts);
    if (lastEmittedSpec === null) {
      // First read is GW's own normalization of the imported spec; baseline only.
      lastEmittedSpec = spec;
      return;
    }
    if (spec !== lastEmittedSpec) {
      lastEmittedSpec = spec;
      emit("specChanged");
    }
  } catch {
    // GW mid-render state; the next poll/render tick retries.
  }
};

const stopSpecWatch = () => {
  if (specArmTimer !== null) {
    clearInterval(specArmTimer);
    specArmTimer = null;
  }
  if (specArmBailout !== null) {
    clearTimeout(specArmBailout);
    specArmBailout = null;
  }
  if (specPollTimer !== null) {
    clearInterval(specPollTimer);
    specPollTimer = null;
  }
  if (disposeRenderStatus) {
    disposeRenderStatus();
    disposeRenderStatus = null;
  }
};

const armSpecWatch = () => {
  // The poll needs only the store, so it works even if the handle never arms.
  specPollTimer = setInterval(maybeEmitSpecChange, SPEC_POLL_MS);
  specArmTimer = setInterval(() => {
    const handle = gwHandleRef?.current;
    if (!handle || typeof handle.onRenderStatusChange !== "function") {
      return;
    }
    if (specArmTimer !== null) {
      clearInterval(specArmTimer);
      specArmTimer = null;
    }
    if (specArmBailout !== null) {
      clearTimeout(specArmBailout);
      specArmBailout = null;
    }
    disposeRenderStatus = handle.onRenderStatusChange(() => maybeEmitSpecChange());
  }, 50);
  specArmBailout = setTimeout(() => {
    if (specArmTimer !== null) {
      clearInterval(specArmTimer);
      specArmTimer = null;
    }
  }, 3000);
};

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
    vizThemeConfig: flowfileVizTheme,
    defaultTab: props.defaultTab === "data" ? "data" : undefined,
    storeRef: internalStoreRef.value,
    hideProfiling: true,
    ...(chartSpecArray.length > 0 && { chart: chartSpecArray }),
    ...(props.hideChartNav && { hideChartNav: true }),
  };

  if (usingComputation) {
    // Omit `data` in compute mode, or GW aggregates in-browser instead.
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
    const [reactModule, reactDomModule, gwModule] = await Promise.all([
      import("react"),
      import("react-dom/client"),
      import("@kanaries/graphic-walker"),
    ]);

    React = reactModule.default;
    ReactDOMClient = reactDomModule;
    GraphicWalker = gwModule.GraphicWalker;
    gwFunctions = graphicWalkerToolkit(gwModule);

    gwHandleRef = React.createRef();
    reactRootInstance = ReactDOMClient.createRoot(container.value);
    const componentProps = getReactProps();
    reactRootInstance.render(
      React.createElement(GraphicWalker, { ...componentProps, ref: gwHandleRef }),
    );
    isLoading.value = false;

    if (props.watchSpec) {
      armSpecWatch();
    }
  } catch (e) {
    console.error("[VueGW] Error mounting GraphicWalker:", e);
    loadError.value = e instanceof Error ? e.message : "Failed to load";
    isLoading.value = false;
  }
});

onUnmounted(() => {
  stopSpecWatch();
  if (reactRootInstance) {
    reactRootInstance.unmount();
    reactRootInstance = null;
  }
});

/** Export the current chart as a base64 PNG data URL (catalog thumbnail). */
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

// The raw store: GW's MobX actions must not run through Vue's reactive proxy.
const gwStore = (): VizSpecStore | null =>
  (toRaw(internalStoreRef.value) as { current: VizSpecStore | null }).current ?? null;

const POOL_KEYS = new Set(["dimensions", "measures"]);
const BUILT_IN_FIDS = new Set(["gw_count_fid", "gw_mea_key_fid", "gw_mea_val_fid"]);

const isBlankChart = (chart: IChart): boolean =>
  Object.entries(chart.encodings).every(
    ([key, fields]) => POOL_KEYS.has(key) || fields.length === 0,
  ) &&
  [...chart.encodings.dimensions, ...chart.encodings.measures].every(
    (f) => !(f.computed || f.expression) || BUILT_IN_FIDS.has(f.fid),
  );

/** Replace the chart on screen as one step Graphic Walker's undo takes back. */
const applyChart = (chart: IChart): boolean => {
  const store = gwStore();
  if (!store) return false;
  try {
    store.applyChart(toRaw(chart));
    return true;
  } catch (error) {
    console.error("[VueGW] applyChart failed:", error);
    return false;
  }
};

/** A plain copy of the chart on screen, or null when the builder holds nothing yet. */
const currentChart = (): IChart | null => {
  const store = gwStore();
  if (!store) return null;
  const chart = toPlainJson(store.currentVis);
  return isBlankChart(chart) ? null : chart;
};

/** A plain copy of the dataset fields the builder's store holds. */
const datasetFields = (): IMutField[] | null => {
  const store = gwStore();
  return store ? toPlainJson(store.meta) : null;
};

/** The Graphic Walker functions of the package this builder loaded, for `chartRequest.ts`. */
const graphicWalker = (): GraphicWalkerFunctions | null => gwFunctions;

defineExpose({
  exportCode,
  exportImage,
  applyChart,
  currentChart,
  datasetFields,
  graphicWalker,
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
  min-height: 0;
}

.gw-wrapper > div {
  width: 100%;
  /* flex item (not height:100%) so GW's chart pane gets a definite box and scrolls internally */
  flex: 1 1 0;
  display: flex;
  flex-direction: column;
  min-width: 0;
  min-height: 0;
  overflow: hidden;
}

/* GW mounts into a shadow root two divs down; force them to inherit height so its chart pane sizes/scrolls */
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
