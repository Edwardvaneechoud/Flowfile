<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, defineExpose, toRaw } from "vue";

// Types only - these don't add to bundle size
import type { IRow, IMutField, IChart, IGWProps } from "@kanaries/graphic-walker/dist/interfaces";
import type { VizSpecStore } from "@kanaries/graphic-walker/dist/store/visualSpecStore";

interface VueGWProps {
  data?: IRow[];
  fields?: IMutField[];
  specList?: IChart[];
  appearance?: IGWProps["appearance"];
  themeKey?: IGWProps["themeKey"];
}

const props = defineProps<VueGWProps>();

const container = ref<HTMLElement | null>(null);
const isLoading = ref(true);
const loadError = ref<string | null>(null);

// These will be set after dynamic import
let reactRootInstance: any = null;
let React: any = null;
let ReactDOMClient: any = null;
let GraphicWalker: any = null;

const internalStoreRef = ref<{ current: VizSpecStore | null }>({ current: null });

const dummyComputation = async (): Promise<IRow[]> => {
  console.warn(
    "Dummy computation function called. This should not happen when providing local data.",
  );
  return [];
};

const getReactProps = () => {
  const chartSpecArray = props.specList ? toRaw(props.specList) : [];

  const reactProps: Record<string, any> = {
    data: props.data ? toRaw(props.data) : undefined,
    fields: props.fields ? toRaw(props.fields) : undefined,
    appearance: props.appearance || "light",
    themeKey: props.themeKey,
    storeRef: internalStoreRef.value,
    ...(chartSpecArray.length > 0 && { chart: chartSpecArray }),
    computation: dummyComputation,
  };

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

    reactRootInstance = ReactDOMClient.createRoot(container.value);
    const componentProps = getReactProps();
    reactRootInstance.render(React.createElement(GraphicWalker, componentProps));
    isLoading.value = false;
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
  min-height: 500px;
  height: 100%;
}

.gw-wrapper > div {
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
}

.error {
  color: var(--el-color-danger, #f56c6c);
}
</style>
