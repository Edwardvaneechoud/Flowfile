<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, defineExpose, toRaw } from "vue";
import React from "react";
import ReactDOMClient from "react-dom/client";
import {
  GraphicWalker,
  ILocalVizAppProps,
  IRemoteComputationProps,
  IComputationFunction,
} from "@kanaries/graphic-walker";
import type {
  IRow,
  IMutField,
  IChart,
  IGWProps,
  IDataQueryPayload,
} from "@kanaries/graphic-walker/dist/interfaces";
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
let reactRootInstance: ReactDOMClient.Root | null = null;
const internalStoreRef = ref<{ current: VizSpecStore | null }>({ current: null });

type GraphicWalkerCombinedProps = ILocalVizAppProps & IRemoteComputationProps;

const dummyComputation: IComputationFunction = async (
  _payload: IDataQueryPayload,
): Promise<IRow[]> => {
  console.warn(
    "Dummy computation function called. This should not happen when providing local data.",
  );
  return [];
};

const getReactProps = (): GraphicWalkerCombinedProps => {
  const chartSpecArray = props.specList ? toRaw(props.specList) : [];

  const reactProps: GraphicWalkerCombinedProps = {
    // Props from ILocalVizAppProps / IVizAppProps
    data: props.data ? toRaw(props.data) : undefined,
    fields: props.fields ? toRaw(props.fields) : undefined,
    appearance: props.appearance || "light",
    themeKey: props.themeKey,
    storeRef: internalStoreRef.value as unknown as React.RefObject<VizSpecStore | null> | undefined,
    ...(chartSpecArray.length > 0 && { chart: chartSpecArray }),

    computation: dummyComputation, // We do not have compute 
  };

  Object.keys(reactProps).forEach((key) => {
    const propKey = key as keyof GraphicWalkerCombinedProps;
    if (reactProps[propKey] === undefined) {
      delete reactProps[propKey];
    }
  });

  return reactProps;
};

onMounted(() => {
  if (!container.value) {
    console.error("[VueGW] Container element not found for mounting.");
    return;
  }

  try {
    reactRootInstance = ReactDOMClient.createRoot(container.value);
    const componentProps = getReactProps();
    reactRootInstance.render(React.createElement(GraphicWalker, componentProps));
  } catch (e) {
    console.error("[VueGW] Error mounting GraphicWalker:", e);
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
  <div ref="container"></div>
</template>

<style scoped>
div {
  width: 100%;
  min-height: 500px;
  height: 100%;
}
</style>
