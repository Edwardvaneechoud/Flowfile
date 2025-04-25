<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, watch, toRaw } from "vue";
import React from "react";
import ReactDOMClient from "react-dom/client";
import { GraphicWalker, ILocalVizAppProps } from "@kanaries/graphic-walker";
import type {
  IRow,
  IMutField,
  IChart,
  IGWProps
} from "@kanaries/graphic-walker/dist/interfaces";

import type {
  VizSpecStore
} from "@kanaries/graphic-walker/dist/store/visualSpecStore"

interface VueGWProps {
  data?: IRow[];
  fields?: IMutField[];
  specList?: IChart[];
  appearance?: IGWProps['appearance'];
  themeKey?: IGWProps['themeKey'];
}

const props = defineProps<VueGWProps>();

const container = ref<HTMLElement | null>(null);
const reactRoot = ref<ReactDOMClient.Root | null>(null);
const internalStoreRef = ref<{ current: VizSpecStore | null }>({ current: null });

const getReactProps = (): any => {
  const chartSpecArray = props.specList ? toRaw(props.specList) : [];

  const reactProps: ILocalVizAppProps = {
    // Base props that are always included
    data: props.data ? toRaw(props.data) : undefined,
    fields: props.fields ? toRaw(props.fields) : undefined,
    appearance: props.appearance || 'light',
    themeKey: props.themeKey,
    storeRef: internalStoreRef.value as unknown as React.RefObject<VizSpecStore | null> | undefined,
    ...(chartSpecArray.length > 0 && { chart: chartSpecArray }),
  };



  Object.keys(reactProps).forEach(key => {
    if (reactProps[key as keyof ILocalVizAppProps] === undefined) {
      delete reactProps[key as keyof ILocalVizAppProps];
    }
  });

  return reactProps;
};

onMounted(() => {
  console.log('props', props)
  if (container.value) {
    try {
      reactRoot.value = ReactDOMClient.createRoot(container.value);
      let  r =  getReactProps()
      console.log(r)
      reactRoot.value.render(React.createElement(GraphicWalker, r));
    } catch (e) {
      console.error("[VueGW] Error mounting GraphicWalker:", e);
    }
  } else {
    console.error("[VueGW] Container element not found for mounting.");
  }
});

onUnmounted(() => {
  if (reactRoot.value) {
    reactRoot.value.unmount();
  }
});

watch(
  [() => props.data, () => props.fields, () => props.specList, () => props.appearance, () => props.themeKey],
  (newValues, oldValues) => {
    if (!reactRoot.value || !container.value) return;
    if (newValues.some((val, i) => val !== oldValues[i])) {
      reactRoot.value.render(React.createElement(GraphicWalker, getReactProps()));
    }
  }
);

const exportCode = async (): Promise<IChart[] | null> => { // Now returns Promise
    console.log("[VueGW] exportCode exposed method called.");
    const storeInstance = internalStoreRef.value?.current;
    console.log("[VueGW] Accessing internalStoreRef.current:", storeInstance);

    try {
        if (storeInstance && typeof storeInstance.exportCode === 'function') {
            console.log("[VueGW] Found exportCode. Calling function...");
            const result = storeInstance.exportCode() ?? [];
             console.log("[VueGW] exportCode call finished. Result:", result);
            return result; 

        } else {
            console.error("[VueGW] exportCode/exportChartList method not available on internal store instance:", storeInstance);
            return null; 
        }
    } catch (error) {
         console.error("[VueGW] Error during exportCode execution:", error);
         return null;
    }
}
defineExpose({
  exportCode
});

</script>

<template>
  <div ref="container"></div>
</template>

<style scoped>
/* Add component-specific styles here if needed */
</style>