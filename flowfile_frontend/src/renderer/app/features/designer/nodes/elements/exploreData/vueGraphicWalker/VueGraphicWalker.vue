<script setup lang="ts">
import { ref, onMounted, defineProps, onBeforeUpdate, watch, toRaw } from "vue";
import { embedGraphicWalker } from "@kanaries/graphic-walker";
import type { IVizAppProps, ILocalComputationProps } from "@kanaries/graphic-walker";

type ILocalVizAppProps = IVizAppProps & ILocalComputationProps;

const props = defineProps<ILocalVizAppProps>();
const container = ref<HTMLElement | null>(null);

// Create a safe copy of data that can be properly cloned by the worker
function makeSafeForWorker<T>(obj: T): T {
  return JSON.parse(JSON.stringify(obj)) as T;
}

function waitForMethodToBeAvailable(timeout = 10000): Promise<void> {
  if (props.storeRef?.current) {
    return Promise.resolve();
  }
  console.log("Waiting for method to be available.");
  return new Promise<void>((resolve, reject) => {
    const unwatch = watch(
      () => props.storeRef?.current && props.storeRef.current.importCode,
      (newVal) => {
        if (typeof newVal === "function") {
          console.log("Method available.");
          unwatch();
          clearTimeout(timeoutId); // Clear the timeout
          resolve();
        }
      },
      {
        immediate: true,
      },
    );

    const timeoutId = setTimeout(() => {
      console.log("Timeout reached without finding importCode method.");
      unwatch(); // Ensure to clean up even in the timeout case
      reject(new Error("Timeout waiting for importCode method to be available."));
    }, timeout);
  });
}

// Import chart data safely, handling any serialization issues
function safeImportCode<T>(chart: T): boolean {
  try {
    // Make a clone that's safe for the worker
    const safeChart = makeSafeForWorker(chart);
    if (props.storeRef?.current) {
      props.storeRef.current.importCode(safeChart);
      return true;
    } else {
      console.error("Store reference is not available");
      return false;
    }
  } catch (error) {
    console.error("Error importing chart:", error);
    return false;
  }
}

async function renderGW(): Promise<void> {
  try {
    if (!container.value) {
      console.error("Container element is not available");
      return;
    }
    
    // Create safe copies of the data and fields
    const safeFields = makeSafeForWorker(toRaw(props.fields || []));
    const safeData = makeSafeForWorker(toRaw(props.data || []));
    
    embedGraphicWalker(container.value, {
      ...props,
      fields: safeFields,
      data: safeData,
    });
    
    if (props.storeRef) {
      await waitForMethodToBeAvailable();
    }
    
    if (props.chart && props.storeRef?.current) {
      safeImportCode(props.chart);
    }
  } catch (error) {
    console.error("Error in renderGW:", error);
  }
}

onMounted(async () => {
  await renderGW();
});

defineExpose({
  waitForMethodToBeAvailable,
  safeImportCode, // Expose the safe import method for external use
});

onBeforeUpdate(async () => {
  await renderGW();
});
</script>

<template>
  <div ref="container"></div>
</template>

<style scoped></style>