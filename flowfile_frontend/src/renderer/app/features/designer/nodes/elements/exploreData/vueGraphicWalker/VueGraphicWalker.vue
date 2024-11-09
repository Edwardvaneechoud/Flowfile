<script setup lang="ts">
import { ref, onMounted, defineProps, onBeforeUpdate, watch, toRaw } from "vue";
import { embedGraphicWalker } from "@kanaries/graphic-walker";
import type {
  IVizAppProps,
  ILocalComputationProps,
} from "@kanaries/graphic-walker";

type ILocalVizAppProps = IVizAppProps & ILocalComputationProps;

const props = defineProps<ILocalVizAppProps>();
const container = ref<HTMLElement | null>(null);

function waitForMethodToBeAvailable(timeout = 10000) {
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
      reject(
        new Error("Timeout waiting for importCode method to be available."),
      );
    }, timeout);
  });
}

async function renderGW() {
  embedGraphicWalker(container.value, {
    ...props,
    fields: toRaw(props.fields),
    data: toRaw(props.data),
  });
  if (props.storeRef) {
    await waitForMethodToBeAvailable();
  }
  if (props.chart && props.storeRef?.current) {
    props.storeRef.current.importCode(props.chart);
  }
}

onMounted(async () => {
  await renderGW();
});

defineExpose({
  waitForMethodToBeAvailable,
});

onBeforeUpdate(async () => {
  renderGW();
});
</script>

<template>
  <div ref="container"></div>
</template>

<style scoped></style>
