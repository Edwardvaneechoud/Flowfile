<template>
  <node-button
    ref="nodeButton"
    :node-id="nodeId"
    :image-src="imageSrc"
    :title="nodeTitle"
    :drawer-component="drawerComponent"
    :node-title-info="nodeTitleInfo"
  />
</template>

<script setup lang="ts">
import {
  markRaw,
  computed,
  defineAsyncComponent,
  onErrorCaptured,
  shallowRef,
  onMounted,
} from "vue";
import NodeButton from "./baseNode/nodeButton.vue";
import { toTitleCase } from "../../views/DesignerView/utils";
import type { NodeTemplate } from "../../types";

const drawerModules = import.meta.glob("./node-types/elements/**/*.vue");

interface Props {
  nodeId: number;
  nodeData: NodeTemplate & {
    id: number;
    label?: string;
  };
}

const props = defineProps<Props>();

const drawerComponent = shallowRef(null);

onMounted(() => {
  loadDrawerComponent();
});

const toCamelCase = (str: string): string => {
  return str.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
};

const loadDrawerComponent = () => {
  try {
    let componentName;
    let folderName;

    if (props.nodeData.custom_node) {
      componentName = "CustomNode";
      folderName = "customNode";
    } else {
      componentName = toTitleCase(props.nodeData.item);
      folderName = toCamelCase(props.nodeData.item.toLowerCase());
    }

    const componentPath = `./node-types/elements/${folderName}/${componentName}.vue`;

    const componentLoader = drawerModules[componentPath];

    if (!componentLoader) {
      console.error(`Component not found at path: ${componentPath}`);
      console.log(
        "Available paths:",
        Object.keys(drawerModules).filter((path) => path.includes(`/${folderName}/`)),
      );
      return;
    }

    drawerComponent.value = markRaw(
      defineAsyncComponent({
        loader: componentLoader as () => Promise<any>,
        errorComponent: undefined,
        timeout: 3000,
        onError(error, retry, fail, attempts) {
          console.error(`Failed to load drawer component for ${props.nodeData.item}:`, error);
          if (attempts <= 3) {
            retry();
          } else {
            fail();
          }
        },
      }),
    );
  } catch (error) {
    console.error(`Error setting up drawer component for ${props.nodeData.item}:`, error);
  }
};

const imageSrc = computed(() => {
  return props.nodeData?.image || "default.png";
});

const nodeTitle = computed(() => {
  const displayName = props.nodeData?.label || props.nodeData?.name || "Node";
  return `${props.nodeId}: ${displayName}`;
});

const nodeTitleInfo = computed(() => {
  return {
    title: props.nodeData?.drawer_title || "Node Configuration",
    intro: props.nodeData?.drawer_intro || "Configure node settings",
  };
});

onErrorCaptured((error) => {
  console.error("Error in GenericNode component:", error);
  return false;
});
</script>

<style scoped>
.error-node {
  padding: 8px;
  background: var(--color-node-error-bg);
  border-radius: var(--border-radius-sm);
  font-size: 12px;
}
</style>
