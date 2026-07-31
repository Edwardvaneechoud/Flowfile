<template>
  <div class="generic-node-shell">
    <node-button
      ref="nodeButton"
      :node-id="nodeId"
      :image-src="imageSrc"
      :title="nodeTitle"
      :drawer-component="drawerComponent"
      :node-title-info="nodeTitleInfo"
    />
    <span v-if="showKernelBadge" class="kernel-corner-badge" :title="kernelBadgeTooltip">
      <KernelBadgeIcon />
    </span>
  </div>
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
import KernelBadgeIcon from "../../views/DesignerView/KernelBadgeIcon.vue";
import { toTitleCase } from "../../views/DesignerView/utils";
import { nodeDocsUrl } from "../../views/DesignerView/nodeDocsLinks";
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
    docsUrl: nodeDocsUrl(props.nodeData),
  };
});

const showKernelBadge = computed(() => props.nodeData?.execution_environment === "kernel");

const kernelBadgeTooltip = computed(() => {
  const deps = props.nodeData?.dependencies ?? [];
  const base = "Runs in isolated kernel";
  return deps.length ? `${base} — deps: ${deps.join(", ")}` : base;
});

onErrorCaptured((error) => {
  console.error("Error in GenericNode component:", error);
  return false;
});
</script>

<style scoped>
.generic-node-shell {
  position: relative;
  display: inline-flex;
}

/* Subtle corner marker for isolated-kernel custom nodes; tooltip lists deps. */
.kernel-corner-badge {
  position: absolute;
  top: -4px;
  right: -4px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background-color: var(--color-background-primary);
  color: var(--color-accent, #0891b2);
  box-shadow: var(--shadow-sm);
  pointer-events: auto;
}

.error-node {
  padding: 8px;
  background: var(--color-node-error-bg);
  border-radius: var(--border-radius-sm);
  font-size: 12px;
}
</style>
