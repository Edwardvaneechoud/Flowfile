<script setup lang="ts">
import { ref, onMounted, onUnmounted, defineProps, toRaw, watch } from "vue";

import type { IRow, IMutField, IChart, IGWProps } from "@kanaries/graphic-walker/interfaces";

interface VueGRProps {
  chart: IChart;
  fields?: IMutField[];
  appearance?: IGWProps["appearance"];
  themeKey?: IGWProps["themeKey"];
  computation: (payload: any) => Promise<IRow[]>;
}

const props = defineProps<VueGRProps>();

const wrapper = ref<HTMLElement | null>(null);
const container = ref<HTMLElement | null>(null);
const isLoading = ref(true);
const loadError = ref<string | null>(null);

let reactRootInstance: any = null;
let React: any = null;
let ReactDOMClient: any = null;
let GraphicRenderer: any = null;
let resizeObserver: ResizeObserver | null = null;
let resizeTimer: ReturnType<typeof setTimeout> | null = null;
let measuredSize = { w: 0, h: 0 };
let shadowReadyObserver: MutationObserver | null = null;

// Suppress two graphic-walker artefacts inside its open Shadow DOM:
// (a) the `border-primary border-2` frame its <Resizable> wrapper draws
// when size.mode is 'fixed', and (b) the eight `cursor: *-resize` drag
// handles that project past the wrapper and trigger the parent
// .overflow-auto's scrollbars. No prop disables either; outside CSS
// can't pierce the boundary; instead we append a <style> into the
// shadow root, which scopes to all descendants.
const SHADOW_STYLE_MARKER = "data-flowfile-gw-overrides";

const findShadowRoot = (root: HTMLElement): ShadowRoot | null => {
  const queue: Element[] = Array.from(root.children);
  while (queue.length) {
    const el = queue.shift()!;
    if (el.shadowRoot) return el.shadowRoot;
    queue.push(...Array.from(el.children));
  }
  return null;
};

const injectShadowOverride = (shadow: ShadowRoot): void => {
  if (shadow.querySelector(`style[${SHADOW_STYLE_MARKER}]`)) return;
  const style = document.createElement("style");
  style.setAttribute(SHADOW_STYLE_MARKER, "");
  // The third rule mimics "remove the class" on the inner
  // <div class="w-full h-full relative" style="overflow:hidden"> wrapper:
  // letting it size to its content (and not clip) lets the outer
  // `.overflow-auto` ancestor render scrollbars when the chart is larger
  // than the tile body.
  style.textContent = `
    .border-primary.border-2 { border-width: 0 !important; }
    [style*="-resize"] { display: none !important; }
    .border-primary.border-2 > .w-full.h-full.relative {
      width: auto !important;
      height: auto !important;
      overflow: visible !important;
    }
  `;
  shadow.appendChild(style);
};

// GraphicRenderer's own ResizeObserver mis-measures height (its outer
// wrapper is missing h-full), so we measure the tile ourselves and pass
// the result as `overrideSize: { mode: 'fixed', ... }` — the library
// treats it as a hard replacement for layout.size.
const getReactProps = (): Record<string, any> => {
  const reactProps: Record<string, any> = {
    fields: props.fields ? toRaw(props.fields) : undefined,
    chart: [toRaw(props.chart)],
    appearance: props.appearance || "light",
    themeKey: props.themeKey,
    computation: props.computation,
    containerStyle: { width: "100%", height: "100%" },
    overrideSize: { mode: "fixed", width: measuredSize.w, height: measuredSize.h },
  };
  Object.keys(reactProps).forEach((key) => {
    if (reactProps[key] === undefined) delete reactProps[key];
  });
  return reactProps;
};

const renderReact = () => {
  if (reactRootInstance && GraphicRenderer && React) {
    reactRootInstance.render(React.createElement(GraphicRenderer, getReactProps()));
  }
};

const measure = (el: HTMLElement) => {
  const rect = el.getBoundingClientRect();
  const w = Math.max(1, Math.floor(rect.width));
  const h = Math.max(1, Math.floor(rect.height));
  if (Math.abs(w - measuredSize.w) < 2 && Math.abs(h - measuredSize.h) < 2) return false;
  measuredSize = { w, h };
  return true;
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
    React = reactModule.default;
    ReactDOMClient = reactDomModule;
    GraphicRenderer = gwModule.GraphicRenderer;

    measure(wrapper.value);

    reactRootInstance = ReactDOMClient.createRoot(container.value);
    renderReact();
    isLoading.value = false;

    const root = container.value;
    const tryInject = (): boolean => {
      const shadow = findShadowRoot(root);
      if (!shadow) return false;
      injectShadowOverride(shadow);
      return true;
    };
    if (!tryInject()) {
      shadowReadyObserver = new MutationObserver(() => {
        if (tryInject()) {
          shadowReadyObserver?.disconnect();
          shadowReadyObserver = null;
        }
      });
      shadowReadyObserver.observe(root, { childList: true, subtree: true });
    }

    resizeObserver = new ResizeObserver(() => {
      if (!wrapper.value) return;
      if (!measure(wrapper.value)) return;
      if (resizeTimer) clearTimeout(resizeTimer);
      resizeTimer = setTimeout(() => {
        resizeTimer = null;
        renderReact();
      }, 80);
    });
    resizeObserver.observe(wrapper.value);
  } catch (e) {
    console.error("[VueGR] Error mounting GraphicRenderer:", e);
    loadError.value = e instanceof Error ? e.message : "Failed to load";
    isLoading.value = false;
  }
});

watch(
  () => [props.chart, props.fields, props.appearance, props.themeKey],
  () => renderReact(),
  { deep: true },
);

onUnmounted(() => {
  if (resizeTimer) {
    clearTimeout(resizeTimer);
    resizeTimer = null;
  }
  resizeObserver?.disconnect();
  resizeObserver = null;
  shadowReadyObserver?.disconnect();
  shadowReadyObserver = null;
  if (reactRootInstance) {
    reactRootInstance.unmount();
    reactRootInstance = null;
  }
});
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
