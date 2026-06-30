<script setup lang="ts">
import { computed, watch, nextTick } from "vue";
import DraggableItem from "../../components/common/DraggableItem/DraggableItem.vue";
import { useItemStore } from "../../components/common/DraggableItem/stateStore";
import { useEditorStore } from "../../stores/editor-store";
import { useNodeStore } from "../../stores/column-store";
import { useFlowStore } from "../../stores/flow-store";
import { useDrawerStore } from "../../stores/drawer-store";
import type { DrawerDef, DrawerCtx } from "../../types/drawer.types";

const props = defineProps<{
  def: DrawerDef;
  heightOverride?: number;
}>();

const itemStore = useItemStore();
const ctx: DrawerCtx = {
  editor: useEditorStore(),
  node: useNodeStore(),
  flow: useFlowStore(),
  drawer: useDrawerStore(),
};

const visibleTabs = computed(() => props.def.tabs.filter((t) => t.visibleWhen(ctx)));
const visible = computed(() =>
  props.def.visibleWhen ? props.def.visibleWhen(ctx) : visibleTabs.value.length > 0,
);
const tabDefs = computed(() => visibleTabs.value.map((t) => ({ id: t.id, label: t.label })));

const activeTabId = computed<string>({
  get: () => ctx.drawer.activeTab[props.def.id] ?? visibleTabs.value[0]?.id ?? "",
  set: (v) => ctx.drawer.setActiveTab(props.def.id, v),
});

// Auto-focus a tab the moment it appears (and front the drawer); when the active
// tab closes, fall back to the first remaining one.
watch(
  () => visibleTabs.value.map((t) => t.id).join(","),
  (now, prev) => {
    const nowIds = now ? now.split(",") : [];
    const prevIds = prev ? prev.split(",") : [];
    const appeared = nowIds.find((id) => !prevIds.includes(id));
    if (appeared) {
      ctx.drawer.setActiveTab(props.def.id, appeared);
      nextTick(() => itemStore.bringToFront(props.def.id));
    } else if (nowIds.length && !nowIds.includes(activeTabId.value)) {
      ctx.drawer.setActiveTab(props.def.id, nowIds[0]);
    }
  },
);

// Always-present tabs (visibleWhen always true) never "appear", so they grab
// focus via an explicit focusWhen signal (e.g. Code on Ctrl+G).
watch(
  () =>
    props.def.tabs
      .filter((t) => t.focusWhen && t.focusWhen(ctx))
      .map((t) => t.id)
      .join(","),
  (now, prev) => {
    const nowIds = now ? now.split(",") : [];
    const prevIds = prev ? prev.split(",") : [];
    const newlyFocused = nowIds.find((id) => !prevIds.includes(id));
    if (newlyFocused) {
      ctx.drawer.setActiveTab(props.def.id, newlyFocused);
      nextTick(() => itemStore.bringToFront(props.def.id));
    }
  },
  { immediate: true },
);

const onMinimize = () => props.def.onMinimize?.(ctx);
</script>

<template>
  <draggable-item
    v-if="visible"
    :id="def.id"
    :show-right="def.side === 'right'"
    :show-bottom="def.side === 'bottom'"
    :initial-position="def.side"
    :initial-width="def.initialWidth"
    :initial-height="heightOverride ?? def.initialHeight"
    :initial-left="def.initialLeft"
    :width-behaviour="def.widthBehaviour"
    :height-behaviour="def.heightBehaviour"
    :allow-full-screen="def.allowFullScreen ?? true"
    :tabs="tabDefs"
    :active-tab="activeTabId"
    :on-minimize="onMinimize"
    @update:active-tab="activeTabId = $event"
  >
    <div class="tabbed-drawer-body">
      <div
        v-for="tab in visibleTabs"
        v-show="tab.id === activeTabId"
        :key="tab.id"
        class="tabbed-drawer-pane"
      >
        <component
          :is="tab.component"
          :key="tab.remountKey ? tab.remountKey(ctx) : tab.id"
          v-bind="tab.props ? tab.props(ctx) : {}"
        />
      </div>
    </div>
  </draggable-item>
</template>

<style scoped>
.tabbed-drawer-body {
  display: flex;
  flex-direction: column;
  height: 100%;
  width: 100%;
}
.tabbed-drawer-pane {
  flex: 1;
  min-height: 0;
  display: flex;
  flex-direction: column;
}
.tabbed-drawer-pane > * {
  flex: 1;
  min-height: 0;
}
</style>
