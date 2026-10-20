<script setup lang="ts">
import { computed, watch, nextTick } from "vue";
import DraggableItem from "../../components/common/DraggableItem/DraggableItem.vue";
import { useItemStore } from "../../components/common/DraggableItem/stateStore";
import { useEditorStore } from "../../stores/editor-store";
import { useNodeStore } from "../../stores/column-store";
import { useFlowStore } from "../../stores/flow-store";
import { useDrawerStore } from "../../stores/drawer-store";
import { usePopout } from "../../composables/usePopout";
import type { DrawerDef, DrawerCtx } from "../../types/drawer.types";

const props = defineProps<{
  def: DrawerDef;
  heightOverride?: number;
  leftOverride?: number;
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

const activeTabDef = computed(() => visibleTabs.value.find((t) => t.id === activeTabId.value));

// The active tab's pop-out, when it declares one for this flow.
const popoutKind = computed(() => {
  const popout = activeTabDef.value?.popout;
  if (!popout || popout.enabled?.(ctx) === false) return null;
  return popout.kind;
});

const popOutActiveTab = () => {
  if (popoutKind.value) void usePopout(popoutKind.value).popOut(ctx.flow.flowId);
};

// Auto-focus a tab the moment it appears (and front the drawer); when the active
// tab closes (or moves to its own window), fall back to the first remaining one.
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
// focus via an explicit focusWhen signal (e.g. Logs on a run).
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
    :initial-height="heightOverride"
    :initial-left="leftOverride"
    :width-behaviour="def.widthBehaviour"
    :height-behaviour="def.heightBehaviour"
    :allow-full-screen="def.allowFullScreen ?? true"
    :tabs="tabDefs"
    :active-tab="activeTabId"
    :on-minimize="onMinimize"
    @update:active-tab="activeTabId = $event"
  >
    <template v-if="popoutKind && activeTabDef" #header-actions>
      <button
        class="tabbed-drawer-popout"
        type="button"
        :title="`Open ${activeTabDef.label} in its own window`"
        :aria-label="`Open ${activeTabDef.label} in its own window`"
        :data-testid="`dock-popout-${activeTabDef.id}`"
        @click="popOutActiveTab"
      >
        <span class="material-icons" aria-hidden="true">open_in_new</span>
      </button>
    </template>
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
.tabbed-drawer-popout {
  display: flex;
  align-items: center;
  justify-content: center;
  width: 25px;
  height: 25px;
  margin: 0 2px;
  padding: 0;
  border: none;
  border-radius: 4px;
  background-color: var(--color-background-tertiary);
  color: var(--color-text-primary);
  cursor: pointer;
}
.tabbed-drawer-popout:hover {
  background-color: var(--color-background-hover);
}
.tabbed-drawer-popout .material-icons {
  font-size: 16px;
}
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
