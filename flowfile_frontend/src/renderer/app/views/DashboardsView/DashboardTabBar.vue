<template>
  <div v-if="tabs.length || mode === 'edit'" class="dashboard-tab-bar" :class="`is-${mode}`">
    <el-tabs
      v-if="tabs.length"
      :model-value="activeTabId ?? undefined"
      class="dashboard-tabs"
      data-testid="dashboard-tabs"
      @tab-change="(name: string | number) => emit('select', String(name))"
    >
      <el-tab-pane v-for="tab in tabs" :key="tab.id" :name="tab.id">
        <template #label>
          <span
            class="dashboard-tab-label"
            :title="mode === 'edit' ? 'Double-click to rename' : undefined"
            @dblclick.stop="mode === 'edit' && startRename(tab)"
          >
            <!-- keydown.stop: the tab list reads arrow keys to move between tabs -->
            <input
              v-if="renamingId === tab.id"
              ref="renameInputRef"
              v-model="renameDraft"
              class="dashboard-tab-rename"
              aria-label="Tab name"
              :maxlength="DASHBOARD_TAB_TITLE_MAX"
              data-testid="dashboard-tab-rename"
              @keydown.enter.prevent="commitRename"
              @keydown.esc.prevent="renamingId = null"
              @keydown.stop
              @blur="commitRename"
              @click.stop
              @mousedown.stop
            />
            <template v-else>{{ tab.title }}</template>
          </span>
        </template>
      </el-tab-pane>
    </el-tabs>
    <div v-if="mode === 'edit'" class="dashboard-tab-actions">
      <el-dropdown v-if="activeTab" trigger="click" placement="bottom-end" @command="onCommand">
        <el-button text size="small" data-testid="dashboard-tab-menu" aria-label="Tab options">
          <el-icon><MoreFilled /></el-icon>
        </el-button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item command="rename">
              <el-icon><EditPen /></el-icon> Rename
            </el-dropdown-item>
            <el-dropdown-item command="left" :disabled="activeIndex <= 0">
              <el-icon><Back /></el-icon> Move left
            </el-dropdown-item>
            <el-dropdown-item command="right" :disabled="activeIndex >= tabs.length - 1">
              <el-icon><Right /></el-icon> Move right
            </el-dropdown-item>
            <el-dropdown-item v-if="tabs.length > 1" divided command="delete">
              <el-icon><Delete /></el-icon> Delete tab
            </el-dropdown-item>
            <el-dropdown-item v-else divided command="dissolve">
              <el-icon><Close /></el-icon> Remove tabs
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
      <el-button text size="small" data-testid="dashboard-add-tab" @click="onAdd">
        <el-icon><Plus /></el-icon> Add tab
      </el-button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, ref } from "vue";
import { ElMessageBox } from "element-plus";
import { Back, Close, Delete, EditPen, MoreFilled, Plus, Right } from "@element-plus/icons-vue";
import { DASHBOARD_TAB_TITLE_MAX, type DashboardLayout, type DashboardTab } from "../../types";
import {
  addTab,
  deleteTab,
  dissolveTabs,
  layoutTabs,
  moveTab,
  renameTab,
  tilesOfTab,
} from "./dashboardTabs";

/**
 * A dashboard's tabs as Element Plus's tab list (roles, roving focus, arrow keys). In edit mode it
 * also adds, renames, reorders and deletes tabs, emitting the whole new layout as one change.
 */
const props = defineProps<{
  layout: DashboardLayout;
  activeTabId: string | null;
  mode: "edit" | "view";
}>();

const emit = defineEmits<{
  (e: "select", tabId: string): void;
  (e: "update:layout", value: DashboardLayout): void;
}>();

const tabs = computed(() => layoutTabs(props.layout));
const activeIndex = computed(() => tabs.value.findIndex((t) => t.id === props.activeTabId));
const activeTab = computed<DashboardTab | null>(() => tabs.value[activeIndex.value] ?? null);

const change = (next: DashboardLayout) => {
  if (next !== props.layout) emit("update:layout", next);
};

const onAdd = () => {
  const { layout, tabId } = addTab(props.layout);
  change(layout);
  emit("select", tabId);
};

const renamingId = ref<string | null>(null);
const renameDraft = ref("");
const renameInputRef = ref<HTMLInputElement[] | HTMLInputElement | null>(null);

const startRename = async (tab: DashboardTab) => {
  renamingId.value = tab.id;
  renameDraft.value = tab.title;
  await nextTick();
  const input = Array.isArray(renameInputRef.value)
    ? renameInputRef.value[0]
    : renameInputRef.value;
  input?.focus();
  input?.select();
};

const commitRename = () => {
  const id = renamingId.value;
  if (id == null) return;
  renamingId.value = null;
  change(renameTab(props.layout, id, renameDraft.value));
};

const plural = (n: number) => (n === 1 ? "1 tile" : `${n} tiles`);

const confirmDelete = async (tab: DashboardTab) => {
  const count = tilesOfTab(props.layout, tab.id).length;
  if (count) {
    try {
      await ElMessageBox.confirm(
        `Delete "${tab.title}" and its ${plural(count)}? Undo brings them back.`,
        "Delete tab",
        { confirmButtonText: "Delete", cancelButtonText: "Keep it", type: "warning" },
      );
    } catch {
      return;
    }
  }
  let next = deleteTab(props.layout, tab.id);
  const left = layoutTabs(next);
  if (left.length === 1) {
    try {
      await ElMessageBox.confirm(
        `Only "${left[0].title}" is left. Go back to a plain canvas? Its tiles stay.`,
        "One tab left",
        { confirmButtonText: "Plain canvas", cancelButtonText: "Keep the tab" },
      );
      next = dissolveTabs(next);
    } catch {
      // keep the one tab
    }
  }
  change(next);
};

const onCommand = (command: string) => {
  const tab = activeTab.value;
  if (!tab) return;
  if (command === "rename") void startRename(tab);
  else if (command === "left") change(moveTab(props.layout, tab.id, activeIndex.value - 1));
  else if (command === "right") change(moveTab(props.layout, tab.id, activeIndex.value + 1));
  else if (command === "delete") void confirmDelete(tab);
  else if (command === "dissolve") change(dissolveTabs(props.layout));
};
</script>

<style scoped>
.dashboard-tab-bar {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  min-height: 32px;
  margin-bottom: var(--spacing-2);
}
.dashboard-tab-bar.is-edit:not(:has(.dashboard-tabs)) {
  justify-content: flex-end;
}
.dashboard-tabs {
  flex: 1;
  min-width: 0;
}
.dashboard-tabs :deep(.el-tabs__header) {
  margin: 0;
}
.dashboard-tabs :deep(.el-tabs__content) {
  display: none;
}
/* Outranks Element Plus's zero padding on the first and last top tab. */
.dashboard-tabs.el-tabs--top :deep(.el-tabs__item.is-top) {
  padding: 0 var(--spacing-4);
  height: 36px;
}
.dashboard-tab-label {
  display: inline-flex;
  align-items: center;
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.dashboard-tab-rename {
  width: 140px;
  font: inherit;
  padding: 0 4px;
  color: var(--el-text-color-primary);
  background: var(--el-fill-color-blank);
  border: 1px solid var(--el-color-primary);
  border-radius: var(--el-border-radius-small);
  outline: none;
}
.dashboard-tab-actions {
  display: flex;
  align-items: center;
  flex-shrink: 0;
}
/* The tab list's 2px rule runs on under the actions, so the bar reads as one line. */
.dashboard-tab-bar:has(.dashboard-tabs) {
  gap: 0;
}
.dashboard-tabs + .dashboard-tab-actions {
  align-self: stretch;
  padding-left: var(--spacing-2);
  box-shadow: inset 0 -2px 0 var(--el-border-color-light);
}
.dashboard-tab-actions :deep(.el-button + .el-button) {
  margin-left: 0;
}
</style>
