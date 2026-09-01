<template>
  <div class="compute-view">
    <div v-if="visibleTabs.length > 1" class="compute-tabs">
      <template v-for="tab in visibleTabs" :key="tab.key">
        <div v-if="tab.groupStart" class="tab-group-divider" aria-hidden="true"></div>
        <span v-if="tab.groupLabel" class="tab-group-label" aria-hidden="true">
          {{ tab.groupLabel }}
        </span>
        <button
          class="compute-tab"
          :class="{ active: activeTab === tab.key }"
          @click="handleTabClick(tab.key)"
        >
          <i :class="tab.icon"></i>
          <span>{{ tab.label }}</span>
        </button>
      </template>
    </div>

    <div class="compute-content">
      <KernelManagerView v-if="activeTab === 'kernels'" />
      <PerformancePanel v-else-if="activeTab === 'performance'" />
      <PrivacyPanel v-else-if="activeTab === 'privacy'" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import KernelManagerView from "../KernelManagerView/KernelManagerView.vue";
import PerformancePanel from "./PerformancePanel.vue";
import PrivacyPanel from "./PrivacyPanel.vue";
import { computeTabs, COMPUTE_TAB_GROUP_LABELS, COMPUTE_TAB_KEYS } from "./computeTabs";
import type { ComputeTabKey } from "./computeTabs";
import { useAuthStore } from "../../stores/auth-store";

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();

// Group captions derive from the *visible* list, so hiding an admin-only tab
// can't leave a caption stranded over the wrong cluster.
const visibleTabs = computed(() => {
  const shown = computeTabs.filter((t) => !t.requiresAdmin || authStore.isAdmin);
  return shown.map((tab, i) => ({
    key: tab.key,
    label: tab.label,
    icon: tab.icon,
    groupStart: i > 0 && tab.group !== shown[i - 1].group,
    groupLabel:
      i === 0 || tab.group !== shown[i - 1].group ? COMPUTE_TAB_GROUP_LABELS[tab.group] : null,
  }));
});

function getInitialTab(): ComputeTabKey {
  // No localStorage stickiness: kernels is the right default every time, and
  // Performance is a rarely-visited admin setting.
  const queryTab = route.query.tab as string;
  if (COMPUTE_TAB_KEYS.includes(queryTab as ComputeTabKey)) {
    return queryTab as ComputeTabKey;
  }
  return "kernels";
}

const activeTab = ref<ComputeTabKey>(getInitialTab());

onMounted(() => {
  router.replace({ query: { ...route.query, tab: activeTab.value } });
});

function handleTabClick(tab: ComputeTabKey) {
  activeTab.value = tab;
  router.replace({ query: { ...route.query, tab } });
}

// React to external navigation that changes ?tab= (the sidebar sub-menu), so the
// active tab follows the URL even when the view is already mounted. Validated
// against ALL tab keys, not visibleTabs: a non-admin deep link to
// ?tab=performance renders the panel's locked explainer instead of silently
// bouncing.
watch(
  () => route.query.tab,
  (tab) => {
    if (
      typeof tab === "string" &&
      COMPUTE_TAB_KEYS.includes(tab as ComputeTabKey) &&
      tab !== activeTab.value
    ) {
      activeTab.value = tab as ComputeTabKey;
    }
  },
);
</script>

<style scoped>
.compute-view {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.compute-tabs {
  display: flex;
  gap: 2px;
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.tab-group-divider {
  width: 1px;
  align-self: stretch;
  margin: var(--spacing-1) var(--spacing-1);
  background: var(--color-border-primary);
}

/* Whisper-quiet group caption at the start of each tab cluster. */
.tab-group-label {
  flex-shrink: 0;
  align-self: center;
  margin: 0 2px 0 var(--spacing-2);
  font-size: 9px;
  font-weight: var(--font-weight-semibold);
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: var(--color-text-tertiary);
  opacity: 0.7;
  white-space: nowrap;
  user-select: none;
  pointer-events: none;
}

.compute-tab {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4);
  border: none;
  background: transparent;
  color: var(--color-text-secondary);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  cursor: pointer;
  border-radius: var(--border-radius-md);
  transition: all var(--transition-fast);
}

.compute-tab:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.compute-tab.active {
  background: var(--color-background-primary);
  color: var(--color-primary);
  box-shadow: var(--shadow-xs);
}

/* No padding here: both panels bring their own centered max-width container. */
.compute-content {
  flex: 1;
  overflow: auto;
}
</style>
