<template>
  <div class="compute-view">
    <div v-if="visibleTabs.length > 1" class="compute-tabs">
      <button
        v-for="tab in visibleTabs"
        :key="tab.key"
        class="compute-tab"
        :class="{ active: activeTab === tab.key }"
        @click="handleTabClick(tab.key)"
      >
        <i :class="tab.icon"></i>
        <span>{{ tab.label }}</span>
      </button>
    </div>

    <div class="compute-content">
      <KernelManagerView v-if="activeTab === 'kernels'" />
      <PerformancePanel v-else-if="activeTab === 'performance'" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import KernelManagerView from "../KernelManagerView/KernelManagerView.vue";
import PerformancePanel from "./PerformancePanel.vue";
import { computeTabs, COMPUTE_TAB_KEYS } from "./computeTabs";
import type { ComputeTabKey } from "./computeTabs";
import { useAuthStore } from "../../stores/auth-store";

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();

const visibleTabs = computed(() =>
  computeTabs.filter((t) => !t.requiresAdmin || authStore.isAdmin),
);

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
