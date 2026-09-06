<template>
  <div class="compute-view">
    <SettingsTabBar
      v-if="visibleTabs.length > 1"
      :tabs="visibleTabs"
      :active-tab="activeTab"
      @select="handleTabClick($event as ComputeTabKey)"
    />

    <div class="compute-content">
      <KernelManagerView v-if="activeTab === 'kernels'" />
      <PerformancePanel v-else-if="activeTab === 'performance'" />
      <PrivacyPanel v-else-if="activeTab === 'privacy'" />
      <BackupsPanel v-else-if="activeTab === 'backups'" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import SettingsTabBar from "../../components/settings/SettingsTabBar.vue";
import KernelManagerView from "../KernelManagerView/KernelManagerView.vue";
import PerformancePanel from "./PerformancePanel.vue";
import PrivacyPanel from "./PrivacyPanel.vue";
import BackupsPanel from "./BackupsPanel.vue";
import { computeTabs, COMPUTE_TAB_GROUP_LABELS, COMPUTE_TAB_KEYS } from "./computeTabs";
import type { ComputeTabKey } from "./computeTabs";
import { useAuthStore } from "../../stores/auth-store";

const route = useRoute();
const router = useRouter();
const authStore = useAuthStore();

// Captions derive from the visible list: hiding an admin tab must not strand one.
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

/* No padding here: both panels bring their own centered max-width container. */
.compute-content {
  flex: 1;
  overflow: auto;
}
</style>
