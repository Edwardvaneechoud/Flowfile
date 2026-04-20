<template>
  <div class="connections-view">
    <div class="connections-tabs">
      <button
        v-for="tab in tabs"
        :key="tab.key"
        class="connections-tab"
        :class="{ active: activeTab === tab.key }"
        @click="handleTabClick(tab.key)"
      >
        <i :class="tab.icon"></i>
        <span>{{ tab.label }}</span>
      </button>
    </div>

    <div class="connections-content">
      <DatabaseView v-if="activeTab === 'database'" />
      <CloudConnectionView v-else-if="activeTab === 'cloud'" />
      <KafkaConnectionView v-else-if="activeTab === 'kafka'" />
      <GoogleAnalyticsConnectionView v-else-if="activeTab === 'google_analytics'" />
      <SecretsView v-else-if="activeTab === 'secrets'" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useRoute, useRouter } from "vue-router";
import DatabaseView from "../DatabaseView/DatabaseView.vue";
import CloudConnectionView from "../CloudConnectionView/CloudConnectionView.vue";
import KafkaConnectionView from "../KafkaConnectionView/KafkaConnectionView.vue";
import GoogleAnalyticsConnectionView from "../GoogleAnalyticsConnectionView/GoogleAnalyticsConnectionView.vue";
import SecretsView from "../SecretsView/SecretsView.vue";

const route = useRoute();
const router = useRouter();

const validTabs = ["database", "cloud", "kafka", "google_analytics", "secrets"] as const;
type TabName = (typeof validTabs)[number];

const STORAGE_KEY = "connections-active-tab";

const tabs: { key: TabName; label: string; icon: string }[] = [
  { key: "database", label: "Database", icon: "fa-solid fa-database" },
  { key: "cloud", label: "Cloud Storage", icon: "fa-solid fa-cloud" },
  { key: "kafka", label: "Kafka", icon: "fa-solid fa-tower-broadcast" },
  { key: "google_analytics", label: "Google Analytics", icon: "fa-solid fa-chart-line" },
  { key: "secrets", label: "Secrets", icon: "fa-solid fa-key" },
];

function getInitialTab(): TabName {
  const queryTab = route.query.tab as string;
  if (validTabs.includes(queryTab as TabName)) {
    return queryTab as TabName;
  }
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored && validTabs.includes(stored as TabName)) {
    return stored as TabName;
  }
  return "database";
}

const activeTab = ref<TabName>(getInitialTab());

onMounted(() => {
  router.replace({ query: { ...route.query, tab: activeTab.value } });
});

function handleTabClick(tab: TabName) {
  activeTab.value = tab;
  localStorage.setItem(STORAGE_KEY, tab);
  router.replace({ query: { ...route.query, tab } });
}
</script>

<style scoped>
.connections-view {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.connections-tabs {
  display: flex;
  gap: 2px;
  padding: var(--spacing-2) var(--spacing-4);
  background: var(--color-background-secondary);
  border-bottom: 1px solid var(--color-border-primary);
}

.connections-tab {
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

.connections-tab:hover {
  background: var(--color-background-hover);
  color: var(--color-text-primary);
}

.connections-tab.active {
  background: var(--color-background-primary);
  color: var(--color-primary);
  box-shadow: var(--shadow-xs);
}

.connections-content {
  flex: 1;
  overflow: auto;
  padding: var(--spacing-4, 1rem);
}
</style>
