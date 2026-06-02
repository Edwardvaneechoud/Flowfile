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
      <ConnectionsOverview
        v-if="activeTab === 'overview'"
        :counts="counts"
        @select="handleTabClick"
      />
      <DatabaseView v-else-if="activeTab === 'database'" />
      <CloudConnectionView v-else-if="activeTab === 'cloud'" />
      <KafkaConnectionView v-else-if="activeTab === 'kafka'" />
      <GoogleAnalyticsConnectionView v-else-if="activeTab === 'google_analytics'" />
      <SecretsView v-else-if="activeTab === 'secrets'" />
      <AiSettingsTab v-else-if="activeTab === 'ai'" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, watch } from "vue";
import { useRoute, useRouter } from "vue-router";
import DatabaseView from "../DatabaseView/DatabaseView.vue";
import CloudConnectionView from "../CloudConnectionView/CloudConnectionView.vue";
import KafkaConnectionView from "../KafkaConnectionView/KafkaConnectionView.vue";
import GoogleAnalyticsConnectionView from "../GoogleAnalyticsConnectionView/GoogleAnalyticsConnectionView.vue";
import SecretsView from "../SecretsView/SecretsView.vue";
import AiSettingsTab from "../AiProvidersView/AiSettingsTab.vue";
import ConnectionsOverview from "./ConnectionsOverview.vue";
import { connectionTypes } from "./connectionTypes";
import type { ConnectionTypeKey } from "./connectionTypes";
import { fetchDatabaseConnectionsInterfaces } from "../DatabaseView/api";
import { fetchCloudStorageConnectionsInterfaces } from "../CloudConnectionView/api";
import { fetchKafkaConnections } from "../KafkaConnectionView/api";
import { fetchGoogleAnalyticsConnections } from "../GoogleAnalyticsConnectionView/api";
import { fetchAiProviders } from "../AiProvidersView/api";
import { fetchSecretsApi } from "../../api/secrets.api";

const route = useRoute();
const router = useRouter();

// The six real connection sections (also the persisted ?tab= values), plus the
// synthetic "overview" landing which is never remembered.
const sectionKeys = connectionTypes.map((t) => t.key);
type ActiveTab = ConnectionTypeKey | "overview";
const validTabs: ActiveTab[] = ["overview", ...sectionKeys];

const STORAGE_KEY = "connections-active-tab";

const tabs: { key: ActiveTab; label: string; icon: string }[] = [
  { key: "overview", label: "Overview", icon: "fa-solid fa-grip" },
  ...connectionTypes.map((t) => ({ key: t.key, label: t.label, icon: t.icon })),
];

function getInitialTab(): ActiveTab {
  // 1. An explicit deep link (help cards, sidebar children, legacy redirects) wins.
  const queryTab = route.query.tab as string;
  if (validTabs.includes(queryTab as ActiveTab)) {
    return queryTab as ActiveTab;
  }
  // 2. No query: reopen the last real section the user worked in.
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored && sectionKeys.includes(stored as ConnectionTypeKey)) {
    return stored as ConnectionTypeKey;
  }
  // 3. First visit ever: show the hub overview.
  return "overview";
}

const activeTab = ref<ActiveTab>(getInitialTab());

onMounted(() => {
  router.replace({ query: { ...route.query, tab: activeTab.value } });
});

function handleTabClick(tab: ActiveTab) {
  activeTab.value = tab;
  // Only remember real sections — never the overview, or it would become sticky
  // and break "reopen my last section" on return visits.
  if (tab !== "overview") {
    localStorage.setItem(STORAGE_KEY, tab);
  }
  router.replace({ query: { ...route.query, tab } });
}

// React to external navigation that changes ?tab= (deep links in the help
// overview, the sidebar sub-menu), so the active tab follows the URL even when
// the view is already mounted.
watch(
  () => route.query.tab,
  (tab) => {
    if (
      typeof tab === "string" &&
      validTabs.includes(tab as ActiveTab) &&
      tab !== activeTab.value
    ) {
      activeTab.value = tab as ActiveTab;
      if (tab !== "overview") {
        localStorage.setItem(STORAGE_KEY, tab);
      }
    }
  },
);

// "Already set up" counts shown as badges on the overview cards. Loaded whenever
// the overview is visible (and refreshed each time the user returns to it) so the
// badges reflect connections added in the individual tabs. Failures per type are
// swallowed — that card simply shows no badge.
const counts = ref<Partial<Record<ConnectionTypeKey, number>>>({});

async function loadCounts() {
  const tasks: [ConnectionTypeKey, Promise<number>][] = [
    ["database", fetchDatabaseConnectionsInterfaces().then((r) => r.length)],
    ["cloud", fetchCloudStorageConnectionsInterfaces().then((r) => r.length)],
    ["kafka", fetchKafkaConnections().then((r) => r.length)],
    ["google_analytics", fetchGoogleAnalyticsConnections().then((r) => r.length)],
    ["secrets", fetchSecretsApi().then((r) => r.length)],
    ["ai", fetchAiProviders().then((r) => r.filter((p) => p.status === "configured").length)],
  ];
  const settled = await Promise.allSettled(tasks.map(([, p]) => p));
  const next: Partial<Record<ConnectionTypeKey, number>> = {};
  settled.forEach((res, i) => {
    if (res.status === "fulfilled") next[tasks[i][0]] = res.value;
  });
  counts.value = next;
}

watch(
  activeTab,
  (tab) => {
    if (tab === "overview") loadCounts();
  },
  { immediate: true },
);
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
