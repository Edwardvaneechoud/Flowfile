<template>
  <div class="connections-view">
    <el-tabs v-model="activeTab" @tab-change="onTabChange">
      <el-tab-pane label="Database" name="database">
        <DatabaseView />
      </el-tab-pane>
      <el-tab-pane label="Cloud Storage" name="cloud">
        <CloudConnectionView />
      </el-tab-pane>
      <el-tab-pane label="Kafka" name="kafka">
        <KafkaConnectionView />
      </el-tab-pane>
      <el-tab-pane label="Secrets" name="secrets">
        <SecretsView />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from "vue";
import { useRoute, useRouter } from "vue-router";
import DatabaseView from "../DatabaseView/DatabaseView.vue";
import CloudConnectionView from "../CloudConnectionView/CloudConnectionView.vue";
import KafkaConnectionView from "../KafkaConnectionView/KafkaConnectionView.vue";
import SecretsView from "../SecretsView/SecretsView.vue";

const route = useRoute();
const router = useRouter();

const validTabs = ["database", "cloud", "kafka", "secrets"] as const;
type TabName = (typeof validTabs)[number];

const STORAGE_KEY = "connections-active-tab";

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

function onTabChange(tab: string | number) {
  const tabName = tab as string;
  localStorage.setItem(STORAGE_KEY, tabName);
  router.replace({ query: { ...route.query, tab: tabName } });
}
</script>

<style scoped>
.connections-view {
  padding: var(--spacing-4, 1rem);
}

.connections-view :deep(.el-tabs__header) {
  margin-bottom: var(--spacing-4, 1rem);
}
</style>
