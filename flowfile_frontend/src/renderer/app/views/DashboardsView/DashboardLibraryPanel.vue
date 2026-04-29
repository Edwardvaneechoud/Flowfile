<template>
  <div class="dash-library">
    <div class="dash-library-header">
      <div>
        <h2>Dashboards</h2>
        <p class="dash-library-sub">Compose multiple saved visualizations on a canvas.</p>
      </div>
      <div class="dash-library-actions">
        <el-input
          v-model="search"
          size="small"
          placeholder="Filter by name"
          class="dash-library-search"
          clearable
        >
          <template #prefix>
            <el-icon><Search /></el-icon>
          </template>
        </el-input>
        <el-button type="primary" size="small" @click="onNew">
          <el-icon><Plus /></el-icon>
          <span>New dashboard</span>
        </el-button>
      </div>
    </div>

    <div v-if="store.loadingLibrary" class="dash-library-state">
      <el-skeleton :rows="6" animated />
    </div>

    <div v-else-if="!filtered.length" class="dash-library-state">
      <el-empty
        :description="
          search
            ? 'No dashboards match your filter.'
            : 'No dashboards yet. Create one and drop saved visualizations onto the canvas.'
        "
      >
        <el-button v-if="!search" type="primary" @click="onNew">Create dashboard</el-button>
      </el-empty>
    </div>

    <div v-else class="dash-library-grid">
      <DashboardCard
        v-for="d in filtered"
        :key="d.id"
        :dashboard="d"
        @view="onView(d)"
        @edit="onEdit(d)"
        @delete="onDelete(d)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { Plus, Search } from "@element-plus/icons-vue";
import { useDashboardsStore } from "../../stores/dashboards-store";
import DashboardCard from "./DashboardCard.vue";
import type { Dashboard } from "../../types";

const router = useRouter();
const store = useDashboardsStore();
const search = ref("");

const filtered = computed(() => {
  const q = search.value.trim().toLowerCase();
  if (!q) return store.library;
  return store.library.filter(
    (d) =>
      d.name.toLowerCase().includes(q) ||
      (d.description ?? "").toLowerCase().includes(q) ||
      (d.namespace_name ?? "").toLowerCase().includes(q),
  );
});

onMounted(() => {
  store.loadLibrary().catch(() => {
    ElMessage.error(store.error ?? "Failed to load dashboards");
  });
});

const onNew = () => {
  router.push({ name: "dashboard-new" });
};

const onView = (d: Dashboard) => {
  router.push({ name: "dashboard-view", params: { id: d.id } });
};

const onEdit = (d: Dashboard) => {
  router.push({ name: "dashboard-edit", params: { id: d.id } });
};

const onDelete = async (d: Dashboard) => {
  try {
    await ElMessageBox.confirm(
      `Delete dashboard "${d.name}"? This cannot be undone.`,
      "Delete dashboard",
      { type: "warning", confirmButtonText: "Delete", cancelButtonText: "Cancel" },
    );
  } catch {
    return;
  }
  try {
    await store.deleteDashboard(d.id);
    ElMessage.success(`Deleted "${d.name}"`);
  } catch {
    ElMessage.error(store.error ?? "Failed to delete");
  }
};
</script>

<style scoped>
.dash-library {
  display: flex;
  flex-direction: column;
  gap: 16px;
  padding: 16px 24px;
  height: 100%;
  overflow-y: auto;
}
.dash-library-header {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}
.dash-library-header h2 {
  margin: 0 0 4px 0;
  font-size: 18px;
  font-weight: 600;
}
.dash-library-sub {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
.dash-library-actions {
  display: flex;
  align-items: center;
  gap: 8px;
}
.dash-library-search {
  width: 240px;
}
.dash-library-state {
  padding: 24px 0;
}
.dash-library-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
  gap: 16px;
}
</style>
