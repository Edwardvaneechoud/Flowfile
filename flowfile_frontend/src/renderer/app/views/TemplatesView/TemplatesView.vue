<template>
  <div class="templates-view">
    <div class="templates-view__header">
      <h1 class="templates-view__title">
        <span class="material-icons">layers</span>
        Flow Templates
      </h1>
      <p class="templates-view__subtitle">
        Get started quickly with pre-built flows. Each template uses sample datasets that are
        downloaded from
        <a href="https://github.com/edwardvaneechoud/flowfile" target="_blank" rel="noopener"
          >GitHub</a
        >
        and cached locally.
      </p>
    </div>

    <div class="templates-view__filters">
      <el-radio-group v-model="selectedCategory" size="default">
        <el-radio-button label="All" value="All" />
        <el-radio-button label="Beginner" value="Beginner" />
        <el-radio-button label="Intermediate" value="Intermediate" />
        <el-radio-button label="Advanced" value="Advanced" />
      </el-radio-group>
    </div>

    <div v-if="isLoading" class="templates-view__loading">
      <p>Loading templates...</p>
    </div>

    <div v-else-if="loadError" class="templates-view__empty">
      <p>Could not load templates. Please check your connection and try again.</p>
      <el-button @click="loadTemplates">Retry</el-button>
    </div>

    <div v-else-if="filteredTemplates.length === 0 && selectedCategory !== 'All'" class="templates-view__empty">
      <p>No {{ selectedCategory }} templates found.</p>
    </div>

    <div v-else-if="filteredTemplates.length === 0" class="templates-view__empty">
      <p>No templates available.</p>
    </div>

    <div v-else class="templates-view__grid">
      <TemplateCard
        v-for="template in filteredTemplates"
        :key="template.template_id"
        :template="template"
        :loading="creatingTemplateId === template.template_id"
        @use-template="handleUseTemplate"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from "vue";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import TemplateCard from "./TemplateCard.vue";
import { TemplatesApi } from "../../api";
import { useNodeStore } from "../../stores/column-store";
import type { FlowTemplateMeta } from "../../types/template.types";

const router = useRouter();
const nodeStore = useNodeStore();

const templates = ref<FlowTemplateMeta[]>([]);
const selectedCategory = ref("All");
const isLoading = ref(true);
const loadError = ref(false);
const creatingTemplateId = ref<string | null>(null);

const filteredTemplates = computed(() => {
  if (selectedCategory.value === "All") return templates.value;
  return templates.value.filter((t) => t.category === selectedCategory.value);
});

const loadTemplates = async () => {
  isLoading.value = true;
  loadError.value = false;
  try {
    // Ensure flow YAMLs are available locally (downloads from GitHub on first visit)
    await TemplatesApi.ensureAvailable();
    templates.value = await TemplatesApi.listTemplates();
  } catch (error) {
    console.error("Failed to load templates:", error);
    loadError.value = true;
    ElMessage.error("Failed to load templates");
  } finally {
    isLoading.value = false;
  }
};

const handleUseTemplate = async (templateId: string) => {
  if (creatingTemplateId.value) return;

  creatingTemplateId.value = templateId;
  try {
    ElMessage.info({ message: "Creating flow from template...", duration: 1500 });
    const flowId = await TemplatesApi.createFromTemplate(templateId);
    nodeStore.setFlowId(flowId);
    ElMessage.success({ message: "Flow created successfully!", duration: 1500 });
    router.push({ name: "designer" });
  } catch (error) {
    console.error("Failed to create flow from template:", error);
    ElMessage.error("Failed to create flow. Please check your internet connection.");
  } finally {
    creatingTemplateId.value = null;
  }
};

onMounted(loadTemplates);
</script>

<style scoped>
.templates-view {
  padding: 32px;
  max-width: 1200px;
  margin: 0 auto;
}

.templates-view__header {
  margin-bottom: 24px;
}

.templates-view__title {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 24px;
  font-weight: 600;
  color: var(--color-text-primary);
  margin: 0 0 8px 0;
}

.templates-view__title .material-icons {
  font-size: 28px;
  color: var(--color-primary);
}

.templates-view__subtitle {
  font-size: 14px;
  color: var(--color-text-secondary);
  line-height: 1.5;
  margin: 0;
}

.templates-view__subtitle a {
  color: var(--color-primary);
  text-decoration: none;
}

.templates-view__subtitle a:hover {
  text-decoration: underline;
}

.templates-view__filters {
  margin-bottom: 24px;
}

.templates-view__grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
  gap: 20px;
}

.templates-view__loading,
.templates-view__empty {
  text-align: center;
  padding: 48px;
  color: var(--color-text-secondary);
}
</style>
