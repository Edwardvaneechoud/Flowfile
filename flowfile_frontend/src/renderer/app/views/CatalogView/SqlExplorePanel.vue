<template>
  <div :class="['sql-explore-panel', { 'is-fullscreen': isFullscreen }]">
    <button
      class="fullscreen-toggle"
      :title="isFullscreen ? 'Exit fullscreen (Esc)' : 'Fullscreen'"
      @click="toggleFullscreen"
    >
      <i :class="isFullscreen ? 'fa-solid fa-compress' : 'fa-solid fa-expand'"></i>
    </button>
    <VueGraphicWalker :data="gwData" :fields="gwFields" default-tab="data" />
  </div>
</template>

<script setup lang="ts">
import { computed, ref, onBeforeUnmount, watch } from "vue";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import type {
  IMutField,
  IRow,
  ISemanticType,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/interfaces";
import type { SqlQueryResult } from "../../types";

const props = defineProps<{
  result: SqlQueryResult;
}>();

const isFullscreen = ref(false);

function toggleFullscreen() {
  isFullscreen.value = !isFullscreen.value;
}

function handleEsc(e: KeyboardEvent) {
  if (e.key === "Escape" && isFullscreen.value) {
    isFullscreen.value = false;
  }
}

watch(isFullscreen, (val) => {
  if (val) {
    window.addEventListener("keydown", handleEsc);
    document.body.style.overflow = "hidden";
  } else {
    window.removeEventListener("keydown", handleEsc);
    document.body.style.overflow = "";
  }
});

onBeforeUnmount(() => {
  window.removeEventListener("keydown", handleEsc);
  document.body.style.overflow = "";
});

function getSemanticType(dtype: string): ISemanticType {
  const d = dtype.toLowerCase();
  if (
    d.includes("utf8") ||
    d.includes("string") ||
    d.includes("categorical") ||
    d.includes("bool")
  ) {
    return "nominal";
  }
  if (
    d.includes("int") ||
    d.includes("float") ||
    d.includes("decimal") ||
    d.includes("uint") ||
    d.includes("duration")
  ) {
    return "quantitative";
  }
  if (d.includes("date") || d.includes("time")) {
    return "temporal";
  }
  return "nominal";
}

const gwFields = computed<IMutField[]>(() =>
  props.result.columns.map((col, idx) => {
    const semanticType = getSemanticType(props.result.dtypes[idx] ?? "");
    return {
      fid: col,
      name: col,
      basename: col,
      key: col,
      semanticType,
      analyticType: semanticType === "quantitative" ? "measure" : "dimension",
    };
  }),
);

const gwData = computed<IRow[]>(() =>
  props.result.rows.map((row) => {
    const obj: Record<string, any> = {};
    props.result.columns.forEach((col, idx) => {
      obj[col] = row[idx];
    });
    return obj;
  }),
);
</script>

<style scoped>
.sql-explore-panel {
  width: 100%;
  height: 100%;
  position: relative;
}

.sql-explore-panel.is-fullscreen {
  position: fixed;
  inset: 0;
  z-index: 2000;
  background: var(--el-bg-color, #ffffff);
  padding: 8px;
}

.fullscreen-toggle {
  position: absolute;
  top: 8px;
  right: 12px;
  z-index: 10;
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--color-border-light, #e4e7ed);
  border-radius: 4px;
  background: var(--el-bg-color, #ffffff);
  color: var(--color-text-secondary, #606266);
  cursor: pointer;
  font-size: 13px;
  transition:
    background 0.15s,
    color 0.15s;
}

.fullscreen-toggle:hover {
  background: var(--color-background-hover, #f0f0f0);
  color: var(--el-color-primary, #409eff);
}

.is-fullscreen .fullscreen-toggle {
  top: 12px;
  right: 16px;
}
</style>
