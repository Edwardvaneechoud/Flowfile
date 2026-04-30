<template>
  <div
    class="tile"
    :class="{
      'tile-text': isText,
      'tile-text-view': isText && mode === 'view',
    }"
  >
    <div v-if="!isText" class="tile-header" :class="{ 'tile-handle': mode === 'edit' }">
      <span class="tile-title">{{ headerTitle }}</span>
      <div class="tile-header-actions">
        <span v-if="lastError" class="tile-error" :title="lastError"
          ><el-icon><Warning /></el-icon
        ></span>
        <el-icon v-if="mode === 'edit'" class="tile-drag-icon" title="Drag to move"
          ><Rank
        /></el-icon>
        <el-dropdown v-if="mode === 'edit'" trigger="click" @click.stop>
          <el-icon class="tile-menu" @click.stop><MoreFilled /></el-icon>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item :disabled="tile.viz_id == null" @click="emit('edit-viz')">
                <el-icon><Edit /></el-icon> Edit chart
              </el-dropdown-item>
              <el-dropdown-item divided @click="emit('remove')">
                <el-icon><Delete /></el-icon> Remove from dashboard
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>

    <div v-if="isText && mode === 'edit' && !textEditing" class="tile-text-actions" @mousedown.stop>
      <el-dropdown trigger="click" @click.stop>
        <el-icon class="tile-menu"><MoreFilled /></el-icon>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item @click="toggleTextEdit">
              <el-icon><EditPen /></el-icon> Edit text
            </el-dropdown-item>
            <el-dropdown-item divided @click="emit('remove')">
              <el-icon><Delete /></el-icon> Remove from dashboard
            </el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
    </div>

    <div class="tile-body" :class="{ 'tile-handle': isText && mode === 'edit' && !textEditing }">
      <template v-if="isText">
        <textarea
          v-if="mode === 'edit' && textEditing"
          ref="textareaEl"
          v-model="textDraft"
          class="text-editor"
          placeholder="# Heading&#10;&#10;Markdown supported."
          @blur="commitText"
        />
        <div
          v-else-if="!renderedHtml"
          class="tile-state tile-text-empty"
          :title="mode === 'edit' ? 'Double-click to edit' : undefined"
          @dblclick="enterTextEdit"
        >
          <el-icon><EditPen /></el-icon>
          <span>{{ mode === "edit" ? "Double-click to add content." : "Empty text block" }}</span>
        </div>
        <!-- eslint-disable vue/no-v-html -- renderedHtml is DOMPurify-sanitised marked output -->
        <div
          v-else
          class="text-rendered"
          :title="mode === 'edit' ? 'Double-click to edit' : undefined"
          @dblclick="enterTextEdit"
          v-html="renderedHtml"
        />
        <!-- eslint-enable vue/no-v-html -->
      </template>
      <template v-else>
        <div
          class="tile-viz-body"
          :class="{ 'tile-viz-clickable': vizClickable }"
          :title="vizClickable ? 'Double-click to edit chart' : undefined"
          @dblclick="onVizDblclick"
        >
          <button
            v-if="vizClickable"
            class="tile-viz-edit-badge"
            type="button"
            title="Edit chart"
            @click.stop="emit('edit-viz')"
          >
            <el-icon><Edit /></el-icon>
          </button>
          <div v-if="loading" class="tile-state">
            <el-skeleton :rows="3" animated />
          </div>
          <div v-else-if="vizMissing" class="tile-state tile-missing">
            <el-icon><WarningFilled /></el-icon>
            <span>Visualization #{{ tile.viz_id }} no longer exists.</span>
          </div>
          <div v-else-if="!chart" class="tile-state">
            <el-empty description="No chart spec at this index" :image-size="48" />
          </div>
          <VueGraphicRenderer
            v-else
            :key="rendererKey"
            :chart="chart as any"
            :fields="fields as any"
            :computation="computation"
            :appearance="appearance"
          />
        </div>
      </template>
    </div>
  </div>
</template>

<script setup lang="ts">
// TODO(refactor): ~484 LOC. Plan to extract:
//   - Move :deep() markdown styles (~lines 399-484) to an external _markdown.css import
//   - TextTile.vue (~62-88) and VizTile.vue (~91-108) child components
//   - useDashboardTileViz composable: viz loading (~lines 155-234)
import { computed, nextTick, onMounted, ref, toRef, watch } from "vue";
import { marked } from "marked";
import DOMPurify from "dompurify";
import {
  Delete,
  Edit,
  EditPen,
  MoreFilled,
  Rank,
  Warning,
  WarningFilled,
} from "@element-plus/icons-vue";
import { CatalogApi } from "../../api/catalog.api";
import {
  useDashboardComputation,
  filtersTargetingTile,
} from "../../composables/useDashboardComputation";
import type { CatalogVisualization, DashboardFilter, DashboardTile } from "../../types";
import VueGraphicRenderer from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicRenderer.vue";

const props = defineProps<{
  tile: DashboardTile;
  mode: "edit" | "view";
  appearance: "light" | "dark" | "media";
  filters: DashboardFilter[];
  /** Bumped by the parent after a viz edit so the renderer remounts and re-fetches. */
  vizRefreshNonce?: number;
  /** Resolver for the tile's underlying CatalogTable id (used to gate datasource-bound filters). */
  tileDatasource?: (tileId: string) => number | null;
}>();

const emit = defineEmits<{
  (e: "remove"): void;
  (e: "edit-viz"): void;
  (e: "update:tile", value: DashboardTile): void;
}>();

const isText = computed(() => props.tile.type === "text");

// ---- viz branch (unchanged behaviour) ----
const viz = ref<CatalogVisualization | null>(null);
const fields = ref<Record<string, any>[]>([]);
const loading = ref(true);
const vizMissing = ref(false);

// View-mode shortcut: double-click the viz body or click the hover badge to
// jump straight into the chart editor. Edit mode already exposes "Edit chart"
// in the tile dropdown, and dragging dominates there, so we don't compete.
const vizClickable = computed(
  () => !isText.value && props.mode === "view" && props.tile.viz_id != null && !vizMissing.value,
);

const onVizDblclick = (e: MouseEvent) => {
  if (!vizClickable.value) return;
  e.stopPropagation();
  emit("edit-viz");
};

const tileRef = toRef(props, "tile");
const filtersRef = toRef(props, "filters");

const { computation, lastError } = useDashboardComputation({
  tile: tileRef,
  filters: filtersRef,
  tileDatasource: (id) => props.tileDatasource?.(id) ?? null,
  onMissing: () => {
    vizMissing.value = true;
  },
});

const vizName = computed(() => {
  if (viz.value?.name) return viz.value.name;
  if (loading.value) return "Loading…";
  if (props.tile.viz_id != null) return `Visualization #${props.tile.viz_id}`;
  return "Untitled";
});

const chart = computed(() => {
  if (!viz.value) return null;
  return viz.value.spec?.[props.tile.chart_index] ?? null;
});

const filterKey = computed(() => {
  const targeted = filtersTargetingTile(
    props.filters,
    props.tile.id,
    (id) => props.tileDatasource?.(id) ?? null,
  );
  return JSON.stringify(targeted.map((f) => [f.id, f.state]));
});
const rendererKey = computed(
  () => `${props.tile.id}:${props.tile.viz_id}:${props.vizRefreshNonce ?? 0}:${filterKey.value}`,
);

const reloadViz = async () => {
  if (isText.value) {
    loading.value = false;
    return;
  }
  loading.value = true;
  vizMissing.value = false;
  if (props.tile.viz_id == null) {
    viz.value = null;
    fields.value = [];
    loading.value = false;
    return;
  }
  try {
    const [vizResp, fieldsResp] = await Promise.all([
      CatalogApi.getVisualization(props.tile.viz_id),
      CatalogApi.getSavedVisualizationFields(props.tile.viz_id),
    ]);
    viz.value = vizResp;
    fields.value = fieldsResp.fields ?? [];
  } catch (err: any) {
    if (err?.response?.status === 404) {
      vizMissing.value = true;
    } else {
      console.error("Tile load failed:", err);
    }
  } finally {
    loading.value = false;
  }
};

onMounted(reloadViz);

watch(
  () => [props.tile.viz_id, props.tile.type, props.vizRefreshNonce],
  () => {
    reloadViz();
  },
);

// ---- text branch ----
const textEditing = ref(false);
const textDraft = ref(props.tile.text_md ?? "");
const textareaEl = ref<HTMLTextAreaElement | null>(null);

watch(
  () => props.tile.text_md,
  (v) => {
    if (!textEditing.value) textDraft.value = v ?? "";
  },
);

const renderedHtml = computed(() => {
  const md = props.tile.text_md ?? "";
  if (!md.trim()) return "";
  // marked v15 returns string for sync parse; cast keeps TS happy across overloads.
  const raw = marked.parse(md, { async: false }) as string;
  return DOMPurify.sanitize(raw);
});

const commitText = () => {
  const next = textDraft.value;
  if (next !== (props.tile.text_md ?? "")) {
    emit("update:tile", { ...props.tile, text_md: next });
  }
  textEditing.value = false;
};

const enterTextEdit = async () => {
  if (props.mode !== "edit" || !isText.value || textEditing.value) return;
  textEditing.value = true;
  await nextTick();
  textareaEl.value?.focus();
};

const toggleTextEdit = async () => {
  if (textEditing.value) {
    commitText();
  } else {
    await enterTextEdit();
  }
};

const headerTitle = computed(() => vizName.value);
</script>

<style scoped>
.tile {
  position: relative;
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--el-bg-color);
  overflow: hidden;
}
.tile-text-actions {
  position: absolute;
  top: 4px;
  right: 4px;
  z-index: 2;
  display: flex;
  align-items: center;
  padding: 2px 4px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 4px;
  background: var(--el-bg-color);
  color: var(--el-text-color-secondary);
  cursor: default;
  opacity: 0;
  transition: opacity 0.15s;
}
.tile:hover .tile-text-actions {
  opacity: 1;
}
/* In view mode text tiles look like part of the page, not a card. */
.tile-text-view {
  border: none;
  background: transparent;
}
.tile-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 6px 10px;
  font-size: 12px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  background: var(--el-fill-color-light);
}
.tile-handle {
  cursor: move;
}
.tile-title {
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.tile-header-actions {
  display: flex;
  align-items: center;
  gap: 6px;
  color: var(--el-text-color-secondary);
}
.tile-drag-icon,
.tile-menu,
.tile-action-icon {
  cursor: pointer;
}
.tile-action-icon:hover {
  color: var(--el-color-primary);
}
.tile-error {
  color: var(--el-color-warning);
  cursor: help;
}
.tile-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  padding: 4px;
}
.tile-viz-body {
  position: relative;
  height: 100%;
  display: flex;
  flex-direction: column;
}
.tile-viz-clickable {
  cursor: pointer;
  border-radius: 4px;
  transition: outline 0.12s ease;
}
.tile-viz-clickable:hover {
  outline: 1px solid var(--el-color-primary-light-5);
  outline-offset: -1px;
}
.tile-viz-edit-badge {
  position: absolute;
  top: 6px;
  right: 6px;
  z-index: 2;
  width: 26px;
  height: 26px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 4px;
  background: var(--el-bg-color);
  color: var(--el-text-color-secondary);
  cursor: pointer;
  opacity: 0;
  transition:
    opacity 0.15s,
    color 0.15s,
    border-color 0.15s;
}
.tile:hover .tile-viz-edit-badge,
.tile-viz-edit-badge:focus-visible {
  opacity: 1;
}
.tile-viz-edit-badge:hover {
  color: var(--el-color-primary);
  border-color: var(--el-color-primary-light-5);
}
.tile-text-view .tile-body {
  padding: 0;
}
.tile-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--el-text-color-secondary);
  font-size: 12px;
  gap: 6px;
}
.tile-text-empty {
  font-style: italic;
}
.tile-missing {
  color: var(--el-color-warning);
}

.text-editor {
  width: 100%;
  height: 100%;
  border: none;
  outline: none;
  resize: none;
  padding: 8px 12px;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
  font-size: 13px;
  line-height: 1.5;
  background: var(--el-bg-color);
  color: var(--el-text-color-regular);
  box-sizing: border-box;
}

.text-rendered {
  height: 100%;
  overflow: auto;
  padding: 10px 14px;
  font-size: 14px;
  line-height: 1.5;
  color: var(--el-text-color-primary);
}
.text-rendered :deep(h1) {
  font-size: 22px;
  font-weight: 600;
  margin: 0 0 8px;
}
.text-rendered :deep(h2) {
  font-size: 18px;
  font-weight: 600;
  margin: 12px 0 6px;
}
.text-rendered :deep(h3) {
  font-size: 16px;
  font-weight: 600;
  margin: 10px 0 4px;
}
.text-rendered :deep(h4),
.text-rendered :deep(h5),
.text-rendered :deep(h6) {
  font-size: 14px;
  font-weight: 600;
  margin: 8px 0 4px;
}
.text-rendered :deep(p) {
  margin: 6px 0;
}
.text-rendered :deep(ul),
.text-rendered :deep(ol) {
  padding-left: 22px;
  margin: 6px 0;
}
.text-rendered :deep(li) {
  margin: 2px 0;
}
.text-rendered :deep(code) {
  background: var(--el-fill-color-light);
  padding: 1px 4px;
  border-radius: 3px;
  font-size: 12.5px;
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, monospace;
}
.text-rendered :deep(pre) {
  background: var(--el-fill-color-light);
  padding: 8px 12px;
  border-radius: 4px;
  overflow: auto;
  margin: 8px 0;
}
.text-rendered :deep(pre code) {
  background: transparent;
  padding: 0;
}
.text-rendered :deep(a) {
  color: var(--el-color-primary);
  text-decoration: none;
}
.text-rendered :deep(a:hover) {
  text-decoration: underline;
}
.text-rendered :deep(blockquote) {
  border-left: 3px solid var(--el-border-color);
  padding-left: 12px;
  margin: 8px 0;
  color: var(--el-text-color-secondary);
}
.text-rendered :deep(img) {
  max-width: 100%;
}
.text-rendered :deep(hr) {
  border: 0;
  border-top: 1px solid var(--el-border-color-lighter);
  margin: 12px 0;
}
.text-rendered :deep(table) {
  border-collapse: collapse;
  margin: 8px 0;
}
.text-rendered :deep(th),
.text-rendered :deep(td) {
  border: 1px solid var(--el-border-color-lighter);
  padding: 4px 8px;
  text-align: left;
}
.text-rendered :deep(th) {
  background: var(--el-fill-color-light);
}
</style>
