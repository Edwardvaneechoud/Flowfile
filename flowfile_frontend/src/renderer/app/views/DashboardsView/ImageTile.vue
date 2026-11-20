<template>
  <div
    class="image-tile"
    :class="{ 'image-tile-empty': !src, 'image-tile-drop': dropOver }"
    @dragover="onDragOver"
    @dragleave="dropOver = false"
    @drop="onDrop"
  >
    <!-- Only ever an <img> source: a browser runs no script from an SVG shown this way. -->
    <img
      v-if="src"
      class="image-tile-img"
      :class="[`image-tile-${fit}`, { 'image-tile-svg': isSvg }]"
      :src="src"
      :alt="tile.image?.alt ?? ''"
      :data-tile-id="tile.id"
      draggable="false"
    />
    <div v-else-if="mode === 'edit'" class="image-tile-placeholder">
      <el-button size="small" :loading="busy" data-testid="image-choose" @click.stop="chooseImage">
        <el-icon v-if="!busy"><Picture /></el-icon>
        <span>Choose image</span>
      </el-button>
      <span class="image-tile-hint">or drop an image file here</span>
    </div>
    <input
      v-if="mode === 'edit'"
      ref="inputEl"
      type="file"
      hidden
      :accept="IMAGE_ACCEPT"
      data-testid="image-file-input"
      @change="onFileChosen"
    />
  </div>
</template>

<script setup lang="ts">
import { computed, ref } from "vue";
import { ElMessage } from "element-plus";
import { Picture } from "@element-plus/icons-vue";
import type { DashboardTile } from "../../types";
import { useDashboardImageBudget } from "../../composables/useDashboardImageBudget";
import {
  IMAGE_ACCEPT,
  NOT_AN_IMAGE_MESSAGE,
  dataUrlInfo,
  draggedFiles,
  imageMimeOf,
  withImage,
} from "./imageTile";
import { ImageFileError, readImageFile } from "./readImageFile";

const props = defineProps<{
  tile: DashboardTile;
  mode: "edit" | "view";
}>();

const emit = defineEmits<{
  (e: "update:tile", value: DashboardTile): void;
}>();

const budget = useDashboardImageBudget();
const inputEl = ref<HTMLInputElement | null>(null);
const busy = ref(false);
const dropOver = ref(false);

const src = computed(() => props.tile.image?.data_url || null);
const fit = computed(() => props.tile.image?.fit ?? "contain");
const isSvg = computed(() => !!src.value && dataUrlInfo(src.value)?.mime === "image/svg+xml");

const take = async (file: File) => {
  busy.value = true;
  try {
    const url = await readImageFile(file, budget(props.tile.id));
    emit("update:tile", withImage(props.tile, url));
  } catch (err) {
    ElMessage.warning(err instanceof ImageFileError ? err.message : "The image could not be read.");
  } finally {
    busy.value = false;
  }
};

/** Opens the file picker; a browser allows it only from a click. */
const chooseImage = () => {
  if (!inputEl.value || busy.value) return;
  inputEl.value.value = "";
  inputEl.value.click();
};

const onFileChosen = () => {
  const file = inputEl.value?.files?.[0];
  if (file) void take(file);
};

// Only an empty tile takes a file; over a filled one the drop reaches the canvas.
const takesDrop = (e: DragEvent): boolean =>
  props.mode === "edit" &&
  !src.value &&
  !!e.dataTransfer &&
  draggedFiles(Array.from(e.dataTransfer.types), Array.from(e.dataTransfer.items)) !== "none";

const onDragOver = (e: DragEvent) => {
  if (!takesDrop(e)) return;
  e.preventDefault();
  e.stopPropagation();
  if (e.dataTransfer) e.dataTransfer.dropEffect = "copy";
  dropOver.value = true;
};

const onDrop = (e: DragEvent) => {
  dropOver.value = false;
  if (!takesDrop(e)) return;
  e.preventDefault();
  e.stopPropagation();
  const file = Array.from(e.dataTransfer?.files ?? []).find((f) => imageMimeOf(f) !== null);
  if (file) void take(file);
  else ElMessage.info(NOT_AN_IMAGE_MESSAGE);
};

defineExpose({ chooseImage });
</script>

<style scoped>
.image-tile {
  width: 100%;
  height: 100%;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
}
.image-tile-img {
  display: block;
  user-select: none;
}
/* Its own size or smaller, never stretched past it. */
.image-tile-contain {
  max-width: 100%;
  max-height: 100%;
  object-fit: contain;
}
.image-tile-cover {
  width: 100%;
  height: 100%;
  object-fit: cover;
}
/* An SVG may have no size of its own and scales without loss, so it takes the box. */
.image-tile-svg.image-tile-contain {
  width: 100%;
  height: 100%;
}
.image-tile-empty {
  border-radius: 6px;
}
.image-tile-drop {
  outline: 2px dashed var(--el-color-primary);
  outline-offset: -2px;
  background: var(--el-color-primary-light-9);
}
.image-tile-placeholder {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 4px;
  max-width: 100%;
  padding: 4px;
  text-align: center;
}
/* A narrow tile shortens the label instead of clipping the button. */
.image-tile-placeholder :deep(.el-button) {
  max-width: 100%;
}
.image-tile-placeholder :deep(.el-button > span) {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
}
.image-tile-hint {
  font-size: 11px;
  color: var(--el-text-color-secondary);
}
</style>
