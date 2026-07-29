<template>
  <el-tooltip :content="tooltip" placement="bottom">
    <span class="save-dot" :class="`is-${tone}`" role="status" :aria-label="tooltip" />
  </el-tooltip>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from "vue";
import { AUTOSAVE_STALE_UNSAVED_MS, type AutosaveState } from "../../composables/autosaveEngine";
import { CATALOG_PERMISSION_MESSAGE } from "../../composables/saveError";

const props = defineProps<{
  state: AutosaveState;
  mode?: "autosave" | "draft";
  /** Draft mode only: whether a local draft has been written this session. */
  draftSaved?: boolean;
}>();

// Tick only while unsaved changes exist so the dot can flip to stale.
const now = ref(Date.now());
let ticker: ReturnType<typeof setInterval> | null = null;
watch(
  () => props.state.dirtySince,
  (dirtySince) => {
    if (ticker) {
      clearInterval(ticker);
      ticker = null;
    }
    if (dirtySince != null) {
      now.value = Date.now();
      ticker = setInterval(() => {
        now.value = Date.now();
      }, 1000);
    }
  },
  { immediate: true },
);
onBeforeUnmount(() => {
  if (ticker) clearInterval(ticker);
});

const staleUnsaved = computed(() => {
  const dirtySince = props.state.dirtySince;
  return dirtySince != null && now.value - dirtySince >= AUTOSAVE_STALE_UNSAVED_MS;
});

const tone = computed<"clean" | "saved" | "stale">(() => {
  if (props.mode === "draft") return props.draftSaved ? "saved" : "clean";
  if (staleUnsaved.value) return "stale";
  return props.state.lastSavedAt != null ? "saved" : "clean";
});

const savedAtLabel = () => {
  if (!props.state.lastSavedAt) return "";
  return new Date(props.state.lastSavedAt).toLocaleTimeString(undefined, {
    hour: "numeric",
    minute: "2-digit",
  });
};

const tooltip = computed(() => {
  if (props.mode === "draft") {
    return props.draftSaved
      ? "Draft — saved on this device"
      : "Saves a draft on this device as you work";
  }
  const s = props.state;
  if (staleUnsaved.value) {
    if (s.status === "conflict") return "Changed somewhere else — choose a version in the banner.";
    if (s.status === "blocked") {
      if (s.blockKind === "gone") return "This item was deleted. Your changes can't be saved.";
      if (s.blockKind === "auth") return "Your session expired — sign in again to keep saving.";
      return CATALOG_PERMISSION_MESSAGE;
    }
    return s.lastError ? `Changes not saved yet — ${s.lastError}` : "Changes not saved yet.";
  }
  if (s.status !== "idle") return "Saving automatically…";
  return s.lastSavedAt ? `All changes saved — ${savedAtLabel()}` : "Changes save automatically";
});
</script>

<style scoped>
.save-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
  cursor: default;
  transition: background-color 0.3s ease;
}
.save-dot.is-clean {
  background: var(--el-text-color-disabled);
}
.save-dot.is-saved {
  background: var(--el-color-primary);
}
.save-dot.is-stale {
  background: var(--el-color-warning);
}
</style>
