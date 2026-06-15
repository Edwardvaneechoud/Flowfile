<template>
  <el-dialog
    :model-value="modelValue"
    :title="title"
    width="78%"
    top="6vh"
    @update:model-value="(v: boolean) => emit('update:modelValue', v)"
  >
    <div v-if="loading" class="ws-muted">Loading diff…</div>
    <p v-else-if="!lines.length" class="ws-muted">No changes.</p>
    <div v-else class="ws-diff">
      <div v-for="(line, i) in lines" :key="i" class="ws-diff-line" :class="line.cls">
        {{ line.text || " " }}
      </div>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = defineProps<{
  modelValue: boolean;
  title: string;
  diff: string;
  loading: boolean;
}>();
const emit = defineEmits<{ (e: "update:modelValue", value: boolean): void }>();

function classify(line: string): string {
  if (line.startsWith("+++") || line.startsWith("---")) return "ws-diff-meta";
  if (line.startsWith("@@")) return "ws-diff-hunk";
  if (line.startsWith("diff ") || line.startsWith("index ")) return "ws-diff-meta";
  if (line.startsWith("+")) return "ws-diff-add";
  if (line.startsWith("-")) return "ws-diff-del";
  return "ws-diff-ctx";
}

const lines = computed(() =>
  props.diff ? props.diff.split("\n").map((text) => ({ text, cls: classify(text) })) : [],
);
</script>
