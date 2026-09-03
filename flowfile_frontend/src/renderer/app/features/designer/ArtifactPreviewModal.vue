<template>
  <el-dialog
    v-model="visible"
    :title="dialogTitle"
    width="70%"
    top="6vh"
    append-to-body
    destroy-on-close
  >
    <div v-if="payload" class="artifact-preview-body">
      <img
        v-if="payload.mime_type === 'image/png'"
        :src="'data:image/png;base64,' + payload.data"
        class="artifact-preview-image"
        alt="Artifact preview"
      />

      <!-- Sandboxed srcdoc has an opaque origin, so its height can't be measured
           from outside — give it a viewport-based height instead. allow-downloads
           lets plotly's "download as png" toolbar button work. -->
      <iframe
        v-else-if="payload.mime_type === 'text/html'"
        :srcdoc="payload.data"
        class="artifact-preview-iframe"
        sandbox="allow-scripts allow-downloads"
      ></iframe>

      <pre v-else class="artifact-preview-text">{{ payload.data }}</pre>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed } from "vue";
import type { DisplayOutput } from "@/types";

interface Props {
  modelValue: boolean;
  payload: DisplayOutput | null;
  name?: string;
}

const props = defineProps<Props>();
const emit = defineEmits<{ (e: "update:modelValue", value: boolean): void }>();

const visible = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit("update:modelValue", value),
});

const dialogTitle = computed(() => props.payload?.title || props.name || "Preview");
</script>

<style scoped>
.artifact-preview-body {
  overflow: auto;
}

.artifact-preview-image {
  display: block;
  max-width: 100%;
  max-height: 72vh;
  margin: 0 auto;
  border-radius: 3px;
  background: white;
}

.artifact-preview-iframe {
  width: 100%;
  height: 72vh;
  border: none;
  border-radius: 3px;
  background: var(--color-background-primary);
}

.artifact-preview-text {
  background: #1e1e2e;
  color: #cdd6f4;
  padding: 0.5rem 0.75rem;
  border-radius: 3px;
  font-size: 0.8rem;
  font-family: "Fira Code", monospace;
  overflow-x: auto;
  white-space: pre-wrap;
  max-height: 72vh;
  overflow-y: auto;
  margin: 0;
}
</style>
