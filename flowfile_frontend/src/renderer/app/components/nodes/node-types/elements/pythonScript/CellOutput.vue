<template>
  <div class="cell-output" v-if="output">
    <!-- Execution meta (time, count) -->
    <div class="output-meta" v-if="output.execution_count">
      [{{ output.execution_count }}] {{ formatTime(output.execution_time_ms) }}
    </div>

    <!-- Error block — show prominently -->
    <div v-if="output.error" class="output-error">
      <pre>{{ output.error }}</pre>
    </div>

    <!-- Display outputs -->
    <div
      v-for="(disp, index) in output.display_outputs"
      :key="index"
      class="display-item"
    >
      <div v-if="disp.title" class="display-title">{{ disp.title }}</div>

      <!-- Image rendering -->
      <img
        v-if="disp.mime_type === 'image/png'"
        :src="'data:image/png;base64,' + disp.data"
        class="display-image"
        alt="Output image"
      />

      <!-- HTML rendering (plotly, custom HTML) -->
      <iframe
        v-else-if="disp.mime_type === 'text/html'"
        :srcdoc="disp.data"
        class="display-iframe"
        sandbox="allow-scripts"
        @load="autoResizeIframe($event)"
      ></iframe>

      <!-- Plain text — NO v-html for security -->
      <div v-else-if="disp.mime_type === 'text/plain'" class="display-text">
        <pre>{{ disp.data }}</pre>
      </div>

      <!-- Fallback for other mime types -->
      <div v-else class="display-text">
        <pre>{{ disp.data }}</pre>
      </div>
    </div>

    <!-- stdout -->
    <div v-if="output.stdout" class="output-stdout">
      <pre>{{ output.stdout }}</pre>
    </div>

    <!-- stderr — hide when there's an error (error contains traceback) -->
    <div v-if="output.stderr && !output.error" class="output-stderr">
      <pre>{{ output.stderr }}</pre>
    </div>
  </div>
</template>

<script lang="ts" setup>
import type { CellOutput } from "../../../../../types/node.types";

interface Props {
  output: CellOutput;
}

defineProps<Props>();

const formatTime = (ms: number): string => {
  if (ms < 1) return "<1ms";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
};

const autoResizeIframe = (event: Event) => {
  const iframe = event.target as HTMLIFrameElement;
  try {
    const height = iframe.contentWindow?.document?.body?.scrollHeight;
    if (height) {
      iframe.style.height = `${Math.min(height + 20, 600)}px`;
    }
  } catch {
    // Cross-origin restriction — keep default height
  }
};
</script>

<style scoped>
.cell-output {
  padding: 0.25rem 0 0.25rem 0.5rem;
  font-size: 0.8rem;
}

.output-meta {
  font-size: 0.7rem;
  color: var(--el-text-color-secondary);
  text-align: right;
  padding-right: 0.5rem;
  margin-bottom: 0.15rem;
}

.output-error pre {
  background: #2d1117;
  color: #f85149;
  padding: 0.5rem 0.75rem;
  border-radius: 3px;
  font-size: 0.8rem;
  font-family: 'Fira Code', monospace;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
  margin: 0;
}

.display-image {
  max-width: 100%;
  border-radius: 3px;
  background: white; /* for transparent PNGs */
}

.display-iframe {
  width: 100%;
  min-height: 300px;
  border: none;
  border-radius: 3px;
}

.display-text pre {
  background: #1e1e2e;
  color: #cdd6f4;
  padding: 0.5rem 0.75rem;
  border-radius: 3px;
  font-size: 0.8rem;
  font-family: 'Fira Code', monospace;
  overflow-x: auto;
  white-space: pre-wrap;
  margin: 0;
}

.output-stdout pre,
.output-stderr pre {
  font-size: 0.8rem;
  font-family: 'Fira Code', monospace;
  padding: 0.25rem 0.5rem;
  white-space: pre-wrap;
  max-height: 200px;
  overflow-y: auto;
  margin: 0;
}

.output-stdout pre {
  color: var(--el-text-color-regular);
}

.output-stderr pre {
  color: #e6a23c;
}

.display-title {
  font-size: 0.75rem;
  font-weight: 500;
  color: var(--el-text-color-primary);
  margin: 0.25rem 0;
}

.display-item {
  margin-bottom: 0.5rem;
}

.display-item:last-child {
  margin-bottom: 0;
}
</style>
