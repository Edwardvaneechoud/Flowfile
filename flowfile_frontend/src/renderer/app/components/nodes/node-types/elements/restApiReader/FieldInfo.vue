<template>
  <el-tooltip effect="dark" placement="top" popper-class="field-info-popper">
    <template #content>
      <div class="field-info-content">
        <p class="field-info-text">{{ help.text }}</p>
        <div v-if="help.example" class="field-info-example">
          <span class="field-info-label">{{ help.example.label }}</span>
          <pre v-if="help.example.sample" class="field-info-sample">{{ help.example.sample }}</pre>
          <div class="field-info-rows">
            <template v-for="[value, result] in help.example.rows" :key="value">
              <span v-if="help.example.options" class="field-info-option">{{ value }}</span>
              <code v-else class="field-info-value">{{ value }}</code>
              <i class="fa-solid fa-arrow-right field-info-arrow"></i>
              <code class="field-info-result">{{ result }}</code>
            </template>
          </div>
        </div>
      </div>
    </template>
    <button type="button" class="field-info" :aria-label="help.text">
      <i class="fa-solid fa-circle-info"></i>
    </button>
  </el-tooltip>
</template>

<script lang="ts" setup>
import type { FieldHelp } from "./restApiView";

defineProps<{ help: FieldHelp }>();
</script>

<style scoped>
.field-info {
  padding: 0;
  border: none;
  background: none;
  color: var(--color-text-secondary, #888);
  font-size: 11px;
  line-height: 1;
  cursor: help;
}

.field-info:hover,
.field-info:focus-visible {
  color: var(--color-text-primary);
}

.field-info-text {
  margin: 0;
}

.field-info-example {
  margin-top: 8px;
  padding-top: 8px;
  border-top: 1px solid rgba(127, 127, 127, 0.35);
}

.field-info-label {
  display: block;
  margin-bottom: 4px;
  font-size: 10px;
  font-weight: 600;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  opacity: 0.7;
}

.field-info-sample,
.field-info-rows code {
  font-family: var(--font-family-mono, monospace);
  font-size: 11px;
}

.field-info-sample {
  margin: 0 0 6px;
  padding: 6px 8px;
  border-radius: 4px;
  background: rgba(127, 127, 127, 0.22);
  line-height: 1.4;
  white-space: pre;
}

.field-info-rows {
  display: grid;
  grid-template-columns: max-content max-content 1fr;
  gap: 4px 6px;
  align-items: baseline;
}

.field-info-value {
  justify-self: start;
  padding: 0 5px;
  border-radius: 3px;
  background: rgba(127, 127, 127, 0.3);
}

.field-info-option {
  font-weight: 600;
}

.field-info-arrow {
  font-size: 9px;
  opacity: 0.6;
}

.field-info-result {
  word-break: break-word;
}
</style>

<style>
.field-info-popper {
  max-width: 340px;
  line-height: 1.45;
}
</style>
