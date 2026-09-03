<template>
  <div v-if="errors.length > 0" class="spellcheck-popup">
    <div v-for="error in errors" :key="error.index" class="error-box">
      <i class="error-icon">&#9888;</i>
      <!-- Unicode warning sign -->
      <span><strong>Warning:</strong> {{ error.type }} at index {{ error.index }}.</span>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { toRefs } from "vue";

const props = defineProps<{
  errors: Array<{ type: string; index: number }>;
}>();

const { errors } = toRefs(props);
</script>

<style>
.spellcheck-popup {
  margin-top: 10px;
}

.error-box {
  border: 1px solid color-mix(in srgb, var(--color-danger) 35%, var(--color-danger-light));
  background-color: var(--color-danger-light);
  padding: 2px;
  margin: 4px 0;
  border-radius: 4px;
}

.error-icon {
  margin-right: 10px;
  color: var(--color-danger-dark);
}
</style>
