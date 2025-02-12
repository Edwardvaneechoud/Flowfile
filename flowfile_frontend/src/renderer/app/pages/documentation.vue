<template>
  <div class="doc-wrapper">
    <button class="flowfile-button" @click="openFlowfile">Open Flowfile in New Tab</button>
    <iframe :src="docsUrl" style="width: 100%; height: 90vh; border: none"></iframe>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

const docsUrl = computed(() =>
  import.meta.env.MODE === "development"
    ? "http://127.0.0.1:8000/"
    : "https://edwardvaneechoud.github.io/Flowfile/",
);

const openFlowfile = () => {
  window.open(docsUrl.value);
};
</script>

<style scoped>
.doc-wrapper {
  width: 100%;
  height: 100vh;
}

/* Modern button styling */
.flowfile-button {
  background-color: var(--color-primary);
  color: var(--color-background);
  padding: var(--spacing-sm) var(--spacing-md);
  border: none;
  border-radius: var(--border-radius-sm);
  cursor: pointer;
  font-size: 16px;
  margin-bottom: 10px; /* spacing between button and iframe */
  transition:
    background-color 0.3s ease,
    transform 0.2s ease,
    box-shadow 0.3s ease;
}

.flowfile-button:hover {
  background-color: var(--color-primary-dark);
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
}

.flowfile-button:active {
  transform: translateY(0);
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
}

.flowfile-button:focus {
  outline: none;
  /* Adjust the focus ring color as desired */
  box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.5);
}
</style>
