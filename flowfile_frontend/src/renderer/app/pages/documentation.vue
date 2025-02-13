<template>
  <div class="doc-wrapper">
    <iframe :src="docsUrl" class="iframe-docs"></iframe>
    <button class="flowfile-button" @click="openFlowfile">
      <i class="fas fa-up-right-from-square"></i> <!-- Pop-out icon -->
    </button>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

// Dynamically set the URL
const docsUrl = computed(() =>
  import.meta.env.MODE === "development"
    ? "http://127.0.0.1:8000/"
    : "https://edwardvaneechoud.github.io/Flowfile/"
);

const openFlowfile = () => {
  window.open(docsUrl.value);
};
</script>

<style scoped>
.doc-wrapper {
  width: 100%;
  height: 99vh;
}

.iframe-docs {
  width: 100%;
  height: 100%;
  border: none;
}

.flowfile-button {
  background-color: var(--color-primary);
  color: var(--color-background);
  border: none;
  border-radius: 50%;
  cursor: pointer;
  font-size: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 50px; /* Circular button */
  height: 50px;
  position: fixed; /* Keep it on top of everything */
  bottom: 20px; /* Distance from bottom */
  right: 20px; /* Distance from right */
  z-index: 1000; /* Make sure it’s above the iframe */
  box-shadow: 0 4px 10px rgba(0, 0, 0, 0.2);
  transition:
    background-color 0.3s ease,
    transform 0.2s ease,
    box-shadow 0.3s ease;
}

/* Hover & Active effects */
.flowfile-button:hover {
  background-color: var(--color-primary-dark);
  transform: scale(1.1);
}

.flowfile-button:active {
  transform: scale(1);
}

.flowfile-button:focus {
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 123, 255, 0.5);
}

/* Adjust icon size */
.flowfile-button i {
  font-size: 20px;
}
</style>
