<template>
  <el-dialog v-model="isVisible" title="About Flowfile" width="440px" align-center>
    <div class="about-body">
      <img src="/images/flowfile.png" alt="Flowfile logo" class="about-logo" />
      <h2 class="about-title">Flowfile</h2>
      <span v-if="version" class="about-version">v{{ version }}</span>
      <p class="about-description">
        Flowfile is an open-source visual ETL platform. Design data pipelines on a drag-and-drop
        canvas — or in Python with the Polars-style FlowFrame API — then run, schedule, and share
        them from the built-in data catalog.
      </p>
      <div class="about-links">
        <button class="about-link" @click="openDocs">
          <i class="fa-solid fa-book"></i>
          <span>Documentation</span>
        </button>
        <a
          class="about-link"
          href="https://github.com/edwardvaneechoud/Flowfile"
          target="_blank"
          rel="noopener"
        >
          <i class="fa-brands fa-github"></i>
          <span>GitHub</span>
        </a>
        <a
          class="about-link"
          href="https://github.com/edwardvaneechoud/Flowfile/discussions"
          target="_blank"
          rel="noopener"
        >
          <i class="fa-solid fa-comments"></i>
          <span>Discussions</span>
        </a>
      </div>
      <p class="about-license">Released under the MIT License.</p>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, watch } from "vue";
import { useRouter } from "vue-router";

const props = defineProps<{ visible: boolean; version?: string }>();

const emit = defineEmits<{
  (e: "update:visible", value: boolean): void;
}>();

const router = useRouter();
const isVisible = ref(props.visible);

watch(
  () => props.visible,
  (v) => {
    isVisible.value = v;
  },
);

watch(isVisible, (v) => {
  if (v !== props.visible) emit("update:visible", v);
});

function openDocs() {
  isVisible.value = false;
  router.push({ name: "documentation" });
}
</script>

<style scoped>
.about-body {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  gap: var(--spacing-2);
  padding: var(--spacing-2) var(--spacing-4) var(--spacing-2);
}

.about-logo {
  width: 64px;
  height: auto;
}

.about-title {
  margin: 0;
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.about-version {
  font-size: var(--font-size-xs);
  font-family: var(--font-family-mono);
  color: var(--color-text-tertiary);
  background-color: var(--color-background-tertiary);
  border-radius: var(--border-radius-full);
  padding: 2px var(--spacing-2);
}

.about-description {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-sm);
  line-height: var(--line-height-relaxed);
  color: var(--color-text-secondary);
}

.about-links {
  display: flex;
  justify-content: center;
  gap: var(--spacing-2);
  margin-top: var(--spacing-3);
  flex-wrap: wrap;
}

.about-link {
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-2);
  padding: var(--spacing-1-5) var(--spacing-3);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: var(--border-radius-md);
  cursor: pointer;
  font-family: inherit;
  font-size: var(--font-size-sm);
  color: var(--color-text-primary);
  text-decoration: none;
  transition: all var(--transition-fast);
}

.about-link:hover {
  background-color: var(--color-background-tertiary);
  border-color: var(--color-accent);
  color: var(--color-accent);
}

.about-license {
  margin: var(--spacing-3) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-muted);
}
</style>
