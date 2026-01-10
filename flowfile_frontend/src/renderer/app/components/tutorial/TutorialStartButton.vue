<script setup lang="ts">
import { ref, computed, onMounted } from "vue";
import { useRoute } from "vue-router";
import { useTutorialStore } from "../../stores/tutorial-store";
import { useNodeStore } from "../../stores/column-store";
import { gettingStartedTutorial } from "./tutorials";

const DISMISSED_KEY = "flowfile-tutorial-dismissed";

const tutorialStore = useTutorialStore();
const nodeStore = useNodeStore();
const route = useRoute();

const isDismissed = ref(false);

onMounted(() => {
  isDismissed.value = localStorage.getItem(DISMISSED_KEY) === "true";
});

// Only show when on designer page, no flow is open, tutorial is not active, and not dismissed
const showButton = computed(() => {
  const isDesignerPage = route.name === "designer";
  const hasNoFlow = !nodeStore.flow_id || nodeStore.flow_id <= 0;
  return isDesignerPage && hasNoFlow && !tutorialStore.isActive && !isDismissed.value;
});

function startTutorial() {
  tutorialStore.startTutorial(gettingStartedTutorial);
}

function dismissButton() {
  isDismissed.value = true;
  localStorage.setItem(DISMISSED_KEY, "true");
}
</script>

<template>
  <Teleport to="body">
    <Transition name="float-in">
      <div v-if="showButton" class="tutorial-start-container">
        <button class="tutorial-dismiss-btn" @click="dismissButton" title="Dismiss">
          <span class="material-icons">close</span>
        </button>
        <button class="tutorial-start-btn" @click="startTutorial">
          <span class="btn-icon material-icons">school</span>
          <div class="btn-content">
            <span class="btn-title">New to Flowfile?</span>
            <span class="btn-subtitle">Start the interactive tutorial</span>
          </div>
          <span class="btn-arrow material-icons">arrow_forward</span>
        </button>
      </div>
    </Transition>
  </Teleport>
</template>

<style scoped>
.tutorial-start-container {
  position: fixed;
  bottom: 24px;
  right: 24px;
  z-index: 1000;
}

.tutorial-dismiss-btn {
  position: absolute;
  top: -8px;
  right: -8px;
  width: 24px;
  height: 24px;
  padding: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-primary);
  border-radius: 50%;
  color: var(--color-text-secondary);
  cursor: pointer;
  transition: all 0.2s ease;
  z-index: 1;
}

.tutorial-dismiss-btn:hover {
  background: var(--color-background-tertiary);
  color: var(--color-text-primary);
}

.tutorial-dismiss-btn .material-icons {
  font-size: 16px;
}

.tutorial-start-btn {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 16px 20px;
  background: var(--color-accent);
  border: none;
  border-radius: 12px;
  color: var(--color-text-on-accent, #fff);
  cursor: pointer;
  box-shadow:
    0 4px 12px rgba(0, 0, 0, 0.15),
    0 0 0 1px rgba(255, 255, 255, 0.1) inset;
  transition: all 0.2s ease;
}

.tutorial-start-btn:hover {
  transform: translateY(-2px);
  box-shadow:
    0 8px 20px rgba(0, 0, 0, 0.2),
    0 0 0 1px rgba(255, 255, 255, 0.15) inset;
}

.tutorial-start-btn:active {
  transform: translateY(0);
}

.btn-icon {
  font-size: 28px;
  opacity: 0.9;
}

.btn-content {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  text-align: left;
}

.btn-title {
  font-size: 15px;
  font-weight: 600;
  line-height: 1.2;
}

.btn-subtitle {
  font-size: 12px;
  opacity: 0.85;
  font-weight: 400;
}

.btn-arrow {
  font-size: 20px;
  opacity: 0.7;
  margin-left: 4px;
}

/* Animation */
.float-in-enter-active {
  animation: float-in 0.4s ease-out;
}

.float-in-leave-active {
  animation: float-in 0.3s ease-in reverse;
}

@keyframes float-in {
  from {
    opacity: 0;
    transform: translateY(20px) scale(0.95);
  }
  to {
    opacity: 1;
    transform: translateY(0) scale(1);
  }
}
</style>
