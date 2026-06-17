<script setup lang="ts">
withDefaults(defineProps<{ label?: string }>(), { label: "Thinking" });
</script>

<template>
  <span class="ai-thinking-dots" role="status" :aria-label="label">
    <span class="ai-thinking-dots__dot" />
    <span class="ai-thinking-dots__dot" />
    <span class="ai-thinking-dots__dot" />
  </span>
</template>

<style scoped>
.ai-thinking-dots {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  height: 12px;
}

.ai-thinking-dots__dot {
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background-color: var(--color-text-tertiary, #a0aec0);
  opacity: 0.3;
  animation: ai-thinking-pulse 1.2s infinite ease-in-out;
}

.ai-thinking-dots__dot:nth-child(1) {
  animation-delay: 0s;
}
.ai-thinking-dots__dot:nth-child(2) {
  animation-delay: 0.2s;
}
.ai-thinking-dots__dot:nth-child(3) {
  animation-delay: 0.4s;
}

@keyframes ai-thinking-pulse {
  0%,
  60%,
  100% {
    opacity: 0.3;
    transform: translateY(0);
  }
  30% {
    opacity: 1;
    transform: translateY(-2px);
  }
}

@media (prefers-reduced-motion: reduce) {
  .ai-thinking-dots__dot {
    animation: ai-thinking-fade 1.4s infinite ease-in-out;
    transform: none !important;
  }
  @keyframes ai-thinking-fade {
    0%,
    100% {
      opacity: 0.3;
    }
    50% {
      opacity: 1;
    }
  }
}
</style>
