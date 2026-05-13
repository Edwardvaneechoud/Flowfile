<script setup lang="ts">
withDefaults(defineProps<{ size?: "sm" | "md" | "lg" }>(), { size: "md" });
</script>

<template>
  <span :class="['ai-avatar', `ai-avatar--${size}`]" aria-hidden="true">
    <span class="material-icons ai-avatar__glyph">auto_awesome</span>
  </span>
</template>

<style scoped>
.ai-avatar {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  border-radius: 50%;
  background: linear-gradient(
    135deg,
    var(--color-gradient-purple-start, #667eea) 0%,
    var(--color-gradient-purple-end, #764ba2) 100%
  );
  color: #ffffff;
  line-height: 1;
}

.ai-avatar--sm {
  width: 18px;
  height: 18px;
}
.ai-avatar--md {
  width: 22px;
  height: 22px;
}
.ai-avatar--lg {
  width: 32px;
  height: 32px;
  box-shadow: 0 2px 6px rgba(102, 126, 234, 0.25);
}

.ai-avatar__glyph {
  font-size: 12px;
  line-height: 1;
}
.ai-avatar--sm .ai-avatar__glyph {
  font-size: 10px;
}
.ai-avatar--md .ai-avatar__glyph {
  font-size: 13px;
}
.ai-avatar--lg .ai-avatar__glyph {
  font-size: 18px;
}
</style>
