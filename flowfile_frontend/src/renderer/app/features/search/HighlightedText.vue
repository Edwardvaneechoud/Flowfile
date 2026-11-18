<script setup lang="ts">
import { computed } from "vue";
import { highlightSegments } from "./highlight";

const props = defineProps<{ text: string; tokens: readonly string[] }>();
const segments = computed(() => highlightSegments(props.text, props.tokens));
</script>

<template>
  <span class="highlighted-text"
    ><template v-for="(segment, index) in segments" :key="index"
      ><mark v-if="segment.match" class="highlighted-text__match">{{ segment.text }}</mark
      ><template v-else>{{ segment.text }}</template></template
    ></span
  >
</template>

<style scoped>
.highlighted-text__match {
  background: transparent;
  color: var(--color-accent);
  font-weight: var(--font-weight-semibold);
  padding: 0;
}
</style>
