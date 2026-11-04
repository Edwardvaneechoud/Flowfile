<template>
  <span v-if="isShared(resource)" class="resource-badge" :title="sharedTitle(resource)">
    <i class="fa-solid fa-share-nodes"></i>
    {{ sharedLabel(resource) }}
  </span>
</template>

<script setup lang="ts">
import { computed } from "vue";
import { useResourceSharing } from "../../composables/useResourceSharing";
import type { AccessInfo } from "../../types/sharing.types";

const props = defineProps<{ access?: AccessInfo | null }>();

const { isShared, sharedLabel, sharedTitle } = useResourceSharing();
const resource = computed(() => ({ access: props.access }));
</script>
