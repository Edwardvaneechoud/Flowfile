<template>
  <button class="btn btn-secondary" @click="showReference = true">
    <i class="fa-solid fa-circle-question"></i>
    Help
  </button>

  <Teleport to="body">
    <ComponentReferenceModal :show="showReference" @close="showReference = false" />
  </Teleport>
</template>

<script setup lang="ts">
import { ref } from "vue";
import ComponentReferenceModal from "../ComponentReferenceModal.vue";

const showReference = ref(false);
</script>
