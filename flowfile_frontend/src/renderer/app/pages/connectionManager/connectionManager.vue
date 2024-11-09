<template>
  <button>Create new connection</button>
  <button>Browse existing connections</button>
  {{ main }}
</template>

<script setup lang="ts">
import { ref } from "vue";
const main = ref<string>("");
</script>
