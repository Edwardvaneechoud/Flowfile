<template>
  <div class="header">{{ title }}</div>
</template>

<script>
import { defineComponent } from "vue";

export default defineComponent({
  props: {
    title: {
      type: String,
      required: true,
    },
  },
});
</script>

<style scoped>
div {
  margin-bottom: 2px;
}
</style>
