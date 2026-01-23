<template>
  <div class="parquet-table-settings">
    <div class="message">
      <h2>You are ready to flow!</h2>
      <p>Your Parquet table setup is complete. Enjoy the smooth data processing experience.</p>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { InputParquetTable } from "../../../baseNode/nodeInput";

defineProps<{
  modelValue: InputParquetTable;
}>();
</script>

<style scoped>
.parquet-table-settings {
  background: var(--color-background-secondary);
  border-radius: 8px;
  padding: 20px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  max-width: 600px;
  margin: 20px auto;
  text-align: center; /* Center the text */
}

.message h2 {
  font-size: 24px;
  color: #3498db;
  margin-bottom: 10px;
}

.message p {
  font-size: 16px;
  color: var(--color-text-primary);
}
</style>
