<template>
  <el-popover
    v-if="showValidationMessages"
    placement="top"
    width="200"
    trigger="hover"
    content="Some required fields are missing"
  >
    <div class="validation-wrapper">
      <p v-if="!pivotInput.pivot_column" class="error-message">Pivot Column cannot be empty.</p>
      <p v-if="!pivotInput.value_col" class="error-message">Value Column cannot be empty.</p>
      <p v-if="pivotInput.aggregations.length === 0" class="error-message">
        At least one aggregation must be selected.
      </p>
    </div>
    <template #reference>
      <el-icon color="#FF6B6B" class="warning-icon">
        <i class="el-icon-warning"></i>
      </el-icon>
    </template>
  </el-popover>
</template>

<script lang="ts" setup>
import { computed, defineProps } from "vue";
import { PivotInput } from "../../../baseNode/nodeInput";
import { ElPopover, ElIcon } from "element-plus";

// Define props to receive pivotInput from parent
const props = defineProps<{ pivotInput: PivotInput }>();

// Computed property to determine if there are validation errors
const showValidationMessages = computed(() => {
  return (
    !props.pivotInput.pivot_column ||
    !props.pivotInput.value_col ||
    props.pivotInput.aggregations.length === 0
  );
});
</script>

<style scoped>
.validation-wrapper {
  background-color: #ffffff;
}

.error-message {
  color: #991b1b;
  margin: 5px 0;
  font-size: 12px;
}

.warning-icon {
  cursor: pointer;
  font-size: 24px;
}
</style>
