<template>
  <el-popover
    v-if="showValidationMessages"
    placement="top"
    width="200"
    trigger="hover"
    content="Some required fields are missing"
  >
    <div class="validation-wrapper">
      <p v-if="!unpivotInput.index_columns" class="error-message">Index Column cannot be empty.</p>
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
import { UnpivotInput } from "../../../baseNode/nodeInput";
import { ElPopover, ElIcon } from "element-plus";

// Define props to receive pivotInput from parent
const props = defineProps<{ unpivotInput: UnpivotInput }>();

// Computed property to determine if there are validation errors
const showValidationMessages = computed(() => {
  return !(props.unpivotInput.index_columns.length === 0);
});
</script>

<style scoped>
.validation-wrapper {
  background-color: var(--color-background-primary);
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
