<template>
  <div class="format-table-settings">
    <div class="input-group">
      <label>Compression:</label>
      <el-select
        v-model="localTable.compression"
        placeholder="Select compression"
        size="small"
        style="max-width: 200px"
        @change="updateParent"
      >
        <el-option
          v-for="option in compressionOptions"
          :key="option"
          :label="option"
          :value="option"
        />
      </el-select>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { ref, watch } from "vue";
import { ElSelect, ElOption } from "element-plus";
import "element-plus/dist/index.css";
import { OutputAvroTable } from "../../../baseNode/nodeInput";

const props = defineProps({
  modelValue: {
    type: Object as () => OutputAvroTable,
    required: true,
  },
});

const emit = defineEmits(["update:modelValue"]);
const localTable = ref(props.modelValue);
const compressionOptions = ["uncompressed", "snappy", "deflate"];

const updateParent = () => {
  emit("update:modelValue", localTable.value);
};

watch(
  () => props.modelValue,
  (newVal) => {
    localTable.value = newVal;
  },
  { deep: true },
);
</script>

<style scoped>
@import "element-plus/dist/index.css";

.format-table-settings {
  width: 100%;
  max-width: 600px;
  margin: 20px auto;
  background: var(--color-background-primary);
  border-radius: 8px;
  padding: 20px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.input-group {
  margin-bottom: 20px;
}

.input-group label {
  display: block;
  margin-bottom: 5px;
  font-weight: bold;
  color: var(--color-text-primary);
}

.el-select {
  width: 100%;
}
</style>
