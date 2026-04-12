<template>
  <div class="sql-explore-panel">
    <VueGraphicWalker :data="gwData" :fields="gwFields" />
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import VueGraphicWalker from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/VueGraphicWalker.vue";
import type {
  IMutField,
  IRow,
  ISemanticType,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/interfaces";
import type { SqlQueryResult } from "../../types";

const props = defineProps<{
  result: SqlQueryResult;
}>();

function getSemanticType(dtype: string): ISemanticType {
  const d = dtype.toLowerCase();
  if (
    d.includes("utf8") ||
    d.includes("string") ||
    d.includes("categorical") ||
    d.includes("bool")
  ) {
    return "nominal";
  }
  if (
    d.includes("int") ||
    d.includes("float") ||
    d.includes("decimal") ||
    d.includes("uint") ||
    d.includes("duration")
  ) {
    return "quantitative";
  }
  if (d.includes("date") || d.includes("time")) {
    return "temporal";
  }
  return "nominal";
}

const gwFields = computed<IMutField[]>(() =>
  props.result.columns.map((col, idx) => {
    const semanticType = getSemanticType(props.result.dtypes[idx] ?? "");
    return {
      fid: col,
      name: col,
      basename: col,
      key: col,
      semanticType,
      analyticType: semanticType === "quantitative" ? "measure" : "dimension",
    };
  }),
);

const gwData = computed<IRow[]>(() =>
  props.result.rows.map((row) => {
    const obj: Record<string, any> = {};
    props.result.columns.forEach((col, idx) => {
      obj[col] = row[idx];
    });
    return obj;
  }),
);
</script>

<style scoped>
.sql-explore-panel {
  width: 100%;
  height: 100%;
}
</style>
