<template>
  <node-button
    ref="nodeButton"
    :node-id="nodeId"
    image-src="filter.png"
    :title="`${nodeId}: Filter`"
    :drawer-component="markRaw(filterInput)"
    :node-title-info=nodeTitleInfo
  />
</template>

<script setup lang="ts">


import { markRaw } from "vue";
import filterInput from "./elements/filter/filter.vue";
import NodeButton from "../baseNode/nodeButton.vue";

const nodeTitleInfo = {
  title:"Filter data",
  intro:"Filter rows in the data" 
}
const props = defineProps({
nodeId: {
  type: Number,
  required: true,
},
});
</script>
