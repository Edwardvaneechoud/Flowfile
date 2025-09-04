<template>
  <node-button
      ref="nodeButton"
      :node-id="nodeId"
      image-src="cross_join.png"
      :title="`${nodeId}: Cross Join`"
    :drawer-component="markRaw(CrossJoin)"
    :node-title-info=nodeTitleInfo
  />
</template>

<script setup lang="ts">

import { markRaw } from "vue";
import CrossJoin from "./elements/crossJoin/crossJoin.vue";
import NodeButton from "../baseNode/nodeButton.vue";

const nodeTitleInfo = {
title: "Join", intro:"Combine datasets based on one or more columns."
}

const props = defineProps({
nodeId: {
  type: Number,
  required: true,
},
});
</script>
