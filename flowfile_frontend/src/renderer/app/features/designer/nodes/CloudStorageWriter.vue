
<template>
  <node-button
      ref="nodeButton"
      :node-id="nodeId"
      image-src="cloud_storage_writer.png"
      :title="`${nodeId}: Cloud storage writer`"
    :drawer-component="markRaw(CloudStorageWriter)"
    :node-title-info=nodeTitleInfo
  />
</template>

<script setup lang="ts">

import { markRaw } from "vue";
import CloudStorageWriter from "./elements/cloudStorageWriter/CloudStorageWriter.vue";
import NodeButton from "../baseNode/nodeButton.vue";

const nodeTitleInfo = {
title: "Write data to cloud provider", intro: "Write data to cloud provider"
}

const props = defineProps({
nodeId: {
  type: Number,
  required: true,
},
});
</script>
