
<template>
  <node-button
    ref="nodeButton"
    :node-id="nodeId"
    image-src="cloud_storage_reader.png"
    :title="`${nodeId}: Cloud storage reader`"
    :drawer-component="markRaw(CloudStorageReader)"
    :node-title-info=nodeTitleInfo
  />
</template>

<script setup lang="ts">


import { markRaw } from "vue";
import CloudStorageReader from "./elements/cloudStorageReader/CloudStorageReader.vue";
import NodeButton from "../baseNode/nodeButton.vue";

const nodeTitleInfo = {
  title:"Read data from cloud provider", 
  intro:"Read data from cloud provider"
}

const props = defineProps({
nodeId: {
  type: Number,
  required: true,
},
});
</script>
