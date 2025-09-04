<template>
    <node-button
      ref="nodeButton"
      :node-id="nodeId"
      image-src="manual_input.png"
      :title="`${nodeId}: Manual input`"
      :drawer-component="markRaw(manualInput)"
      :node-title-info=nodeTitleInfo
    />
</template>

<script setup lang="ts">

const nodeTitleInfo = {
  title:"Provide manual input",
  intro:"Provide a fixed data that can be used and combined with other tables."
}

import { markRaw } from "vue";
import NodeButton from "../baseNode/nodeButton.vue";
import manualInput from "./elements/manualInput/manualInput.vue";

const props = defineProps({
  nodeId: {
    type: Number,
    required: true,
  },
});
</script>
