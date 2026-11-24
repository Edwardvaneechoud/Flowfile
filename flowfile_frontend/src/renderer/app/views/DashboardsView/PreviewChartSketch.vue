<template>
  <div class="sketch" :class="`sketch--${kind}`" aria-hidden="true">
    <div v-if="kind === 'bar'" class="sketch-bars">
      <span
        v-for="(h, i) in bars"
        :key="i"
        class="sketch-bar"
        :style="{ height: `${h}%`, background: color(0) }"
      ></span>
    </div>

    <svg
      v-else-if="kind === 'line' || kind === 'area'"
      class="sketch-svg"
      viewBox="0 0 100 50"
      preserveAspectRatio="none"
    >
      <polygon v-if="kind === 'area'" :points="area" :fill="color(0)" fill-opacity="0.28" />
      <polyline
        :points="line"
        fill="none"
        :stroke="color(0)"
        stroke-width="1.6"
        stroke-linejoin="round"
        vector-effect="non-scaling-stroke"
      />
      <polyline
        v-if="kind === 'line'"
        :points="secondLine"
        fill="none"
        :stroke="color(1)"
        stroke-width="1.6"
        stroke-linejoin="round"
        vector-effect="non-scaling-stroke"
      />
    </svg>

    <div v-else-if="kind === 'point' || kind === 'map'" class="sketch-dots">
      <span
        v-for="(d, i) in dots"
        :key="i"
        class="sketch-dot"
        :style="{ left: `${d.x}%`, top: `${d.y}%`, background: color(d.slot) }"
      ></span>
    </div>

    <svg v-else-if="kind === 'arc'" class="sketch-svg" viewBox="0 0 42 42">
      <circle
        v-for="s in slices"
        :key="s.slot"
        cx="21"
        cy="21"
        r="15.9155"
        fill="none"
        :stroke="color(s.slot)"
        stroke-width="7"
        :stroke-dasharray="`${s.share * 100} ${100 - s.share * 100}`"
        :stroke-dashoffset="25 - s.offset * 100"
      />
    </svg>

    <div v-else-if="kind === 'boxplot'" class="sketch-boxes">
      <span v-for="(b, i) in boxList" :key="i" class="sketch-box-col">
        <span
          class="sketch-whisker"
          :style="{ bottom: `${b.low}%`, height: `${b.high - b.low}%` }"
        ></span>
        <span
          class="sketch-box"
          :style="{ bottom: `${b.q1}%`, height: `${b.q3 - b.q1}%`, borderColor: color(0) }"
        ></span>
      </span>
    </div>

    <div v-else class="sketch-heat">
      <span
        v-for="(v, i) in cells"
        :key="i"
        :style="{ background: color(0), opacity: 0.15 + v * 0.75 }"
      ></span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";
import {
  arcSlices,
  areaToPoints,
  barHeights,
  boxes,
  heatCells,
  linePoints,
  lineToPoints,
  mapDots,
  scatterDots,
  type SketchKind,
} from "./previewSketch";

const props = defineProps<{
  kind: SketchKind;
  /** Picks the sketch's shape, so a chart looks the same on every render. */
  seed: number;
  /** The categorical palette for the current theme. */
  palette: string[];
}>();

const color = (slot: number) => props.palette[slot % props.palette.length];

const bars = computed(() => barHeights(props.seed));
const points = computed(() => linePoints(props.seed));
const line = computed(() => lineToPoints(points.value));
const area = computed(() => areaToPoints(points.value));
const secondLine = computed(() =>
  lineToPoints(linePoints(props.seed + 7919).map(([x, y]) => [x, Math.min(48, y + 6)])),
);
const dots = computed(() => (props.kind === "map" ? mapDots(props.seed) : scatterDots(props.seed)));
const slices = computed(() => arcSlices(props.seed));
const boxList = computed(() => boxes(props.seed));
const cells = computed(() => heatCells(props.seed));
</script>

<style scoped>
.sketch {
  position: relative;
  width: 100%;
  height: 100%;
}
.sketch-bar,
.sketch-dot,
.sketch-box-col,
.sketch-heat > span {
  display: block;
}
.sketch-bars {
  display: flex;
  align-items: flex-end;
  gap: 6%;
  height: 100%;
  padding: 0 4%;
  border-bottom: 1px solid var(--preview-rule);
}
.sketch-bar {
  flex: 1;
  max-width: 22%;
  border-radius: 1px 1px 0 0;
}
.sketch-svg {
  display: block;
  width: 100%;
  height: 100%;
}
.sketch--line .sketch-svg,
.sketch--area .sketch-svg {
  border-bottom: 1px solid var(--preview-rule);
}
.sketch-dots {
  position: relative;
  height: 100%;
  border-left: 1px solid var(--preview-rule);
  border-bottom: 1px solid var(--preview-rule);
}
.sketch--map .sketch-dots {
  border: none;
  border-radius: 2px;
  background:
    linear-gradient(var(--preview-grid) 1px, transparent 1px) 0 0 / 100% 25%,
    linear-gradient(90deg, var(--preview-grid) 1px, transparent 1px) 0 0 / 16.66% 100%,
    var(--preview-water);
}
.sketch-dot {
  position: absolute;
  width: 4px;
  height: 4px;
  margin: -2px 0 0 -2px;
  border-radius: 50%;
  opacity: 0.85;
}
.sketch-boxes {
  display: flex;
  gap: 10%;
  height: 100%;
  padding: 0 6%;
  border-bottom: 1px solid var(--preview-rule);
}
.sketch-box-col {
  position: relative;
  flex: 1;
}
.sketch-whisker {
  position: absolute;
  left: 50%;
  width: 1px;
  background: var(--preview-rule);
}
.sketch-box {
  position: absolute;
  left: 15%;
  right: 15%;
  border: 1.5px solid;
  border-radius: 1px;
  background: var(--preview-tile);
}
.sketch-heat {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  grid-template-rows: repeat(4, 1fr);
  gap: 2px;
  height: 100%;
}
.sketch-heat > span {
  border-radius: 1px;
}
</style>
