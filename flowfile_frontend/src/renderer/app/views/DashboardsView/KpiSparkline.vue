<template>
  <div ref="rootRef" class="kpi-spark" :class="{ 'kpi-spark-dimmed': dimmed }">
    <svg
      v-if="width > 0 && height > 0"
      class="kpi-spark-svg"
      :width="width"
      :height="height"
      :viewBox="`0 0 ${width} ${height}`"
      role="img"
      :aria-label="description"
      tabindex="0"
      :data-points="points.length"
      @pointermove="onPointerMove"
      @pointerleave="onPointerLeave"
      @focus="onFocus"
      @blur="onBlur"
      @keydown="onKeydown"
    >
      <path class="kpi-spark-area" :d="geometry.area" :fill="color" />
      <path
        class="kpi-spark-line"
        :d="geometry.line"
        :stroke="color"
        fill="none"
        stroke-width="2"
        stroke-linejoin="round"
        stroke-linecap="round"
      />
      <line
        v-if="activePoint"
        class="kpi-spark-cursor"
        :x1="activePoint.x"
        :x2="activePoint.x"
        :y1="0"
        :y2="height"
      />
      <circle
        v-if="lastPoint"
        class="kpi-spark-marker"
        :cx="lastPoint.x"
        :cy="lastPoint.y"
        r="4"
        :fill="color"
      />
      <circle
        v-if="activePoint && activeIndex !== geometry.points.length - 1"
        class="kpi-spark-marker"
        :cx="activePoint.x"
        :cy="activePoint.y"
        r="4"
        :fill="color"
      />
    </svg>
    <div v-if="activeText" class="kpi-spark-tip" :style="tipStyle" aria-hidden="true">
      <span class="kpi-spark-tip-bucket">{{ activeText.bucket }}</span>
      <span class="kpi-spark-tip-value">{{ activeText.value }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useThemeStore } from "../../stores/theme-store";
import {
  CATEGORY_DARK,
  CATEGORY_LIGHT,
} from "../../components/nodes/node-types/elements/exploreData/vueGraphicWalker/vizTheme";
import type { DashboardKpi } from "../../types";
import {
  formatKpiValue,
  formatTrendBucket,
  trendDescription,
  type TrendGrain,
  type TrendPoint,
} from "./kpi";
import { nearestPointIndex, sparklineGeometry } from "./sparkline";

const props = defineProps<{
  points: TrendPoint[];
  grain: TrendGrain;
  format: Pick<DashboardKpi, "compact" | "decimals" | "prefix" | "suffix">;
  label: string;
  dimmed?: boolean;
  /** The tile's own text colour: on a custom background the palette blue may not read. */
  inkColor?: string | null;
}>();

const themeStore = useThemeStore();
const color = computed(
  () => props.inkColor || (themeStore.isDark ? CATEGORY_DARK[0] : CATEGORY_LIGHT[0]),
);

const rootRef = ref<HTMLElement | null>(null);
const width = ref(0);
const height = ref(0);
let observer: ResizeObserver | null = null;

onMounted(() => {
  const el = rootRef.value;
  if (!el) return;
  const measure = () => {
    width.value = Math.floor(el.clientWidth);
    height.value = Math.floor(el.clientHeight);
  };
  measure();
  observer = new ResizeObserver(measure);
  observer.observe(el);
});

onBeforeUnmount(() => observer?.disconnect());

const geometry = computed(() => sparklineGeometry(props.points, width.value, height.value));
const lastPoint = computed(() => geometry.value.points[geometry.value.points.length - 1] ?? null);

const description = computed(() => trendDescription(props.points, props.grain, props.label));

const activeIndex = ref<number | null>(null);
const focused = ref(false);

watch(
  () => props.points,
  () => {
    if (activeIndex.value != null && activeIndex.value >= props.points.length) {
      activeIndex.value = props.points.length ? props.points.length - 1 : null;
    }
  },
);

const activePoint = computed(() =>
  activeIndex.value == null ? null : (geometry.value.points[activeIndex.value] ?? null),
);

const activeText = computed(() => {
  const i = activeIndex.value;
  const p = i == null ? null : props.points[i];
  if (!p) return null;
  return {
    bucket: formatTrendBucket(p.t, props.grain),
    value: formatKpiValue(p.value, props.format),
  };
});

// Slides the tip from left-aligned at the left edge to right-aligned at the right edge, so it never clips.
const tipStyle = computed(() => {
  const x = activePoint.value?.x ?? 0;
  const share = width.value > 0 ? Math.min(1, Math.max(0, x / width.value)) : 0;
  return { left: `${x}px`, transform: `translateX(${-share * 100}%)` };
});

const onPointerMove = (e: PointerEvent) => {
  const svg = e.currentTarget as SVGSVGElement;
  const x = e.clientX - svg.getBoundingClientRect().left;
  const i = nearestPointIndex(geometry.value.points, x);
  activeIndex.value = i < 0 ? null : i;
};

const onPointerLeave = () => {
  if (!focused.value) activeIndex.value = null;
};

const onFocus = () => {
  focused.value = true;
  if (activeIndex.value == null && props.points.length) activeIndex.value = props.points.length - 1;
};

const onBlur = () => {
  focused.value = false;
  activeIndex.value = null;
};

const onKeydown = (e: KeyboardEvent) => {
  const n = props.points.length;
  if (!n) return;
  const current = activeIndex.value ?? n - 1;
  const next: Record<string, number> = {
    ArrowLeft: Math.max(0, current - 1),
    ArrowRight: Math.min(n - 1, current + 1),
    Home: 0,
    End: n - 1,
  };
  if (e.key in next) {
    e.preventDefault();
    e.stopPropagation();
    activeIndex.value = next[e.key];
  } else if (e.key === "Escape") {
    activeIndex.value = null;
  }
};
</script>

<style scoped>
.kpi-spark {
  position: relative;
  width: 100%;
  height: 100%;
  min-height: 0;
  transition: opacity 0.15s;
}
.kpi-spark-dimmed {
  opacity: 0.55;
}
.kpi-spark-svg {
  position: absolute;
  inset: 0;
  display: block;
  overflow: visible;
  outline: none;
  cursor: crosshair;
}
.kpi-spark-svg:focus-visible {
  outline: 2px solid var(--el-color-primary);
  outline-offset: 1px;
  border-radius: 2px;
}
.kpi-spark-area {
  fill-opacity: 0.12;
}
.kpi-spark-cursor {
  stroke: var(--el-text-color-secondary);
  stroke-width: 1;
  stroke-opacity: 0.5;
}
.kpi-spark-tip {
  position: absolute;
  bottom: 100%;
  margin-bottom: 2px;
  display: flex;
  gap: 6px;
  align-items: baseline;
  padding: 2px 6px;
  border-radius: 4px;
  border: 1px solid var(--el-border-color-light);
  background: var(--el-bg-color-overlay);
  box-shadow: var(--el-box-shadow-lighter);
  font-size: 11px;
  line-height: 1.4;
  white-space: nowrap;
  pointer-events: none;
  z-index: 1;
}
.kpi-spark-tip-bucket {
  color: var(--el-text-color-secondary);
}
.kpi-spark-tip-value {
  color: var(--el-text-color-primary);
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
</style>
