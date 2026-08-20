<template>
  <div v-if="isAdmin" class="pool-root">
    <div v-if="loadError" class="pool-banner">
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ loadError }}</span>
    </div>

    <div class="pool-stats">
      <el-tooltip placement="top" :show-after="400" :content="serviceTooltip">
        <div class="pool-stat" :class="{ 'pool-stat--down': serviceDown }">
          <div class="pool-stat__icon">
            <i :class="serviceDown ? 'fa-solid fa-plug-circle-xmark' : 'fa-solid fa-server'"></i>
          </div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">{{ serviceStatusText }}</div>
            <div class="pool-stat__label">Worker service</div>
          </div>
        </div>
      </el-tooltip>

      <el-tooltip
        placement="top"
        :show-after="400"
        content="Node jobs currently executing, in warm or fresh processes."
      >
        <div class="pool-stat">
          <div class="pool-stat__icon"><i class="fa-solid fa-bolt"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">
              {{ serviceDown ? "—" : state ? state.activeTasks : "…" }}
            </div>
            <div class="pool-stat__label">Jobs running</div>
          </div>
        </div>
      </el-tooltip>

      <el-tooltip
        v-if="state && poolEnabled"
        placement="top"
        :show-after="400"
        :content="warmTooltip"
      >
        <div class="pool-stat">
          <div class="pool-stat__icon"><i class="fa-solid fa-layer-group"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">
              {{ serviceDown ? "—" : aliveCount
              }}<span v-if="!serviceDown" class="pool-stat__value-suffix">/ {{ state.size }}</span>
            </div>
            <div class="pool-stat__label">Warm processes</div>
            <div v-if="warmSub" class="pool-stat__sub">{{ warmSub }}</div>
          </div>
        </div>
      </el-tooltip>

      <el-tooltip
        v-if="state && !poolEnabled"
        placement="top"
        :show-after="400"
        content="Each node starts a fresh process. Turn the pool on to keep processes warm."
      >
        <div class="pool-stat">
          <div class="pool-stat__icon"><i class="fa-solid fa-layer-group"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">Off</div>
            <div class="pool-stat__label">Warm pool</div>
          </div>
        </div>
      </el-tooltip>

      <el-tooltip
        v-if="state && poolEnabled"
        placement="top"
        :show-after="400"
        content="Node jobs handled by warm processes since the worker service started."
      >
        <div class="pool-stat">
          <div class="pool-stat__icon"><i class="fa-solid fa-clipboard-check"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">{{ serviceDown ? "—" : state.tasksCompleted }}</div>
            <div class="pool-stat__label">Pool jobs served</div>
            <div class="pool-stat__sub">since worker start</div>
          </div>
        </div>
      </el-tooltip>
    </div>

    <div v-if="state" class="pool-config">
      <div class="pool-config__header">
        <div>
          <h3 class="pool-card-title">Warm worker pool</h3>
          <p class="pool-card-description">
            Keeps worker processes warm between data nodes, saving ~0.5 s of startup each. When all
            of them are busy, extra nodes start fresh processes.
          </p>
        </div>
        <div class="pool-toggle">
          <span class="pool-toggle__word">{{ poolEnabled ? "On" : "Off" }}</span>
          <el-switch
            v-model="poolEnabled"
            :disabled="loading || applying"
            aria-label="Warm worker pool"
            @change="onToggle"
          />
        </div>
      </div>

      <div v-if="poolEnabled" class="pool-size-row">
        <label class="pool-size-label" for="pool-size-input">Keep warm</label>
        <el-input-number
          id="pool-size-input"
          v-model="desiredSize"
          :min="1"
          :max="32"
          :disabled="applying"
          size="small"
        />
        <span class="pool-size-suffix">processes</span>
        <el-tooltip
          content="Applies immediately to the whole Flowfile instance and persists across restarts."
          placement="top"
          :show-after="400"
        >
          <span>
            <el-button
              type="primary"
              size="small"
              :loading="applying"
              :disabled="desiredSize === state.size"
              @click="apply(desiredSize)"
            >
              Apply
            </el-button>
          </span>
        </el-tooltip>
      </div>

      <div v-else class="pool-offstate">
        <p class="pool-offstate__line1">Each data node runs in a fresh, isolated worker process.</p>
        <p class="pool-offstate__line2">
          Turn it on to reuse up to {{ desiredSize }} warm processes between nodes.
        </p>
      </div>

      <p v-if="state.envOverride" class="pool-hint--warning">
        <i class="fa-solid fa-triangle-exclamation"></i>
        <span>
          <code>FLOWFILE_WORKER_POOL_SIZE</code> is set in the environment and overrides this
          setting when the worker restarts. Unset it to manage the pool size here.
        </span>
      </p>
    </div>

    <div v-if="state && poolEnabled" class="pool-processes">
      <h4 class="pool-processes__title">Pool processes</h4>

      <table v-if="state.members.length" class="pool-members">
        <thead>
          <tr>
            <th>PID</th>
            <th>State</th>
            <th :title="jobsServedTitle">Jobs served</th>
            <th :title="idleForTitle">Idle for</th>
            <th :title="memoryTitle">Memory</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="member in state.members" :key="member.pid">
            <td>{{ member.pid }}</td>
            <td>
              <span :class="['pool-member-state', `pool-member-state--${member.state}`]">
                {{ memberStateLabel(member.state) }}
              </span>
            </td>
            <td>
              {{
                state.maxTasksPerMember > 0
                  ? `${member.tasksServed} / ${state.maxTasksPerMember}`
                  : member.tasksServed
              }}
            </td>
            <td>{{ member.state === "idle" ? formatIdle(member.idleSeconds) : "—" }}</td>
            <td>{{ member.rssMb === null ? "—" : `${Math.round(member.rssMb)} MB` }}</td>
          </tr>
        </tbody>
      </table>

      <div v-if="state.members.length" class="pool-lifecycle">{{ lifecycleLine }}</div>

      <div v-else class="pool-empty">
        <i class="fa-solid fa-moon"></i>
        <div class="pool-empty__title">No warm processes</div>
        <div class="pool-empty__hint">{{ emptyHint }}</div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from "vue";
import { ElMessage } from "element-plus";
import { getWorkerPool, setWorkerPool, type WorkerPoolState } from "../../api/system.api";
import { useAuthStore } from "../../stores/auth-store";

const POLL_INTERVAL_MS = 2000;

const authStore = useAuthStore();
// The backend admin-gates /system/worker_pool; hide the card for everyone else.
const isAdmin = computed(() => authStore.isAdmin);

const state = ref<WorkerPoolState | null>(null);
const poolEnabled = ref(false);
const desiredSize = ref(4);
const loading = ref(true);
const applying = ref(false);
const loadError = ref("");
const pollMisses = ref(0);

let pollTimer: ReturnType<typeof setInterval> | null = null;

const hydrate = (next: WorkerPoolState) => {
  pollMisses.value = 0;
  state.value = next;
  poolEnabled.value = next.enabled;
  // Fresh state proves the worker is reachable; drop any stale banner.
  loadError.value = "";
};

const syncSizeInput = (next: WorkerPoolState) => {
  // Only load() and a successful apply() may write the size input: the 2s poll
  // must never clobber an edit the user has not applied yet.
  if (next.size > 0) desiredSize.value = next.size;
};

const load = async () => {
  if (!isAdmin.value) return;
  loading.value = true;
  loadError.value = "";
  try {
    const next = await getWorkerPool();
    hydrate(next);
    syncSizeInput(next);
  } catch {
    loadError.value = "Can't reach the worker service. Reconnecting automatically.";
  } finally {
    loading.value = false;
  }
};

const poll = async () => {
  // Silent refresh: never clobber an in-flight apply, and keep errors quiet
  // (the next successful poll clears any stale banner via hydrate).
  if (!isAdmin.value || applying.value) return;
  try {
    hydrate(await getWorkerPool());
  } catch {
    pollMisses.value += 1; // counted for the service tile only
  }
};

const apply = async (size: number) => {
  applying.value = true;
  try {
    const next = await setWorkerPool(size);
    hydrate(next);
    syncSizeInput(next);
    ElMessage.success(
      size === 1
        ? "Warm pool set to 1 process"
        : size > 0
          ? `Warm pool set to ${size} processes`
          : "Warm pool disabled",
    );
  } catch {
    ElMessage.error("Failed to update the worker pool");
    await load();
  } finally {
    applying.value = false;
  }
};

const onToggle = (enabled: boolean | string | number) => {
  void apply(enabled ? desiredSize.value : 0);
};

const serviceTooltip = "Runs all regular data nodes. Checked every 2 seconds.";

const serviceDown = computed(() => loadError.value !== "" || pollMisses.value >= 3);
const serviceStatusText = computed(() =>
  serviceDown.value ? "Unreachable" : state.value ? "Connected" : "…",
);
const aliveCount = computed(() => (state.value ? state.value.idle + state.value.busy : 0));
const warmSub = computed(() => {
  if (!state.value || serviceDown.value) return "";
  if (aliveCount.value === 0) return "start on demand";
  return `${state.value.idle} ready · ${state.value.busy} busy`;
});
const ttlText = computed<string | null>(() => {
  const s = state.value?.idleTtlSeconds ?? 0;
  if (s <= 0) return null;
  if (s < 120) return `${Math.round(s)} seconds`;
  return `${Math.round(s / 60)} minutes`;
});
const memText = computed<string | null>(() => {
  const mb = state.value?.rssLimitMb ?? 0;
  if (mb <= 0) return null;
  if (mb >= 1024) {
    const gb = mb / 1024;
    return Number.isInteger(gb) ? `${gb} GB` : `${gb.toFixed(1)} GB`;
  }
  return `${mb} MB`;
});
const formatIdle = (s: number) =>
  s < 60 ? `${Math.round(s)}s` : `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
const memberStateLabel = (s: "idle" | "busy") => (s === "idle" ? "Ready" : "Busy");

const maxTasksPerMember = computed(() => state.value?.maxTasksPerMember ?? 0);

const warmTooltip = computed(() =>
  ttlText.value
    ? `Live processes out of the configured pool size. They start on demand and retire after ${ttlText.value} idle.`
    : "Live processes out of the configured pool size. They start on demand and retire when idle.",
);

const jobsServedTitle = computed(() =>
  maxTasksPerMember.value > 0
    ? `A process is recycled after ${maxTasksPerMember.value} jobs.`
    : undefined,
);
const idleForTitle = computed(() =>
  ttlText.value ? `Idle processes retire after ${ttlText.value}.` : undefined,
);
const memoryTitle = computed(() =>
  memText.value ? `A process is recycled if it grows past ${memText.value}.` : undefined,
);

const lifecycleLine = computed(() => {
  const clauses: string[] = [];
  if (ttlText.value) clauses.push(`${ttlText.value} idle`);
  if (maxTasksPerMember.value > 0) clauses.push(`${maxTasksPerMember.value} jobs`);
  if (clauses.length === 0) return "Warm processes start on demand and retire when idle.";
  return `Warm processes retire after ${clauses.join(" or ")}.`;
});

const emptyHint = computed(() =>
  ttlText.value
    ? `Processes start when a flow runs and retire after ${ttlText.value} idle.`
    : "Processes start when a flow runs and retire when idle.",
);

onMounted(() => {
  void load();
  pollTimer = setInterval(poll, POLL_INTERVAL_MS);
});

onUnmounted(() => {
  if (pollTimer !== null) clearInterval(pollTimer);
});
</script>

<style scoped>
.pool-root {
  display: flex;
  flex-direction: column;
}

.pool-banner {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  padding: var(--spacing-3) var(--spacing-4);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
  line-height: var(--line-height-normal);
  background-color: var(--color-danger-light);
  color: var(--color-danger);
  border: 1px solid var(--color-danger);
  margin-bottom: var(--spacing-3);
}

.pool-banner i {
  margin-top: 2px;
  flex-shrink: 0;
}

/* Stat tiles: visual parity with .km-stat in KernelManagerView.vue */
.pool-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: var(--spacing-3);
  margin-bottom: var(--spacing-4);
}

.pool-stat {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  background-color: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  transition:
    transform var(--transition-base) var(--transition-timing),
    box-shadow var(--transition-base) var(--transition-timing);
}

.pool-stat:hover {
  transform: translateY(-1px);
  box-shadow: var(--shadow-sm);
}

.pool-stat__icon {
  width: 40px;
  height: 40px;
  border-radius: var(--border-radius-md);
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 16px;
  flex-shrink: 0;
  background-color: var(--color-background-tertiary);
  color: var(--color-text-secondary);
}

.pool-stat--down .pool-stat__icon {
  background-color: var(--color-danger-light);
  color: var(--color-danger);
}

.pool-stat__body {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.pool-stat__value {
  font-size: var(--font-size-2xl);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
  line-height: 1.1;
}

.pool-stat__value-suffix {
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-muted);
  margin-left: var(--spacing-1);
}

.pool-stat__label {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  margin-top: var(--spacing-0-5);
}

.pool-stat__sub {
  font-size: var(--font-size-2xs);
  color: var(--color-text-muted);
  margin-top: var(--spacing-0-5);
}

/* Shared card shell for config + processes */
.pool-config,
.pool-processes {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  padding: var(--spacing-4) var(--spacing-5);
}

.pool-config {
  margin-bottom: var(--spacing-4);
}

.pool-config__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.pool-card-title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.pool-card-description {
  margin: 0;
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
  text-wrap: pretty;
}

.pool-toggle {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
  flex-shrink: 0;
  white-space: nowrap;
}

.pool-toggle__word {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
  min-width: 2ch;
  text-align: right;
}

.pool-size-row {
  display: flex;
  align-items: center;
  gap: var(--spacing-3);
  margin-top: var(--spacing-3);
}

.pool-size-label {
  font-size: var(--font-size-md);
  color: var(--color-text-primary);
}

.pool-size-suffix {
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.pool-offstate {
  margin-top: var(--spacing-3);
  padding: var(--spacing-3) var(--spacing-4);
  background: var(--color-background-secondary);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.pool-offstate__line1 {
  margin: 0;
  color: var(--color-text-primary);
}

.pool-offstate__line2 {
  margin: var(--spacing-1) 0 0;
  color: var(--color-text-secondary);
}

.pool-hint--warning {
  display: flex;
  align-items: flex-start;
  gap: var(--spacing-2);
  margin: var(--spacing-3) 0 0;
  padding: var(--spacing-2) var(--spacing-3);
  background: var(--color-warning-light);
  color: var(--color-warning-dark);
  border-radius: var(--border-radius-md);
  font-size: var(--font-size-sm);
}

.pool-hint--warning i {
  margin-top: 2px;
  flex-shrink: 0;
}

.pool-processes__title {
  margin: 0 0 var(--spacing-3);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.pool-members {
  width: 100%;
  border-collapse: collapse;
}

.pool-members th {
  text-align: left;
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  padding: var(--spacing-1) var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.pool-members td {
  font-size: var(--font-size-md);
  color: var(--color-text-primary);
  padding: var(--spacing-1-5) var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.pool-member-state {
  display: inline-block;
  padding: 1px var(--spacing-2);
  border-radius: var(--border-radius-full);
  font-size: var(--font-size-xs);
}

.pool-member-state--idle {
  background: var(--color-success-light);
  color: var(--color-success-dark);
}

.pool-member-state--busy {
  background: var(--color-info-light);
  color: var(--color-info);
}

.pool-lifecycle {
  margin-top: var(--spacing-3);
  padding-top: var(--spacing-2);
  border-top: 1px solid var(--color-border-light);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.pool-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: var(--spacing-6) var(--spacing-4);
  gap: var(--spacing-1);
}

.pool-empty i {
  font-size: 20px;
  color: var(--color-text-muted);
  margin-bottom: var(--spacing-1);
}

.pool-empty__title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.pool-empty__hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  max-width: 46ch;
}
</style>
