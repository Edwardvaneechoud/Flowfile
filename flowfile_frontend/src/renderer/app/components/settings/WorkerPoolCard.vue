<template>
  <div v-if="isAdmin" class="pool-card">
    <div class="pool-card-header">
      <div>
        <h3 class="pool-card-title">Warm worker pool</h3>
        <p class="pool-card-description">
          Flowfile starts a helper process for every data node it runs. Keeping a few processes warm
          skips that start-up cost (about half a second per node), so flows with many nodes finish
          faster. When every warm process is busy, extra nodes start fresh processes as usual —
          nothing waits.
        </p>
      </div>
      <el-switch
        v-model="poolEnabled"
        :disabled="loading || applying"
        active-text="Warm pool"
        @change="onToggle"
      />
    </div>

    <div v-if="loadError" class="pool-error">
      {{ loadError }}
    </div>

    <div v-else-if="state" class="pool-card-body">
      <div class="pool-stats">
        <div class="pool-stat pool-stat--warm">
          <div class="pool-stat__icon"><i class="fa-solid fa-fire"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">
              {{ state.idle }}<span class="pool-stat__value-suffix">/ {{ state.size }}</span>
            </div>
            <div class="pool-stat__label">Warm processes</div>
          </div>
        </div>
        <div class="pool-stat pool-stat--busy">
          <div class="pool-stat__icon"><i class="fa-solid fa-bolt"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">{{ state.busy }}</div>
            <div class="pool-stat__label">Busy</div>
          </div>
        </div>
        <div class="pool-stat pool-stat--running">
          <div class="pool-stat__icon"><i class="fa-solid fa-gears"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">{{ state.activeTasks }}</div>
            <div class="pool-stat__label">Jobs running</div>
          </div>
        </div>
        <div class="pool-stat pool-stat--served">
          <div class="pool-stat__icon"><i class="fa-solid fa-check-double"></i></div>
          <div class="pool-stat__body">
            <div class="pool-stat__value">{{ state.tasksCompleted }}</div>
            <div class="pool-stat__label">Pool tasks served</div>
          </div>
        </div>
      </div>

      <table v-if="state.members.length" class="pool-members">
        <thead>
          <tr>
            <th>PID</th>
            <th>State</th>
            <th>Tasks served</th>
            <th>Idle</th>
            <th>Memory</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="member in state.members" :key="member.pid">
            <td>{{ member.pid }}</td>
            <td>
              <span :class="['pool-member-state', `pool-member-state--${member.state}`]">
                {{ member.state }}
              </span>
            </td>
            <td>{{ member.tasksServed }}</td>
            <td>{{ member.state === "idle" ? `${Math.round(member.idleSeconds)}s` : "—" }}</td>
            <td>{{ member.rssMb === null ? "—" : `${Math.round(member.rssMb)} MB` }}</td>
          </tr>
        </tbody>
      </table>

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
        <el-button
          type="primary"
          size="small"
          :loading="applying"
          :disabled="desiredSize === state.size"
          @click="apply(desiredSize)"
        >
          Apply
        </el-button>
      </div>

      <p v-if="state.envOverride" class="pool-hint pool-hint--warning">
        <code>FLOWFILE_WORKER_POOL_SIZE</code> is set in the environment — it overrides this setting
        whenever the worker restarts. Unset it to let this setting take over.
      </p>
      <p v-else class="pool-hint">
        Changes apply immediately and persist across restarts
        <template v-if="state.platformDefaultSize > 0">
          (platform default when never changed: {{ state.platformDefaultSize }})
        </template>
        <template v-else>(platform default when never changed: off)</template>.
      </p>
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

let pollTimer: ReturnType<typeof setInterval> | null = null;

const hydrate = (next: WorkerPoolState) => {
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
    loadError.value =
      "Can't reach the worker service, so pool status is unavailable. If the worker is still starting, this page reconnects automatically.";
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
    /* transient poll failure — the load()/apply() paths surface real errors */
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

onMounted(() => {
  void load();
  pollTimer = setInterval(poll, POLL_INTERVAL_MS);
});

onUnmounted(() => {
  if (pollTimer !== null) clearInterval(pollTimer);
});
</script>

<style scoped>
.pool-card {
  border: 1px solid var(--el-border-color, #dcdfe6);
  border-radius: 8px;
  padding: 16px 20px;
  background: var(--el-bg-color, #fff);
}
.pool-card-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
}
.pool-card-title {
  margin: 0 0 4px;
  font-size: 15px;
  font-weight: 600;
}
.pool-card-description {
  margin: 0;
  font-size: 13px;
  color: var(--el-text-color-secondary, #909399);
}
.pool-card-body {
  margin-top: 14px;
  display: flex;
  flex-direction: column;
  gap: 14px;
}
.pool-stats {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 12px;
}
.pool-stat {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border: 1px solid var(--el-border-color-lighter, #ebeef5);
  border-radius: 8px;
}
.pool-stat__icon {
  width: 32px;
  height: 32px;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  color: #fff;
}
.pool-stat--warm .pool-stat__icon {
  background: #e6a23c;
}
.pool-stat--busy .pool-stat__icon {
  background: #409eff;
}
.pool-stat--running .pool-stat__icon {
  background: #67c23a;
}
.pool-stat--served .pool-stat__icon {
  background: #909399;
}
.pool-stat__value {
  font-size: 18px;
  font-weight: 600;
  line-height: 1.1;
}
.pool-stat__value-suffix {
  font-size: 12px;
  font-weight: 400;
  color: var(--el-text-color-secondary, #909399);
  margin-left: 4px;
}
.pool-stat__label {
  font-size: 12px;
  color: var(--el-text-color-secondary, #909399);
}
.pool-members {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}
.pool-members th {
  text-align: left;
  font-weight: 600;
  font-size: 12px;
  color: var(--el-text-color-secondary, #909399);
  padding: 4px 8px;
  border-bottom: 1px solid var(--el-border-color-lighter, #ebeef5);
}
.pool-members td {
  padding: 5px 8px;
  border-bottom: 1px solid var(--el-border-color-lighter, #ebeef5);
}
.pool-member-state {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 10px;
  font-size: 12px;
  text-transform: capitalize;
}
.pool-member-state--idle {
  background: #f0f9eb;
  color: #67c23a;
}
.pool-member-state--busy {
  background: #ecf5ff;
  color: #409eff;
}
.pool-size-row {
  display: flex;
  align-items: center;
  gap: 12px;
}
.pool-size-label {
  font-size: 13px;
}
.pool-hint {
  margin: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary, #909399);
}
.pool-hint--warning {
  color: var(--el-color-warning, #e6a23c);
}
.pool-error {
  margin-top: 12px;
  font-size: 13px;
  color: var(--el-color-danger, #f56c6c);
}
</style>
