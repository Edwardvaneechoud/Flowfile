<template>
  <div class="backup-card">
    <div v-if="loadError" class="backup-banner backup-banner--error">
      <i class="fa-solid fa-circle-exclamation"></i>
      <span>{{ loadError }}</span>
    </div>

    <div class="backup-card__header">
      <div>
        <h3 class="backup-card-title">Catalog database snapshots</h3>
        <p class="backup-card-description">
          Copies of <code>flowfile_catalog.db</code> — connections, secrets, schedules and catalog
          metadata. Flow files and table data are not included.
        </p>
        <p v-if="status" class="backup-directory">{{ status.directory }}</p>
      </div>
      <el-button
        type="primary"
        size="small"
        :loading="creating"
        :disabled="busy"
        @click="backupNow"
      >
        Back up now
      </el-button>
    </div>

    <div v-if="status && !status.enabled" class="backup-hint--warning">
      <i class="fa-solid fa-triangle-exclamation"></i>
      <span>
        Snapshots are disabled: <code>FLOWFILE_DB_BACKUP_KEEP</code> is set to {{ status.keep }}. No
        new snapshot is taken before a migration or an update.
      </span>
    </div>
    <p v-else-if="status" class="backup-retention">
      Flowfile keeps the newest {{ status.keep }}; older snapshots are pruned. Change the number
      with <code>FLOWFILE_DB_BACKUP_KEEP</code>.
    </p>

    <p v-if="loading && !status" class="backup-loading">Loading snapshots…</p>

    <table v-if="backups.length" class="backup-table">
      <thead>
        <tr>
          <th>Created</th>
          <th>Made by</th>
          <th>Reason</th>
          <th>Size</th>
          <th v-if="isDesktop"></th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="entry in backups" :key="entry.fileName">
          <td :title="entry.fileName">{{ formatCreated(entry.createdAt) }}</td>
          <td>{{ entry.appVersion ? `v${entry.appVersion}` : "—" }}</td>
          <td>{{ describeReason(entry) }}</td>
          <td>{{ formatSize(entry.sizeBytes) }}</td>
          <td v-if="isDesktop">
            <button class="link-btn" type="button" @click="reveal(entry.path)">
              Show in folder
            </button>
          </td>
        </tr>
      </tbody>
    </table>

    <div v-else-if="!loading && !loadError" class="backup-empty">
      <i class="fa-solid fa-box-open"></i>
      <div class="backup-empty__title">No snapshots yet</div>
      <div class="backup-empty__hint">
        One is taken before every migration and before desktop updates.
      </div>
    </div>

    <p class="backup-footer">
      To restore one, quit Flowfile and copy the snapshot over
      <code>flowfile_catalog.db</code>, then start Flowfile again. This restores catalog metadata
      only — not your table data or flow files.
      <button class="link-btn" type="button" @click="openDocs">
        <i class="fa-solid fa-arrow-up-right-from-square"></i>
        Read the restore guide
      </button>
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { ElMessage } from "element-plus";
import { desktop, isDesktop } from "../../../lib/desktop";
import {
  createDbBackup,
  listDbBackups,
  type DbBackup,
  type DbBackupsStatus,
} from "../../api/system.api";
import { docsUrl } from "../../lib/docsLinks";

const BACKUPS_DOCS_URL = docsUrl("users/deployment/backups.html");

const status = ref<DbBackupsStatus | null>(null);
const loading = ref(true);
const creating = ref(false);
const loadError = ref("");

const backups = computed<DbBackup[]>(() => status.value?.backups ?? []);
const busy = computed(() => loading.value || creating.value);

const load = async () => {
  loading.value = true;
  try {
    status.value = await listDbBackups();
    loadError.value = "";
  } catch {
    loadError.value = "Couldn't read the database snapshots.";
  } finally {
    loading.value = false;
  }
};

// The route answers 409 BACKUPS_DISABLED / 503 BACKUP_FAILED with a typed {error_code, message}.
const backendMessage = (e: unknown, fallback: string): string => {
  const detail = (e as { response?: { data?: { detail?: { message?: unknown } } } })?.response?.data
    ?.detail;
  return typeof detail?.message === "string" ? detail.message : fallback;
};

const backupNow = async () => {
  creating.value = true;
  try {
    await createDbBackup("manual");
    await load();
    ElMessage.success("Database snapshot created");
  } catch (e) {
    ElMessage.error(backendMessage(e, "Couldn't create a database snapshot"));
  } finally {
    creating.value = false;
  }
};

const formatCreated = (iso: string): string =>
  new Date(iso).toLocaleString(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });

const formatSize = (bytes: number): string => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
};

const describeReason = (entry: DbBackup): string => {
  if (entry.kind === "pre_update") return "Before update";
  if (entry.kind === "manual") return "Manual";
  return `Before migration ${entry.fromRevision ?? "?"} → ${entry.toRevision ?? "?"}`;
};

const reveal = (path: string) => {
  void desktop.revealInFolder(path);
};

const openDocs = () => {
  void desktop.openExternal(BACKUPS_DOCS_URL);
};

onMounted(() => {
  void load();
});
</script>

<style scoped>
.backup-card {
  background: var(--color-background-primary);
  border: 1px solid var(--color-border-light);
  border-radius: var(--border-radius-lg);
  box-shadow: var(--shadow-xs);
  padding: var(--spacing-4) var(--spacing-5);
}

.backup-banner--error {
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

.backup-banner--error i {
  margin-top: 2px;
  flex-shrink: 0;
}

.backup-card__header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: var(--spacing-4);
}

.backup-card-title {
  margin: 0 0 var(--spacing-1);
  font-size: var(--font-size-base);
  font-weight: var(--font-weight-semibold);
  color: var(--color-text-primary);
}

.backup-card-description {
  margin: 0;
  font-size: var(--font-size-md);
  color: var(--color-text-secondary);
  text-wrap: pretty;
}

.backup-directory {
  margin: var(--spacing-2) 0 0;
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  word-break: break-all;
}

.backup-retention {
  margin: var(--spacing-3) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.backup-hint--warning {
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

.backup-hint--warning i {
  margin-top: 2px;
  flex-shrink: 0;
}

.backup-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: var(--spacing-4);
}

.backup-table th {
  text-align: left;
  font-weight: var(--font-weight-semibold);
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  padding: var(--spacing-1) var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.backup-table td {
  font-size: var(--font-size-md);
  color: var(--color-text-primary);
  padding: var(--spacing-1-5) var(--spacing-2);
  border-bottom: 1px solid var(--color-border-light);
}

.backup-loading {
  margin: var(--spacing-4) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.backup-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: var(--spacing-6) var(--spacing-4);
  gap: var(--spacing-1);
}

.backup-empty i {
  font-size: 20px;
  color: var(--color-text-muted);
  margin-bottom: var(--spacing-1);
}

.backup-empty__title {
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.backup-empty__hint {
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
  max-width: 46ch;
}

.backup-footer {
  margin: var(--spacing-4) 0 0;
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.link-btn {
  background: none;
  border: none;
  padding: 0;
  cursor: pointer;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  display: inline-flex;
  align-items: center;
  gap: var(--spacing-1);
  margin-left: var(--spacing-1);
}

.link-btn:hover {
  text-decoration: underline;
}
</style>
