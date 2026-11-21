<template>
  <section class="card mb-3" data-testid="project-branches">
    <div class="card-header">
      <h3 class="card-title">
        <i class="fa-solid fa-code-branch card-title__icon"></i>
        Branch and remote
      </h3>
    </div>
    <div class="card-content">
      <p v-if="error" class="branch-error">{{ error }}</p>
      <template v-else-if="state">
        <label class="field-label" for="project-branch-select">Branch</label>
        <div class="branch-row">
          <el-select
            id="project-branch-select"
            :model-value="state.current ?? ''"
            :disabled="busy || !!merge"
            size="default"
            data-testid="project-branch-select"
            @change="onSwitch"
          >
            <el-option v-for="b in state.branches" :key="b.name" :label="b.name" :value="b.name" />
          </el-select>
          <el-button :disabled="busy || !!merge" @click="onNewBranch">New branch</el-button>
        </div>
        <p v-if="syncLine" class="sync-line" data-testid="project-branch-sync">{{ syncLine }}</p>

        <div class="remote-row">
          <span class="remote-address" :title="state.remote ?? ''">
            <i class="fa-solid fa-cloud"></i>
            {{ state.remote ?? "No remote" }}
            <span v-if="state.remote && !state.has_token" class="no-token">· no token</span>
          </span>
          <button type="button" class="link-btn" @click="openRemoteDialog">
            {{ state.remote ? "Change" : "Set remote" }}
          </button>
        </div>
        <div v-if="state.remote" class="sync-actions">
          <el-button :loading="pulling" :disabled="busy || !!merge" @click="onPull">
            <i class="fa-solid fa-arrow-down"></i>&nbsp;Pull
          </el-button>
          <el-button type="primary" :loading="pushing" :disabled="busy || !!merge" @click="onPush">
            <i class="fa-solid fa-arrow-up"></i>&nbsp;Push
          </el-button>
        </div>

        <div v-if="merge" class="merge-panel" data-testid="project-merge">
          <p class="merge-title">
            Pulling {{ merge.sides.theirs }}: {{ merge.conflicts.length }} to settle
          </p>
          <p class="merge-text">
            Nothing is applied until every item is settled; then the merge is saved as a version and
            the database is rebuilt from it.
          </p>
          <ul class="merge-items">
            <li
              v-for="item in merge.conflicts"
              :key="`${item.path}:${item.key}`"
              class="merge-item"
            >
              <p class="merge-item__label">
                <strong>{{ itemFlowName(item) }}</strong> · {{ item.label }}
              </p>
              <p class="merge-item__message">{{ item.message }}</p>
              <div class="merge-sides">
                <div class="merge-side">
                  <p class="merge-side__name">{{ item.ours_label }}</p>
                  <p v-for="line in item.ours" :key="line" class="merge-side__line">{{ line }}</p>
                </div>
                <div class="merge-side">
                  <p class="merge-side__name">{{ item.theirs_label }}</p>
                  <p v-for="line in item.theirs" :key="line" class="merge-side__line">{{ line }}</p>
                </div>
              </div>
              <div class="merge-item__actions">
                <el-button size="small" :disabled="busy" @click="onSettle(item, 'ours')">
                  {{ takeLabel(item, "ours") }}
                </el-button>
                <el-button size="small" :disabled="busy" @click="onSettle(item, 'theirs')">
                  {{ takeLabel(item, "theirs") }}
                </el-button>
                <el-button
                  v-if="opensInEditor(item)"
                  size="small"
                  link
                  type="primary"
                  :disabled="busy"
                  @click="onOpenInEditor(item)"
                >
                  Open in editor
                </el-button>
              </div>
            </li>
          </ul>
          <div class="merge-footer">
            <el-button
              v-if="!merge.conflicts.length"
              type="primary"
              size="small"
              @click="onConclude"
            >
              Finish the merge
            </el-button>
            <el-button size="small" type="danger" plain :disabled="busy" @click="onAbort">
              Abort the merge
            </el-button>
          </div>
        </div>
      </template>
    </div>

    <el-dialog v-model="remoteDialog" title="Remote" width="460px" append-to-body>
      <label class="field-label" for="project-remote-url">Address</label>
      <el-input
        id="project-remote-url"
        v-model="remoteUrl"
        placeholder="https://git.example.com/team/project.git"
      />
      <label class="field-label" for="project-remote-user">User name (optional)</label>
      <el-input id="project-remote-user" v-model="remoteUser" placeholder="x-access-token" />
      <label class="field-label" for="project-remote-token">Token</label>
      <el-input
        id="project-remote-token"
        v-model="remoteToken"
        type="password"
        show-password
        :placeholder="
          state?.has_token ? 'Kept; type a new one to replace it' : 'Personal access token'
        "
      />
      <p class="field-hint">
        The token is kept as your secret on this server and sent only to the remote, never written
        into the address or the project folder.
      </p>
      <template #footer>
        <el-button v-if="state?.has_token" link type="danger" @click="saveRemote(true)">
          Forget the token
        </el-button>
        <el-button @click="remoteDialog = false">Cancel</el-button>
        <el-button type="primary" :disabled="!remoteUrl.trim()" @click="saveRemote(false)">
          Save
        </el-button>
      </template>
    </el-dialog>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useRouter } from "vue-router";
import { ElMessage, ElMessageBox } from "element-plus";
import { ProjectApi } from "../../api/project.api";
import { ProjectUnsavedChanges } from "../../api/project.errors";
import { confirmRecoveryPoint } from "../../components/project/recoveryPoint";
import { alertIncomplete } from "../../components/project/importNotice";
import { useProjectStore } from "../../stores/project-store";
import { useFlowStore } from "../../stores/flow-store";
import {
  branchNameError,
  itemFlowName,
  opensInEditor,
  pullMessage,
  syncText,
  takeLabel,
} from "./branchSync";
import type {
  ImportResult,
  ProjectBranchState,
  ProjectMergeItem,
  ProjectMergeState,
  ProjectPullResult,
} from "../../types";

const store = useProjectStore();
const flowStore = useFlowStore();
const router = useRouter();

const state = ref<ProjectBranchState | null>(null);
const merge = ref<ProjectMergeState | null>(null);
const error = ref("");
const busy = ref(false);
const pulling = ref(false);
const pushing = ref(false);
const remoteDialog = ref(false);
const remoteUrl = ref("");
const remoteUser = ref("");
const remoteToken = ref("");

const currentBranch = computed(() => state.value?.branches.find((b) => b.current) ?? null);
const syncLine = computed(() => syncText(currentBranch.value, !!state.value?.remote));

const load = async () => {
  try {
    [state.value, merge.value] = await Promise.all([
      ProjectApi.getBranches(),
      ProjectApi.getMerge(),
    ]);
    error.value = "";
  } catch (e: any) {
    error.value = e?.message || "Could not load the branches";
  }
};

onMounted(load);

const afterApply = async (result: ImportResult | null, done: string) => {
  store.onCatalogRewritten();
  await Promise.all([store.refreshActive(), store.loadVersions(), load()]);
  if (result && (await alertIncomplete(result, "Not finished"))) return;
  ElMessage.success(done);
};

/** Run `call(force)`; a refusal for unsaved changes asks once, then runs it again with a recovery point. */
const withRecovery = async <T,>(call: (force: boolean) => Promise<T>): Promise<T | null> => {
  try {
    return await call(false);
  } catch (e: any) {
    if (!(e instanceof ProjectUnsavedChanges)) throw e;
    if (!(await confirmRecoveryPoint(e))) return null;
    return await call(true);
  }
};

const run = async (flag: typeof busy, work: () => Promise<void>) => {
  busy.value = true;
  flag.value = true;
  try {
    await work();
  } catch (e: any) {
    ElMessage.error(e?.message || "That did not work");
  } finally {
    busy.value = false;
    flag.value = false;
  }
};

const onSwitch = (name: string) =>
  run(busy, async () => {
    if (!name || name === state.value?.current) return;
    const result = await withRecovery((force) => ProjectApi.switchBranch(name, force));
    if (result) await afterApply(result, `Switched to ${name}`);
    else await load();
  });

const onNewBranch = async () => {
  let name: string;
  try {
    ({ value: name } = await ElMessageBox.prompt(
      "It starts at the current version and is checked out.",
      "New branch",
      {
        confirmButtonText: "Create",
        inputPlaceholder: "feature/fx-rates",
        inputValidator: (v: string) => branchNameError(v) ?? true,
      },
    ));
  } catch {
    return;
  }
  await run(busy, async () => {
    await ProjectApi.createBranch(name.trim());
    await load();
    ElMessage.success(`On new branch ${name.trim()}`);
  });
};

const openRemoteDialog = () => {
  remoteUrl.value = state.value?.remote ?? "";
  remoteUser.value = "";
  remoteToken.value = "";
  remoteDialog.value = true;
};

const saveRemote = (forget: boolean) =>
  run(busy, async () => {
    const token = forget ? "" : remoteToken.value || undefined;
    state.value = await ProjectApi.setRemote(
      remoteUrl.value.trim(),
      token,
      remoteUser.value.trim() || undefined,
    );
    remoteToken.value = "";
    remoteDialog.value = false;
    ElMessage.success(forget ? "Token forgotten" : "Remote saved");
  });

const onPush = () =>
  run(pushing, async () => {
    const answer = await ProjectApi.push();
    await load();
    if (answer.unsaved) {
      ElMessage.warning(
        `Pushed ${answer.branch}. Changes not saved as a version were not pushed: save a version first.`,
      );
    } else {
      ElMessage.success(`Pushed ${answer.branch}`);
    }
  });

const reportPull = async (result: ProjectPullResult) => {
  if (result.state === "conflicted") {
    await load();
    ElMessage.warning(pullMessage(result));
    return;
  }
  await afterApply(result.result, pullMessage(result));
};

const onPull = () =>
  run(pulling, async () => {
    const result = await withRecovery((force) => ProjectApi.pull(force));
    if (result) await reportPull(result);
  });

const onSettle = (item: ProjectMergeItem, take: "ours" | "theirs") =>
  run(busy, async () => {
    const answer = await withRecovery((force) =>
      ProjectApi.settleMerge(item.path, item.key, take, force),
    );
    if (!answer) return;
    if (answer.pull) await reportPull(answer.pull);
    else merge.value = answer.merge;
  });

const onConclude = () =>
  run(busy, async () => {
    const result = await withRecovery((force) => ProjectApi.concludeMerge(force));
    if (result) await reportPull(result);
  });

const onAbort = async () => {
  try {
    await ElMessageBox.confirm(
      "Drop the merge? Nothing was applied, so this project stays as it is.",
      "Abort the merge",
      { confirmButtonText: "Abort the merge", cancelButtonText: "Keep it", type: "warning" },
    );
  } catch {
    return;
  }
  await run(busy, async () => {
    await ProjectApi.abortMerge();
    await load();
  });
};

const onOpenInEditor = (item: ProjectMergeItem) =>
  run(busy, async () => {
    const flowId = await ProjectApi.openMergeFlow(item.path);
    flowStore.setFlowId(flowId);
    await router.push({ name: "designer" });
  });

defineExpose({ load });
</script>

<style scoped>
.field-label {
  display: block;
  margin: var(--spacing-2) 0 var(--spacing-1);
  font-size: var(--font-size-sm);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-secondary);
}

.field-hint {
  margin: var(--spacing-2) 0 0;
  font-size: var(--font-size-xs);
  color: var(--color-text-tertiary);
}

.branch-row,
.remote-row,
.sync-actions,
.merge-item__actions,
.merge-footer {
  display: flex;
  align-items: center;
  gap: var(--spacing-2);
}

.branch-row .el-select {
  flex: 1;
}

.sync-line,
.branch-error {
  margin: var(--spacing-1) 0 0;
  font-size: var(--font-size-sm);
  color: var(--color-text-tertiary);
}

.branch-error {
  color: var(--color-danger);
}

.remote-row {
  justify-content: space-between;
  margin-top: var(--spacing-3);
  font-size: var(--font-size-sm);
}

.remote-address {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-family: var(--font-family-mono);
  color: var(--color-text-secondary);
}

.no-token {
  color: var(--color-warning-dark);
}

.link-btn {
  flex-shrink: 0;
  padding: 0;
  border: none;
  background: none;
  color: var(--color-accent);
  font-size: var(--font-size-sm);
  cursor: pointer;
}

.sync-actions {
  margin-top: var(--spacing-3);
}

.merge-panel {
  margin-top: var(--spacing-3);
  padding-top: var(--spacing-3);
  border-top: 1px solid var(--color-border-light);
}

.merge-title {
  margin: 0;
  font-weight: var(--font-weight-medium);
  color: var(--color-text-primary);
}

.merge-text,
.merge-item__message {
  margin: var(--spacing-1) 0 var(--spacing-2);
  font-size: var(--font-size-sm);
  color: var(--color-text-secondary);
}

.merge-items {
  margin: 0;
  padding: 0;
  list-style: none;
}

.merge-item {
  padding: var(--spacing-2) 0;
  border-bottom: 1px solid var(--color-border-light);
}

.merge-item__label {
  margin: 0;
  font-size: var(--font-size-sm);
}

.merge-sides {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: var(--spacing-2);
  margin-bottom: var(--spacing-2);
}

.merge-side {
  padding: var(--spacing-2);
  border-radius: var(--border-radius-md);
  background: var(--color-background-muted);
}

.merge-side__name {
  margin: 0 0 2px;
  font-size: var(--font-size-xs);
  font-weight: var(--font-weight-medium);
  color: var(--color-text-tertiary);
}

.merge-side__line {
  margin: 0;
  font-family: var(--font-family-mono);
  font-size: var(--font-size-xs);
  word-break: break-word;
}

.merge-footer {
  margin-top: var(--spacing-2);
}
</style>
