import { ref } from "vue";
import type { Ref } from "vue";
import {
  connectHub,
  disconnectHub,
  getHubConnection,
  syncHubComputeNodes,
  type HubConnectInput,
  type HubConnection,
} from "../../api/hubConnection.api";
import { listComputeBackends, type ComputeBackend } from "../../api/computeBackends.api";
import { detailOf } from "../../utils/apiDetail";

/** The hub's compute nodes among the configured backends: the rows the hub connection filled. */
export function hubNodesOf(backends: ComputeBackend[]): ComputeBackend[] {
  return backends.filter((backend) => backend.source === "hub");
}

/** The node name a hub row stands for: its id without the `worker:` prefix. */
export function nodeNameOf(backend: ComputeBackend): string {
  return backend.id.replace(/^worker:/, "");
}

/** The line a connect leaves about its node sync: the count, or why the hub's list could not be read. */
export function syncNoticeOf(connection: HubConnection): string {
  if (connection.syncError) return `Team nodes were not synced: ${connection.syncError}`;
  if (connection.nodesSynced === null) return "";
  return nodesSyncedLabel(connection.nodesSynced);
}

export function nodesSyncedLabel(count: number): string {
  return count === 1 ? "1 team node synced" : `${count} team nodes synced`;
}

/**
 * This user's hub connection, and the connect and disconnect that change it, plus the hub's team nodes as the
 * backends table lists them and the explicit sync that refreshes them. The caller loads; nothing polls. A refused
 * connect leaves the state as it was and throws the route's words.
 */
export function useHubConnection() {
  const state: Ref<HubConnection | null> = ref(null);
  const loading = ref(false);
  const loadError = ref("");
  const busy = ref(false);
  const nodes: Ref<ComputeBackend[]> = ref([]);
  const syncing = ref(false);
  const syncNotice = ref("");

  const loadNodes = async () => {
    try {
      nodes.value = hubNodesOf(await listComputeBackends());
    } catch (error) {
      syncNotice.value = detailOf(error, "Couldn't read the team nodes.");
    }
  };

  const load = async () => {
    loading.value = true;
    try {
      state.value = await getHubConnection();
      loadError.value = "";
      if (state.value.connected) await loadNodes();
      else nodes.value = [];
    } catch (error) {
      loadError.value = detailOf(error, "Couldn't read the hub connection.");
    } finally {
      loading.value = false;
    }
  };

  const connect = async (input: HubConnectInput): Promise<HubConnection> => {
    busy.value = true;
    try {
      state.value = await connectHub(input);
      syncNotice.value = syncNoticeOf(state.value);
      await loadNodes();
      return state.value;
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't connect to the hub."));
    } finally {
      busy.value = false;
    }
  };

  const syncNodes = async (): Promise<number> => {
    syncing.value = true;
    try {
      const synced = await syncHubComputeNodes();
      nodes.value = hubNodesOf(synced.backends);
      syncNotice.value = nodesSyncedLabel(nodes.value.length);
      if (synced.skipped.length) {
        syncNotice.value += `; kept the worker added here under ${synced.skipped.join(", ")}`;
      }
      return nodes.value.length;
    } catch (error) {
      syncNotice.value = detailOf(error, "Couldn't sync the team nodes.");
      throw new Error(syncNotice.value);
    } finally {
      syncing.value = false;
    }
  };

  const disconnect = async () => {
    busy.value = true;
    try {
      await disconnectHub();
      state.value = await getHubConnection();
      nodes.value = [];
      syncNotice.value = "";
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't disconnect from the hub."));
    } finally {
      busy.value = false;
    }
  };

  return {
    state,
    loading,
    loadError,
    busy,
    nodes,
    syncing,
    syncNotice,
    load,
    connect,
    disconnect,
    syncNodes,
  };
}
