/** This Flowfile's hub connection, shared by every part of the editor that shows it; only the Hub card changes it. */
import { defineStore } from "pinia";
import { computed, ref } from "vue";
import {
  connectHub,
  disconnectHub,
  getHubConnection,
  type HubConnectInput,
  type HubConnection,
} from "../api/hubConnection.api";
import { detailOf } from "../utils/apiDetail";

export const useHubConnectionStore = defineStore("hub-connection", () => {
  const connection = ref<HubConnection | null>(null);
  const loading = ref(false);
  const loadError = ref("");
  const busy = ref(false);

  /** Connected to a hub: a desktop that signed in, or a server that joined one. */
  const connected = computed(() => connection.value?.connected ?? false);
  /** This server joined its hub with keys of its own; it shows the hub's servers with no connect step. */
  const joined = computed(() => connection.value?.joined ?? false);

  const read = async () => {
    loading.value = true;
    try {
      connection.value = await getHubConnection();
      loadError.value = "";
    } catch (error) {
      loadError.value = detailOf(error, "Couldn't read the hub connection.");
    } finally {
      loading.value = false;
    }
  };

  let inFlight: Promise<void> | null = null;
  /** Reads the connection from core; readers that mount together share the one request under way. */
  const load = (): Promise<void> => {
    inFlight ??= read().finally(() => {
      inFlight = null;
    });
    return inFlight;
  };

  /** Signs in at the hub; a refused connect leaves the state as it was and throws the route's words. */
  const connect = async (input: HubConnectInput): Promise<HubConnection> => {
    busy.value = true;
    try {
      connection.value = await connectHub(input);
      return connection.value;
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't connect to the hub."));
    } finally {
      busy.value = false;
    }
  };

  const disconnect = async () => {
    busy.value = true;
    try {
      await disconnectHub();
      connection.value = await getHubConnection();
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't disconnect from the hub."));
    } finally {
      busy.value = false;
    }
  };

  return { connection, loading, loadError, busy, connected, joined, load, connect, disconnect };
});
