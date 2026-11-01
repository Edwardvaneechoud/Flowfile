import { ref } from "vue";
import type { Ref } from "vue";
import {
  connectHub,
  disconnectHub,
  getHubConnection,
  type HubConnectInput,
  type HubConnection,
} from "../../api/hubConnection.api";
import { detailOf } from "../../utils/apiDetail";

/**
 * This user's hub connection, and the connect and disconnect that change it. The caller loads; nothing polls.
 * A refused connect leaves the state as it was and throws the route's words.
 */
export function useHubConnection() {
  const state: Ref<HubConnection | null> = ref(null);
  const loading = ref(false);
  const loadError = ref("");
  const busy = ref(false);

  const load = async () => {
    loading.value = true;
    try {
      state.value = await getHubConnection();
      loadError.value = "";
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
      return state.value;
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
      state.value = await getHubConnection();
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't disconnect from the hub."));
    } finally {
      busy.value = false;
    }
  };

  return { state, loading, loadError, busy, load, connect, disconnect };
}
