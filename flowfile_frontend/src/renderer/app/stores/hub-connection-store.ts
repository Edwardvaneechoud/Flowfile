/** This Flowfile's hub connection, shared by every part of the editor that shows it; only the Hub card changes it. */
import { defineStore } from "pinia";
import { computed, ref } from "vue";
import {
  cancelBrowserSignIn as cancelBrowserSignInApi,
  connectHub,
  disconnectHub,
  getBrowserSignIn,
  getHubConnection,
  PASSWORD_ONLY,
  startBrowserSignIn,
  type HubBrowserSignInInput,
  type HubBrowserSignInStatus,
  type HubConnectInput,
  type HubConnection,
} from "../api/hubConnection.api";
import { detailOf } from "../utils/apiDetail";

/** How often a sign-in through the browser is asked whether the browser came back. */
export const BROWSER_POLL_MS = 1000;

/** This hub or this machine signs in with a username and password only; the message says which. */
export class BrowserSignInUnavailable extends Error {}

function refusal(error: unknown, fallback: string): Error {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (
    detail &&
    typeof detail === "object" &&
    (detail as { kind?: unknown }).kind === PASSWORD_ONLY
  ) {
    const message = (detail as { message?: unknown }).message;
    return new BrowserSignInUnavailable(typeof message === "string" ? message : fallback);
  }
  return new Error(detailOf(error, fallback));
}

const wait = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

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

  /** The sign-in through the browser under way, while the person signs in on the hub's page. */
  const browserSignIn = ref<string | null>(null);
  /** The hub's sign-in page of the sign-in under way, to open again when the browser did not show it. */
  const browserSignInUrl = ref<string | null>(null);
  const waitingForBrowser = computed(() => browserSignIn.value !== null);

  /**
   * Starts a sign-in through the browser, opens the hub's page with `open` and polls until the browser came back.
   * Answers the connection, or `null` when cancelled; a failed sign-in throws its reason, and a hub or machine that
   * only takes a password throws `BrowserSignInUnavailable`.
   */
  const signInWithBrowser = async (
    input: HubBrowserSignInInput,
    open: (url: string) => Promise<void> | void,
  ): Promise<HubConnection | null> => {
    if (browserSignIn.value !== null) return null;
    busy.value = true;
    let flowId = "";
    try {
      const started = await startBrowserSignIn(input);
      flowId = started.flow_id;
      browserSignIn.value = flowId;
      browserSignInUrl.value = started.authorize_url;
      await open(started.authorize_url);
    } catch (error) {
      browserSignIn.value = null;
      throw refusal(error, "Couldn't start the sign-in.");
    } finally {
      busy.value = false;
    }
    while (browserSignIn.value === flowId) {
      await wait(BROWSER_POLL_MS);
      if (browserSignIn.value !== flowId) return null;
      let status: HubBrowserSignInStatus;
      try {
        status = await getBrowserSignIn(flowId);
      } catch (error) {
        if (browserSignIn.value === flowId) browserSignIn.value = null;
        throw new Error(detailOf(error, "Couldn't read the sign-in."));
      }
      if (browserSignIn.value !== flowId) return null;
      if (status.status === "connected" && status.connection) {
        browserSignIn.value = null;
        connection.value = status.connection;
        return status.connection;
      }
      if (status.status === "failed") {
        browserSignIn.value = null;
        throw new Error(status.detail ?? "The sign-in did not complete.");
      }
    }
    return null;
  };

  /** Stops waiting for the sign-in under way; core closes its local port. */
  const cancelBrowserSignIn = async () => {
    const flowId = browserSignIn.value;
    browserSignIn.value = null;
    if (flowId === null) return;
    try {
      await cancelBrowserSignInApi(flowId);
    } catch {
      // It ends by itself after a few minutes.
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

  return {
    connection,
    loading,
    loadError,
    busy,
    connected,
    joined,
    browserSignIn,
    browserSignInUrl,
    waitingForBrowser,
    load,
    connect,
    signInWithBrowser,
    cancelBrowserSignIn,
    disconnect,
  };
});
