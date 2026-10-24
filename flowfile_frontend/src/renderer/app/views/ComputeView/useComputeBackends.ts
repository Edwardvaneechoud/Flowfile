import { ref } from "vue";
import type { Ref } from "vue";
import {
  addComputeBackend,
  handshakeComputeBackend,
  listComputeBackends,
  removeComputeBackend,
  type ComputeBackend,
  type ComputeBackendInput,
  type HandshakeVerdict,
} from "../../api/computeBackends.api";
import { detailOf } from "../../utils/apiDetail";

const LOOPBACK_HOSTS = new Set(["localhost", "127.0.0.1", "[::1]"]);

/** Whether `url` reaches a worker over plain HTTP on another host; a URL that does not parse yet is not. */
export function plainHttpOffLoopback(url: string): boolean {
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return false;
  }
  return parsed.protocol === "http:" && !LOOPBACK_HOSTS.has(parsed.hostname);
}

/**
 * The configured workers and the admin's actions on them; the caller loads, nothing polls. Loading seeds each
 * worker's verdict from the listing, so one a run produced shows without pressing Handshake.
 */
export function useComputeBackends() {
  const backends: Ref<ComputeBackend[]> = ref([]);
  const loading = ref(false);
  const loadError = ref("");
  const adding = ref(false);
  const busy: Ref<Record<string, boolean>> = ref({});
  const verdicts: Ref<Record<string, HandshakeVerdict>> = ref({});

  const load = async () => {
    loading.value = true;
    try {
      backends.value = await listComputeBackends();
      for (const backend of backends.value) {
        if (backend.verdict) verdicts.value[backend.id] = backend.verdict;
      }
      loadError.value = "";
    } catch (error) {
      loadError.value = detailOf(error, "Couldn't read the configured workers.");
    } finally {
      loading.value = false;
    }
  };

  const add = async (input: ComputeBackendInput): Promise<ComputeBackend> => {
    adding.value = true;
    const { ca_cert, ...rest } = input;
    try {
      const row = await addComputeBackend(ca_cert ? { ...rest, ca_cert } : rest);
      await load();
      return row;
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't add the worker."));
    } finally {
      adding.value = false;
    }
  };

  const remove = async (id: string) => {
    busy.value[id] = true;
    try {
      await removeComputeBackend(id);
      delete verdicts.value[id];
      await load();
    } catch (error) {
      throw new Error(detailOf(error, `Couldn't remove ${id}.`));
    } finally {
      busy.value[id] = false;
    }
  };

  const handshake = async (id: string): Promise<HandshakeVerdict> => {
    busy.value[id] = true;
    try {
      const verdict = await handshakeComputeBackend(id);
      verdicts.value[id] = verdict;
      return verdict;
    } catch (error) {
      throw new Error(detailOf(error, `Couldn't handshake ${id}.`));
    } finally {
      busy.value[id] = false;
    }
  };

  return { backends, loading, loadError, adding, busy, verdicts, load, add, remove, handshake };
}
