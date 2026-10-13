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

/** The route's own words: a string detail as is, FastAPI's validation list as its messages. */
function detailOf(error: unknown, fallback: string): string {
  const detail = (error as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail;
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const messages = detail
      .map((entry) => (entry as { msg?: unknown })?.msg)
      .filter((msg): msg is string => typeof msg === "string");
    if (messages.length) return messages.join("; ");
  }
  return fallback;
}

/** The configured workers and the admin's actions on them; the caller loads, nothing polls. */
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
      loadError.value = "";
    } catch (error) {
      loadError.value = detailOf(error, "Couldn't read the configured workers.");
    } finally {
      loading.value = false;
    }
  };

  const add = async (input: ComputeBackendInput): Promise<ComputeBackend> => {
    adding.value = true;
    try {
      const row = await addComputeBackend(input);
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
