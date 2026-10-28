import { ref } from "vue";
import type { Ref } from "vue";
import {
  getExchangeStore,
  setExchangeStore,
  type ExchangeStoreInput,
  type ExchangeStoreState,
} from "../../api/system.api";
import { detailOf } from "../../utils/apiDetail";

/**
 * The exchange store as core uses it, and the admin's save or clear of it. The caller loads; nothing polls.
 * A save answers the new state (environment override included), which is what the next run uses.
 */
export function useExchangeStore() {
  const state: Ref<ExchangeStoreState | null> = ref(null);
  const loading = ref(false);
  const loadError = ref("");
  const saving = ref(false);

  const load = async () => {
    loading.value = true;
    try {
      state.value = await getExchangeStore();
      loadError.value = "";
    } catch (error) {
      loadError.value = detailOf(error, "Couldn't read the exchange store.");
    } finally {
      loading.value = false;
    }
  };

  const save = async (input: ExchangeStoreInput): Promise<ExchangeStoreState> => {
    saving.value = true;
    try {
      state.value = await setExchangeStore(input);
      return state.value;
    } catch (error) {
      throw new Error(detailOf(error, "Couldn't save the exchange store."));
    } finally {
      saving.value = false;
    }
  };

  const clear = () => save({ uri: null, connection: null, maxRows: null });

  return { state, loading, loadError, saving, load, save, clear };
}
