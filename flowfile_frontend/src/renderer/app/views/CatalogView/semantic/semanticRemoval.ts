// Removing a metric or a join: asked first, sent as a retract, so the history keeps it
import { ref } from "vue";
import { ElMessage, ElMessageBox } from "element-plus";
import { SemanticModelApi } from "../../../api/semanticModel.api";
import { useSemanticModelStore } from "../../../stores/semantic-model-store";
import {
  errorText,
  type JoinDependent,
  type SemanticJoin,
  type SemanticMetric,
} from "../../../types/semantic.types";

const metricQuestion = ({ name, status }: SemanticMetric): string =>
  status === "accepted"
    ? `Remove ${name}? Questions and queries can no longer use it. Its history is kept.`
    : `Remove the proposed metric ${name}? It was never accepted, so nothing uses it. ` +
      "Its history is kept.";

/** "revenue" or "revenue (by customers.country)", for the list of metrics a join removal breaks. */
const dependentText = (d: JoinDependent): string => {
  const parts = d.uses.filter((u) => u !== "formula").map((u) => u.replace(/^dimension /, "by "));
  return parts.length ? `${d.metric} (${parts.join(", ")})` : d.metric;
};

/** Names the accepted metrics that need the join (read from core, no data read). */
async function joinQuestion(namespaceId: number, join: SemanticJoin): Promise<string> {
  const head = `Remove the join ${join.name}?`;
  let dependents: JoinDependent[];
  try {
    dependents = await SemanticModelApi.joinDependents(namespaceId, join.key);
  } catch {
    return `${head} Which metrics use it could not be checked. Its history is kept.`;
  }
  if (dependents.length === 0) return `${head} No metric uses it. Its history is kept.`;
  return (
    `${head} These metrics use it and will stop working: ` +
    `${dependents.map(dependentText).join(", ")}. Its history is kept.`
  );
}

/** `removing` holds the key being removed; both calls resolve true once it is gone. */
export function useSemanticRemoval(onError: (message: string) => void = ElMessage.error) {
  const store = useSemanticModelStore();
  const removing = ref<string | null>(null);

  async function remove(of: { key: string; name: string }, noun: string, question: string) {
    try {
      await ElMessageBox.confirm(question, `Remove ${noun}`, {
        confirmButtonText: "Remove",
        cancelButtonText: "Cancel",
        type: "warning",
      });
    } catch {
      return false;
    }
    removing.value = of.key;
    try {
      await store.removeSubject(of.key);
      ElMessage.success(`Removed ${of.name}.`);
      return true;
    } catch (e) {
      onError(errorText(e, `Could not remove the ${noun}`));
      return false;
    } finally {
      removing.value = null;
    }
  }

  return {
    removing,
    removeMetric: (metric: SemanticMetric) => remove(metric, "metric", metricQuestion(metric)),
    removeJoin: async (namespaceId: number, join: SemanticJoin) =>
      remove(join, "join", await joinQuestion(namespaceId, join)),
  };
}
