// Confirmations for removing a metric or a join; removing is a retract, so the history keeps it
import { ElMessageBox } from "element-plus";
import { SemanticModelApi } from "../../../api/semanticModel.api";
import type { JoinDependent, SemanticJoin, SemanticMetric } from "../../../types/semantic.types";

const confirmed = async (message: string, title: string): Promise<boolean> => {
  try {
    await ElMessageBox.confirm(message, title, {
      confirmButtonText: "Remove",
      cancelButtonText: "Cancel",
      type: "warning",
    });
    return true;
  } catch {
    return false;
  }
};

export const confirmRemoveMetric = (metric: SemanticMetric): Promise<boolean> =>
  confirmed(
    metric.status === "accepted"
      ? `Remove ${metric.name}? Questions and queries can no longer use it. Its history is kept.`
      : `Remove the proposed metric ${metric.name}? It was never accepted, so nothing uses it. ` +
          "Its history is kept.",
    "Remove metric",
  );

/** "revenue" or "revenue (by customers.country)", for the list of metrics a join removal breaks. */
export const dependentText = (d: JoinDependent): string => {
  const parts = d.uses.filter((u) => u !== "formula").map((u) => u.replace(/^dimension /, "by "));
  return parts.length ? `${d.metric} (${parts.join(", ")})` : d.metric;
};

export function joinRemovalMessage(join: SemanticJoin, dependents: JoinDependent[] | null): string {
  const head = `Remove the join ${join.name}?`;
  if (dependents === null)
    return `${head} Which metrics use it could not be checked. Its history is kept.`;
  if (dependents.length === 0) return `${head} No metric uses it. Its history is kept.`;
  return (
    `${head} These metrics use it and will stop working: ` +
    `${dependents.map(dependentText).join(", ")}. Its history is kept.`
  );
}

/** Ask before removing a join, naming the accepted metrics that need it (read from core, no data read). */
export async function confirmRemoveJoin(namespaceId: number, join: SemanticJoin): Promise<boolean> {
  let dependents: JoinDependent[] | null = null;
  try {
    dependents = await SemanticModelApi.joinDependents(namespaceId, join.key);
  } catch {
    dependents = null;
  }
  return confirmed(joinRemovalMessage(join, dependents), "Remove join");
}
