/** A join as the reach walk reads it: its many side, its one side and its review status. */
export interface ReachJoin {
  from_table: string;
  to_table: string;
  status: string;
}

const OPEN_STATUSES = ["proposed", "reviewed", "stale"];

/**
 * The tables a metric on `base` can read: `base` first, then every table reached by following
 * joins from their many side to their one side, nearest first (as the compiler walks them).
 * Accepted joins always count; open ones only with `allowPending`.
 */
export function reachableTables(base: string, joins: ReachJoin[], allowPending: boolean): string[] {
  const seen = [base];
  for (let i = 0; i < seen.length; i++) {
    for (const j of joins) {
      if (j.from_table !== seen[i] || seen.includes(j.to_table)) continue;
      if (j.status !== "accepted" && !(allowPending && OPEN_STATUSES.includes(j.status))) continue;
      seen.push(j.to_table);
    }
  }
  return seen;
}

const shortName = (key: string) => key.slice(key.lastIndexOf(".") + 1);

/** One line saying what a metric can use: "Columns of orders, and of customers (through joins)". */
export function reachLine(tables: string[]): string {
  if (tables.length === 0) return "";
  const [base, ...rest] = tables.map(shortName);
  if (rest.length === 0) return `Columns of ${base}`;
  return `Columns of ${base}, and of ${rest.join(", ")} (through joins)`;
}
