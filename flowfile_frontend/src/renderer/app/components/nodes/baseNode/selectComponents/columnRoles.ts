/**
 * Role assignment shared by the Pivot and Unpivot drawers: every column in the
 * settings holds exactly one role, and some roles (pivot column, value column)
 * have a single holder.
 *
 * No Vue or DOM imports so it runs under vitest's `node` environment.
 */

export interface RoleRow {
  name: string;
  role: string;
}

export interface RoleSpec {
  value: string;
  label: string;
  /** Only one column can hold the role at a time. */
  single?: boolean;
}

export interface AssignResult {
  rows: RoleRow[];
  /** Indices in `rows` of the columns that were assigned (new or already there). */
  touched: number[];
}

/**
 * Gives the named columns a role. A column already in the settings moves to
 * the new role. Taking a single-holder role swaps roles with the previous
 * holder when the mover had one, and drops the holder when the mover is new.
 */
export const assignRole = (
  rows: readonly RoleRow[],
  names: readonly string[],
  role: string,
  specs: readonly RoleSpec[],
): AssignResult => {
  const single = specs.find((spec) => spec.value === role)?.single ?? false;
  let next = [...rows];
  for (const name of names) {
    const current = next.find((row) => row.name === name);
    if (current?.role === role) continue;
    if (single) {
      const holder = next.findIndex((row) => row.role === role && row.name !== name);
      if (holder !== -1) {
        if (current) next[holder] = { ...next[holder], role: current.role };
        else next.splice(holder, 1);
      }
    }
    next = next.filter((row) => row.name !== name);
    next.push({ name, role });
  }
  const touched = names
    .map((name) => next.findIndex((row) => row.name === name))
    .filter((index) => index !== -1);
  return { rows: next, touched };
};

export const namesWithRole = (rows: readonly RoleRow[], role: string): string[] =>
  rows.filter((row) => row.role === role).map((row) => row.name);
