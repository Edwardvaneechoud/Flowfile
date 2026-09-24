/** Why an advanced filter cannot be saved, or null: an empty predicate would drop every row. */
export function advancedFilterError(expression: string | null | undefined): string | null {
  return expression?.trim()
    ? null
    : "Enter a filter expression, or switch back to the basic filter.";
}
