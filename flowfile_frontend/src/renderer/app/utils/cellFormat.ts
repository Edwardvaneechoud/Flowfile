// AG Grid renders cells with String(value), so a List/Struct cell that arrives
// as a JSON array/object would show as "[object Object]". Format those as JSON.
export const formatCellValue = (value: unknown): string => {
  if (value === null || value === undefined) return "";
  return typeof value === "object" ? JSON.stringify(value) : String(value);
};

export const cellValueFormatter = (params: { value: unknown }): string =>
  formatCellValue(params.value);
