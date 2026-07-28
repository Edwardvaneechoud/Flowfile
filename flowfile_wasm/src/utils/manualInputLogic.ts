/**
 * Pure data-type inference for Manual Input columns.
 *
 * Numeric detection is strict (Number(), not parseFloat) so values like
 * '3 apples' stay String instead of misclassifying as Float64 and failing
 * the Polars cast at run time.
 */
export function inferColumnDataType(values: Array<string | null | undefined>): string {
  const validValues = values.filter(
    (v): v is string => v !== '' && v !== null && v !== undefined
  )
  if (validValues.length === 0) return 'String'

  const allBooleans = validValues.every(v => v === 'true' || v === 'false')
  if (allBooleans) return 'Boolean'

  const allNumeric = validValues.every(v => {
    const parsed = Number(v)
    return v.trim() !== '' && !Number.isNaN(parsed)
  })
  if (allNumeric) {
    const allIntegers = validValues.every(v => Number.isInteger(Number(v)))
    return allIntegers ? 'Int64' : 'Float64'
  }

  return 'String'
}
