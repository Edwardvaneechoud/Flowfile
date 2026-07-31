// Display-only dtype grouping for badge colors. Mirrors the backend's
// get_readable_datatype_group, where Boolean/Binary fold into Numeric.

export type DataTypeGroup = 'String' | 'Numeric' | 'Date' | 'Other'

const STRING_TYPES = new Set(['Utf8', 'VARCHAR', 'CHAR', 'NVARCHAR', 'String'])

const NUMERIC_TYPES = new Set([
  'fixed_decimal', 'decimal', 'float', 'integer', 'boolean', 'double',
  'Int8', 'Int16', 'Int32', 'Int64', 'Int128',
  'Float16', 'Float32', 'Float64', 'Decimal', 'Binary', 'Boolean',
  'Uint8', 'Uint16', 'Uint32', 'Uint64',
  'UInt8', 'UInt16', 'UInt32', 'UInt64', 'UInt128',
])

const DATE_TYPES = new Set(['datetime', 'date', 'Date', 'Datetime', 'Time'])

export function dataTypeGroup(dataType: string | undefined | null): DataTypeGroup {
  if (!dataType) return 'Other'
  const base = dataType.split('(')[0].trim()
  if (STRING_TYPES.has(base)) return 'String'
  if (NUMERIC_TYPES.has(base)) return 'Numeric'
  if (DATE_TYPES.has(base)) return 'Date'
  return 'Other'
}

/** Badge text: the dtype with its params stripped. */
export function dataTypeLabel(dataType: string | undefined | null): string {
  if (!dataType) return 'unknown'
  return dataType.split('(')[0].trim() || 'unknown'
}

export function dataTypeBadgeClass(group: DataTypeGroup): string {
  switch (group) {
    case 'Numeric':
      return 'badge-numeric'
    case 'String':
      return 'badge-string'
    case 'Date':
      return 'badge-date'
    default:
      return 'badge-other'
  }
}
