// Maps a Polars dtype string to a coarse, display-only group used to color the
// data-type badges in the Formula node's field list. Mirrors the desktop/web
// backend (flowfile_core .../flow_file_column/main.py::get_readable_datatype_group)
// so badges look identical to the main app — Boolean/Binary fold into "Numeric"
// there, so they do here too. dtype params are stripped first (e.g.
// "Datetime(time_unit='us')" -> "Datetime", "Decimal(38,0)" -> "Decimal").

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
