/**
 * Explicit icon imports for library build compatibility.
 * Vite inlines these as base64 data URIs when assetsInlineLimit is set.
 * This avoids import.meta.url issues in library mode.
 */
import inputData from '../assets/icons/input_data.png'
import manualInput from '../assets/icons/manual_input.png'
import filter from '../assets/icons/filter.png'
import select from '../assets/icons/select.png'
import sort from '../assets/icons/sort.png'
import polarsCode from '../assets/icons/polars_code.png'
import unique from '../assets/icons/unique.png'
import sample from '../assets/icons/sample.png'
import join from '../assets/icons/join.png'
import groupBy from '../assets/icons/group_by.png'
import pivot from '../assets/icons/pivot.png'
import unpivot from '../assets/icons/unpivot.png'
import view from '../assets/icons/view.png'
import output from '../assets/icons/output.png'

export const iconUrls: Record<string, string> = {
  'input_data.png': inputData,
  'manual_input.png': manualInput,
  'filter.png': filter,
  'select.png': select,
  'sort.png': sort,
  'polars_code.png': polarsCode,
  'unique.png': unique,
  'sample.png': sample,
  'join.png': join,
  'group_by.png': groupBy,
  'pivot.png': pivot,
  'unpivot.png': unpivot,
  'view.png': view,
  'output.png': output,
}
