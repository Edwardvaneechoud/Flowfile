/**
 * Explicit icon imports for library build compatibility.
 * SVG icons are inlined by Vite, avoiding import.meta.url issues in library mode.
 */
import inputData from '../assets/icons/input_data.svg'
import manualInput from '../assets/icons/manual_input.svg'
import filter from '../assets/icons/filter.svg'
import select from '../assets/icons/select.svg'
import sort from '../assets/icons/sort.svg'
import polarsCode from '../assets/icons/polars_code.svg'
import unique from '../assets/icons/unique.svg'
import sample from '../assets/icons/sample.svg'
import join from '../assets/icons/join.svg'
import groupBy from '../assets/icons/group_by.svg'
import pivot from '../assets/icons/pivot.svg'
import unpivot from '../assets/icons/unpivot.svg'
import view from '../assets/icons/view.svg'
import output from '../assets/icons/output.svg'
import externalData from '../assets/icons/external_data.svg'
import externalOutput from '../assets/icons/external_output.svg'

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
  'external_data.svg': externalData,
  'external_output.svg': externalOutput,
}
