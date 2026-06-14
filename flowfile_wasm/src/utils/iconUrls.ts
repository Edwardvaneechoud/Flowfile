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
import formula from '../assets/icons/formula.png'
import unique from '../assets/icons/unique.png'
import recordId from '../assets/icons/record_id.png'
import sample from '../assets/icons/sample.png'
import join from '../assets/icons/join.png'
import crossJoin from '../assets/icons/cross_join.png'
import union from '../assets/icons/union.png'
import dynamicRename from '../assets/icons/dynamic_rename.svg'
import groupBy from '../assets/icons/group_by.png'
import pivot from '../assets/icons/pivot.png'
import unpivot from '../assets/icons/unpivot.png'
import view from '../assets/icons/view.png'
import exploreData from '../assets/icons/explore_data.png'
import output from '../assets/icons/output.png'
import externalData from '../assets/icons/external_data.svg'
import externalOutput from '../assets/icons/external_output.svg'
import catalogReader from '../assets/icons/catalog_reader.svg'
import catalogWriter from '../assets/icons/catalog_writer.svg'

export const iconUrls: Record<string, string> = {
  'input_data.png': inputData,
  'manual_input.png': manualInput,
  'filter.png': filter,
  'select.png': select,
  'sort.png': sort,
  'polars_code.png': polarsCode,
  'formula.png': formula,
  'unique.png': unique,
  'record_id.png': recordId,
  'sample.png': sample,
  'join.png': join,
  'cross_join.png': crossJoin,
  'union.png': union,
  'dynamic_rename.svg': dynamicRename,
  'group_by.png': groupBy,
  'pivot.png': pivot,
  'unpivot.png': unpivot,
  'view.png': view,
  'explore_data.png': exploreData,
  'output.png': output,
  'external_data.svg': externalData,
  'external_output.svg': externalOutput,
  'catalog_reader.svg': catalogReader,
  'catalog_writer.svg': catalogWriter,
}
