/**
 * Explicit icon imports for library build compatibility.
 * Vite inlines these as base64 data URIs when assetsInlineLimit is set.
 * This avoids import.meta.url issues in library mode.
 */
import inputData from '../assets/icons/input_data.svg'
import manualInput from '../assets/icons/manual_input.svg'
import filter from '../assets/icons/filter.svg'
import select from '../assets/icons/select.svg'
import sort from '../assets/icons/sort.svg'
import polarsCode from '../assets/icons/polars_code.svg'
import formula from '../assets/icons/formula.svg'
import unique from '../assets/icons/unique.svg'
import recordId from '../assets/icons/record_id.svg'
import sample from '../assets/icons/sample.svg'
import join from '../assets/icons/join.svg'
import crossJoin from '../assets/icons/cross_join.svg'
import union from '../assets/icons/union.svg'
import dynamicRename from '../assets/icons/dynamic_rename.svg'
import groupBy from '../assets/icons/group_by.svg'
import pivot from '../assets/icons/pivot.svg'
import unpivot from '../assets/icons/unpivot.svg'
import view from '../assets/icons/view.png'
import exploreData from '../assets/icons/explore_data.svg'
import output from '../assets/icons/output.svg'
import externalData from '../assets/icons/external_data.svg'
import externalOutput from '../assets/icons/external_output.svg'
import catalogReader from '../assets/icons/catalog_reader.svg'
import catalogWriter from '../assets/icons/catalog_writer.svg'

export const iconUrls: Record<string, string> = {
  'input_data.svg': inputData,
  'manual_input.svg': manualInput,
  'filter.svg': filter,
  'select.svg': select,
  'sort.svg': sort,
  'polars_code.svg': polarsCode,
  'formula.svg': formula,
  'unique.svg': unique,
  'record_id.svg': recordId,
  'sample.svg': sample,
  'join.svg': join,
  'cross_join.svg': crossJoin,
  'union.svg': union,
  'dynamic_rename.svg': dynamicRename,
  'group_by.svg': groupBy,
  'pivot.svg': pivot,
  'unpivot.svg': unpivot,
  'view.png': view,
  'explore_data.svg': exploreData,
  'output.svg': output,
  'external_data.svg': externalData,
  'external_output.svg': externalOutput,
  'catalog_reader.svg': catalogReader,
  'catalog_writer.svg': catalogWriter,
}
