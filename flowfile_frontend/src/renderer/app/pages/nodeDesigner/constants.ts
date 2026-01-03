/**
 * Constants for the Node Designer
 */
import type { AvailableComponent } from './types';

/** Session storage key for persisting designer state */
export const STORAGE_KEY = 'nodeDesigner_state';

/** Available component types for the palette */
export const availableComponents: AvailableComponent[] = [
  { type: 'TextInput', label: 'Text Input', icon: 'fa-solid fa-font' },
  { type: 'NumericInput', label: 'Numeric Input', icon: 'fa-solid fa-hashtag' },
  { type: 'ToggleSwitch', label: 'Toggle Switch', icon: 'fa-solid fa-toggle-on' },
  { type: 'SingleSelect', label: 'Single Select', icon: 'fa-solid fa-list' },
  { type: 'MultiSelect', label: 'Multi Select', icon: 'fa-solid fa-list-check' },
  { type: 'ColumnSelector', label: 'Column Selector', icon: 'fa-solid fa-table-columns' },
  { type: 'SliderInput', label: 'Slider', icon: 'fa-solid fa-sliders' },
  { type: 'SecretSelector', label: 'Secret Selector', icon: 'fa-solid fa-key' },
];

/** Default process code template */
export const defaultProcessCode = `def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
    # Get the first input LazyFrame
    lf = inputs[0]

    # Your transformation logic here
    # Example: lf = lf.filter(pl.col("column") > 0)

    return lf`;

/** Default node metadata */
export const defaultNodeMetadata = {
  node_name: '',
  node_category: 'Custom',
  title: '',
  intro: '',
  number_of_inputs: 1,
  number_of_outputs: 1,
};

/** Get component icon by type */
export function getComponentIcon(type: string): string {
  const comp = availableComponents.find((c) => c.type === type);
  return comp?.icon || 'fa-solid fa-puzzle-piece';
}
