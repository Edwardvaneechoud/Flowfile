/**
 * Composable for Polars autocompletion in CodeMirror
 */
import { EditorView, keymap } from '@codemirror/view';
import { EditorState, Extension } from '@codemirror/state';
import { python } from '@codemirror/lang-python';
import { oneDark } from '@codemirror/theme-one-dark';
import { autocompletion, completionKeymap, CompletionContext, CompletionResult, acceptCompletion } from '@codemirror/autocomplete';
import { indentMore } from '@codemirror/commands';
import type { DesignerSection } from '../types';
import { toSnakeCase } from './useCodeGeneration';

// LazyFrame methods for autocompletion
const lazyFrameMethods = [
  // Selection & Filtering
  { label: 'select', type: 'method', info: 'Select columns', apply: 'select()' },
  { label: 'filter', type: 'method', info: 'Filter rows by condition', apply: 'filter()' },
  { label: 'with_columns', type: 'method', info: 'Add or modify columns', apply: 'with_columns()' },
  { label: 'drop', type: 'method', info: 'Drop columns', apply: 'drop()' },
  { label: 'rename', type: 'method', info: 'Rename columns', apply: 'rename({})' },
  { label: 'cast', type: 'method', info: 'Cast column types', apply: 'cast({})' },

  // Sorting & Limiting
  { label: 'sort', type: 'method', info: 'Sort by columns', apply: 'sort("")' },
  { label: 'head', type: 'method', info: 'Get first n rows', apply: 'head()' },
  { label: 'tail', type: 'method', info: 'Get last n rows', apply: 'tail()' },
  { label: 'limit', type: 'method', info: 'Limit number of rows', apply: 'limit()' },
  { label: 'slice', type: 'method', info: 'Slice rows by offset and length', apply: 'slice()' },
  { label: 'unique', type: 'method', info: 'Get unique rows', apply: 'unique()' },

  // Grouping & Aggregation
  { label: 'group_by', type: 'method', info: 'Group by columns', apply: 'group_by().agg()' },
  { label: 'agg', type: 'method', info: 'Aggregate expressions', apply: 'agg()' },
  { label: 'rolling', type: 'method', info: 'Rolling window operations', apply: 'rolling()' },
  { label: 'group_by_dynamic', type: 'method', info: 'Dynamic time-based grouping', apply: 'group_by_dynamic()' },

  // Joins
  { label: 'join', type: 'method', info: 'Join with another LazyFrame', apply: 'join(other, on="", how="left")' },
  { label: 'join_asof', type: 'method', info: 'As-of join for time series', apply: 'join_asof()' },
  { label: 'cross_join', type: 'method', info: 'Cross join (cartesian product)', apply: 'cross_join()' },

  // Reshaping
  { label: 'explode', type: 'method', info: 'Explode list column to rows', apply: 'explode("")' },
  { label: 'unpivot', type: 'method', info: 'Unpivot wide to long format', apply: 'unpivot()' },
  { label: 'pivot', type: 'method', info: 'Pivot long to wide format', apply: 'pivot()' },
  { label: 'unnest', type: 'method', info: 'Unnest struct column', apply: 'unnest("")' },

  // Missing data
  { label: 'fill_null', type: 'method', info: 'Fill null values', apply: 'fill_null()' },
  { label: 'fill_nan', type: 'method', info: 'Fill NaN values', apply: 'fill_nan()' },
  { label: 'drop_nulls', type: 'method', info: 'Drop rows with nulls', apply: 'drop_nulls()' },
  { label: 'interpolate', type: 'method', info: 'Interpolate null values', apply: 'interpolate()' },

  // Other
  { label: 'with_row_index', type: 'method', info: 'Add row index column', apply: 'with_row_index("index")' },
  { label: 'reverse', type: 'method', info: 'Reverse row order', apply: 'reverse()' },
  { label: 'collect', type: 'method', info: 'Execute and collect to DataFrame', apply: 'collect()' },
  { label: 'lazy', type: 'method', info: 'Convert to LazyFrame', apply: 'lazy()' },

  // Expression methods (chainable)
  { label: 'alias', type: 'method', info: 'Rename expression result', apply: 'alias("")' },
  { label: 'is_null', type: 'method', info: 'Check for null', apply: 'is_null()' },
  { label: 'is_not_null', type: 'method', info: 'Check for not null', apply: 'is_not_null()' },
  { label: 'sum', type: 'method', info: 'Sum values', apply: 'sum()' },
  { label: 'mean', type: 'method', info: 'Calculate mean', apply: 'mean()' },
  { label: 'min', type: 'method', info: 'Get minimum', apply: 'min()' },
  { label: 'max', type: 'method', info: 'Get maximum', apply: 'max()' },
  { label: 'count', type: 'method', info: 'Count values', apply: 'count()' },
  { label: 'first', type: 'method', info: 'Get first value', apply: 'first()' },
  { label: 'last', type: 'method', info: 'Get last value', apply: 'last()' },
  { label: 'str', type: 'property', info: 'String operations namespace', apply: 'str.' },
  { label: 'dt', type: 'property', info: 'Datetime operations namespace', apply: 'dt.' },
  { label: 'list', type: 'property', info: 'List operations namespace', apply: 'list.' },
  { label: 'over', type: 'method', info: 'Window function over groups', apply: 'over("")' },
];

// Common Polars completions
const polarsCompletions = [
  { label: 'self', type: 'keyword', info: 'Access node instance' },
  { label: 'inputs[0]', type: 'variable', info: 'First input LazyFrame' },
  { label: 'inputs[1]', type: 'variable', info: 'Second input LazyFrame' },

  // Polars expressions
  { label: 'pl.col', type: 'function', info: 'Select a column by name', apply: 'pl.col("")' },
  { label: 'pl.lit', type: 'function', info: 'Create a literal value', apply: 'pl.lit()' },
  { label: 'pl.all', type: 'function', info: 'Select all columns', apply: 'pl.all()' },
  { label: 'pl.exclude', type: 'function', info: 'Select all except specified', apply: 'pl.exclude("")' },
  { label: 'pl.when', type: 'function', info: 'Start conditional expression', apply: 'pl.when().then().otherwise()' },
  { label: 'pl.concat', type: 'function', info: 'Concatenate LazyFrames', apply: 'pl.concat([])' },
  { label: 'pl.struct', type: 'function', info: 'Create struct column', apply: 'pl.struct([])' },

  // LazyFrame methods with lf. prefix
  { label: 'lf.select', type: 'method', info: 'Select columns', apply: 'lf.select()' },
  { label: 'lf.filter', type: 'method', info: 'Filter rows by condition', apply: 'lf.filter()' },
  { label: 'lf.with_columns', type: 'method', info: 'Add or modify columns', apply: 'lf.with_columns()' },
  { label: 'lf.drop', type: 'method', info: 'Drop columns', apply: 'lf.drop()' },
  { label: 'lf.rename', type: 'method', info: 'Rename columns', apply: 'lf.rename({})' },
  { label: 'lf.cast', type: 'method', info: 'Cast column types', apply: 'lf.cast({})' },
  { label: 'lf.sort', type: 'method', info: 'Sort by columns', apply: 'lf.sort("")' },
  { label: 'lf.head', type: 'method', info: 'Get first n rows', apply: 'lf.head()' },
  { label: 'lf.tail', type: 'method', info: 'Get last n rows', apply: 'lf.tail()' },
  { label: 'lf.limit', type: 'method', info: 'Limit number of rows', apply: 'lf.limit()' },
  { label: 'lf.slice', type: 'method', info: 'Slice rows by offset and length', apply: 'lf.slice()' },
  { label: 'lf.unique', type: 'method', info: 'Get unique rows', apply: 'lf.unique()' },
  { label: 'lf.group_by', type: 'method', info: 'Group by columns', apply: 'lf.group_by().agg()' },
  { label: 'lf.agg', type: 'method', info: 'Aggregate expressions', apply: 'lf.agg()' },
  { label: 'lf.rolling', type: 'method', info: 'Rolling window operations', apply: 'lf.rolling()' },
  { label: 'lf.group_by_dynamic', type: 'method', info: 'Dynamic time-based grouping', apply: 'lf.group_by_dynamic()' },
  { label: 'lf.join', type: 'method', info: 'Join with another LazyFrame', apply: 'lf.join(other, on="", how="left")' },
  { label: 'lf.join_asof', type: 'method', info: 'As-of join for time series', apply: 'lf.join_asof()' },
  { label: 'lf.cross_join', type: 'method', info: 'Cross join (cartesian product)', apply: 'lf.cross_join()' },
  { label: 'lf.explode', type: 'method', info: 'Explode list column to rows', apply: 'lf.explode("")' },
  { label: 'lf.unpivot', type: 'method', info: 'Unpivot wide to long format', apply: 'lf.unpivot()' },
  { label: 'lf.pivot', type: 'method', info: 'Pivot long to wide format', apply: 'lf.pivot()' },
  { label: 'lf.unnest', type: 'method', info: 'Unnest struct column', apply: 'lf.unnest("")' },
  { label: 'lf.fill_null', type: 'method', info: 'Fill null values', apply: 'lf.fill_null()' },
  { label: 'lf.fill_nan', type: 'method', info: 'Fill NaN values', apply: 'lf.fill_nan()' },
  { label: 'lf.drop_nulls', type: 'method', info: 'Drop rows with nulls', apply: 'lf.drop_nulls()' },
  { label: 'lf.interpolate', type: 'method', info: 'Interpolate null values', apply: 'lf.interpolate()' },
  { label: 'lf.with_row_index', type: 'method', info: 'Add row index column', apply: 'lf.with_row_index("index")' },
  { label: 'lf.reverse', type: 'method', info: 'Reverse row order', apply: 'lf.reverse()' },
  { label: 'lf.collect', type: 'method', info: 'Execute and collect to DataFrame', apply: 'lf.collect()' },
  { label: 'lf.lazy', type: 'method', info: 'Convert to LazyFrame', apply: 'lf.lazy()' },

  // Expression methods
  { label: '.alias', type: 'method', info: 'Rename expression result', apply: '.alias("")' },
  { label: '.cast', type: 'method', info: 'Cast to type', apply: '.cast(pl.Utf8)' },
  { label: '.is_null', type: 'method', info: 'Check for null', apply: '.is_null()' },
  { label: '.is_not_null', type: 'method', info: 'Check for not null', apply: '.is_not_null()' },
  { label: '.fill_null', type: 'method', info: 'Fill null values', apply: '.fill_null()' },
  { label: '.sum', type: 'method', info: 'Sum values', apply: '.sum()' },
  { label: '.mean', type: 'method', info: 'Calculate mean', apply: '.mean()' },
  { label: '.min', type: 'method', info: 'Get minimum', apply: '.min()' },
  { label: '.max', type: 'method', info: 'Get maximum', apply: '.max()' },
  { label: '.count', type: 'method', info: 'Count values', apply: '.count()' },
  { label: '.first', type: 'method', info: 'Get first value', apply: '.first()' },
  { label: '.last', type: 'method', info: 'Get last value', apply: '.last()' },
  { label: '.str', type: 'property', info: 'String operations namespace', apply: '.str.' },
  { label: '.dt', type: 'property', info: 'Datetime operations namespace', apply: '.dt.' },
  { label: '.list', type: 'property', info: 'List operations namespace', apply: '.list.' },
  { label: '.over', type: 'method', info: 'Window function over groups', apply: '.over("")' },
];

export function usePolarsAutocompletion(getSections: () => DesignerSection[]) {
  // Dynamic autocompletion based on schema
  function schemaCompletions(context: CompletionContext): CompletionResult | null {
    const beforeCursor = context.state.doc.sliceString(0, context.pos);
    const sections = getSections();

    // Check for SecretStr method completion (after .secret_value.)
    const secretStrMethodMatch = beforeCursor.match(/\.secret_value\.(\w*)$/);
    if (secretStrMethodMatch) {
      const typed = secretStrMethodMatch[1];
      return {
        from: context.pos - typed.length,
        options: [
          {
            label: 'get_secret_value',
            type: 'method',
            info: 'Get the decrypted secret value as a string',
            apply: 'get_secret_value()',
            detail: 'SecretStr',
          },
        ],
        validFor: /^\w*$/,
      };
    }

    // Check for ".value" or ".secret_value" completion after a component field
    for (const section of sections) {
      const sectionName = section.name || toSnakeCase(section.title || 'section');
      for (const comp of section.components) {
        const fieldName = toSnakeCase(comp.field_name);
        const valueMatch = beforeCursor.match(
          new RegExp(`self\\.settings_schema\\.${sectionName}\\.${fieldName}\\.(\\w*)$`)
        );
        if (valueMatch) {
          const typed = valueMatch[1];
          // For SecretSelector, show secret_value instead of value
          if (comp.component_type === 'SecretSelector') {
            return {
              from: context.pos - typed.length,
              options: [
                {
                  label: 'secret_value',
                  type: 'property',
                  info: 'Get the decrypted secret value (SecretStr)',
                  detail: 'SecretSelector',
                },
              ],
              validFor: /^\w*$/,
            };
          }
          return {
            from: context.pos - typed.length,
            options: [
              { label: 'value', type: 'property', info: 'Get the setting value', detail: comp.component_type },
            ],
            validFor: /^\w*$/,
          };
        }
      }
    }

    // Check for component field completion
    for (const section of sections) {
      const sectionName = section.name || toSnakeCase(section.title || 'section');
      const sectionMatch = beforeCursor.match(new RegExp(`self\\.settings_schema\\.${sectionName}\\.(\\w*)$`));

      if (sectionMatch) {
        const typed = sectionMatch[1];
        const componentOptions = section.components.map((comp) => {
          const fieldName = toSnakeCase(comp.field_name);
          return {
            label: fieldName,
            type: 'property',
            info: `${comp.component_type}: ${comp.label}`,
            detail: comp.component_type,
          };
        });

        return {
          from: context.pos - typed.length,
          options: componentOptions,
          validFor: /^\w*$/,
        };
      }
    }

    // Check for section completion
    const settingsMatch = beforeCursor.match(/self\.settings_schema\.(\w*)$/);
    if (settingsMatch) {
      const typed = settingsMatch[1];
      const sectionOptions = sections.map((section) => {
        const sectionName = section.name || toSnakeCase(section.title || 'section');
        const sectionTitle = section.title || section.name || 'Section';
        return {
          label: sectionName,
          type: 'property',
          info: `Section: ${sectionTitle}`,
          detail: 'Section',
        };
      });

      return {
        from: context.pos - typed.length,
        options: sectionOptions,
        validFor: /^\w*$/,
      };
    }

    // Check for "self."
    const selfDotMatch = beforeCursor.match(/self\.(\w*)$/);
    if (selfDotMatch) {
      const typed = selfDotMatch[1];
      return {
        from: context.pos - typed.length,
        options: [{ label: 'settings_schema', type: 'property', info: 'Access node settings' }],
        validFor: /^\w*$/,
      };
    }

    // Check for LazyFrame method completion after a dot
    const lfMethodMatch = beforeCursor.match(/(\w+)\.(\w*)$/);
    if (lfMethodMatch) {
      const typed = lfMethodMatch[2];
      return {
        from: context.pos - typed.length,
        options: lazyFrameMethods,
        validFor: /^\w*$/,
      };
    }

    // Common Polars completions
    const wordMatch = context.matchBefore(/\w+/);
    if (!wordMatch && !context.explicit) return null;

    return {
      from: wordMatch ? wordMatch.from : context.pos,
      options: polarsCompletions,
      validFor: /^\w*$/,
    };
  }

  // Tab keymap for accepting completions
  const tabKeymap = keymap.of([
    {
      key: 'Tab',
      run: (view: EditorView): boolean => {
        if (acceptCompletion(view)) {
          return true;
        }
        return indentMore(view);
      },
    },
  ]);

  // CodeMirror extensions for editing
  const extensions: Extension[] = [
    python(),
    oneDark,
    EditorState.tabSize.of(4),
    autocompletion({
      override: [schemaCompletions],
      defaultKeymap: false,
      closeOnBlur: false,
    }),
    keymap.of(completionKeymap),
    tabKeymap,
  ];

  // Read-only CodeMirror extensions for code preview
  const readOnlyExtensions: Extension[] = [
    python(),
    oneDark,
    EditorState.tabSize.of(4),
    EditorView.editable.of(false),
    EditorState.readOnly.of(true),
  ];

  return {
    extensions,
    readOnlyExtensions,
    schemaCompletions,
  };
}
