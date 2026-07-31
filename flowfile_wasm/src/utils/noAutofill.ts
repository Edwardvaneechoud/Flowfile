/** Keeps password managers off column-name fields. Mirrors the copy in flowfile_frontend. */
export const NO_AUTOFILL = {
  autocomplete: 'off',
  autocorrect: 'off',
  autocapitalize: 'off',
  spellcheck: 'false',
  'data-lpignore': 'true',
  'data-1p-ignore': 'true',
  'data-bwignore': 'true',
  'data-form-type': 'other'
} as const
