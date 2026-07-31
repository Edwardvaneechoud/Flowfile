/** Keeps password managers off column-name fields. autocomplete alone isn't enough. */
export const NO_AUTOFILL = {
  autocomplete: "off",
  autocorrect: "off",
  autocapitalize: "off",
  spellcheck: "false",
  "data-lpignore": "true",
  "data-1p-ignore": "true",
  "data-bwignore": "true",
  "data-form-type": "other",
} as const;
