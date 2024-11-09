import { createI18n } from 'vue-i18n'

// Explicitly cast the type of the imported modules as JSON
const fileNameToLocaleModuleDict = import.meta.globEager('./locales/*.json') as {
  [key: string]: { default: Record<string, string> }
}

const messages: { [locale: string]: Record<string, string> } = {}

Object.entries(fileNameToLocaleModuleDict)
  .map(([fileName, localeModule]): [string, Record<string, string>] => {
    const fileNameParts = fileName.split('/')
    const fileNameWithoutPath = fileNameParts[fileNameParts.length - 1]
    const localeName = fileNameWithoutPath.split('.json')[0]

    return [localeName, localeModule.default]
  })
  .forEach(([localeName, localeMessages]) => {
    messages[localeName] = localeMessages
  })

export default createI18n({
  legacy: false,
  locale: 'gb',
  fallbackLocale: 'gb',
  messages,
})
