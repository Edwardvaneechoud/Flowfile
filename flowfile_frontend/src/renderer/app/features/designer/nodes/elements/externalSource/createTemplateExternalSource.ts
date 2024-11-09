import { SampleUsers, GoogleSheet } from '../../../baseNode/nodeInput'

type ConfigType = 'SAMPLE_USERS' | 'GOOGLE_SHEET'

// Function to generate placeholder configuration based on the type
export function get_template_source_type(type: ConfigType, options?: any): SampleUsers | GoogleSheet {
  switch (type) {
    case 'SAMPLE_USERS':
      return {
        SAMPLE_USERS: true,
        size: options?.size || 100, // Default size is 100 if not provided
        orientation: options?.orientation || 'row', // Default orientation is 'ROWS'
        fields: []
      } as SampleUsers
    case 'GOOGLE_SHEET':
      return {
        GOOGLE_SHEET: true,
        class_name: 'GoogleSheet',
        access_token: options?.access_token || '', // Expecting options to have access_token, else default is empty
        sheet_id: options?.sheet_id || '', // Expecting options to have sheet_id, else default is empty
        worksheet_name: options?.worksheet_name || '', // Default worksheet name is empty if not provided
        orientation: options?.orientation || 'row',
        sheet_name: options?.sheet_name || '',
        fields: []
      } as GoogleSheet
    default:
      throw new Error('Unsupported configuration type')
  }
}
