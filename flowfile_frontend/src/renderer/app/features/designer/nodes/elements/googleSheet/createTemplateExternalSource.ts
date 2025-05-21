import { SampleUsers } from '../../../baseNode/nodeInput'

type ConfigType = 'SAMPLE_USERS' | 'GOOGLE_SHEET'

// Function to generate placeholder configuration based on the type
export function get_template_source_type(type: ConfigType, options?: any): SampleUsers {
  switch (type) {
    case 'SAMPLE_USERS':
      return {
        SAMPLE_USERS: true,
        size: options?.size || 100, // Default size is 100 if not provided
        orientation: options?.orientation || 'row', // Default orientation is 'ROWS'
        fields: []
      } as SampleUsers
    default:
      throw new Error('Unsupported configuration type')
  }
}
