import axios from 'axios'
import { Field, AirbyteConfig, AirbyteConfigTemplate, FieldProperty, AirbyteConnectorSpecification } from './types'

export const getAirbyteConnectors = async () => {
  const response = await axios.get(`/airbyte/available_connectors`)
  return response.data
}

export const getAirbyteConnectorTemplate = async (connector_name: string): Promise<AirbyteConfigTemplate> => {
  const response = await axios.get(`/airbyte/config_template?connector_name=${connector_name}`)
  return response.data
}

export const getAirbyteConnectorInput = async (connector_name: string): Promise<AirbyteConfig> => {
  const response = await axios.get<AirbyteConfig>(`/airbyte/config_airbyte?connector_name=${connector_name}`)
  return response.data
}

export const setAirbyteConfigGetStreams = async (data: AirbyteConfig) => {
  await axios.post(`/airbyte/set_airbyte_configs_for_streams`, data)
}

export const computeSchema = (schema: AirbyteConnectorSpecification): Field[] => {
  const entries = Object.entries(schema.properties)
  const localParsedConfig = entries.map(([key, value]): Field => {
    const isRequired = schema.required?.includes(key) || false

    if (value.properties) {
      return {
        title: value.title,
        type: value.type,
        key: key,
        required: isRequired,
        description: value.description,
        properties: Object.entries(value.properties).map(([propKey, propValue]) => ({
          key: propKey,
          type: propValue.type,
          description: propValue.description ?? '',
          title: propValue.title ?? '',
          airbyte_secret: propValue.airbyte_secret || false,
          input_value: propValue.default || null,
          default: propValue.default || null,
        })),
        isOpen: false,
        airbyte_secret: value.airbyte_secret || false,
        input_value: value.default || null,
        default: value.default || null,
      }
    } else {
      return {
        title: value.title,
        type: value.type,
        key: key,
        properties: [],
        required: isRequired,
        description: value.description ?? '',
        isOpen: false,
        airbyte_secret: value.airbyte_secret || false,
        input_value: value.default || null,
        default: value.default || null,
      }
    }
  })
  return localParsedConfig
}

export const getConfigSettings = (parsedConfig: Field[]) => {
  const result: Record<string, any> = {}
  parsedConfig.forEach((item: Field) => {
    if (item.properties.length > 0) {
      result[item.key] = {}
      item.properties.forEach((property: FieldProperty) => {
        if (property.input_value === null) {
          if (property.default) {
            result[item.key][property.key] = property.default
          }
        } else {
          result[item.key][property.key] = property.input_value
        }
      })
    } else {
      if (item.input_value === null) {
        if (item.default) {
          result[item.key] = item.default
        }
      } else if (item.input_value === '') {
        /* empty */
      } else {
        if ((item.type === 'integer' || item.type === 'number') && item.input_value) {
          result[item.key] = parseInt(item.input_value)
        } else {
          result[item.key] = item.input_value
        }
      }
    }
  })
  return result
}
