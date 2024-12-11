import axios from 'axios'
import { Field, AirbyteConfig, AirbyteConfigTemplate, FieldProperty, AirbyteConnectorSpecification, OneOfPropertyValue } from './types'

export const getAirbyteConnectors = async () => {
  const response = await axios.get(`/airbyte/available_connectors`)
  return response.data
}

export const getAirbyteConnectorTemplate = async (connector_name: string): Promise<AirbyteConfigTemplate> => {
  const response = await axios.get(`/airbyte/config_template?connector_name=${connector_name}`)
  return response.data
}

export const getAirbyteAvailableConfigs = async ():Promise<string[]>  => {
  const response = await axios.get(`/airbyte/available_configs`)
  return response.data
}

export const getAirbyteConnectorInput = async (connector_name: string): Promise<AirbyteConfig> => {
  const response = await axios.get<AirbyteConfig>(`/airbyte/config_airbyte?connector_name=${connector_name}`)
  return response.data
}

export const setAirbyteConfigGetStreams = async (data: AirbyteConfig) => {
  await axios.post(`/airbyte/set_airbyte_configs_for_streams`, data)
}

const processProperties = (properties: Record<string, any>): FieldProperty[] => {
  return Object.entries(properties).map(([propKey, propValue]) => ({
    key: propKey,
    type: propValue.type,
    description: propValue.description ?? '',
    title: propValue.title ?? '',
    airbyte_secret: propValue.airbyte_secret || false,
    input_value: propValue.default || null,
    default: propValue.default || null,
  }))
}

export const computeSchema = (schema: AirbyteConnectorSpecification): Field[] => {
  const entries = Object.entries(schema.properties)
  const localParsedConfig = entries.map(([key, value]): Field => {
    const isRequired = schema.required?.includes(key) || false
    const baseField = {
      title: value.title,
      type: value.type,
      key: key,
      required: isRequired,
      description: value.description,
      isOpen: false,
      airbyte_secret: value.airbyte_secret || false,
      input_value: value.default || null,
      default: value.default || null,
      properties: [] as FieldProperty[],
    }

    // Handle oneOf fields (like credentials)
    if ('oneOf' in value && Array.isArray(value.oneOf)) {
      return {
        ...baseField,
        oneOf: value.oneOf.map(option => {
          const mappedProperties = option.properties ? 
            Object.entries(option.properties).reduce((acc, [propKey, propValue]) => {
              // Include auth_type but set its value based on the authentication type
              if (propKey === 'auth_type') {
                acc[propKey] = {
                  type: propValue.type,
                  const: propValue.const,
                  input_value: propValue.const,
                  default: propValue.const,
                };
              } else {
                acc[propKey] = {
                  title: propValue.title,
                  type: propValue.type,
                  description: propValue.description,
                  airbyte_secret: propValue.airbyte_secret,
                  input_value: propValue.default || null,
                  default: propValue.default,
                };
              }
              return acc;
            }, {} as Record<string, OneOfPropertyValue>) 
            : {};

          return {
            title: option.title,
            type: option.type,
            description: option.description,
            required: option.required || [],
            properties: mappedProperties,
          };
        }),
        selectedOption: undefined,
      } as Field;
    }
    // Handle regular nested properties
    else if (value.properties) {
      return {
        ...baseField,
        properties: processProperties(value.properties),
      }
    }
    // Handle simple fields
    else {
      return baseField
    }
  })
  return localParsedConfig
}

export const processPropertyValue = (value: any, type: string): any => {
  // Handle empty, null, or undefined values
  if (value === null || value === undefined || value === '') {
    return null;
  }

  // Handle numeric types
  if (type === 'integer' || type === 'number') {
    return typeof value === 'string' ? Number(value) : value;
  }

  // Handle array type
  if (type === 'array' && Array.isArray(value)) {
    return value;
  }

  // Handle JSON string inputs (for credentials_json)
  if (type === 'string' && typeof value === 'string') {
    try {
      // Check if it's a JSON string
      JSON.parse(value);
      return value;
    } catch {
      // If the value looks like a JSON object but is a regular string
      if (value.includes('{') && value.includes('}')) {
        try {
          // Try to parse and re-stringify to ensure proper format
          const parsed = JSON.parse(value.replace(/\s+/g, ' '));
          return JSON.stringify(parsed, null, 2);
        } catch {
          // If that fails, just trim whitespace
          return value.trim();
        }
      }
      // For regular strings, just return as is
      return value;
    }
  }

  // Handle string and other types
  return value;
};

export const getConfigSettings = (parsedConfig: Field[]) => {
  const result: Record<string, any> = {};
  
  parsedConfig.forEach((item: Field) => {
    // Handle oneOf fields
    if (item.oneOf && Array.isArray(item.oneOf) && 
        item.selectedOption !== undefined && 
        item.selectedOption < item.oneOf.length) {
      
      const selectedOption = item.oneOf[item.selectedOption];
      if (!selectedOption?.properties) {
        return;
      }

      // For oneOf fields, we can directly use the item.input_value object
      if (item.input_value) {
        result[item.key] = {};
        Object.entries(item.input_value).forEach(([key, value]) => {
          const processedValue = processPropertyValue(value, 
            selectedOption.properties?.[key]?.type ?? 'string');
          if (processedValue !== null) {
            result[item.key][key] = processedValue;
          }
        });
      }
    }
    // Rest of the function remains the same...
    else if (item.properties && item.properties.length > 0) {
      result[item.key] = {};
      item.properties.forEach((property: FieldProperty) => {
        const value = processPropertyValue(property.input_value, property.type);
        if (value !== null) {
          result[item.key][property.key] = value;
        } else if (property.default !== null) {
          result[item.key][property.key] = property.default;
        }
      });
    }
    else if (item.input_value !== null && item.input_value !== '') {
      const value = processPropertyValue(item.input_value, item.type);
      if (value !== null) {
        result[item.key] = value;
      }
    } else if (item.default !== null) {
      result[item.key] = item.default;
    }
  });
  
  return result;
};