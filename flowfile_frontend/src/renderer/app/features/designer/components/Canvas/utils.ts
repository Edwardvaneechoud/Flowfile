

export function toCamelCase(str: string) {
  return str
    .split("_")
    .map((word, index) => {
      if (index === 0) {
        return word;
      }
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join("");
}

export function toTitleCase(str: string): string {
  return str
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join("");
}

export function toSnakeCase(str: string): string {
  // Handle empty strings
  if (!str) return str;
  
  return str
    // Insert underscore before any uppercase letter and convert to lowercase
    .replace(/([A-Z])/g, '_$1')
    // Handle consecutive uppercase letters (like API, HTTP)
    .replace(/^_/, '')
    .toLowerCase();
}
