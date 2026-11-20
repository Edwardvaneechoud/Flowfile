// Installed with @kanaries/graphic-walker, which formats charts with them; no @types package is installed.
declare module "d3-format" {
  export function format(specifier: string): (value: number) => string;
}

declare module "d3-time-format" {
  export function utcFormat(specifier: string): (date: Date) => string;
}
