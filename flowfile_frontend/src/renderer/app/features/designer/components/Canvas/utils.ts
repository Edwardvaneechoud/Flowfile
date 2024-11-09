import { useNodeStore } from "../../../../stores/column-store";

const nodeStore = useNodeStore();

export const handleOutsideClick = (event: Event) => {
  const target = event.target as HTMLElement;
  if (target.className == "drawflow" || target.className == "parent-drawflow") {
    nodeStore.closeDrawer();
  }
};

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

