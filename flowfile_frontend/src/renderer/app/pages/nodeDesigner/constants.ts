/**
 * Constants for the Node Designer
 */
import { componentDocs } from "./componentDocs";
import type { AvailableComponent } from "./types";

/** Legacy sessionStorage key; kept only for the one-time draft migration in node-designer-store */
export const STORAGE_KEY = "nodeDesigner_state";

/** Available component types for the palette, derived from the component reference. */
export const availableComponents: AvailableComponent[] = componentDocs.map((c) => ({
  type: c.type,
  label: c.label,
  icon: c.icon,
}));

/** Default process code template */
export const defaultProcessCode = `def process(self, *inputs: pl.LazyFrame) -> pl.LazyFrame:
    # Get the first input LazyFrame
    lf = inputs[0]

    # Your transformation logic here
    # Example: lf = lf.filter(pl.col("column") > 0)

    return lf`;

/** Default node metadata */
export const defaultNodeMetadata = {
  node_name: "",
  node_category: "Custom",
  title: "",
  intro: "",
  number_of_inputs: 1,
  number_of_outputs: 1,
  node_icon: "user-defined-icon.png",
  requires_kernel: false,
  kernel_id: null as string | null,
  output_names: ["main"],
};

/** Get component icon by type */
export function getComponentIcon(type: string): string {
  const comp = availableComponents.find((c) => c.type === type);
  return comp?.icon || "fa-solid fa-puzzle-piece";
}
