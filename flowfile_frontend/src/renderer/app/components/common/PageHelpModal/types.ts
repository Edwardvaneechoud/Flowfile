import type { RouteLocationRaw } from "vue-router";

export interface HelpFeature {
  icon: string;
  title: string;
  description: string;
  // When set, the feature card becomes a link to this route (closing the modal
  // on click). Omit for a plain, non-interactive card.
  link?: RouteLocationRaw;
}

export interface HelpTip {
  type: "success" | "warning";
  title: string;
  description: string;
}

export interface HelpSection {
  title: string;
  icon?: string;
  description?: string;
  features?: HelpFeature[];
  tips?: HelpTip[];
}

export interface PageHelpContent {
  title: string;
  icon: string;
  sections: HelpSection[];
}
