// Tutorial definitions index
export { gettingStartedTutorial } from "./getting-started";

import { gettingStartedTutorial } from "./getting-started";
import type { Tutorial } from "../../../stores/tutorial-store";

// All available tutorials
export const tutorials: Tutorial[] = [gettingStartedTutorial];

// Get tutorial by ID
export function getTutorialById(id: string): Tutorial | undefined {
  return tutorials.find((t) => t.id === id);
}

// Export default tutorial (for quick start)
export const defaultTutorial = gettingStartedTutorial;
