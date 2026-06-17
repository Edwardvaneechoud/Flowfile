export { gettingStartedTutorial } from "./getting-started";

import { gettingStartedTutorial } from "./getting-started";
import type { Tutorial } from "../../../stores/tutorial-store";

export const tutorials: Tutorial[] = [gettingStartedTutorial];

export function getTutorialById(id: string): Tutorial | undefined {
  return tutorials.find((t) => t.id === id);
}

export const defaultTutorial = gettingStartedTutorial;
