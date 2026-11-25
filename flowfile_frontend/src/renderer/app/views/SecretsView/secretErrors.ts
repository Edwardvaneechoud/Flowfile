// What the Secrets page says when a delete fails.
import { detailOf } from "../../utils/apiDetail";

export const DELETE_FAILED = "Failed to delete secret. Please try again.";

/** Core's sentence for a refused delete (a catalog's sign-in, say); the page's own text when it gave none. */
export function deleteErrorMessage(error: unknown): string {
  const said = detailOf(error, DELETE_FAILED);
  return said.trim() ? said : DELETE_FAILED;
}
