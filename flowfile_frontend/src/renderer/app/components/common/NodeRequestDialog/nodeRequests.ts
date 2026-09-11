// No Vue/axios imports here, so these helpers unit-test as a plain module.
import type { NodeRequest } from "../../../api/nodeRequests.api";
import { NODE_REQUEST_ISSUE_URL } from "../../../lib/docsLinks";

// Case-insensitive match of every whitespace-separated word against the title, most upvoted first.
export function filterRequests(requests: NodeRequest[], query: string): NodeRequest[] {
  const words = query.toLowerCase().split(/\s+/).filter(Boolean);
  return requests
    .filter((request) => {
      const title = request.title.toLowerCase();
      return words.every((word) => title.includes(word));
    })
    .sort((a, b) => b.upvotes - a.upvotes || a.number - b.number);
}

export function newRequestUrl(query: string): string {
  const title = query.trim();
  if (!title) return NODE_REQUEST_ISSUE_URL;
  return `${NODE_REQUEST_ISSUE_URL}&${new URLSearchParams({ title })}`;
}
