import axios from "../services/axios.config";
import type { SearchKind, SearchResponse } from "../types/search.types";

export interface SearchOptions {
  kinds?: SearchKind[];
  limitPerKind?: number;
  signal?: AbortSignal;
}

export class SearchApi {
  /** `GET /search` (no trailing slash): everything the caller may see that matches `query`. */
  static async search(query: string, options: SearchOptions = {}): Promise<SearchResponse> {
    const params = new URLSearchParams({ q: query });
    for (const kind of options.kinds ?? []) params.append("kinds", kind);
    if (options.limitPerKind !== undefined)
      params.set("limit_per_kind", String(options.limitPerKind));
    const response = await axios.get<SearchResponse>("/search", { params, signal: options.signal });
    return response.data;
  }
}
