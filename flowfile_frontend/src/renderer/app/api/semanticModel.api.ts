// Semantic model API: one function per route of routes/catalog_semantic.py (no trailing slashes)
import axios from "../services/axios.config";
import type {
  JoinCheck,
  JoinDependent,
  MetricQuery,
  MetricQueryResult,
  SemanticActivityOut,
  SemanticAskOut,
  SemanticDraftOut,
  SemanticEvent,
  SemanticFact,
  SemanticModel,
  SemanticModelPick,
  SemanticModelUpdate,
  SemanticProposal,
  SemanticReviewAction,
  SemanticReviewItem,
  SemanticReviewResult,
  SemanticSuggestKind,
  SemanticSuggestRequestOut,
  SemanticSyncResult,
  SuggestOut,
} from "../types/semantic.types";

const url = (id: number, path = "") => `/catalog/namespaces/${id}/semantic-model${path}`;

const get = async <T>(path: string, config?: object): Promise<T> =>
  (await axios.get<T>(path, config)).data;

const post = async <T>(path: string, body?: unknown): Promise<T> =>
  (await axios.post<T>(path, body)).data;

export const SemanticModelApi = {
  get: (id: number) => get<SemanticModel>(url(id)),
  update: async (id: number, body: SemanticModelUpdate) =>
    (await axios.put<SemanticModel>(url(id), body)).data,
  propose: (id: number, proposals: SemanticProposal[]) =>
    post<SemanticFact[]>(url(id, "/proposals"), { proposals }),
  checkJoins: (id: number) => post<JoinCheck[]>(url(id, "/joins/check")),
  joinDependents: (id: number, key: string) =>
    get<JoinDependent[]>(url(id, "/joins/dependents"), { params: { key } }),
  getReviewQueue: (id: number) => get<SemanticReviewItem[]>(url(id, "/review")),
  review: (id: number, action: SemanticReviewAction) =>
    post<SemanticReviewResult>(url(id, "/review"), action),
  getHistory: (id: number, subject: string) =>
    get<SemanticEvent[]>(url(id, "/history"), { params: { subject } }),
  sync: (id: number) => post<SemanticSyncResult>(url(id, "/sync")),
  activity: (id: number) => get<SemanticActivityOut>(url(id, "/activity")),
  exportYaml: (id: number) =>
    get<string>(url(id, "/export"), {
      responseType: "text",
      transformResponse: (data: string) => data,
    }),
  queryMetric: (id: number, query: MetricQuery) =>
    post<MetricQueryResult>(url(id, "/query"), query),
  saveMetricQueryAsFlow: (id: number, query: MetricQuery, name: string) =>
    post<{ flow_id: number }>(url(id, "/query/save-as-flow"), { query, name }),
  suggest: (id: number, kinds?: SemanticSuggestKind[]) =>
    post<SuggestOut>(url(id, "/suggest"), kinds && { kinds }),
  suggestOnRequest: (
    id: number,
    body: SemanticModelPick & { request?: string; kinds?: SemanticSuggestKind[] },
  ) => post<SemanticSuggestRequestOut>("/ai/semantic/suggest", { namespace_id: id, ...body }),
  draft: (id: number, pick: SemanticModelPick) =>
    post<SemanticDraftOut>("/ai/semantic/draft", { namespace_id: id, ...pick }),
  ask: (id: number, question: string, pick: SemanticModelPick) =>
    post<SemanticAskOut>("/ai/semantic/ask", { namespace_id: id, question, ...pick }),
};
