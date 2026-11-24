// Semantic model API: one function per route of routes/catalog_semantic.py (no trailing slashes)
import axios from "../services/axios.config";
import type {
  JoinCheck,
  JoinDependent,
  MetricQuery,
  SemanticAskOut,
  SemanticAskRequest,
  SemanticDraftOut,
  SemanticDraftRequest,
  SemanticSuggestRequestIn,
  SemanticSuggestRequestOut,
  SuggestOut,
  SuggestRequest,
  MetricQueryResult,
  MetricQuerySaveAsFlow,
  MetricQuerySaveAsFlowOut,
  SemanticEvent,
  SemanticFact,
  SemanticModel,
  SemanticModelUpdate,
  SemanticProposal,
  SemanticReviewAction,
  SemanticReviewItem,
  SemanticReviewResult,
  SemanticSyncResult,
} from "../types/semantic.types";

const base = (namespaceId: number) => `/catalog/namespaces/${namespaceId}/semantic-model`;

export class SemanticModelApi {
  static async get(namespaceId: number): Promise<SemanticModel> {
    const response = await axios.get<SemanticModel>(base(namespaceId));
    return response.data;
  }

  static async update(namespaceId: number, body: SemanticModelUpdate): Promise<SemanticModel> {
    const response = await axios.put<SemanticModel>(base(namespaceId), body);
    return response.data;
  }

  static async propose(
    namespaceId: number,
    proposals: SemanticProposal[],
  ): Promise<SemanticFact[]> {
    const response = await axios.post<SemanticFact[]>(`${base(namespaceId)}/proposals`, {
      proposals,
    });
    return response.data;
  }

  static async checkJoins(namespaceId: number): Promise<JoinCheck[]> {
    const response = await axios.post<JoinCheck[]>(`${base(namespaceId)}/joins/check`);
    return response.data;
  }

  static async joinDependents(namespaceId: number, key: string): Promise<JoinDependent[]> {
    const response = await axios.get<JoinDependent[]>(`${base(namespaceId)}/joins/dependents`, {
      params: { key },
    });
    return response.data;
  }

  static async getReviewQueue(namespaceId: number): Promise<SemanticReviewItem[]> {
    const response = await axios.get<SemanticReviewItem[]>(`${base(namespaceId)}/review`);
    return response.data;
  }

  static async review(
    namespaceId: number,
    action: SemanticReviewAction,
  ): Promise<SemanticReviewResult> {
    const response = await axios.post<SemanticReviewResult>(`${base(namespaceId)}/review`, action);
    return response.data;
  }

  static async getHistory(namespaceId: number, subject: string): Promise<SemanticEvent[]> {
    const response = await axios.get<SemanticEvent[]>(`${base(namespaceId)}/history`, {
      params: { subject },
    });
    return response.data;
  }

  static async sync(namespaceId: number): Promise<SemanticSyncResult> {
    const response = await axios.post<SemanticSyncResult>(`${base(namespaceId)}/sync`);
    return response.data;
  }

  static async exportYaml(namespaceId: number): Promise<string> {
    const response = await axios.get<string>(`${base(namespaceId)}/export`, {
      responseType: "text",
      transformResponse: (data) => data,
    });
    return response.data;
  }

  static async queryMetric(namespaceId: number, query: MetricQuery): Promise<MetricQueryResult> {
    const response = await axios.post<MetricQueryResult>(`${base(namespaceId)}/query`, query);
    return response.data;
  }

  static async saveMetricQueryAsFlow(
    namespaceId: number,
    body: MetricQuerySaveAsFlow,
  ): Promise<MetricQuerySaveAsFlowOut> {
    const response = await axios.post<MetricQuerySaveAsFlowOut>(
      `${base(namespaceId)}/query/save-as-flow`,
      body,
    );
    return response.data;
  }

  static async suggest(namespaceId: number, body?: SuggestRequest): Promise<SuggestOut> {
    const response = await axios.post<SuggestOut>(`${base(namespaceId)}/suggest`, body);
    return response.data;
  }

  static async suggestOnRequest(
    body: SemanticSuggestRequestIn,
  ): Promise<SemanticSuggestRequestOut> {
    const response = await axios.post<SemanticSuggestRequestOut>("/ai/semantic/suggest", body);
    return response.data;
  }

  static async draft(body: SemanticDraftRequest): Promise<SemanticDraftOut> {
    const response = await axios.post<SemanticDraftOut>("/ai/semantic/draft", body);
    return response.data;
  }

  static async ask(body: SemanticAskRequest): Promise<SemanticAskOut> {
    const response = await axios.post<SemanticAskOut>("/ai/semantic/ask", body);
    return response.data;
  }
}
