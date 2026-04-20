// Artifacts API — lightweight helpers for the Data Science Predict picker.
import axios from "../services/axios.config";

export interface ArtifactListItem {
  id: number;
  name: string;
  namespace_id: number | null;
  version: number;
  status: string;
  source_registration_id: number;
  python_type: string | null;
  serialization_format: string;
  size_bytes: number | null;
  created_at: string;
  tags: string[];
  owner_id: number;
}

export interface ArtifactVersionInfo {
  version: number;
  id: number;
  created_at: string;
  size_bytes: number | null;
  sha256: string | null;
}

export interface ArtifactWithVersions {
  id: number;
  name: string;
  version: number;
  status: string;
  serialization_format: string;
  python_type: string | null;
  output_schema: { name: string; data_type: string }[] | null;
  all_versions: ArtifactVersionInfo[];
}

export class ArtifactsApi {
  /** List every active artefact the caller can see. */
  static async list(): Promise<ArtifactListItem[]> {
    const response = await axios.get<ArtifactListItem[]>("/artifacts/");
    return response.data;
  }

  /** Get one artefact (latest active) with all of its versions. */
  static async getByNameWithVersions(name: string): Promise<ArtifactWithVersions> {
    const response = await axios.get<ArtifactWithVersions>(
      `/artifacts/by-name/${encodeURIComponent(name)}/versions`,
    );
    return response.data;
  }
}
