import axios from "../services/axios.config";

const API_BASE = "/file_manager";

export interface ManagedFile {
  name: string;
  path: string;
  is_directory: boolean;
  size: number;
  file_type: string;
  last_modified: string;
  created_date: string;
  is_hidden: boolean;
  exists: boolean;
}

export interface UploadResponse {
  filename: string;
  filepath: string;
  size: number;
}

export class FileManagerApi {
  static async listFiles(): Promise<ManagedFile[]> {
    const response = await axios.get<ManagedFile[]>(`${API_BASE}/files`);
    return response.data;
  }

  static async uploadFile(
    file: File,
    onProgress?: (percent: number) => void,
  ): Promise<UploadResponse> {
    const formData = new FormData();
    formData.append("file", file);
    const response = await axios.post<UploadResponse>(
      `${API_BASE}/upload`,
      formData,
      {
        headers: { "Content-Type": "multipart/form-data" },
        onUploadProgress: (e) => {
          if (onProgress && e.total) {
            onProgress(Math.round((e.loaded * 100) / e.total));
          }
        },
      },
    );
    return response.data;
  }

  static async deleteFile(filename: string): Promise<void> {
    await axios.delete(
      `${API_BASE}/files/${encodeURIComponent(filename)}`,
    );
  }
}
