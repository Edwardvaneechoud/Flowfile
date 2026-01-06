import { ref } from "vue";

export interface FileInfo {
  id?: number;
  name: string;
  creation_date: number;
  type: string;
  mimetype: string;
  size: number;
  path: string;
  number_of_items: number;
  analysis_file_available: boolean;
  analysis_file_location: string;
  analysis_file_error: string;
}

export interface DirResponse {
  name?: string;
  source_path: string;
  path: string;
  size?: number;
  creation_date?: number;
  access_date?: number;
  modification_date?: number;
  items?: [FileInfo];
  root?: boolean;
  all_items: [string];
}

export interface uploadFile {
  path: string;
  data: Blob | Uint8Array | ArrayBuffer;
}

export interface uploadStatus {
  active: boolean;
  file_type: string;
  file_name_ok: boolean;
}

export interface newDirInput {
  source_path: string;
  dir_name: string;
}

export interface TableSchema {
  name: string;
  data_type?: string;
  is_unique?: boolean;
  max_value?: string;
  min_value?: string;
  number_of_empty_values?: number;
  number_of_filled_values?: number;
  number_of_unique_values?: number;
  size?: number;
}

export interface TableInfo {
  number_of_records?: number;
  number_of_columns?: number;
  name?: string;
  table_schema?: [TableSchema];
  columns?: [string];
  data: [];
}

export const tableInfo = ref<TableInfo | null>(null);
