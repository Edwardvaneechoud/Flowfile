// types.ts

export interface FileInfo {
    name: string;
    path: string;
    is_directory: boolean;
    size: number;
    file_type: string;
    last_modified: Date;
    created_date: Date;
    is_hidden: boolean;
    exists?: boolean;
}
