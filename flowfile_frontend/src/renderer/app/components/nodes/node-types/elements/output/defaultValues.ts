import {
  OutputCsvTable,
  OutputExcelTable,
  OutputParquetTable,
  OutputSettings,
} from "../../../baseNode/nodeInput";

export const createDefaultParquetSettings = (): OutputParquetTable => {
  return {
    file_type: "parquet",
  };
};

export const createDefaultCsvSettings = (): OutputCsvTable => {
  return {
    delimiter: ",",
    encoding: "utf-8",
    file_type: "csv",
  };
};

export const createDefaultExcelSettings = (): OutputExcelTable => {
  return {
    sheet_name: "Sheet1",
    file_type: "excel",
  };
};

export function createDefaultOutputSettings(): OutputSettings {
  return {
    name: "output.csv",
    directory: ".",
    file_type: "csv",
    write_mode: "overwrite",
    table_settings: {
      file_type: "csv",
      delimiter: ",",
      encoding: "utf-8",
    },
  };
}

// Helper functions for creating specific table settings
export function createCsvTableSettings(): OutputCsvTable {
  return {
    file_type: "csv",
    delimiter: ",",
    encoding: "utf-8",
  };
}

export function createParquetTableSettings(): OutputParquetTable {
  return {
    file_type: "parquet",
  };
}

export function createExcelTableSettings(): OutputExcelTable {
  return {
    file_type: "excel",
    sheet_name: "Sheet1",
  };
}
