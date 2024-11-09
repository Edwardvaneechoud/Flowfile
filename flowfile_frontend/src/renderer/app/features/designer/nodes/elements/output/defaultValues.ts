import { OutputCsvTable, OutputExcelTable, OutputParquetTable, OutputSettings } from "../../../baseNode/nodeInput";


export const createDefaultParquetSettings = (): OutputParquetTable => {
        return {
            file_type: 'parquet'
        }
    }


export const createDefaultCsvSettings = (): OutputCsvTable => {
        return {
            delimiter: ',',
            encoding: 'utf-8',
            file_type: 'csv'
        }
    }


export const createDefaultExcelSettings = (): OutputExcelTable => {
        return {
            sheet_name: 'Sheet1',
            file_type: 'excel'
        }
    }


export const createDefaultOutputSettings = (): OutputSettings => {
        return {
            name: '',
            directory: '',
            file_type: 'parquet',
            fields: [],
            write_mode: 'overwrite',
            output_csv_table: createDefaultCsvSettings(),
            output_parquet_table: createDefaultParquetSettings(),
            output_excel_table: createDefaultExcelSettings()
        }
    }
