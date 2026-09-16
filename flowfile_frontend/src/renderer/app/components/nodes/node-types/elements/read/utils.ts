import axios from "axios";
import { ref } from "vue";

export const getXlsxSheetNamesForPath = async (path: string): Promise<string[]> => {
  const response = await axios.get(`/api/get_xlsx_sheet_names?path=${path}`);
  return response.data;
};

/**
 * Module-scoped so the Excel reader's optional settings stay expanded for the
 * rest of the session; the settings drawer re-mounts the component on every open.
 */
export const excelOptionalSettingsOpen = ref(false);
