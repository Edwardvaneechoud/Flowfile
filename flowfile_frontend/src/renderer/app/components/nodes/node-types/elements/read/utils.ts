import axios from "axios";

export const getXlsxSheetNamesForPath = async (path: string): Promise<string[]> => {
  const response = await axios.get(`/api/get_xlsx_sheet_names?path=${path}`);
  return response.data;
};
