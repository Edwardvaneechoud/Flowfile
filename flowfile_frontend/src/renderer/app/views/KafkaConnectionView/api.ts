import axios from "axios";
import type {
  KafkaConnectionCreate,
  KafkaConnectionOut,
  KafkaConnectionTestResult,
  KafkaConnectionUpdate,
  KafkaTopicInfo,
} from "./KafkaConnectionTypes";

const API_BASE_URL = "/kafka";

export const fetchKafkaConnections = async (): Promise<KafkaConnectionOut[]> => {
  try {
    const response = await axios.get<KafkaConnectionOut[]>(`${API_BASE_URL}/connections`);
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to load Kafka connections:", error);
    throw error;
  }
};

export const createKafkaConnection = async (
  data: KafkaConnectionCreate,
): Promise<KafkaConnectionOut> => {
  try {
    const response = await axios.post<KafkaConnectionOut>(`${API_BASE_URL}/connections`, data);
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to create Kafka connection:", error);
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to create Kafka connection";
    throw new Error(errorMsg);
  }
};

export const updateKafkaConnection = async (
  id: number,
  data: KafkaConnectionUpdate,
): Promise<KafkaConnectionOut> => {
  try {
    const response = await axios.put<KafkaConnectionOut>(
      `${API_BASE_URL}/connections/${id}`,
      data,
    );
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to update Kafka connection:", error);
    const errorMsg =
      (error as any).response?.data?.detail || "Failed to update Kafka connection";
    throw new Error(errorMsg);
  }
};

export const deleteKafkaConnection = async (id: number): Promise<void> => {
  try {
    await axios.delete(`${API_BASE_URL}/connections/${id}`);
  } catch (error) {
    console.error("API Error: Failed to delete Kafka connection:", error);
    throw error;
  }
};

export const testKafkaConnection = async (
  id: number,
): Promise<KafkaConnectionTestResult> => {
  try {
    const response = await axios.post<KafkaConnectionTestResult>(
      `${API_BASE_URL}/connections/${id}/test`,
    );
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to test Kafka connection:", error);
    const errorMsg = (error as any).response?.data?.detail || "Connection test failed";
    return { success: false, message: errorMsg, topics_found: 0 };
  }
};

export const fetchKafkaTopics = async (id: number): Promise<KafkaTopicInfo[]> => {
  try {
    const response = await axios.get<KafkaTopicInfo[]>(
      `${API_BASE_URL}/connections/${id}/topics`,
    );
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to fetch Kafka topics:", error);
    throw error;
  }
};

export const resetKafkaOffsets = async (
  syncName: string,
  connectionId: number,
  topic: string,
): Promise<void> => {
  try {
    await axios.post(
      `${API_BASE_URL}/sync/${encodeURIComponent(syncName)}/reset`,
      null,
      { params: { connection_id: connectionId, topic } },
    );
  } catch (error) {
    console.error("API Error: Failed to reset Kafka offsets:", error);
    const errorMsg = (error as any).response?.data?.detail || "Failed to reset offsets";
    throw new Error(errorMsg);
  }
};

export const inferKafkaTopicSchema = async (
  id: number,
  topic: string,
  sampleSize: number = 10,
): Promise<{ name: string; dtype: string }[]> => {
  try {
    const response = await axios.get<{ name: string; dtype: string }[]>(
      `${API_BASE_URL}/connections/${id}/topics/${encodeURIComponent(topic)}/schema`,
      { params: { sample_size: sampleSize } },
    );
    return response.data;
  } catch (error) {
    console.error("API Error: Failed to infer Kafka topic schema:", error);
    throw error;
  }
};
