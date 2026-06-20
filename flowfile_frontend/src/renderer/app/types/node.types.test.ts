import { describe, it, expect } from "vitest";
import {
  isInputCsvTable,
  isInputParquetTable,
  isInputIpcTable,
  isInputNdjsonTable,
  isInputAvroTable,
  isOutputParquetTable,
  isOutputIpcTable,
  isOutputNdjsonTable,
  isOutputAvroTable,
  type InputTableSettings,
  type OutputTableSettings,
} from "./node.types";

describe("input table type guards", () => {
  it("discriminates the new read formats on file_type", () => {
    const ipc: InputTableSettings = { file_type: "ipc" };
    const ndjson: InputTableSettings = { file_type: "ndjson" };
    const avro: InputTableSettings = { file_type: "avro" };

    expect(isInputIpcTable(ipc)).toBe(true);
    expect(isInputNdjsonTable(ndjson)).toBe(true);
    expect(isInputAvroTable(avro)).toBe(true);

    // each guard is exclusive
    expect(isInputIpcTable(ndjson)).toBe(false);
    expect(isInputNdjsonTable(avro)).toBe(false);
    expect(isInputAvroTable(ipc)).toBe(false);
    expect(isInputCsvTable(ipc)).toBe(false);
    expect(isInputParquetTable(avro)).toBe(false);
  });
});

describe("output table type guards", () => {
  it("discriminates the new write formats on file_type", () => {
    const ipc: OutputTableSettings = { file_type: "ipc" };
    const ndjson: OutputTableSettings = { file_type: "ndjson" };
    const avro: OutputTableSettings = { file_type: "avro" };

    expect(isOutputIpcTable(ipc)).toBe(true);
    expect(isOutputNdjsonTable(ndjson)).toBe(true);
    expect(isOutputAvroTable(avro)).toBe(true);

    expect(isOutputParquetTable(ipc)).toBe(false);
    expect(isOutputIpcTable(ndjson)).toBe(false);
    expect(isOutputAvroTable(ndjson)).toBe(false);
  });
});
