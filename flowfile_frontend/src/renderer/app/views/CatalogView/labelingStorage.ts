// Pure persistence helpers for the catalog labeling workspace.
// Label settings go to localStorage (a class list is real user effort and
// should survive a restart); collapsed columns are per-tab view state in
// sessionStorage. Keys are per-table so one corrupt entry can't wipe the rest.

export const LABEL_SETTINGS_KEY_PREFIX = "flowfile.catalog.labelSettings.v1";
export const LABELING_COLLAPSED_KEY_PREFIX = "flowfile.catalog.labelingCollapsed.v1";

export const MAX_LABEL_CLASSES = 50;

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

export interface StoredSuggestionSettings {
  predictionTableId: number;
  predictionTableName: string;
  classColumn: string;
  probabilityColumn: string | null;
  leastConfidentFirst: boolean;
}

export interface StoredLabelSettings {
  targetMode: "new" | "existing";
  newColumnName: string;
  existingTarget: string;
  classes: string[];
  suggestion: StoredSuggestionSettings | null;
}

const labelSettingsKey = (tableId: number): string => `${LABEL_SETTINGS_KEY_PREFIX}.${tableId}`;

const collapsedColumnsKey = (tableId: number): string =>
  `${LABELING_COLLAPSED_KEY_PREFIX}.${tableId}`;

const resolveLocalStorage = (storage?: StorageLike | null): StorageLike | null => {
  if (storage) return storage;
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
};

const resolveSessionStorage = (storage?: StorageLike | null): StorageLike | null => {
  if (storage) return storage;
  if (typeof window === "undefined") return null;
  try {
    return window.sessionStorage;
  } catch {
    return null;
  }
};

/** Reads and parses one key. Returns `undefined` for absent, unreadable or
 * corrupt entries; a corrupt entry is dropped so the next read starts clean. */
const readJson = (store: StorageLike, key: string): unknown => {
  let raw: string | null;
  try {
    raw = store.getItem(key);
  } catch {
    return undefined;
  }
  if (raw === null) return undefined;

  try {
    return JSON.parse(raw);
  } catch {
    try {
      store.removeItem(key);
    } catch {
      // Best effort — storage rejected the removal.
    }
    return undefined;
  }
};

const writeJson = (store: StorageLike, key: string, value: unknown): void => {
  let payload: string;
  try {
    payload = JSON.stringify(value);
  } catch {
    return;
  }

  try {
    store.setItem(key, payload);
  } catch {
    // QuotaExceededError or storage disabled — the workspace still works
    // in-memory, it just loses refresh-survival.
  }
};

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const asString = (value: unknown): string => (typeof value === "string" ? value : "");

const sanitizeClasses = (raw: unknown): string[] => {
  if (!Array.isArray(raw)) return [];
  const seen = new Set<string>();
  const classes: string[] = [];
  for (const entry of raw) {
    if (typeof entry !== "string" || entry.length === 0) continue;
    if (seen.has(entry)) continue;
    seen.add(entry);
    classes.push(entry);
    if (classes.length >= MAX_LABEL_CLASSES) break;
  }
  return classes;
};

/** A suggestion block is only usable when it names both a prediction table
 * and the column holding the suggested label — anything else degrades to
 * `null` without taking the rest of the settings with it. */
const sanitizeSuggestion = (raw: unknown): StoredSuggestionSettings | null => {
  if (!isPlainObject(raw)) return null;

  const predictionTableId = raw.predictionTableId;
  if (typeof predictionTableId !== "number") return null;
  if (!Number.isInteger(predictionTableId) || predictionTableId <= 0) return null;

  const classColumn = raw.classColumn;
  if (typeof classColumn !== "string" || classColumn.length === 0) return null;

  return {
    predictionTableId,
    predictionTableName: asString(raw.predictionTableName),
    classColumn,
    probabilityColumn: typeof raw.probabilityColumn === "string" ? raw.probabilityColumn : null,
    leastConfidentFirst: Boolean(raw.leastConfidentFirst),
  };
};

export const sanitizeLabelSettings = (raw: unknown): StoredLabelSettings | null => {
  if (!isPlainObject(raw)) return null;

  const targetMode = raw.targetMode;
  if (targetMode !== "new" && targetMode !== "existing") return null;

  return {
    targetMode,
    newColumnName: asString(raw.newColumnName),
    existingTarget: asString(raw.existingTarget),
    classes: sanitizeClasses(raw.classes),
    suggestion: sanitizeSuggestion(raw.suggestion),
  };
};

export const readLabelSettings = (
  tableId: number,
  storage?: StorageLike | null,
): StoredLabelSettings | null => {
  const store = resolveLocalStorage(storage);
  if (!store) return null;

  const parsed = readJson(store, labelSettingsKey(tableId));
  if (parsed === undefined) return null;
  return sanitizeLabelSettings(parsed);
};

/** Writes the class list through the same sanitizer the read path uses, so a
 * stored list can never exceed what hydration will accept — otherwise labels
 * beyond the cap survive the write and vanish on the next load. */
export const writeLabelSettings = (
  tableId: number,
  settings: StoredLabelSettings,
  storage?: StorageLike | null,
): void => {
  const store = resolveLocalStorage(storage);
  if (!store) return;
  const capped = { ...settings, classes: sanitizeClasses(settings.classes) };
  writeJson(store, labelSettingsKey(tableId), capped);
};

export const readCollapsedColumns = (tableId: number, storage?: StorageLike | null): string[] => {
  const store = resolveSessionStorage(storage);
  if (!store) return [];

  const parsed = readJson(store, collapsedColumnsKey(tableId));
  if (!Array.isArray(parsed)) return [];
  return parsed.filter((entry): entry is string => typeof entry === "string" && entry.length > 0);
};

export const writeCollapsedColumns = (
  tableId: number,
  columns: string[],
  storage?: StorageLike | null,
): void => {
  const store = resolveSessionStorage(storage);
  if (!store) return;
  writeJson(store, collapsedColumnsKey(tableId), columns);
};
