/** The settings with the fingerprint they loaded at; core answers 409 when the node's settings moved since. */
export function withExpectation<T extends object>(
  inputData: T,
  expectedFingerprint?: string,
): T | (T & { expected_settings_fingerprint: string }) {
  return expectedFingerprint
    ? { ...inputData, expected_settings_fingerprint: expectedFingerprint }
    : inputData;
}
