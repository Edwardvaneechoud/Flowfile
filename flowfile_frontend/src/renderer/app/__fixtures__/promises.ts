/** An axios-shaped rejection carrying the route's `detail`. */
export const httpError = (status: number, detail: unknown) =>
  Object.assign(new Error(`Request failed with status code ${status}`), {
    response: { status, data: { detail } },
  });

/** A promise the test settles when it chooses. */
export const deferred = <T>() => {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
};
