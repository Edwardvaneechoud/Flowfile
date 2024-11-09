const cache: Map<string, () => unknown> = new Map()

export function getFromCacheOrFetch<T>(url: string): Promise<T> {
  if (cache.has(url)) {
    return Promise.resolve(cache.get(url)! as T)
  }

  return fetch(url)
    .then((resp) => resp.json() as T)
    .then((data) => {
      cache.set(url, () => data)
      return data
    })
}

export async function useFetch<T>(url: string): Promise<T> {
  return await getFromCacheOrFetch<T>(url)
}
