import { getTracker } from './tracker'

export const timingProvider = <
  // oxlint-disable-next-line typescript/no-explicit-any
  F extends (...args: any[]) => Promise<any>
>(
  name: keyof ReturnType<typeof getTracker>,
  tracker: ReturnType<typeof getTracker>,
  fn: F
) => {
  return async (...args: Parameters<F>) => {
    const start = performance.now()
    const result = await fn(...args)
    const end = performance.now()
    if (process.env.LOG_LEVEL === 'debug') {
      console.log(`${name} took ${end - start}ms`)
    }
    tracker[name] += end - start
    return result
  }
}

/**
 * Maps `items` through `fn` with at most `limit` calls in flight, keeping the
 * input order. The first rejection rejects the whole map and stops starting
 * new calls.
 */
export const mapLimit = async <T, R>(
  items: readonly T[],
  limit: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> => {
  const results = new Array<R>(items.length)
  let next = 0
  let failed = false
  const worker = async (): Promise<void> => {
    while (!failed && next < items.length) {
      const i = next++
      try {
        results[i] = await fn(items[i])
      } catch (error) {
        failed = true
        throw error
      }
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(limit, items.length) }, worker)
  )
  return results
}
