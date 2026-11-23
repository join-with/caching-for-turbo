import { Readable } from 'stream'
import { TListFile } from './server/cleanup'
import { RequestContext } from './server'
import { getGithubProvider } from './providers/cache'
import { getS3Provider } from './providers/s3'
import { core } from './core'
import { getTracker } from './tracker'

export { ExistsNotSupportedError } from './providers/errors'

export type TProvider = {
  name: 'github' | 's3'
  save: (
    ctx: RequestContext,
    hash: string,
    tag: string,
    stream: Readable
  ) => Promise<void>
  get: (
    ctx: RequestContext,
    hash: string
  ) => Promise<
    [number | undefined, Readable | ReadableStream, string | undefined] | null
  >
  /**
   * Whether an artifact exists, without reading it. Throws
   * ExistsNotSupportedError when the provider can't tell without a download.
   */
  exists: (ctx: RequestContext, hash: string) => Promise<boolean>
  /**
   * Whether each artifact exists, with an answer for every hash. Throws when
   * it can't answer them all, so a caller never mistakes a failure for misses.
   */
  existsMany: (
    ctx: RequestContext,
    hashes: readonly string[]
  ) => Promise<Map<string, boolean>>
  delete: (key: string) => Promise<void>
  list: () => Promise<TListFile[]>
}

export const getProvider = (
  tracker: ReturnType<typeof getTracker>
): TProvider => {
  const provider = core.getInput('provider') || process.env.PROVIDER

  if (!provider) {
    throw new Error(
      'Provider is required. Set PROVIDER environment variable or provider input.'
    )
  }

  if (provider === 'github') {
    return getGithubProvider(tracker)
  }
  if (provider === 's3') {
    return getS3Provider(tracker)
  }

  throw new Error(`Provider ${provider} not supported`)
}
