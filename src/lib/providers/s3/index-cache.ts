import { mapLimit } from '@/lib/utils'

/** One ListObjectsV2 page: the full keys, and the token for the next page. */
export type ListPage = (
  prefix: string,
  continuationToken?: string
) => Promise<{ keys: string[]; next?: string }>

export type KeyIndexOptions = {
  /** Lists one page of keys under a prefix. */
  listPage: ListPage
  /** What comes before the hash in every key, e.g. `turbogha/turbogha_`. */
  keyPrefix: string
  /** Prefix listings in flight at once. */
  concurrency?: number
  /** How long after a build starts callers wait for it. */
  readyTimeoutMs?: number
  log?: (message: string) => void
}

export type KeyIndexStats = { keys: number; pages: number; ms: number }

// Turbo hashes are hex, so two hex digits split the bucket into 256 listings
// that run side by side. One serial listing of ~185k keys takes ~90 s; 256
// of them, 32 at a time, ~3–4 s (join-with/joinwith#18525).
export const shardPrefixes = (): string[] =>
  Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, '0'))

const shardable = /^[0-9a-f]{2}/

/**
 * The set of hashes in the bucket, listed once per process the first time
 * it is asked for and kept current by add() after each save.
 *
 * A failed build resets it, so the next caller starts a new one. Callers
 * wait for a build at most readyTimeoutMs from its start; after that they
 * get undefined and check R2 per hash while the build carries on.
 *
 * The index only answers for hashes that start with two lowercase hex
 * digits, the ones the shard listing covers (covers()).
 */
export class KeyIndex {
  private readonly listPage: ListPage
  private readonly keyPrefix: string
  private readonly concurrency: number
  private readonly readyTimeoutMs: number
  private readonly log: (message: string) => void

  /** The hashes of the current build, filled while it lists. */
  private keys: Set<string> | undefined
  private build: Promise<Set<string>> | undefined
  private ready: Set<string> | undefined
  private deadline = 0

  stats: KeyIndexStats | undefined

  constructor(options: KeyIndexOptions) {
    this.listPage = options.listPage
    this.keyPrefix = options.keyPrefix
    this.concurrency = options.concurrency ?? 32
    this.readyTimeoutMs = options.readyTimeoutMs ?? 15_000
    this.log = options.log ?? (() => {})
  }

  /** Whether the index can answer for this hash. */
  covers(hash: string): boolean {
    return shardable.test(hash)
  }

  /**
   * The index once built, or undefined when the build failed or is not done
   * by its deadline. Starts a build when none is running.
   */
  async whenReady(): Promise<ReadonlySet<string> | undefined> {
    if (this.ready) return this.ready
    if (!this.build) this.start()
    const build = this.build as Promise<Set<string>>
    const remaining = this.deadline - Date.now()
    if (remaining <= 0) return undefined

    let timer: ReturnType<typeof setTimeout> | undefined
    const timeout = new Promise<undefined>(resolve => {
      timer = setTimeout(() => resolve(undefined), remaining)
    })
    try {
      return await Promise.race([build.catch(() => undefined), timeout])
    } finally {
      clearTimeout(timer)
    }
  }

  /** Records a saved hash; a no-op before the first build starts. */
  add(hash: string): void {
    this.keys?.add(hash)
  }

  private start(): void {
    const keys = new Set<string>()
    this.keys = keys
    this.deadline = Date.now() + this.readyTimeoutMs
    const build = this.list(keys).then(
      stats => {
        this.stats = stats
        this.ready = keys
        this.log(
          `Key index: ${stats.keys} keys from ${stats.pages} pages in ${stats.ms} ms`
        )
        return keys
      },
      (error: unknown) => {
        // Only reset if no newer build replaced this one
        if (this.build === build) {
          this.build = undefined
          this.keys = undefined
        }
        this.log(`Key index build failed, checking per hash: ${error}`)
        throw error
      }
    )
    // whenReady() handles the rejection; don't let it go unhandled meanwhile
    build.catch(() => {})
    this.build = build
  }

  private async list(keys: Set<string>): Promise<KeyIndexStats> {
    const started = Date.now()
    let pages = 0
    await mapLimit(shardPrefixes(), this.concurrency, async shard => {
      let token: string | undefined
      do {
        const page = await this.listPage(`${this.keyPrefix}${shard}`, token)
        pages++
        for (const key of page.keys) {
          if (!key.startsWith(this.keyPrefix)) continue
          const hash = key.slice(this.keyPrefix.length)
          // `hash#tag` keys predate tags in metadata and are never read
          if (hash.includes('#')) continue
          keys.add(hash)
        }
        token = page.next
      } while (token)
    })
    return { keys: keys.size, pages, ms: Date.now() - started }
  }
}
