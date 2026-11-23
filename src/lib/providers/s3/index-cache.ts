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
 * Nothing waits for a build: current() starts one and answers undefined
 * until it is done, and callers check R2 per hash meanwhile, so a small
 * batch never pays the listing (join-with/joinwith#18540). A failed build
 * resets it, so the next caller starts a new one.
 *
 * The index only answers for hashes that start with two lowercase hex
 * digits, the ones the shard listing covers (covers()).
 */
export class KeyIndex {
  private readonly listPage: ListPage
  private readonly keyPrefix: string
  private readonly concurrency: number
  private readonly log: (message: string) => void

  /** The hashes of the current build, filled while it lists. */
  private keys: Set<string> | undefined
  private build: Promise<Set<string>> | undefined
  private ready: Set<string> | undefined

  stats: KeyIndexStats | undefined

  constructor(options: KeyIndexOptions) {
    this.listPage = options.listPage
    this.keyPrefix = options.keyPrefix
    this.concurrency = options.concurrency ?? 32
    this.log = options.log ?? (() => {})
  }

  /** Whether the index can answer for this hash. */
  covers(hash: string): boolean {
    return shardable.test(hash)
  }

  /**
   * The index once built, otherwise undefined. Starts a build when none is
   * running and never waits for it.
   */
  current(): ReadonlySet<string> | undefined {
    if (this.ready) return this.ready
    if (!this.build) this.start()
    return undefined
  }

  /**
   * Waits for the running build, starting one when none is running: the
   * index, or undefined when the build fails.
   */
  async whenReady(): Promise<ReadonlySet<string> | undefined> {
    this.current()
    await this.settled()
    return this.ready
  }

  /** Waits for the running build, if any, to finish or fail. */
  async settled(): Promise<void> {
    await this.build?.catch(() => {})
  }

  /** Records a saved hash; a no-op before the first build starts. */
  add(hash: string): void {
    this.keys?.add(hash)
  }

  private start(): void {
    const keys = new Set<string>()
    this.keys = keys
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
    // Logged and reset above; callers never see the rejection
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
