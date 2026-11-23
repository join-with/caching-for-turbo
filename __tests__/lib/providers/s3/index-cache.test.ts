import { describe, expect, it, vi } from 'vitest'
import {
  KeyIndex,
  type ListPage,
  shardPrefixes
} from '../../../../src/lib/providers/s3/index-cache'

const keyPrefix = 'turbogha/turbogha_'

/** A listing over `keys`, `pageSize` keys per page. */
const listing = (keys: string[], pageSize = 3) =>
  vi.fn<ListPage>(async (prefix, token) => {
    const matching = keys.filter(key => key.startsWith(prefix)).sort()
    const start = Number(token ?? 0)
    const end = start + pageSize
    return {
      keys: matching.slice(start, end),
      next: end < matching.length ? String(end) : undefined
    }
  })

describe('KeyIndex', () => {
  it('lists 256 two-hex-digit shards', () => {
    const shards = shardPrefixes()
    expect(shards).toHaveLength(256)
    expect(new Set(shards).size).toBe(256)
    expect(shards[0]).toBe('00')
    expect(shards[255]).toBe('ff')
  })

  it('collects every key of every shard, following continuation tokens', async () => {
    // 1,000 hashes spread over all shards, 7 under shard 3a
    const hashes = Array.from(
      { length: 1000 },
      (_, i) => `${(i % 256).toString(16).padStart(2, '0')}${i}`
    )
    for (let i = 0; i < 7; i++) hashes.push(`3a00${i}`)
    const listPage = listing(hashes.map(h => `${keyPrefix}${h}`))
    const index = new KeyIndex({ listPage, keyPrefix })

    const ready = await index.whenReady()

    expect(ready).toEqual(new Set(hashes))
    expect(index.stats?.keys).toBe(hashes.length)
    const shard3a = listPage.mock.calls.filter(([p]) => p === `${keyPrefix}3a`)
    // 4 + 7 keys under 3a, 3 per page
    expect(shard3a.map(([, token]) => token)).toEqual([
      undefined,
      '3',
      '6',
      '9'
    ])
  })

  it('lists at most `concurrency` shards at once', async () => {
    let inFlight = 0
    let maxInFlight = 0
    const index = new KeyIndex({
      keyPrefix,
      concurrency: 32,
      listPage: async () => {
        maxInFlight = Math.max(maxInFlight, ++inFlight)
        await new Promise(resolve => setTimeout(resolve, 1))
        inFlight--
        return { keys: [] }
      }
    })

    await index.whenReady()

    expect(maxInFlight).toBe(32)
  })

  it('skips old `hash#tag` keys', async () => {
    const index = new KeyIndex({
      keyPrefix,
      listPage: listing([`${keyPrefix}aa01`, `${keyPrefix}aa02#tag`])
    })

    expect(await index.whenReady()).toEqual(new Set(['aa01']))
  })

  it('builds once for concurrent and later callers', async () => {
    const listPage = listing([`${keyPrefix}aa01`])
    const index = new KeyIndex({ keyPrefix, listPage })

    await Promise.all([index.whenReady(), index.whenReady()])
    await index.whenReady()

    expect(listPage).toHaveBeenCalledTimes(256)
  })

  it('answers undefined at once while the listing is pending, then the index', async () => {
    let release = (): void => {}
    const gate = new Promise<void>(resolve => {
      release = resolve
    })
    const listPage = vi.fn<ListPage>(async prefix => {
      await gate
      return { keys: prefix.endsWith('aa') ? [`${keyPrefix}aa01`] : [] }
    })
    const index = new KeyIndex({ keyPrefix, listPage })

    // current() starts the build and returns without waiting for it
    expect(index.current()).toBeUndefined()
    expect(listPage).toHaveBeenCalled()
    expect(index.current()).toBeUndefined()

    release()
    await index.settled()
    expect(index.current()).toEqual(new Set(['aa01']))
    expect(listPage).toHaveBeenCalledTimes(256)
  })

  it('resets after a failed build, so the next caller retries', async () => {
    let fail = true
    const log = vi.fn()
    const index = new KeyIndex({
      keyPrefix,
      log,
      listPage: async () => {
        if (fail) throw new Error('R2 down')
        return { keys: [`${keyPrefix}aa01`] }
      }
    })

    expect(await index.whenReady()).toBeUndefined()
    expect(log).toHaveBeenCalledWith(expect.stringContaining('R2 down'))
    // Settling doesn't start a new build; the next caller does
    await index.settled()
    fail = false
    expect(index.current()).toBeUndefined()
    await index.settled()
    expect(index.current()).toEqual(new Set(['aa01']))
  })

  it('adds saved hashes during and after a build, not before one', async () => {
    const index = new KeyIndex({ keyPrefix, listPage: listing([]) })

    index.add('aa01')
    const building = index.whenReady()
    index.add('aa02')
    await building
    index.add('aa03')

    expect(await index.whenReady()).toEqual(new Set(['aa02', 'aa03']))
  })

  it('covers only hashes that start with two lowercase hex digits', () => {
    const index = new KeyIndex({ keyPrefix, listPage: listing([]) })

    expect(index.covers('0a1b2c3d4e5f6071')).toBe(true)
    expect(index.covers('ping-test-file')).toBe(false)
    expect(index.covers('AB12')).toBe(false)
    expect(index.covers('a')).toBe(false)
  })
})
