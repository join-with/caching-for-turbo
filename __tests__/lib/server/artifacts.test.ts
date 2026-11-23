import { Readable } from 'stream'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// A fake bucket behind a fake S3 client: HeadObject, GetObject and
// ListObjectsV2 answer from `bucket`, and every call is recorded.
const { send, bucket, state } = vi.hoisted(() => ({
  send: vi.fn(),
  bucket: new Set<string>(),
  state: {
    headInFlight: 0,
    maxHeadInFlight: 0,
    listFails: false,
    pageSize: 2,
    // When set, every ListObjectsV2 waits for it
    listGate: undefined as Promise<void> | undefined
  }
}))

vi.mock('@aws-sdk/client-s3', async importOriginal => ({
  ...(await importOriginal<typeof import('@aws-sdk/client-s3')>()),
  S3Client: vi.fn(function () {
    return { send }
  })
}))

vi.mock('@aws-sdk/lib-storage', () => ({
  Upload: vi.fn(function (options: { params: { Key: string } }) {
    return {
      done: async () => {
        bucket.add(options.params.Key)
        return {}
      }
    }
  })
}))

import {
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  NotFound,
  NoSuchKey,
  S3ServiceException
} from '@aws-sdk/client-s3'
import {
  resetS3KeyIndexes,
  settleS3KeyIndexes
} from '../../../src/lib/providers/s3'
import { buildServer } from '../../../src/lib/server'

type Command = {
  input: { Key?: string; Prefix?: string; ContinuationToken?: string }
}

/** ListObjectsV2 over the fake bucket, state.pageSize keys per page. */
const listPage = (command: Command) => {
  if (state.listFails) throw r2Error()
  const matching = [...bucket]
    .filter(key => key.startsWith(command.input.Prefix ?? ''))
    .sort()
  const start = Number(command.input.ContinuationToken ?? 0)
  const end = start + state.pageSize
  return {
    Contents: matching.slice(start, end).map(Key => ({ Key })),
    NextContinuationToken: end < matching.length ? String(end) : undefined
  }
}

const fakeS3 = async (command: Command): Promise<unknown> => {
  const isHead = command instanceof HeadObjectCommand
  if (isHead) {
    state.headInFlight++
    state.maxHeadInFlight = Math.max(state.maxHeadInFlight, state.headInFlight)
  }
  try {
    await new Promise(resolve => setTimeout(resolve, 1))
    const key = command.input.Key as string
    if (command instanceof ListObjectsV2Command) {
      await state.listGate
      return listPage(command)
    }
    if (isHead) {
      if (bucket.has(key)) return {}
      throw new NotFound({ message: 'not found', $metadata: {} })
    }
    if (command instanceof GetObjectCommand) {
      if (bucket.has(key)) {
        return { Body: Readable.from('artifact'), ContentLength: 8 }
      }
      throw new NoSuchKey({ message: 'missing', $metadata: {} })
    }
    throw new Error(`unexpected command ${command.constructor.name}`)
  } finally {
    if (isHead) state.headInFlight--
  }
}

const r2Error = () =>
  new S3ServiceException({
    name: 'InternalError',
    $fault: 'server',
    message: 'We encountered an internal error',
    $metadata: { httpStatusCode: 500 }
  })

const commandsSent = (type: new (...args: never[]) => unknown) =>
  send.mock.calls.filter(([command]) => command instanceof type).length

const hash = (n: number) => n.toString(16).padStart(16, '0')

describe('artifact existence routes', () => {
  const app = buildServer()

  beforeEach(() => {
    send.mockReset()
    send.mockImplementation(fakeS3)
    bucket.clear()
    resetS3KeyIndexes()
    state.headInFlight = 0
    state.maxHeadInFlight = 0
    state.listFails = false
    state.pageSize = 2
    state.listGate = undefined
    process.env.PROVIDER = 's3'
    process.env.S3_ACCESS_KEY_ID = 'id'
    process.env.S3_SECRET_ACCESS_KEY = 'secret'
    process.env.S3_BUCKET = 'bucket'
    process.env.S3_REGION = 'auto'
    process.env.S3_PREFIX = 'prefix/'
  })

  afterEach(() => {
    delete process.env.PROVIDER
  })

  describe('POST /v8/artifacts, checking R2 per hash', () => {
    // Without a listing the key index never gets built
    beforeEach(() => {
      state.listFails = true
    })

    it('answers every hash: a hit with taskDurationMs, a miss with null', async () => {
      bucket.add('prefix/turbogha_aaaa')
      bucket.add('prefix/turbogha_cccc')

      const res = await app.inject({
        method: 'POST',
        url: '/v8/artifacts?teamId=turbogha',
        payload: { hashes: ['aaaa', 'bbbb', 'cccc', 'dddd'] }
      })

      expect(res.statusCode).toBe(200)
      expect(res.json()).toEqual({
        aaaa: { taskDurationMs: 0 },
        bbbb: null,
        cccc: { taskDurationMs: 0 },
        dddd: null
      })
      expect(commandsSent(GetObjectCommand)).toBe(0)
    })

    it('checks a 100-hash batch with at most 32 R2 calls at once', async () => {
      const hashes = Array.from({ length: 100 }, (_, i) => hash(i))
      for (const h of hashes.slice(0, 40)) bucket.add(`prefix/turbogha_${h}`)

      const res = await app.inject({
        method: 'POST',
        url: '/v8/artifacts',
        payload: { hashes }
      })

      expect(res.statusCode).toBe(200)
      const body = res.json()
      expect(Object.keys(body)).toHaveLength(100)
      expect(Object.values(body).filter(v => v !== null)).toHaveLength(40)
      expect(commandsSent(HeadObjectCommand)).toBe(100)
      expect(state.maxHeadInFlight).toBeLessThanOrEqual(32)
      expect(state.maxHeadInFlight).toBeGreaterThan(1)
    })

    it('answers 501 on an R2 error, never a batch of misses', async () => {
      send.mockImplementation(async (command: Command) => {
        if (command.input.Key === 'prefix/turbogha_bbbb') throw r2Error()
        return fakeS3(command)
      })

      const res = await app.inject({
        method: 'POST',
        url: '/v8/artifacts',
        payload: { hashes: ['aaaa', 'bbbb', 'cccc'] }
      })

      expect(res.statusCode).toBe(501)
      expect(res.json().ok).toBe(false)
    })

    it('answers 400 to a body without a hashes array', async () => {
      const res = await app.inject({
        method: 'POST',
        url: '/v8/artifacts',
        payload: { hash: 'aaaa' }
      })

      expect(res.statusCode).toBe(400)
      expect(send).not.toHaveBeenCalled()
    })
  })

  describe('POST /v8/artifacts, from the key index', () => {
    const post = (hashes: string[]) =>
      app.inject({ method: 'POST', url: '/v8/artifacts', payload: { hashes } })

    /** Starts the index with a first POST, waits for it, forgets the calls. */
    const buildIndex = async () => {
      await post(['00'])
      await settleS3KeyIndexes()
      send.mockClear()
    }

    it('answers the first POST per hash without waiting for a slow listing', async () => {
      bucket.add('prefix/turbogha_aaaa')
      let release = (): void => {}
      state.listGate = new Promise<void>(resolve => {
        release = resolve
      })

      const first = await post(['aaaa', 'bbbb'])

      // Answered while every listing is still held at the gate
      const expected = { aaaa: { taskDurationMs: 0 }, bbbb: null }
      expect(first.statusCode).toBe(200)
      expect(first.json()).toEqual(expected)
      expect(commandsSent(HeadObjectCommand)).toBe(2)
      expect(commandsSent(ListObjectsV2Command)).toBe(32)

      release()
      await settleS3KeyIndexes()
      send.mockClear()
      const later = await post(['aaaa', 'bbbb'])

      // Same answers, now from the index alone
      expect(later.json()).toEqual(expected)
      expect(commandsSent(HeadObjectCommand)).toBe(0)
      expect(commandsSent(ListObjectsV2Command)).toBe(0)
    })

    it('answers every hash from one sharded listing, with no HeadObject', async () => {
      // 5 keys under shard 00 (3 pages of 2), one each under 7f and ff,
      // an old `hash#tag` key and a key outside the cache prefix
      for (const h of ['0001', '0002', '0003', '0004', '0005', '7f00', 'ff00'])
        bucket.add(`prefix/turbogha_${h}`)
      bucket.add('prefix/turbogha_abcd#tag')
      bucket.add('prefix/other_abcd')
      const hashes = ['0001', '0005', '7f00', 'ff00', '0006', 'abcd', 'ab00']
      const expected = {
        '0001': { taskDurationMs: 0 },
        '0005': { taskDurationMs: 0 },
        '7f00': { taskDurationMs: 0 },
        ff00: { taskDurationMs: 0 },
        '0006': null,
        abcd: null,
        ab00: null
      }

      const perHash = await post(hashes)
      await settleS3KeyIndexes()
      const prefixes = send.mock.calls
        .map(([command]) => command)
        .filter(command => command instanceof ListObjectsV2Command)
        .map(command => command.input.Prefix)
      send.mockClear()
      const fromIndex = await post(hashes)

      expect(perHash.json()).toEqual(expected)
      expect(fromIndex.statusCode).toBe(200)
      expect(fromIndex.json()).toEqual(expected)
      expect(commandsSent(HeadObjectCommand)).toBe(0)
      // 256 shards, plus two continuation pages for shard 00
      expect(prefixes).toHaveLength(258)
      expect(new Set(prefixes).size).toBe(256)
      expect(prefixes).toContain('prefix/turbogha_00')
      expect(prefixes).toContain('prefix/turbogha_ff')
    })

    it('builds the index once across POSTs', async () => {
      bucket.add('prefix/turbogha_aaaa')

      await Promise.all([post(['aaaa']), post(['bbbb'])])
      await settleS3KeyIndexes()
      const res = await post(['aaaa', 'cccc'])

      expect(res.json()).toEqual({ aaaa: { taskDurationMs: 0 }, cccc: null })
      expect(commandsSent(ListObjectsV2Command)).toBe(256)
      // Only the two first POSTs checked per hash
      expect(commandsSent(HeadObjectCommand)).toBe(2)
    })

    it('adds a saved hash to the index', async () => {
      await buildIndex()

      const put = await app.inject({
        method: 'PUT',
        url: '/v8/artifacts/aaaa',
        headers: { 'content-type': 'application/octet-stream' },
        payload: Buffer.from('artifact')
      })
      // Gone from R2 again: only the index can still answer a hit
      bucket.clear()
      const res = await post(['aaaa'])

      expect(put.statusCode).toBe(200)
      expect(res.json()).toEqual({ aaaa: { taskDurationMs: 0 } })
      expect(commandsSent(ListObjectsV2Command)).toBe(0)
      expect(commandsSent(HeadObjectCommand)).toBe(0)
    })

    it('checks R2 for a hash the shard listing does not cover', async () => {
      await buildIndex()
      bucket.add('prefix/turbogha_ping-test-file')

      const res = await post(['ping-test-file', 'zz'])

      expect(res.json()).toEqual({
        'ping-test-file': { taskDurationMs: 0 },
        zz: null
      })
      expect(commandsSent(HeadObjectCommand)).toBe(2)
    })

    it('answers 501 when R2 fails a check the index left to it', async () => {
      await buildIndex()
      send.mockImplementation(async (command: Command) => {
        if (command instanceof HeadObjectCommand) throw r2Error()
        return fakeS3(command)
      })

      const res = await post(['aaaa', 'not-hex'])

      expect(res.statusCode).toBe(501)
    })

    it('answers per hash after a failed listing and retries it on a later POST', async () => {
      bucket.add('prefix/turbogha_aaaa')
      state.listFails = true
      const expected = { aaaa: { taskDurationMs: 0 }, bbbb: null }

      const first = await post(['aaaa', 'bbbb'])
      await settleS3KeyIndexes()
      state.listFails = false
      // Starts a new build, answered per hash while it runs
      const second = await post(['aaaa', 'bbbb'])
      await settleS3KeyIndexes()
      const third = await post(['aaaa', 'bbbb'])

      expect(first.json()).toEqual(expected)
      expect(second.json()).toEqual(expected)
      expect(third.json()).toEqual(expected)
      // The first two POSTs checked R2 per hash; the third used the index
      expect(commandsSent(HeadObjectCommand)).toBe(4)
      expect(commandsSent(ListObjectsV2Command)).toBeGreaterThan(256)
    })
  })

  describe('HEAD /v8/artifacts/:hash', () => {
    it('answers a hit with 200 and a miss with 404 from HeadObject alone', async () => {
      bucket.add('prefix/turbogha_aaaa')

      const hit = await app.inject({
        method: 'HEAD',
        url: '/v8/artifacts/aaaa?teamId=turbogha'
      })
      const miss = await app.inject({
        method: 'HEAD',
        url: '/v8/artifacts/bbbb?teamId=turbogha'
      })

      expect(hit.statusCode).toBe(200)
      expect(miss.statusCode).toBe(404)
      expect(commandsSent(HeadObjectCommand)).toBe(2)
      expect(commandsSent(GetObjectCommand)).toBe(0)
    })

    it('answers an R2 error as a miss, like GET does', async () => {
      send.mockRejectedValue(r2Error())

      const res = await app.inject({
        method: 'HEAD',
        url: '/v8/artifacts/aaaa'
      })

      expect(res.statusCode).toBe(404)
      expect(commandsSent(GetObjectCommand)).toBe(0)
    })
  })

  it('GET still reads the artifact', async () => {
    bucket.add('prefix/turbogha_aaaa')

    const res = await app.inject({ method: 'GET', url: '/v8/artifacts/aaaa' })

    expect(res.statusCode).toBe(200)
    expect(res.body).toBe('artifact')
    expect(commandsSent(GetObjectCommand)).toBe(1)
  })
})
