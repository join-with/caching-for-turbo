import { Readable } from 'stream'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

// A fake bucket behind a fake S3 client: HeadObject, GetObject and
// ListObjectsV2 answer from `bucket`, and every call is recorded.
const { send, bucket, state } = vi.hoisted(() => ({
  send: vi.fn(),
  bucket: new Set<string>(),
  state: { inFlight: 0, maxInFlight: 0 }
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
  NotFound,
  NoSuchKey,
  S3ServiceException
} from '@aws-sdk/client-s3'
import { buildServer } from '../../../src/lib/server'

type Command = { input: { Key?: string } }

const fakeS3 = async (command: Command): Promise<unknown> => {
  state.inFlight++
  state.maxInFlight = Math.max(state.maxInFlight, state.inFlight)
  try {
    await new Promise(resolve => setTimeout(resolve, 1))
    const key = command.input.Key as string
    if (command instanceof HeadObjectCommand) {
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
    state.inFlight--
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
    state.inFlight = 0
    state.maxInFlight = 0
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

  describe('POST /v8/artifacts', () => {
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
      expect(state.maxInFlight).toBeLessThanOrEqual(32)
      expect(state.maxInFlight).toBeGreaterThan(1)
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
