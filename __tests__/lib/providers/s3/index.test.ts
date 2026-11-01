import { Readable } from 'stream'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { send, uploads } = vi.hoisted(() => ({
  send: vi.fn(),
  uploads: [] as { Key: string; Metadata?: Record<string, string> }[]
}))

vi.mock('@aws-sdk/client-s3', async importOriginal => ({
  ...(await importOriginal<typeof import('@aws-sdk/client-s3')>()),
  S3Client: vi.fn(function () {
    return { send }
  })
}))

vi.mock('@aws-sdk/lib-storage', () => ({
  Upload: vi.fn(function (options: { params: (typeof uploads)[number] }) {
    uploads.push(options.params)
    return { done: async () => ({}) }
  })
}))

import { GetObjectCommand, NoSuchKey } from '@aws-sdk/client-s3'
import { getS3Provider } from '../../../../src/lib/providers/s3'
import { getTracker } from '../../../../src/lib/tracker'

const ctx = { log: { info: () => {} } }

describe('s3 provider', () => {
  beforeEach(() => {
    send.mockReset()
    uploads.length = 0
    process.env.S3_ACCESS_KEY_ID = 'id'
    process.env.S3_SECRET_ACCESS_KEY = 'secret'
    process.env.S3_BUCKET = 'bucket'
    process.env.S3_REGION = 'auto'
    process.env.S3_PREFIX = 'prefix/'
  })

  it('saves under the hash alone, with the tag in metadata', async () => {
    await getS3Provider(getTracker()).save(
      ctx,
      'abc',
      'sig+/=',
      Readable.from('x')
    )

    expect(uploads[0].Key).toBe('prefix/turbogha_abc')
    expect(uploads[0].Metadata).toEqual({ 'artifact-tag': 'sig+/=' })
  })

  it('sends no metadata for an unsigned artifact', async () => {
    await getS3Provider(getTracker()).save(ctx, 'abc', '', Readable.from('x'))

    expect(uploads[0].Key).toBe('prefix/turbogha_abc')
    expect(uploads[0].Metadata).toBeUndefined()
  })

  it('reads a hit with one GetObject and returns its tag', async () => {
    const body = Readable.from('x')
    send.mockResolvedValueOnce({
      Body: body,
      ContentLength: 1,
      Metadata: { 'artifact-tag': 'sig' }
    })

    const result = await getS3Provider(getTracker()).get(ctx, 'abc')

    expect(result).toEqual([1, body, 'sig'])
    expect(send).toHaveBeenCalledTimes(1)
    const command = send.mock.calls[0][0]
    expect(command).toBeInstanceOf(GetObjectCommand)
    expect(command.input.Key).toBe('prefix/turbogha_abc')
  })

  it('treats a missing object as a miss with one GetObject', async () => {
    send.mockRejectedValueOnce(
      new NoSuchKey({ message: 'missing', $metadata: {} })
    )

    expect(await getS3Provider(getTracker()).get(ctx, 'abc')).toBeNull()
    expect(send).toHaveBeenCalledTimes(1)
    expect(send.mock.calls[0][0]).toBeInstanceOf(GetObjectCommand)
  })
})
