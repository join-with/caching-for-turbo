import { createServer, type Server } from 'http'
import type { AddressInfo, Socket } from 'net'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import {
  getS3Provider,
  resetS3Clients,
  resetS3KeyIndexes
} from '../../../../src/lib/providers/s3'
import { getTracker } from '../../../../src/lib/tracker'

// A real S3 client against a local fake S3 that answers every GetObject with a
// small body and counts the TCP connections it accepts. The server builds a
// provider per request, so this is the request pattern of a long-running
// server.
const ctx = { log: { info: () => {} } }

describe('s3 provider connections', () => {
  let server: Server
  let accepted = 0
  const open = new Set<Socket>()

  beforeEach(async () => {
    accepted = 0
    server = createServer((req, res) => {
      res.setHeader('Content-Type', 'application/octet-stream')
      res.setHeader('x-amz-meta-artifact-tag', 'tag')
      res.end('body')
    })
    server.on('connection', socket => {
      accepted++
      open.add(socket)
      socket.on('close', () => open.delete(socket))
    })
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve))
    const { port } = server.address() as AddressInfo
    process.env.S3_ACCESS_KEY_ID = 'id'
    process.env.S3_SECRET_ACCESS_KEY = 'secret'
    process.env.S3_BUCKET = 'bucket'
    process.env.S3_REGION = 'auto'
    process.env.S3_PREFIX = 'prefix/'
    process.env.S3_ENDPOINT = `http://127.0.0.1:${port}`
    resetS3Clients()
    resetS3KeyIndexes()
  })

  afterEach(async () => {
    resetS3Clients()
    resetS3KeyIndexes()
    for (const socket of open) socket.destroy()
    await new Promise<void>(resolve => server.close(() => resolve()))
    delete process.env.S3_ENDPOINT
  })

  it('reuses one connection for sequential reads through per-request providers', async () => {
    for (let i = 0; i < 20; i++) {
      const result = await getS3Provider(getTracker()).get(ctx, `hash-${i}`)
      expect(result).not.toBeNull()
      const body = result![1] as NodeJS.ReadableStream
      for await (const _ of body) {
        // drain, as the server's reply does
      }
    }

    expect(accepted).toBe(1)
    expect(open.size).toBe(1)
  })
})
