import { TProvider } from '@/lib/providers'

import { createHash } from 'crypto'
import { Readable } from 'stream'
import { RequestContext } from '../../server'
import { TListFile } from '../../server/cleanup'
import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  NoSuchKey
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { cachePrefix, getCacheKey } from '@/lib/constants'
import { KeyIndex } from './index-cache'
import { core } from '@/lib/core'
import { mapLimit, timingProvider } from '@/lib/utils'
import { getTracker } from '@/lib/tracker'

// Helper function to get input value, prioritizing environment variables for local development
const getInput = (name: string, envNames?: string[]): string | undefined => {
  // In GitHub Actions context, try core.getInput first
  if (process.env.GITHUB_ACTIONS === 'true') {
    const coreInput = core.getInput(name)
    if (coreInput) return coreInput
  }

  // Fall back to environment variable
  const envVars = envNames || [name.toUpperCase().replace(/-/g, '_')]
  for (const envVar of envVars) {
    if (process.env[envVar]) {
      return process.env[envVar]
    }
  }
  return undefined
}

const artifactTagMetadata = 'artifact-tag'

// Turbo asks about up to 100 hashes per batch and sends the batches one after
// another; 32 checks in flight keep a batch to a few round trips.
const existsConcurrency = 32

// One key index per bucket and prefix, kept for the life of the process:
// getS3Provider runs on every request.
const keyIndexes = new Map<string, KeyIndex>()

/** Drops every key index; for tests. */
export const resetS3KeyIndexes = (): void => keyIndexes.clear()

// One S3 client per endpoint, region and credentials, kept for the life of the
// process: getS3Provider runs on every request, and each S3Client owns its own
// keep-alive connection pool. A client per request left its pooled sockets open
// to the endpoint, and a long-running server built up thousands of them.
const s3Clients = new Map<string, S3Client>()

/** Destroys and drops every cached S3 client; for tests. */
export const resetS3Clients = (): void => {
  for (const client of s3Clients.values()) client.destroy()
  s3Clients.clear()
}

const getS3Client = (config: {
  region: string
  endpoint: string
  accessKeyId: string
  secretAccessKey: string
  sessionToken?: string
}): S3Client => {
  // Hashed so no secret sits in a map key.
  const id = createHash('sha256')
    .update(
      JSON.stringify([
        config.endpoint,
        config.region,
        config.accessKeyId,
        config.secretAccessKey,
        config.sessionToken ?? ''
      ])
    )
    .digest('hex')
  let client = s3Clients.get(id)
  if (!client) {
    client = new S3Client({
      region: config.region,
      endpoint: config.endpoint,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
        sessionToken: config.sessionToken
      }
    })
    s3Clients.set(id, client)
  }
  return client
}

/** Waits for every running key index build to finish or fail; for tests. */
export const settleS3KeyIndexes = async (): Promise<void> => {
  await Promise.all([...keyIndexes.values()].map(index => index.settled()))
}

/** HeadObject reports a missing key as a bare 404 (`NotFound`), not NoSuchKey. */
const isNotFound = (error: unknown): boolean => {
  if (error instanceof NoSuchKey) return true
  const e = error as { name?: string; $metadata?: { httpStatusCode?: number } }
  return e?.name === 'NotFound' || e?.$metadata?.httpStatusCode === 404
}

export const getS3Provider = (
  tracker: ReturnType<typeof getTracker>
): TProvider => {
  const s3AccessKeyId = getInput('s3-access-key-id', [
    'AWS_ACCESS_KEY_ID',
    'S3_ACCESS_KEY_ID'
  ])
  const s3SecretAccessKey = getInput('s3-secret-access-key', [
    'AWS_SECRET_ACCESS_KEY',
    'S3_SECRET_ACCESS_KEY'
  ])
  const s3SessionToken = getInput('s3-session-token', [
    'AWS_SESSION_TOKEN',
    'S3_SESSION_TOKEN'
  ])
  const s3Bucket = getInput('s3-bucket', ['S3_BUCKET'])
  const s3Region = getInput('s3-region', [
    'AWS_REGION',
    'AWS_DEFAULT_REGION',
    'S3_REGION'
  ])
  const s3Endpoint =
    getInput('s3-endpoint', [
      'AWS_ENDPOINT_URL_S3',
      'AWS_ENDPOINT_URL',
      'S3_ENDPOINT'
    ]) || 'https://s3.amazonaws.com'
  const s3Prefix = getInput('s3-prefix', ['S3_PREFIX']) || 'turbogha/'

  if (!s3AccessKeyId || !s3SecretAccessKey || !s3Bucket || !s3Region) {
    throw new Error(
      'S3 provider requires s3-access-key-id, s3-secret-access-key, s3-bucket, and s3-region. Set these as environment variables or GitHub Actions inputs.'
    )
  }

  const s3Client = getS3Client({
    region: s3Region,
    endpoint: s3Endpoint,
    accessKeyId: s3AccessKeyId,
    secretAccessKey: s3SecretAccessKey,
    sessionToken: s3SessionToken
  })

  // The artifact tag lives in object metadata, not the key, so a lookup can read
  // the object directly from the hash. A list before each read bills as a write
  // on R2 ($4.50 per million against $0.36).
  const getS3Key = (hash: string) => {
    const key = getCacheKey(hash)
    if (s3Prefix) {
      return `${s3Prefix}${key}`
    }
    return key
  }

  const keyPrefix = `${s3Prefix}${cachePrefix}`
  const indexId = `${s3Endpoint}|${s3Bucket}|${keyPrefix}`
  let keyIndex = keyIndexes.get(indexId)
  if (!keyIndex) {
    const listPage = timingProvider(
      'list',
      tracker,
      async (prefix: string, continuationToken?: string) => {
        const response = await s3Client.send(
          new ListObjectsV2Command({
            Bucket: s3Bucket,
            Prefix: prefix,
            MaxKeys: 1000,
            ContinuationToken: continuationToken
          })
        )
        return {
          keys: (response.Contents ?? []).flatMap(o => (o.Key ? [o.Key] : [])),
          next: response.NextContinuationToken
        }
      }
    )
    keyIndex = new KeyIndex({
      listPage,
      keyPrefix,
      log: message => console.log(message)
    })
    keyIndexes.set(indexId, keyIndex)
  }
  const index = keyIndex

  const save = async (
    ctx: RequestContext,
    hash: string,
    tag: string,
    stream: Readable
  ): Promise<void> => {
    const objectKey = getS3Key(hash)
    try {
      // Use the S3 Upload utility which handles multipart uploads for large files
      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: s3Bucket,
          Key: objectKey,
          Body: stream,
          ContentType: 'application/octet-stream',
          Metadata: tag ? { [artifactTagMetadata]: tag } : undefined
        }
      })

      await upload.done()
      index.add(hash)
      ctx.log.info(`Saved artifact to S3: ${objectKey}`)
    } catch (error) {
      ctx.log.info(`Error saving artifact to S3: ${error}`)
      throw error
    }
  }

  const get = async (
    ctx: RequestContext,
    hash: string
  ): Promise<
    [number | undefined, Readable | ReadableStream, string | undefined] | null
  > => {
    const objectKey = getS3Key(hash)

    try {
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: s3Bucket,
          Key: objectKey
        })
      )

      if (!response.Body) {
        ctx.log.info(`Failed to get artifact body from S3`)
        return null
      }

      ctx.log.info(`Retrieved artifact from S3: ${objectKey}`)
      return [
        response.ContentLength,
        response.Body as Readable,
        response.Metadata?.[artifactTagMetadata]
      ]
    } catch (error) {
      if (error instanceof NoSuchKey) {
        ctx.log.info(`No cached artifact found for ${hash}`)
        return null
      }
      ctx.log.info(`Error getting artifact from S3: ${error}`)
      return null
    }
  }

  const exists = async (
    _ctx: RequestContext,
    hash: string
  ): Promise<boolean> => {
    try {
      await s3Client.send(
        new HeadObjectCommand({ Bucket: s3Bucket, Key: getS3Key(hash) })
      )
      return true
    } catch (error) {
      if (isNotFound(error)) return false
      throw error
    }
  }
  const timedExists = timingProvider('exists', tracker, exists)

  const existsMany = async (
    ctx: RequestContext,
    hashes: readonly string[]
  ): Promise<Map<string, boolean>> => {
    // The index answers what it covers once built; until then, and for the
    // hashes it doesn't cover, R2 answers one HeadObject per hash. The first
    // batch starts the build without waiting for it (#18540).
    const ready = index.current()
    const answers = new Map<string, boolean>()
    const unanswered: string[] = []
    for (const hash of hashes) {
      if (ready && index.covers(hash)) answers.set(hash, ready.has(hash))
      else unanswered.push(hash)
    }
    const found = await mapLimit(unanswered, existsConcurrency, hash =>
      timedExists(ctx, hash)
    )
    unanswered.forEach((hash, i) => answers.set(hash, found[i]))
    return answers
  }

  const deleteObj = async (key: string): Promise<void> => {
    try {
      const deleteCommand = new DeleteObjectCommand({
        Bucket: s3Bucket,
        Key: key
      })

      await s3Client.send(deleteCommand)
    } catch (error) {
      core.error(`Error deleting artifact from S3: ${error}`)
      throw error
    }
  }

  const list = async (): Promise<TListFile[]> => {
    try {
      const files: TListFile[] = []
      let continuationToken: string | undefined

      do {
        // Create a new command for each request with the current continuation token
        const listCommand = new ListObjectsV2Command({
          Bucket: s3Bucket,
          Prefix: s3Prefix,
          MaxKeys: 1000,
          ContinuationToken: continuationToken
        })

        core.debug(
          `Listing S3 objects with prefix ${s3Prefix}${continuationToken ? ' and continuation token' : ''}`
        )

        const response = await s3Client.send(listCommand)

        if (response.Contents && response.Contents.length > 0) {
          core.debug(`Found ${response.Contents.length} objects`)

          const objects = response.Contents.filter(obj => obj.Key).map(
            (obj): TListFile => {
              return {
                path: obj.Key as string,
                createdAt: (obj.LastModified || new Date()).toISOString(),
                size: obj.Size || 0
              }
            }
          )

          files.push(...objects)
        }

        continuationToken = response.NextContinuationToken
        if (continuationToken) {
          core.debug(`NextContinuationToken: ${continuationToken}`)
        }
      } while (continuationToken)

      core.debug(`Total files listed: ${files.length}`)
      return files
    } catch (error) {
      core.error(`Error listing artifacts from S3: ${error}`)
      throw error
    }
  }

  return {
    name: 's3',
    save: timingProvider('save', tracker, save),
    get: timingProvider('get', tracker, get),
    exists: timedExists,
    existsMany,
    delete: timingProvider('delete', tracker, deleteObj),
    list: timingProvider('list', tracker, list)
  }
}
