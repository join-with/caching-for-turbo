import { TProvider } from '@/lib/providers'

import { Readable } from 'stream'
import { RequestContext } from '../../server'
import { TListFile } from '../../server/cleanup'
import {
  S3Client,
  GetObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  NoSuchKey
} from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { getCacheKey } from '@/lib/constants'
import { core } from '@/lib/core'
import { timingProvider } from '@/lib/utils'
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

  const s3Client = new S3Client({
    region: s3Region,
    endpoint: s3Endpoint,
    credentials: {
      accessKeyId: s3AccessKeyId,
      secretAccessKey: s3SecretAccessKey,
      sessionToken: s3SessionToken
    }
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
    delete: timingProvider('delete', tracker, deleteObj),
    list: timingProvider('list', tracker, list)
  }
}
