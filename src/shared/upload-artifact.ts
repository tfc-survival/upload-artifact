import * as core from '@actions/core'
import * as os from 'os'
import * as github from '@actions/github'
import artifact, {
  UploadArtifactOptions,
  GHESNotSupportedError,
  UploadArtifactResponse,
  InvalidResponseError,
  NetworkError
} from '@actions/artifact'
import {ReadStream} from 'fs'
import {createReadStream} from 'fs'
import * as crypto from 'crypto'
import * as stream from 'stream'
import {BlobClient, BlockBlobUploadStreamOptions} from '@azure/storage-blob'
import {TransferProgressEvent} from '@azure/core-http'
import {
  validateArtifactName,
  validateRootDirectory
} from './path-and-artifact-name-validation'
import {CreateArtifactRequest, FinalizeArtifactRequest} from './api/v1/artifact'
import {StringValue} from './google/protobuf/wrappers'
import {Timestamp} from './google/protobuf/timestamp'
import {getBackendIdsFromToken} from './utils'
import {internalArtifactTwirpClient} from './artifact-twirp-client'

export async function uploadArtifact(
  artifactName: string,
  filesToUpload: string[],
  rootDirectory: string,
  options: UploadArtifactOptions
) {
  const uploadResponse = await uploadArtifact1(
    artifactName,
    filesToUpload,
    rootDirectory,
    options
  )

  core.info(
    `Artifact ${artifactName} has been successfully uploaded! Final size is ${uploadResponse.size} bytes. Artifact ID is ${uploadResponse.id}`
  )
  core.setOutput('artifact-id', uploadResponse.id)

  const repository = github.context.repo
  const artifactURL = `${github.context.serverUrl}/${repository.owner}/${repository.repo}/actions/runs/${github.context.runId}/artifacts/${uploadResponse.id}`

  core.info(`Artifact download URL: ${artifactURL}`)
  core.setOutput('artifact-url', artifactURL)
}

export function isGhes(): boolean {
  const ghUrl = new URL(
    process.env['GITHUB_SERVER_URL'] || 'https://github.com'
  )
  return ghUrl.hostname.toUpperCase() !== 'GITHUB.COM'
}

async function uploadArtifact1(
  name: string,
  files: string[],
  rootDirectory: string,
  options?: UploadArtifactOptions
): Promise<UploadArtifactResponse> {
  try {
    if (isGhes()) {
      throw new GHESNotSupportedError()
    }

    if (Array.isArray(files) && files?.length === 1) {
      core.info(`uploading single artifact`)
      return uploadSingleArtifact(
        name,
        files.at(0) as string,
        rootDirectory,
        options
      )
    }

    core.info(`uploading multiple artifact bundle`)
    return artifact.uploadArtifact(name, files, rootDirectory, options)
  } catch (error) {
    core.warning(
      `Artifact upload failed with error: ${error}.

    Errors can be temporary, so please try again and optionally run the action with debug mode enabled for more information.

    If the error persists, please check whether Actions is operating normally at [https://githubstatus.com](https://www.githubstatus.com).`
    )

    throw error
  }
}

function getRetentionDays(): number | undefined {
  const retentionDays = process.env['GITHUB_RETENTION_DAYS']
  if (!retentionDays) {
    return undefined
  }
  const days = parseInt(retentionDays)
  if (isNaN(days)) {
    return undefined
  }

  return days
}

export function getExpiration(retentionDays?: number): Timestamp | undefined {
  if (!retentionDays) {
    return undefined
  }

  const maxRetentionDays = getRetentionDays()
  if (maxRetentionDays && maxRetentionDays < retentionDays) {
    core.warning(
      `Retention days cannot be greater than the maximum allowed retention set within the repository. Using ${maxRetentionDays} instead.`
    )
    retentionDays = maxRetentionDays
  }

  const expirationDate = new Date()
  expirationDate.setDate(expirationDate.getDate() + retentionDays)

  return Timestamp.fromDate(expirationDate)
}

async function uploadSingleArtifact(
  name: string,
  file: string,
  rootDirectory: string,
  options?: UploadArtifactOptions | undefined
): Promise<UploadArtifactResponse> {
  validateArtifactName(name)
  validateRootDirectory(rootDirectory)

  // get the IDs needed for the artifact creation
  const backendIds = getBackendIdsFromToken()

  // create the artifact client
  const artifactClient = internalArtifactTwirpClient()

  // create the artifact
  const createArtifactReq: CreateArtifactRequest = {
    workflowRunBackendId: backendIds.workflowRunBackendId,
    workflowJobRunBackendId: backendIds.workflowJobRunBackendId,
    name,
    version: 4
  }

  // if there is a retention period, add it to the request
  const expiresAt = getExpiration(options?.retentionDays)
  if (expiresAt) {
    createArtifactReq.expiresAt = expiresAt
  }

  const createArtifactResp = await artifactClient.CreateArtifact(
    createArtifactReq
  )
  if (!createArtifactResp.ok) {
    throw new InvalidResponseError(
      'CreateArtifact: response from backend was not ok'
    )
  }

  const uploadArtifactStream = createReadStream(file)

  /* TODO check stream validity */

  const uploadResult = await uploadActifactToBlobStorage(
    createArtifactResp.signedUploadUrl,
    uploadArtifactStream
  )

  // finalize the artifact
  const finalizeArtifactReq: FinalizeArtifactRequest = {
    workflowRunBackendId: backendIds.workflowRunBackendId,
    workflowJobRunBackendId: backendIds.workflowJobRunBackendId,
    name,
    size: uploadResult.uploadSize ? uploadResult.uploadSize.toString() : '0'
  }

  if (uploadResult.sha256Hash) {
    finalizeArtifactReq.hash = StringValue.create({
      value: `sha256:${uploadResult.sha256Hash}`
    })
  }

  core.info(`Finalizing artifact upload`)

  const finalizeArtifactResp = await artifactClient.FinalizeArtifact(
    finalizeArtifactReq
  )
  if (!finalizeArtifactResp.ok) {
    throw new InvalidResponseError(
      'FinalizeArtifact: response from backend was not ok'
    )
  }

  const artifactId = BigInt(finalizeArtifactResp.artifactId)
  core.info(
    `Artifact ${name}.zip successfully finalized. Artifact ID ${artifactId}`
  )

  return {
    size: uploadResult.uploadSize,
    id: Number(artifactId)
  }
}

export interface BlobUploadResponse {
  /**
   * The total reported upload size in bytes. Empty if the upload failed
   */
  uploadSize?: number

  /**
   * The SHA256 hash of the uploaded file. Empty if the upload failed
   */
  sha256Hash?: string
}

// Mimics behavior of azcopy: https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-optimize
// If your machine has fewer than 5 CPUs, then the value of this variable is set to 32.
// Otherwise, the default value is equal to 16 multiplied by the number of CPUs. The maximum value of this variable is 300.
export function getConcurrency(): number {
  const numCPUs = os.cpus().length

  if (numCPUs <= 4) {
    return 32
  }

  const concurrency = 16 * numCPUs
  return concurrency > 300 ? 300 : concurrency
}

// Used for controlling the highWaterMark value of the zip that is being streamed
// The same value is used as the chunk size that is use during upload to blob storage
export function getUploadChunkSize(): number {
  return 8 * 1024 * 1024 // 8 MB Chunks
}

async function uploadActifactToBlobStorage(
  authenticatedUploadURL: string,
  artifactUploadStream: ReadStream
): Promise<BlobUploadResponse> {
  let uploadByteCount = 0

  const maxConcurrency = getConcurrency()
  const bufferSize = getUploadChunkSize()
  const blobClient = new BlobClient(authenticatedUploadURL)
  const blockBlobClient = blobClient.getBlockBlobClient()

  core.debug(
    `Uploading artifact zip to blob storage with maxConcurrency: ${maxConcurrency}, bufferSize: ${bufferSize}`
  )

  const uploadCallback = (progress: TransferProgressEvent): void => {
    core.info(`Uploaded bytes ${progress.loadedBytes}`)
    uploadByteCount = progress.loadedBytes
  }

  const options: BlockBlobUploadStreamOptions = {
    blobHTTPHeaders: {blobContentType: 'zip'},
    onProgress: uploadCallback
  }

  let sha256Hash: string | undefined = undefined
  const uploadStream = new stream.PassThrough()
  const hashStream = crypto.createHash('sha256')

  artifactUploadStream.pipe(uploadStream) // This stream is used for the upload
  artifactUploadStream.pipe(hashStream).setEncoding('hex') // This stream is used to compute a hash of the zip content that gets used. Integrity check

  core.info('Beginning upload of artifact content to blob storage')

  try {
    await blockBlobClient.uploadStream(
      uploadStream,
      bufferSize,
      maxConcurrency,
      options
    )
  } catch (error: unknown) {
    if (NetworkError.isNetworkErrorCode((error as any)?.code)) {
      throw new NetworkError((error as any)?.code)
    }

    throw error
  }

  core.info('Finished uploading artifact content to blob storage!')

  hashStream.end()
  sha256Hash = hashStream.read() as string
  core.info(`SHA256 hash of uploaded artifact zip is ${sha256Hash}`)

  if (uploadByteCount === 0) {
    core.warning(
      `No data was uploaded to blob storage. Reported upload byte count is 0.`
    )
  }

  return {
    uploadSize: uploadByteCount,
    sha256Hash
  }
}
