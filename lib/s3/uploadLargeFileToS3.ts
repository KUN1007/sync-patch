import { s3 } from './client'
import {
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
} from '@aws-sdk/client-s3'
import { createReadStream } from 'fs'
import { stat } from 'fs/promises'

const CHUNK_SIZE =
  (Number(process.env.KUN_S3_CHUNK_SIZE_MB || 5) | 0) * 1024 * 1024
const MAX_CONCURRENT_UPLOADS = Math.max(
  1,
  Number(process.env.KUN_S3_MAX_CONCURRENCY || 4)
)
const MAX_PART_RETRIES = 3

export const uploadLargeFileToS3 = async (
  key: string,
  filePath: string,
  onProgress?: (info: {
    uploadedParts: number
    totalParts: number
    percent: number
  }) => void
) => {
  const bucket = process.env.KUN_VISUAL_NOVEL_S3_STORAGE_BUCKET_NAME!

  const fileStats = await stat(filePath)
  const fileSize = fileStats.size
  let uploadId: string | undefined

  try {
    const multipartUpload = await s3.send(
      new CreateMultipartUploadCommand({
        Bucket: bucket,
        Key: key,
        ContentType: 'application/octet-stream',
      })
    )

    uploadId = multipartUpload.UploadId
    if (!uploadId) {
      return '上传过程错误, 未找到 S3 Upload ID'
    }

    const totalParts = Math.ceil(fileSize / CHUNK_SIZE)
    const uploadPromises: Promise<{ PartNumber: number; ETag: string }>[] = []
    const completedParts: { PartNumber: number; ETag: string }[] = []

    const uploadOnePart = async (
      partNumber: number,
      start: number,
      end: number
    ): Promise<{ PartNumber: number; ETag: string }> => {
      let lastErr: any = null
      for (let attempt = 1; attempt <= MAX_PART_RETRIES; attempt++) {
        try {
          const fileStream = createReadStream(filePath, { start, end: end - 1 })
          const uploadPartCommand = new UploadPartCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
            PartNumber: partNumber,
            Body: fileStream,
            ContentLength: end - start,
          } as any)
          const response = await s3.send(uploadPartCommand)
          return { PartNumber: partNumber, ETag: response.ETag! }
        } catch (e) {
          lastErr = e
          await new Promise((r) => setTimeout(r, 500 * attempt))
        }
      }
      throw lastErr
    }

    for (let partNumber = 1; partNumber <= totalParts; partNumber++) {
      const start = (partNumber - 1) * CHUNK_SIZE
      const end = Math.min(start + CHUNK_SIZE, fileSize)

      const p = uploadOnePart(partNumber, start, end)
      uploadPromises.push(p)

      if (
        uploadPromises.length >= MAX_CONCURRENT_UPLOADS ||
        partNumber === totalParts
      ) {
        try {
          const results = await Promise.all(uploadPromises)
          completedParts.push(...results)
          uploadPromises.length = 0
          if (onProgress) {
            const uploadedParts = completedParts.length
            const percent = totalParts > 0 ? uploadedParts / totalParts : 0
            try {
              onProgress({ uploadedParts, totalParts, percent })
            } catch {}
          }
        } catch (batchErr) {
          try {
            if (uploadId) {
              await s3.send(
                new AbortMultipartUploadCommand({
                  Bucket: bucket,
                  Key: key,
                  UploadId: uploadId,
                })
              )
            }
          } catch {}
          const errMsg = batchErr?.message || '上传过程发生错误, 已终止上传过程'
          return `上传过程发生错误, 已终止上传过程: ${errMsg}`
        }
      }
    }

    try {
      await s3.send(
        new CompleteMultipartUploadCommand({
          Bucket: bucket,
          Key: key,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: completedParts.sort((a, b) => a.PartNumber - b.PartNumber),
          },
        })
      )
    } catch (completeErr) {
      try {
        if (uploadId) {
          await s3.send(
            new AbortMultipartUploadCommand({
              Bucket: bucket,
              Key: key,
              UploadId: uploadId,
            })
          )
        }
      } catch {}
      const msg = completeErr?.message || '上传过程发生错误, 已终止上传过程'
      return `上传过程发生错误, 已终止上传过程: ${msg}`
    }

    return {}
  } catch (error: any) {
    try {
      if (uploadId) {
        await s3.send(
          new AbortMultipartUploadCommand({
            Bucket: bucket,
            Key: key,
            UploadId: uploadId,
          })
        )
      }
    } catch {}
    const msg = error?.message || '上传过程发生错误, 已终止上传过程'
    return `上传过程发生错误, 已终止上传过程: ${msg}`
  }
}
