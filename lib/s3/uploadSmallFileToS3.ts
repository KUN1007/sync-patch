import { s3 } from './client'
import { readFile } from 'fs/promises'
import { PutObjectCommand } from '@aws-sdk/client-s3'

const MAX_SMALL_RETRIES = 3

export const uploadSmallFileToS3 = async (key: string, filePath: string) => {
  const bucket = process.env.KUN_VISUAL_NOVEL_S3_STORAGE_BUCKET_NAME!
  let lastErr: any = null
  for (let attempt = 1; attempt <= MAX_SMALL_RETRIES; attempt++) {
    try {
      const fileBuffer = await readFile(filePath)
      const uploadCommand = new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: fileBuffer,
        ContentType: 'application/octet-stream',
      })
      await s3.send(uploadCommand)
      return
    } catch (error) {
      lastErr = error
      await new Promise((r) => setTimeout(r, 500 * attempt))
    }
  }
  return `上传文件错误, uploadSmallFileToS3 function ERROR: ${
    (lastErr as any)?.message || String(lastErr)
  }`
}
