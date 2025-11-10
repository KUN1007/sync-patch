import sharp from 'sharp'
import { uploadImageToS3 } from '../lib/s3/uploadImageToS3'

export const COMPRESS_QUALITY = 60
export const MAX_SIZE = 1.007

export const checkBufferSize = (buffer: Buffer, maxSizeInMegabyte: number) => {
  const maxSizeInBytes = maxSizeInMegabyte * 1024 * 1024
  return buffer.length <= maxSizeInBytes
}

export const uploadPatchBanner = async (image: ArrayBuffer, id: number) => {
  const banner = await sharp(image)
    .resize(1920, 1080, {
      fit: 'inside',
      withoutEnlargement: true,
    })
    .avif({ quality: COMPRESS_QUALITY })
    .toBuffer()
  const miniBanner = await sharp(image)
    .resize(460, 259, {
      fit: 'inside',
      withoutEnlargement: true,
    })
    .avif({ quality: COMPRESS_QUALITY })
    .toBuffer()

  if (!checkBufferSize(miniBanner, MAX_SIZE)) {
    return '图片体积过大'
  }

  const bucketName = `patch/${id}/banner`

  await uploadImageToS3(`${bucketName}/banner.avif`, banner)
  await uploadImageToS3(`${bucketName}/banner-mini.avif`, miniBanner)
}
