import path from 'path'
import { createReadStream, createWriteStream } from 'fs'
import { stat as fsStat } from 'fs/promises'
import { mkdir, writeFile } from 'fs/promises'
import { blake3 } from '@noble/hashes/blake3.js'
import * as utils from '@noble/hashes/utils.js'

const { bytesToHex } = utils

export const generateFileHash = async (
  filePath: string,
  onProgress?: (info: {
    bytesRead: number
    total: number
    percent: number
  }) => void
): Promise<string> => {
  const { size: total } = await fsStat(filePath)
  return new Promise((resolve, reject) => {
    const hashInstance = blake3.create({})
    let bytesRead = 0
    let lastEmit = 0
    const fileStream = createReadStream(filePath)
    fileStream.on('data', (chunk) => {
      const c: any = chunk as any
      bytesRead += c?.length || 0
      hashInstance.update(c)
      if (onProgress) {
        const now = Date.now()
        if (now - lastEmit >= 1000 || bytesRead >= total) {
          lastEmit = now
          const percent = total > 0 ? bytesRead / total : 0
          try {
            onProgress({ bytesRead, total, percent })
          } catch {}
        }
      }
    })
    fileStream.on('end', () => {
      const hashString = bytesToHex(hashInstance.digest())
      resolve(hashString)
    })
    fileStream.on('error', (err) => {
      reject(err)
    })
  })
}

export const calculateFileStreamHash = async (
  fileBuffer: Buffer,
  fileDir: string,
  filename: string
) => {
  await mkdir(fileDir, { recursive: true })

  const tempFilePath = path.posix.join(fileDir, 'temp')
  const hashInstance = blake3.create({})

  const writeStream = createWriteStream(tempFilePath)

  try {
    await new Promise<void>((resolve, reject) => {
      writeStream.on('error', reject)
      writeStream.on('finish', resolve)

      writeStream.write(fileBuffer, (error) => {
        if (error) {
          reject(error)
        } else {
          hashInstance.update(fileBuffer)
          writeStream.end()
        }
      })
    })

    const fileHash = bytesToHex(hashInstance.digest())
    await mkdir(`${fileDir}/${fileHash}`, { recursive: true })
    const finalFilePath = path.posix.join(fileDir, `${fileHash}/${filename}`)

    await writeFile(finalFilePath, fileBuffer)

    return { fileHash, finalFilePath }
  } finally {
    writeStream.destroy()
  }
}
