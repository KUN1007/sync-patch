import path from 'path'
import { readdir } from 'fs/promises'
import { prisma } from './db/prisma'
import { parsePatchFileName } from './vn-sync/parse'
import {
  createPatchIfMissing,
  createPatchResourceForFile,
} from './vn-sync/index'

export async function syncPatchesToS3(dir = 'patch') {
  const files = await readdir(dir)
  const results: Array<{ file: string; ok: boolean; error?: string }> = []
  for (const name of files) {
    const filePath = path.posix.join(dir, name)
    try {
      const parsed = parsePatchFileName(filePath)
      if (!parsed) {
        const msg = 'Unrecognized filename'
        console.error(`[sync] ${name}: ${msg}`)
        results.push({ file: name, ok: false, error: msg })
        continue
      }

      const patchId = await createPatchIfMissing(parsed)
      if (!patchId) {
        const msg = 'Failed to create or find patch'
        console.error(`[sync] ${name}: ${msg}`)
        results.push({ file: name, ok: false, error: msg })
        continue
      }
      const resource = await createPatchResourceForFile(patchId, parsed)
      if (!resource) {
        const msg = 'Failed to create patch resource'
        console.error(`[sync] ${name}: ${msg}`)
        results.push({ file: name, ok: false, error: msg })
        continue
      }

      const patch = await prisma.patch.findUnique({
        where: { id: patchId },
        select: { name_ja_jp: true, banner: true },
      })

      const jaName = patch?.name_ja_jp || ''
      const banner = patch?.banner || ''
      const bannerMini = banner
        ? banner.replace(/\/banner\.(avif|webp)$/i, '/banner-mini.$1')
        : ''
      const fileUrl = (resource as any)?.content || ''

      // Print requested details
      console.log(`Patch: ${jaName}`)
      if (banner) console.log(`banner: ${banner}`)
      if (bannerMini) console.log(`banner-mini: ${bannerMini}`)
      if (fileUrl) console.log(`file: ${fileUrl}`)

      results.push({ file: name, ok: true })
    } catch (e: any) {
      const message = e?.message || String(e)
      console.error(`[sync] ${name}: ${message}`)
      results.push({ file: name, ok: false, error: message })
    }
  }
  return results
}

// Run when executed directly; print a summary and exit non-zero on failures
;(async () => {
  try {
    const results = await syncPatchesToS3()
    const failed = (results || []).filter((r) => !r.ok)
    if (failed.length) {
      console.error(`\nFailed files: ${failed.length}`)
      for (const f of failed) {
        console.error(` - ${f.file}: ${f.error || 'Unknown error'}`)
      }
      process.exitCode = 1
    } else {
      console.log('\nAll files processed successfully.')
    }
  } catch (err) {
    console.error('Fatal error while syncing patches:', err)
    process.exitCode = 1
  } finally {
    try {
      await prisma.$disconnect()
    } catch {}
  }
})()
