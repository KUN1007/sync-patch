import path from 'path'
import { readdir } from 'fs/promises'
import { prisma } from './db/prisma'
import { parsePatchFileName, type ParsedPatchFileName } from './vn-sync/parse'
import { createPatchResourceForFile, formatDateYYMMDD } from './vn-sync/index'
import { sanitizeFileName } from './lib/sanitizeFileName'
import { vndbGetVnById } from './api/vndb'

type Item = { filePath: string; parsed: ParsedPatchFileName }
type VMap = Map<string, Item[]>

async function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms))
}

async function scanDir(dir: string): Promise<VMap> {
  const out: VMap = new Map()
  const names = await readdir(dir)
  for (const name of names) {
    const filePath = path.posix.join(dir, name)
    const parsed = parsePatchFileName(filePath)
    if (!parsed) {
      console.error(`[repair] skip unrecognized filename: ${name}`)
      continue
    }
    const list = out.get(parsed.vndbId) || []
    list.push({ filePath, parsed })
    out.set(parsed.vndbId, list)
  }
  return out
}

function deriveJaTitleFromVn(vn: any): string {
  try {
    const titles = Array.isArray(vn?.titles) ? vn.titles : []
    const jaItem = titles.find(
      (t: any) => String(t?.lang || '').split('-')[0] === 'ja'
    )
    if (jaItem?.title) return String(jaItem.title)
    const olang = String(vn?.olang || '').toLowerCase()
    if (olang === 'ja' && vn?.alttitle) return String(vn.alttitle)
  } catch {}
  return ''
}

async function ensurePatchExistsWithVndb(
  parsed: ParsedPatchFileName
): Promise<number> {
  // 1) If exists, return id
  const existing = await prisma.patch.findFirst({
    where: { vndb_id: parsed.vndbId },
    select: { id: true },
  })
  if (existing) return existing.id

  // 2) Fetch VNDB data for accurate fields (retry a bit)
  let vn: any = null
  let lastErr: any = null
  for (let i = 0; i < 3; i++) {
    try {
      vn = await vndbGetVnById(parsed.vndbId)
      if (vn) break
    } catch (e) {
      lastErr = e
    }
    await sleep(500 * (i + 1))
  }
  if (!vn) {
    throw new Error(
      `VNDB fetch failed for ${parsed.vndbId}: ${lastErr?.message || lastErr || 'unknown error'}`
    )
  }

  const nameEn = (vn as any)?.title || ''
  const nameJa = parsed.gameName || deriveJaTitleFromVn(vn)
  const releasedRaw = String((vn as any)?.released || '').trim()
  const released = releasedRaw ? formatDateYYMMDD(releasedRaw) : 'unknown'

  const patch = await prisma.patch.create({
    data: {
      name: '',
      name_en_us: nameEn || '',
      name_ja_jp: nameJa || '',
      vndb_id: parsed.vndbId,
      user_id: 1,
      banner: '',
      released,
      content_limit: 'sfw',
      type: [],
      language: [],
      engine: [],
      platform: [],
    },
    select: { id: true },
  })
  return patch.id
}

async function resourceExists(patchId: number, fileName: string) {
  const r = await prisma.patch_resource.findFirst({
    where: {
      patch_id: patchId,
      storage: 's3',
      content: { endsWith: `/${fileName}` },
    },
    select: { id: true },
  })
  return !!r
}

export async function repairPatchesAndResources(options?: {
  dir?: string
  onlyVndb?: string[]
  maxRetries?: number
}) {
  const dir = options?.dir ?? 'patch'
  const maxRetries = options?.maxRetries ?? 2
  const all = await scanDir(dir)

  const targetV = options?.onlyVndb?.length
    ? options.onlyVndb
    : Array.from(all.keys())

  const results: Array<{
    vndbId: string
    file: string
    ok: boolean
    error?: string
  }> = []

  console.log(`[repair] Targets: ${targetV.length} VNDB IDs`)
  for (const vndbId of targetV) {
    const items = all.get(vndbId) || []
    if (!items.length) continue

    // Use the first item to create the patch if missing
    const first = items[0]
    let patchId: number | null = null
    try {
      patchId = await ensurePatchExistsWithVndb(first.parsed)
      console.log(`[repair] ${vndbId}: patch id ${patchId}`)
    } catch (e: any) {
      const msg = e?.message || String(e)
      console.error(`[repair] ${vndbId}: failed to create/find patch: ${msg}`)
      for (const it of items) {
        results.push({
          vndbId,
          file: path.basename(it.filePath),
          ok: false,
          error: `patch error: ${msg}`,
        })
      }
      continue
    }

    // Ensure resources
    for (const it of items) {
      const sanitizedName = sanitizeFileName(it.parsed.fileName)
      const prefix = `[repair] ${vndbId} ${sanitizedName}`
      // If resource already exists in DB, skip
      try {
        const exists = await resourceExists(patchId, sanitizedName)
        if (exists) {
          console.log(
            `[repair] ${vndbId}: ${sanitizedName} already exists, skip`
          )
          results.push({ vndbId, file: sanitizedName, ok: true })
          continue
        }
      } catch {}
      // createPatchResourceForFile uses sanitize internally for note only; the S3 key uses sanitizeFileName in upload.ts
      // To check existence, we only rely on DB `content` endsWith(fileName) may fail if sanitized; so skip DB check and try creation directly with guard on duplicates.
      // Safer approach: attempt create; if Prisma unique violation or createPatchResourceForFile returns falsy, we retry.

      let attempt = 0
      let ok = false
      let lastErr: any = null
      while (attempt <= maxRetries && !ok) {
        attempt++
        try {
          console.log(`${prefix}: start (attempt ${attempt})`)
          let lastHashPercent = -1
          let lastUploadPercent = -1
          const res = await createPatchResourceForFile(patchId, it.parsed, {
            log: (m) => {
              if (m === 'hash:start') console.log(`${prefix}: hashing start`)
              if (m === 'hash:done') console.log(`${prefix}: hashing done`)
              if (m.startsWith('upload:start')) {
                const sizeMatch = m.match(/size=(\d+)/)
                const size = sizeMatch ? Number(sizeMatch[1]) : 0
                console.log(`${prefix}: upload start (${size} bytes)`)
              }
              if (m === 'upload:done') console.log(`${prefix}: upload done`)
            },
            onHashProgress: ({ percent, bytesRead, total }) => {
              const p = Math.floor((percent || 0) * 100)
              if (p !== lastHashPercent && p % 5 === 0) {
                lastHashPercent = p
                console.log(
                  `${prefix}: hashing ${p}% (${bytesRead}/${total} bytes)`
                )
              }
            },
            onUploadProgress: ({ percent, uploadedParts, totalParts }) => {
              const p = Math.floor((percent || 0) * 100)
              if (p !== lastUploadPercent && p % 5 === 0) {
                lastUploadPercent = p
                console.log(
                  `${prefix}: upload ${p}% (${uploadedParts}/${totalParts} parts)`
                )
              }
            },
          })
          if (!res || typeof res === 'string') {
            throw new Error(`resource create returned ${String(res)}`)
          }
          ok = true
          results.push({ vndbId, file: sanitizedName, ok: true })
        } catch (e: any) {
          lastErr = e
          console.error(
            `[repair] ${vndbId}: ${sanitizedName} attempt ${attempt} failed: ${e?.message || String(e)}`
          )
          if (attempt <= maxRetries) await sleep(2000 * attempt)
        }
      }
      if (!ok) {
        results.push({
          vndbId,
          file: sanitizedName,
          ok: false,
          error: lastErr?.message || String(lastErr),
        })
      }
    }
  }

  // Summary
  const failed = results.filter((r) => !r.ok)
  if (failed.length) {
    console.error(`\n[repair] Failed ${failed.length} files:`)
    for (const f of failed) {
      console.error(` - ${f.vndbId} ${f.file}: ${f.error}`)
    }
    return results
  }
  console.log('\n[repair] All files processed successfully.')
  return results
}

// CLI
;(async () => {
  const args = process.argv.slice(2)
  const getArg = (key: string) => {
    const i = args.findIndex((a) => a === key || a.startsWith(`${key}=`))
    if (i === -1) return undefined
    const a = args[i]
    if (a.includes('=')) return a.split('=').slice(1).join('=')
    return args[i + 1]
  }

  const dir = getArg('--dir') || getArg('-d') || 'patch'
  const onlyV = getArg('--vndb') || getArg('-v')
  const onlyVndb = onlyV
    ? onlyV
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean)
    : undefined
  const retriesRaw = getArg('--retries')
  const maxRetries = retriesRaw ? Number(retriesRaw) : 2

  try {
    const res = await repairPatchesAndResources({ dir, onlyVndb, maxRetries })
    if (res.some((r) => !r.ok)) process.exitCode = 1
  } catch (err) {
    console.error('[repair] Fatal error:', err)
    process.exitCode = 1
  } finally {
    try {
      await prisma.$disconnect()
    } catch {}
  }
})()
