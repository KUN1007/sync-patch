import path from 'path'
import { readdir } from 'fs/promises'
import { prisma } from './db/prisma'
import { parsePatchFileName } from './vn-sync/parse'
import { sanitizeFileName } from './lib/sanitizeFileName'

type ExpectedMap = Map<string, Set<string>>

async function buildExpectedFromDir(dir: string): Promise<ExpectedMap> {
  const entries = await readdir(dir)
  const map: ExpectedMap = new Map()
  for (const name of entries) {
    const filePath = path.posix.join(dir, name)
    const parsed = parsePatchFileName(filePath)
    if (!parsed) continue
    const sanitized = sanitizeFileName(parsed.fileName)
    if (!map.has(parsed.vndbId)) map.set(parsed.vndbId, new Set())
    map.get(parsed.vndbId)!.add(sanitized)
  }
  return map
}

function lastPathSegment(url: string): string {
  try {
    const idx = url.lastIndexOf('/')
    return idx >= 0 ? url.slice(idx + 1) : url
  } catch {
    return url
  }
}

export async function checkMissingResourcesByVndb(options?: {
  dir?: string
  onlyVndb?: string[]
}) {
  const dir = options?.dir ?? 'patch'
  const expected = await buildExpectedFromDir(dir)

  const vndbIds = options?.onlyVndb?.length
    ? options.onlyVndb
    : Array.from(expected.keys())

  if (!vndbIds.length) {
    console.log('[check] No VNDB IDs found to check.')
    return [] as Array<{ vndbId: string; missing: string[]; reason: string }>
  }

  const patches = await prisma.patch.findMany({
    where: { vndb_id: { in: vndbIds } },
    select: {
      id: true,
      vndb_id: true,
      resource: { select: { content: true, storage: true } },
    },
  })

  const byVndb = new Map(patches.map((p) => [p.vndb_id || '', p]))

  const results: Array<{ vndbId: string; missing: string[]; reason: string }> =
    []

  for (const v of vndbIds) {
    const expSet = expected.get(v) || new Set<string>()
    const expList = Array.from(expSet)
    const p = byVndb.get(v || '')
    if (!p) {
      if (expList.length) {
        results.push({
          vndbId: v,
          missing: expList,
          reason: 'Patch not found in DB',
        })
      } else {
        results.push({
          vndbId: v,
          missing: [],
          reason: 'Patch not found; no local expectation',
        })
      }
      continue
    }

    const presentNames = new Set(
      p.resource
        .filter((r) => r.storage === 's3' && !!r.content)
        .map((r) => lastPathSegment(r.content))
    )

    const missing: string[] = []
    for (const fname of expList) {
      if (!presentNames.has(fname)) missing.push(fname)
    }

    if (missing.length) {
      results.push({
        vndbId: v,
        missing,
        reason: 'Resource missing in DB or content empty',
      })
    }
  }

  if (!results.length) {
    console.log('[check] All expected resources are present.')
  } else {
    console.log(
      `[check] Found ${results.length} VNDB IDs with missing resources:`
    )
    for (const r of results) {
      console.log(` - ${r.vndbId}: ${r.missing.join(', ')} (${r.reason})`)
    }
  }

  return results
}

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

  try {
    const res = await checkMissingResourcesByVndb({ dir, onlyVndb })
    if (res.length) process.exitCode = 1
  } catch (err) {
    console.error('[check] Fatal error:', err)
    process.exitCode = 1
  } finally {
    try {
      await prisma.$disconnect()
    } catch {}
  }
})()
