export interface GfDocumentRow {
  id?: number | string | null
  gfd_id?: number | string | null
  created: string | Date
  language: string
  url: string
  digest_sri: string
}

export interface GfVersionRow {
  corporation_id?: number | string | null
  ecosystem_id?: number | string | null
  version: number
  created: string | Date
  active_since?: string | Date | null
  gfv_id?: number | string | null
  documents?: GfDocumentRow[]
}

// The chain replaces a document of the same language in place; the indexer keeps one row for each add.
export function currentGfDocuments<T extends Pick<GfDocumentRow, 'id' | 'gfd_id' | 'created' | 'language'>>(
  documents: T[],
  asOf?: Date
): T[] {
  const isBefore = (a: T, b: T) => {
    const gap = new Date(a.created).getTime() - new Date(b.created).getTime()
    return gap < 0 || (gap === 0 && Number(a.id ?? 0) < Number(b.id ?? 0))
  }
  const first = new Map<string, T>()
  const latest = new Map<string, T>()
  for (const document of documents) {
    if (asOf && new Date(document.created) > asOf) continue
    const earliest = first.get(document.language)
    if (!earliest || isBefore(document, earliest)) first.set(document.language, document)
    const newest = latest.get(document.language)
    if (!newest || isBefore(newest, document)) latest.set(document.language, document)
  }

  return [...latest.entries()].map(([language, document]) => {
    const earliest = first.get(language) ?? document
    return earliest === document
      ? document
      : { ...document, created: earliest.created, gfd_id: document.gfd_id ?? earliest.gfd_id }
  })
}

// The GovernanceFrameworkVersion object (IDX-GF-QRY-1) of the governance framework and corporation endpoints.
export function buildGfVersionObject(
  row: GfVersionRow,
  isCgf: boolean,
  asOf: Date | undefined,
  arrange: (documents: GfDocumentRow[]) => GfDocumentRow[]
): Record<string, unknown> {
  const chainGfvId = row.gfv_id ?? null
  const activeSince = row.active_since && (!asOf || new Date(row.active_since) <= asOf) ? row.active_since : null

  return {
    id: chainGfvId,
    ecosystem_id: isCgf ? null : Number(row.ecosystem_id),
    corporation_id: isCgf ? Number(row.corporation_id) : null,
    created: row.created,
    version: row.version,
    active_since: activeSince,
    documents: arrange(currentGfDocuments(row.documents ?? [], asOf)).map((d) => ({
      id: d.gfd_id ?? null,
      gfv_id: chainGfvId,
      created: d.created,
      language: d.language,
      url: d.url,
      digest_sri: d.digest_sri,
    })),
  }
}

interface SnapshotDocument {
  id: number
  version_id?: number
  gfv_id?: number
  created: string
  language: string
  url: string
  digest_sri: string
}

// A stored ecosystem snapshot names the parent version of a document "version_id".
export function snapshotGfVersions(stored: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(stored)) return []
  return stored.map((version: Record<string, unknown>) => ({
    ...version,
    documents: ((version.documents as SnapshotDocument[] | undefined) ?? []).map((d) => ({
      id: d.id,
      gfv_id: d.gfv_id ?? d.version_id,
      created: d.created,
      language: d.language,
      url: d.url,
      digest_sri: d.digest_sri,
    })),
  }))
}
