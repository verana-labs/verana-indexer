export interface AddGfDocumentEvent {
  gfvId: number | null
  gfdId: number | null
  version: number | null
  language: string | null
}

interface TxEvent {
  type?: string
  attributes?: Array<{ key?: string; value?: string }>
}

function toId(value: string | undefined): number | null {
  const n = Number(value)
  return Number.isInteger(n) && n > 0 ? n : null
}

// Attributes arrive already decoded by CrawlTransactionService via ChainRegistry.decodeAttribute.
export function extractAddGfDocumentEvents(txEvents: TxEvent[] | undefined): AddGfDocumentEvent[] {
  if (!Array.isArray(txEvents)) return []
  const out: AddGfDocumentEvent[] = []
  for (const ev of txEvents) {
    if ((ev.type ?? '') !== 'add_gf_document') continue
    const attrs = new Map<string, string>()
    for (const a of ev.attributes ?? []) attrs.set(a.key ?? '', (a.value ?? '').replace(/^"|"$/g, ''))
    out.push({
      gfvId: toId(attrs.get('gfv_id')),
      gfdId: toId(attrs.get('gfd_id')),
      version: toId(attrs.get('version')),
      language: attrs.get('language') || null,
    })
  }
  return out
}

export interface IncreaseGfActiveEvent {
  gfvId: number | null
  version: number | null
}

export function extractIncreaseGfActiveEvent(
  txEvents: TxEvent[] | undefined,
  ecosystemId: number,
  corporation?: string | null
): IncreaseGfActiveEvent | undefined {
  if (!Array.isArray(txEvents)) return undefined
  for (const ev of txEvents) {
    if ((ev.type ?? '') !== 'increase_active_gf_version') continue
    const attrs = new Map<string, string>()
    for (const a of ev.attributes ?? []) attrs.set(a.key ?? '', (a.value ?? '').replace(/^"|"$/g, ''))
    if (Number(attrs.get('ecosystem_id')) !== ecosystemId) continue
    const evCorporation = attrs.get('corporation')
    if (corporation && evCorporation && evCorporation !== corporation) continue
    return {
      gfvId: toId(attrs.get('gfv_id')),
      version: toId(attrs.get('version')),
    }
  }
  return undefined
}

export function matchAddGfDocumentEvent(
  events: AddGfDocumentEvent[],
  version: number,
  language?: string | null
): AddGfDocumentEvent | undefined {
  if (events.length === 1) return events[0]
  return (
    events.find((e) => e.version === version && (!language || e.language === language)) ??
    events.find((e) => e.version === version)
  )
}
