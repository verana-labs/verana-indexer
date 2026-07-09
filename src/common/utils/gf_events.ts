import { Buffer } from 'node:buffer'

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

function decodeAttr(raw: string | undefined): string {
  if (!raw) return ''
  if (/^\d+$/.test(raw)) return raw
  if (/^[A-Za-z0-9+/=]+$/.test(raw) && raw.length % 4 === 0 && !raw.startsWith('verana')) {
    try {
      return Buffer.from(raw, 'base64').toString('utf-8')
    } catch {
      return raw
    }
  }
  return raw
}

function toId(value: string | undefined): number | null {
  const n = Number(value)
  return Number.isInteger(n) && n > 0 ? n : null
}

// x/gf emits add_gf_document with the chain-assigned global gfv_id/gfd_id on every GFV/GFD
// creation path (corporation and ecosystem seeds included).
export function extractAddGfDocumentEvents(txEvents: TxEvent[] | undefined): AddGfDocumentEvent[] {
  if (!Array.isArray(txEvents)) return []
  const out: AddGfDocumentEvent[] = []
  for (const ev of txEvents) {
    if ((ev.type ?? '') !== 'add_gf_document') continue
    const attrs = new Map<string, string>()
    for (const a of ev.attributes ?? []) attrs.set(decodeAttr(a.key), decodeAttr(a.value).replace(/^"|"$/g, ''))
    out.push({
      gfvId: toId(attrs.get('gfv_id')),
      gfdId: toId(attrs.get('gfd_id')),
      version: toId(attrs.get('version')),
      language: attrs.get('language') || null,
    })
  }
  return out
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
