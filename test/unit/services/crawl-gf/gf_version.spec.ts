import { currentGfDocuments, snapshotGfVersions } from '../../../../src/services/crawl-gf/gf_version'

const doc = (id: number, language: string, created: string, url: string) => ({
  id,
  gfd_id: language === 'en' ? 14 : 15,
  language,
  created,
  url,
  digest_sri: `sha384-${url}`,
})

describe('currentGfDocuments', () => {
  const frAdded = doc(1, 'fr', '2026-11-19T10:00:05Z', 'fr-1')
  const enAdded = doc(2, 'en', '2026-11-19T10:00:00Z', 'en-1')
  const enReplaced = doc(3, 'en', '2026-11-19T10:00:10Z', 'en-2')

  it('keeps the order in which each language first appears in the input', () => {
    expect(currentGfDocuments([frAdded, enAdded, enReplaced]).map((d) => d.language)).toEqual(['fr', 'en'])
  })

  it('returns the latest row of a language with the created time of its first row', () => {
    const [, en] = currentGfDocuments([frAdded, enAdded, enReplaced])

    expect(en).toMatchObject({ url: 'en-2', created: enAdded.created, gfd_id: 14 })
  })

  it('ignores rows added after the given time', () => {
    const live = currentGfDocuments([frAdded, enAdded, enReplaced], new Date('2026-11-19T10:00:03Z'))

    expect(live).toEqual([enAdded])
  })

  it('returns the row itself when a language has one row', () => {
    expect(currentGfDocuments([frAdded])).toEqual([frAdded])
  })
})

describe('snapshotGfVersions', () => {
  const created = '2026-11-19T12:45:37.616Z'
  const document = { id: 41, created, language: 'en', url: 'https://example.com/v2.md', digest_sri: 'sha384-v2' }
  const version = { id: 40, ecosystem_id: 14, created, version: 2, active_since: null }

  it('names the parent version of a document gfv_id', () => {
    const stored = [{ ...version, documents: [{ ...document, version_id: 40 }] }]

    expect(snapshotGfVersions(stored)).toEqual([{ ...version, documents: [{ ...document, gfv_id: 40 }] }])
  })

  it('returns no version when the snapshot stores none', () => {
    expect(snapshotGfVersions(null)).toEqual([])
  })
})
