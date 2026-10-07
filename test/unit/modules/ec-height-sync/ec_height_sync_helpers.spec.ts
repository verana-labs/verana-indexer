import { extractEcosystemIdsFromEvents } from '../../../../src/modules/ec-height-sync/ec_height_sync_helpers'

const event = (type: string, ecosystemId: number) => ({
  type,
  attributes: [{ key: 'ecosystem_id', value: String(ecosystemId) }],
})

describe('extractEcosystemIdsFromEvents', () => {
  it('collects the ecosystem id from a gf add_gf_document event', () => {
    expect(extractEcosystemIdsFromEvents([event('add_gf_document', 1)], true)).toEqual([1])
  })

  it('ignores a CGF add_gf_document event (ecosystem_id 0)', () => {
    expect(extractEcosystemIdsFromEvents([event('add_gf_document', 0)], true)).toEqual([])
  })

  it('collects ids from ec and gf events, deduplicated', () => {
    const events = [
      event('create_ecosystem', 3),
      event('increase_active_gf_version', 3),
      event('add_gf_document', 4),
      event('archive_ecosystem', 5),
    ]
    expect(extractEcosystemIdsFromEvents(events, true)).toEqual([3, 4, 5])
  })

  it('ignores unrelated events', () => {
    expect(extractEcosystemIdsFromEvents([event('transfer', 9), event('create_corporation', 9)], true)).toEqual([])
  })
})
