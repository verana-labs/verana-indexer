const mockParticipantsWhere = jest.fn()
const mockParticipantsWhereIn = jest.fn()
const mockEcosystemCount = jest.fn()

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: jest.fn((table: string) => {
    if (table === 'participants') {
      return { where: mockParticipantsWhere, whereIn: mockParticipantsWhereIn }
    }
    if (table === 'ecosystem') {
      const chain = {
        whereIn: jest.fn(() => chain),
        groupBy: jest.fn(() => chain),
        select: jest.fn(() => chain),
        count: mockEcosystemCount,
      }
      return chain
    }
    return {}
  }),
}))

jest.mock('../../../../src/models/ecosystem', () => ({ Ecosystem: { query: jest.fn() } }))
jest.mock('../../../../src/models/trust_deposit', () => ({ __esModule: true, default: { query: jest.fn() } }))

import TrustDeposit from '../../../../src/models/trust_deposit'
import {
  buildCorporationObject,
  calculateCorporationParticipantStatsBatch,
  countControlledEcosystemsBatch,
  emptyTrustDepositSnapshot,
  getCorporationTrustDepositBatch,
  parseCorporationListPagination,
} from '../../../../src/services/crawl-co/co_stats'

describe('co_stats.parseCorporationListPagination', () => {
  it('defaults to limit 64, descending id, no cursors', () => {
    const result = parseCorporationListPagination({})
    expect(result).toEqual({ ok: true, value: { limit: 64, minId: undefined, maxId: undefined, direction: 'desc' } })
  })

  it('rejects a limit outside 1..1024', () => {
    expect(parseCorporationListPagination({ limit: '0' }).ok).toBe(false)
    expect(parseCorporationListPagination({ limit: '1025' }).ok).toBe(false)
    expect(parseCorporationListPagination({ limit: 'abc' }).ok).toBe(false)
  })

  it('accepts the boundary limits 1 and 1024', () => {
    expect(parseCorporationListPagination({ limit: '1' })).toMatchObject({ ok: true, value: { limit: 1 } })
    expect(parseCorporationListPagination({ limit: '1024' })).toMatchObject({ ok: true, value: { limit: 1024 } })
  })

  it('validates min_id/max_id as non-negative integer cursors', () => {
    expect(parseCorporationListPagination({ min_id: 'x' }).ok).toBe(false)
    expect(parseCorporationListPagination({ max_id: '-1' }).ok).toBe(false)
    expect(parseCorporationListPagination({ min_id: '5', max_id: '10' })).toMatchObject({
      ok: true,
      value: { minId: '5', maxId: '10' },
    })
  })

  it('maps sort to a direction and rejects any non-id sort', () => {
    expect(parseCorporationListPagination({ sort: 'id' })).toMatchObject({ ok: true, value: { direction: 'asc' } })
    expect(parseCorporationListPagination({ sort: '+id' })).toMatchObject({ ok: true, value: { direction: 'asc' } })
    expect(parseCorporationListPagination({ sort: '-id' })).toMatchObject({ ok: true, value: { direction: 'desc' } })
    expect(parseCorporationListPagination({ sort: 'modified' }).ok).toBe(false)
  })
})

describe('co_stats.buildCorporationObject', () => {
  const participantStats = {
    participants: 2,
    participants_ecosystem: 0,
    participants_issuer_grantor: 0,
    participants_issuer: 1,
    participants_verifier_grantor: 0,
    participants_verifier: 0,
    participants_holder: 1,
  }

  it('assembles the on-chain fields, aggregates and CGF versions into one object', () => {
    const obj = buildCorporationObject({
      plain: {
        id: 1,
        did: 'did:example:co',
        policy_address: 'verana1pol',
        language: 'en',
        created: '2024-01-01',
        modified: '2024-02-01',
      },
      cgfVersions: [{ version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }] }],
      participantStats,
      controlledEcosystems: 2,
      trustDeposit: { ...emptyTrustDepositSnapshot(), deposit: 100, refunded: 9 },
      gfData: 'all',
    })

    expect(obj).toMatchObject({
      id: 1,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      language: 'en',
      active_version: 1,
      controlled_ecosystems: 2,
      participants: 2,
      deposit: 100,
      refunded: 9,
    })
    expect(obj.versions).toEqual([
      { version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }], ecosystem_id: null },
    ])
  })

  it('omits versions entirely when gf_data is none', () => {
    const obj = buildCorporationObject({
      plain: { id: 1, did: 'did:example:co', policy_address: 'verana1pol' },
      cgfVersions: [{ version: 1, active_since: '2024-01-01' }],
      participantStats,
      controlledEcosystems: 0,
      trustDeposit: emptyTrustDepositSnapshot(),
      gfData: 'none',
    })
    expect('versions' in obj).toBe(false)
  })

  it('falls back to the legacy corporation column for policy_address', () => {
    const obj = buildCorporationObject({
      plain: { id: 1, did: 'did:example:co', corporation: 'verana1legacy' },
      cgfVersions: [],
      participantStats,
      controlledEcosystems: 0,
      trustDeposit: emptyTrustDepositSnapshot(),
      gfData: 'none',
    })
    expect(obj.policy_address).toBe('verana1legacy')
  })
})

describe('co_stats.calculateCorporationParticipantStatsBatch', () => {
  beforeEach(() => jest.clearAllMocks())

  it('groups ACTIVE participants by corporation with one whereIn query', async () => {
    const past = new Date('2020-01-01T00:00:00Z')
    mockParticipantsWhereIn.mockResolvedValueOnce([
      { corporation_id: 1, role: 'ISSUER', effective_from: past },
      { corporation_id: 1, role: 'HOLDER', effective_from: past },
      { corporation_id: 2, role: 'VERIFIER', effective_from: past },
      { corporation_id: 2, role: 'VERIFIER', effective_from: past, revoked: past },
    ])

    const map = await calculateCorporationParticipantStatsBatch([1, 2])

    expect(mockParticipantsWhereIn).toHaveBeenCalledWith('corporation_id', [1, 2])
    expect(map.get('1')).toMatchObject({ participants: 2, participants_issuer: 1, participants_holder: 1 })
    expect(map.get('2')).toMatchObject({ participants: 1, participants_verifier: 1 })
  })

  it('seeds every requested id with a zeroed entry and skips the query when empty', async () => {
    const map = await calculateCorporationParticipantStatsBatch([])
    expect(map.size).toBe(0)
    expect(mockParticipantsWhereIn).not.toHaveBeenCalled()

    mockParticipantsWhereIn.mockResolvedValueOnce([])
    const seeded = await calculateCorporationParticipantStatsBatch([9])
    expect(seeded.get('9')).toMatchObject({ participants: 0 })
  })
})

describe('co_stats.countControlledEcosystemsBatch', () => {
  beforeEach(() => jest.clearAllMocks())

  it('returns a per-corporation count, defaulting missing ids to 0', async () => {
    mockEcosystemCount.mockResolvedValueOnce([{ corporation_id: 1, count: 2 }])

    const map = await countControlledEcosystemsBatch([1, 2])

    expect(map.get('1')).toBe(2)
    expect(map.get('2')).toBe(0)
  })
})

describe('co_stats.getCorporationTrustDepositBatch', () => {
  beforeEach(() => jest.clearAllMocks())

  it('keys snapshots by policy address and dedupes/ignores null addresses', async () => {
    const whereIn = jest.fn().mockResolvedValue([{ corporation: 'verana1a', deposit: 100, claimable: 9, share: 5 }])
    ;(TrustDeposit.query as jest.Mock).mockReturnValue({ whereIn })

    const map = await getCorporationTrustDepositBatch(['verana1a', 'verana1a', null])

    expect(whereIn).toHaveBeenCalledWith('corporation', ['verana1a'])
    expect(map.get('verana1a')).toMatchObject({ deposit: 100, refunded: 9, share: 5 })
  })

  it('returns an empty map without querying when there are no addresses', async () => {
    const map = await getCorporationTrustDepositBatch([null, null])
    expect(map.size).toBe(0)
    expect(TrustDeposit.query as jest.Mock).not.toHaveBeenCalled()
  })
})
