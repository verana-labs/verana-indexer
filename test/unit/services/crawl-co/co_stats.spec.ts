const mockParticipantsWhere = jest.fn()
const mockCheckpointFirst = jest.fn()
const mockBlockMaxFirst = jest.fn()

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: jest.fn((table: string) => {
    if (table === 'block_checkpoint') {
      return { where: jest.fn(() => ({ first: mockCheckpointFirst })) }
    }
    if (table === 'block') {
      return { max: jest.fn(() => ({ first: mockBlockMaxFirst })) }
    }
    return { where: mockParticipantsWhere }
  }),
}))

jest.mock('../../../../src/models/ecosystem', () => ({ Ecosystem: { query: jest.fn() } }))
jest.mock('../../../../src/models/trust_deposit', () => ({ __esModule: true, default: { query: jest.fn() } }))

import { Ecosystem } from '../../../../src/models/ecosystem'
import TrustDeposit from '../../../../src/models/trust_deposit'
import {
  applyGfData,
  calculateCorporationParticipantStats,
  countControlledEcosystems,
  deriveActiveVersion,
  getCorporationTrustDeposit,
  getResolvedBlockHeight,
  parseGfDataMode,
} from '../../../../src/services/crawl-co/co_stats'

describe('co_stats.calculateCorporationParticipantStats', () => {
  beforeEach(() => jest.clearAllMocks())

  it('counts only ACTIVE participants, broken down by role, scoped to the corporation', async () => {
    const past = new Date('2020-01-01T00:00:00Z')
    mockParticipantsWhere.mockResolvedValueOnce([
      { corporation_id: 5, role: 'ISSUER', effective_from: past },
      { corporation_id: 5, role: 'HOLDER', effective_from: past },
      { corporation_id: 5, role: 'VERIFIER', effective_from: past, revoked: past },
    ])

    const stats = await calculateCorporationParticipantStats(5)

    expect(stats).toEqual({
      participants: 2,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 1,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 1,
    })
    expect(mockParticipantsWhere).toHaveBeenCalledWith('corporation_id', 5)
  })

  it('counts an ACTIVE unmapped role (UNSPECIFIED) in the total but in no role bucket', async () => {
    const past = new Date('2020-01-01T00:00:00Z')
    mockParticipantsWhere.mockResolvedValueOnce([
      { corporation_id: 5, role: 'ISSUER', effective_from: past },
      { corporation_id: 5, role: 'UNSPECIFIED', effective_from: past },
    ])

    const stats = await calculateCorporationParticipantStats(5)

    expect(stats.participants).toBe(2)
    expect(stats.participants_issuer).toBe(1)
    // UNSPECIFIED is counted in the total but has no bucket, so the buckets sum to less than the total.
    const bucketSum =
      stats.participants_ecosystem +
      stats.participants_issuer_grantor +
      stats.participants_issuer +
      stats.participants_verifier_grantor +
      stats.participants_verifier +
      stats.participants_holder
    expect(bucketSum).toBe(1)
  })

  it('normalizes role casing/aliases before bucketing', async () => {
    const past = new Date('2020-01-01T00:00:00Z')
    mockParticipantsWhere.mockResolvedValueOnce([{ corporation_id: 5, role: 'holder', effective_from: past }])

    const stats = await calculateCorporationParticipantStats(5)

    expect(stats.participants).toBe(1)
    expect(stats.participants_holder).toBe(1)
  })
})

describe('co_stats.countControlledEcosystems', () => {
  beforeEach(() => jest.clearAllMocks())

  it('counts ecosystems whose corporation_id matches', async () => {
    const where = jest.fn().mockReturnValue({ resultSize: jest.fn().mockResolvedValue(3) })
    ;(Ecosystem.query as jest.Mock).mockReturnValue({ where })

    const n = await countControlledEcosystems(7)

    expect(n).toBe(3)
    expect(where).toHaveBeenCalledWith('corporation_id', 7)
  })
})

describe('co_stats.deriveActiveVersion', () => {
  it('returns the version number with the most recent active_since', () => {
    const result = deriveActiveVersion([
      { version: 1, active_since: '2020-01-01T00:00:00Z' },
      { version: 3, active_since: '2022-01-01T00:00:00Z' },
      { version: 2, active_since: '2021-01-01T00:00:00Z' },
      { version: 4, active_since: null },
    ])
    expect(result).toBe(3)
  })

  it('returns null when no version has been activated', () => {
    expect(deriveActiveVersion([{ version: 1, active_since: null }])).toBeNull()
    expect(deriveActiveVersion([])).toBeNull()
  })

  it('breaks ties on equal active_since deterministically by highest version', () => {
    const sameInstant = '2022-01-01T00:00:00Z'
    expect(
      deriveActiveVersion([
        { version: 2, active_since: sameInstant },
        { version: 5, active_since: sameInstant },
        { version: 3, active_since: sameInstant },
      ])
    ).toBe(5)
    // Order-independent: the result must not depend on input ordering.
    expect(
      deriveActiveVersion([
        { version: 5, active_since: sameInstant },
        { version: 3, active_since: sameInstant },
        { version: 2, active_since: sameInstant },
      ])
    ).toBe(5)
  })
})

describe('co_stats.getCorporationTrustDeposit', () => {
  beforeEach(() => jest.clearAllMocks())

  it('maps the trust-deposit row (claimable -> refunded)', async () => {
    ;(TrustDeposit.query as jest.Mock).mockReturnValue({
      findOne: jest.fn().mockResolvedValue({
        deposit: 100,
        share: 5,
        claimable: 9,
        slashed_deposit: 3,
        repaid_deposit: 2,
        slash_count: 1,
        last_slashed: '2021-01-01',
        last_repaid: '2021-02-01',
      }),
    })

    const td = await getCorporationTrustDeposit('verana1abc')

    expect(td).toEqual({
      deposit: 100,
      share: 5,
      refunded: 9,
      slashed_deposit: 3,
      repaid_deposit: 2,
      slash_count: 1,
      last_slashed: '2021-01-01',
      last_repaid: '2021-02-01',
    })
  })

  it('returns a zeroed snapshot when no trust-deposit row exists', async () => {
    ;(TrustDeposit.query as jest.Mock).mockReturnValue({ findOne: jest.fn().mockResolvedValue(undefined) })

    const td = await getCorporationTrustDeposit('verana1none')

    expect(td).toEqual({
      deposit: 0,
      share: 0,
      refunded: 0,
      slashed_deposit: 0,
      repaid_deposit: 0,
      slash_count: 0,
      last_slashed: null,
      last_repaid: null,
    })
  })

  it('returns a zeroed snapshot without querying when address is empty', async () => {
    const td = await getCorporationTrustDeposit(null)
    expect(td.deposit).toBe(0)
    expect(TrustDeposit.query as jest.Mock).not.toHaveBeenCalled()
  })
})

describe('co_stats.applyGfData', () => {
  const versions = [
    { version: 1, active_since: '2020-01-01T00:00:00Z', documents: [{ language: 'en' }, { language: 'fr' }] },
    { version: 2, active_since: '2021-01-01T00:00:00Z', documents: [{ language: 'en' }] },
  ]

  it('returns an empty array when gf_data is "none"', () => {
    expect(applyGfData(versions, 'none')).toEqual([])
  })

  it('returns only the most recently activated version when gf_data is "only_active"', () => {
    const result = applyGfData(versions, 'only_active')
    expect(result).toHaveLength(1)
    expect(result[0].version).toBe(2)
  })

  it('returns all versions when gf_data is "all"', () => {
    expect(applyGfData(versions, 'all')).toHaveLength(2)
  })

  it("orders each version's documents by preferred_language (preferred first, none dropped)", () => {
    const result = applyGfData(versions, 'all', 'fr')
    expect(result[0].documents).toEqual([{ language: 'fr' }, { language: 'en' }])
    expect(result[1].documents).toEqual([{ language: 'en' }])
  })

  it('normalizes ecosystem_id to null for CGF versions (spec: ecosystem_id set iff EGF)', () => {
    const result = applyGfData(
      [{ version: 1, ecosystem_id: 0, corporation_id: 7, active_since: '2020-01-01T00:00:00Z', documents: [] }],
      'all'
    )
    expect(result[0].ecosystem_id).toBeNull()
    expect(result[0].corporation_id).toBe(7)
  })

  it('returns no version for "only_active" when none has been activated', () => {
    const result = applyGfData(
      [
        { version: 1, active_since: null, documents: [] },
        { version: 2, active_since: null, documents: [] },
      ],
      'only_active'
    )
    expect(result).toEqual([])
  })

  it('at a height (asOf) excludes versions not yet created and documents added after that block', () => {
    const asOf = new Date('2020-06-01T00:00:00Z')
    const vs = [
      {
        version: 1,
        created: '2020-01-01T00:00:00Z',
        active_since: '2020-01-01T00:00:00Z',
        documents: [
          { language: 'en', created: '2020-01-01T00:00:00Z' },
          { language: 'fr', created: '2020-09-01T00:00:00Z' }, // added after asOf
        ],
      },
      { version: 2, created: '2021-01-01T00:00:00Z', active_since: '2021-01-01T00:00:00Z', documents: [] }, // created after asOf
    ]
    const result = applyGfData(vs, 'all', undefined, asOf)
    // v2 (created after asOf) excluded; v1 kept but its late fr document dropped
    expect(result).toHaveLength(1)
    expect(result[0].version).toBe(1)
    expect(result[0].documents).toEqual([{ language: 'en', created: '2020-01-01T00:00:00Z' }])
  })
})

describe('co_stats.getResolvedBlockHeight', () => {
  beforeEach(() => jest.clearAllMocks())

  it('returns the provided block height without a DB lookup', async () => {
    const h = await getResolvedBlockHeight(42)
    expect(h).toBe(42)
    expect(mockCheckpointFirst).not.toHaveBeenCalled()
  })

  it('falls back to the latest indexed height from block_checkpoint', async () => {
    mockCheckpointFirst.mockResolvedValueOnce({ height: 175 })
    expect(await getResolvedBlockHeight()).toBe(175)
  })

  it('falls back to the latest block-table height when no checkpoint row exists', async () => {
    mockCheckpointFirst.mockResolvedValueOnce(undefined)
    mockBlockMaxFirst.mockResolvedValueOnce({ max: 1234 })
    expect(await getResolvedBlockHeight()).toBe(1234)
  })

  it('returns 0 when neither a checkpoint row nor an indexed block exists', async () => {
    mockCheckpointFirst.mockResolvedValueOnce(undefined)
    mockBlockMaxFirst.mockResolvedValueOnce(undefined)
    expect(await getResolvedBlockHeight()).toBe(0)
  })
})

describe('co_stats.parseGfDataMode', () => {
  it('defaults to only_active when omitted', () => {
    expect(parseGfDataMode(undefined)).toEqual({ ok: true, mode: 'only_active' })
    expect(parseGfDataMode('')).toEqual({ ok: true, mode: 'only_active' })
    expect(parseGfDataMode(null)).toEqual({ ok: true, mode: 'only_active' })
  })

  it('accepts the three enum values (case/space-insensitive)', () => {
    expect(parseGfDataMode('none')).toEqual({ ok: true, mode: 'none' })
    expect(parseGfDataMode('only_active')).toEqual({ ok: true, mode: 'only_active' })
    expect(parseGfDataMode(' ALL ')).toEqual({ ok: true, mode: 'all' })
  })

  it('fails closed on an unrecognized value (no silent fall-through to all)', () => {
    const result = parseGfDataMode('actve')
    expect(result.ok).toBe(false)
    if (!result.ok) expect(result.message).toContain('gf_data')
  })
})
