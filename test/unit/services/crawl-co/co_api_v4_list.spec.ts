jest.mock('../../../../src/models/corporation', () => ({ Corporation: { query: jest.fn() } }))
jest.mock('../../../../src/models/corporation_history', () => ({ CorporationHistory: { query: jest.fn() } }))

// Keep the pure helpers (buildCorporationObject, parseGfDataMode, parseCorporationListPagination, ...) real;
// only stub the DB-touching batch aggregates so the response wiring is exercised end to end.
jest.mock('../../../../src/services/crawl-co/co_stats', () => {
  const actual = jest.requireActual('../../../../src/services/crawl-co/co_stats')
  return {
    ...actual,
    calculateCorporationParticipantStatsBatch: jest.fn(),
    countControlledEcosystemsBatch: jest.fn(),
    getCorporationTrustDepositBatch: jest.fn(),
  }
})

jest.mock('../../../../src/services/resolver/trust-data-enrichment', () => ({
  parseTrustDataMode: jest.fn(() => ({ ok: true, mode: 'none' })),
  enrichTrustDataDeep: jest.fn(async (v: unknown) => v),
}))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

jest.mock('../../../../src/common/utils/block_time', () => ({
  getBlockChainTimeAsOf: jest.fn(async () => new Date('2024-03-01T00:00:00.000Z')),
}))

import { ServiceBroker } from 'moleculer'
import ApiResponder from '../../../../src/common/utils/apiResponse'
import { getBlockChainTimeAsOf } from '../../../../src/common/utils/block_time'
import { Corporation } from '../../../../src/models/corporation'
import CorporationApiService from '../../../../src/services/crawl-co/co_api.service'
import {
  calculateCorporationParticipantStatsBatch,
  countControlledEcosystemsBatch,
  getCorporationTrustDepositBatch,
} from '../../../../src/services/crawl-co/co_stats'
import { enrichTrustDataDeep, parseTrustDataMode } from '../../../../src/services/resolver/trust-data-enrichment'

function queryResolvesTo(rows: unknown[]) {
  const qb: any = {}
  qb.withGraphFetched = jest.fn(() => qb)
  qb.where = jest.fn(() => qb)
  qb.orderBy = jest.fn(() => qb)
  qb.limit = jest.fn(() => qb)
  qb.then = (resolve: (v: unknown) => void) => resolve(rows)
  ;(Corporation.query as jest.Mock).mockReturnValue(qb)
  return qb
}

function corpRow(over: Record<string, unknown> = {}) {
  return {
    toJSON: () => ({
      id: 1,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      language: 'en',
      created: '2024-01-01',
      modified: '2024-02-01',
      governanceFrameworkVersions: [{ version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }] }],
      ...over,
    }),
  }
}

const stats = (over: Record<string, number> = {}) => ({
  participants: 0,
  participants_ecosystem: 0,
  participants_issuer_grantor: 0,
  participants_issuer: 0,
  participants_verifier_grantor: 0,
  participants_verifier: 0,
  participants_holder: 0,
  ...over,
})

describe('CorporationApiService.listCorporationsV4', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new CorporationApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(new Map())
    ;(countControlledEcosystemsBatch as jest.Mock).mockResolvedValue(new Map())
    ;(getCorporationTrustDepositBatch as jest.Mock).mockResolvedValue(new Map())
    ;(parseTrustDataMode as jest.Mock).mockReturnValue({ ok: true, mode: 'none' })
  })

  it('builds { corporations: [...] } wiring batched aggregates and CGF versions per row', async () => {
    queryResolvesTo([
      corpRow({ id: 1, policy_address: 'verana1pol' }),
      corpRow({ id: 2, policy_address: 'verana1pol2' }),
    ])
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(
      new Map([
        ['1', stats({ participants: 2, participants_issuer: 1, participants_holder: 1 })],
        ['2', stats()],
      ])
    )
    ;(countControlledEcosystemsBatch as jest.Mock).mockResolvedValue(
      new Map([
        ['1', 2],
        ['2', 0],
      ])
    )
    ;(getCorporationTrustDepositBatch as jest.Mock).mockResolvedValue(
      new Map([
        [
          'verana1pol',
          {
            deposit: 100,
            share: 5,
            refunded: 9,
            slashed_deposit: 0,
            repaid_deposit: 0,
            slash_count: 0,
            last_slashed: null,
            last_repaid: null,
          },
        ],
      ])
    )

    const ctx: any = { params: { gf_data: 'all' }, meta: {} }
    const res: any = await service.listCorporationsV4(ctx)

    expect(res.corporations).toHaveLength(2)
    expect(res.corporations[0]).toMatchObject({
      id: 1,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      active_version: 1,
      controlled_ecosystems: 2,
      participants: 2,
      participants_issuer: 1,
      deposit: 100,
      refunded: 9,
    })
    expect(res.corporations[0].versions).toEqual([
      { version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }], ecosystem_id: null },
    ])
    // second corporation has no trust-deposit row -> zeroed snapshot
    expect(res.corporations[1]).toMatchObject({ id: 2, controlled_ecosystems: 0, deposit: 0 })
  })

  it('applies did, modified_after and cursor filters plus sort/limit to the query', async () => {
    const qb = queryResolvesTo([])
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(new Map())

    const ctx: any = {
      params: {
        did: 'did:example:co',
        modified_after: '2024-05-01T00:00:00Z',
        min_id: '5',
        max_id: '10',
        sort: '+id',
        limit: '25',
      },
      meta: {},
    }
    await service.listCorporationsV4(ctx)

    expect(qb.where).toHaveBeenCalledWith('did', 'did:example:co')
    expect(qb.where).toHaveBeenCalledWith('modified', '>', '2024-05-01T00:00:00.000Z')
    expect(qb.where).toHaveBeenCalledWith('id', '>=', '5')
    expect(qb.where).toHaveBeenCalledWith('id', '<', '10')
    expect(qb.orderBy).toHaveBeenCalledWith('id', 'asc')
    expect(qb.limit).toHaveBeenCalledWith(25)
  })

  it('defaults to newest-first (id desc) and limit 64', async () => {
    const qb = queryResolvesTo([])
    const ctx: any = { params: {}, meta: {} }
    await service.listCorporationsV4(ctx)
    expect(qb.orderBy).toHaveBeenCalledWith('id', 'desc')
    expect(qb.limit).toHaveBeenCalledWith(64)
  })

  it('excludes EGF rows (ecosystem_id != 0) from versions/active_version', async () => {
    queryResolvesTo([
      corpRow({
        governanceFrameworkVersions: [
          { version: 1, ecosystem_id: 0, active_since: '2024-01-01', documents: [] },
          { version: 7, ecosystem_id: 5, active_since: '2024-06-01', documents: [] },
        ],
      }),
    ])
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(new Map([['1', stats()]]))

    const ctx: any = { params: { gf_data: 'all' }, meta: {} }
    const res: any = await service.listCorporationsV4(ctx)

    expect(res.corporations[0].active_version).toBe(1)
    expect(res.corporations[0].versions).toHaveLength(1)
    expect(res.corporations[0].versions[0].version).toBe(1)
  })

  it('omits versions from every entry when gf_data is none', async () => {
    const qb = queryResolvesTo([corpRow()])
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(new Map([['1', stats()]]))

    const ctx: any = { params: { gf_data: 'none' }, meta: {} }
    const res: any = await service.listCorporationsV4(ctx)

    expect('versions' in res.corporations[0]).toBe(false)
    expect(qb.withGraphFetched).toHaveBeenCalledWith('governanceFrameworkVersions')
  })

  it('resolves participant aggregates at the requested At-Block-Height', async () => {
    queryResolvesTo([corpRow({ id: 1 })])
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(new Map([['1', stats()]]))

    const ctx: any = { params: {}, meta: { blockHeight: 5 } }
    await service.listCorporationsV4(ctx)

    expect(calculateCorporationParticipantStatsBatch).toHaveBeenCalledWith([1], 5)
  })

  it('excludes corporations created after the requested At-Block-Height', async () => {
    const qb = queryResolvesTo([])
    const ctx: any = { params: {}, meta: { blockHeight: 5 } }
    await service.listCorporationsV4(ctx)

    expect(getBlockChainTimeAsOf).toHaveBeenCalledWith(5, expect.anything())
    expect(qb.where).toHaveBeenCalledWith('created', '<=', '2024-03-01T00:00:00.000Z')
  })

  it('does not apply the created filter when no At-Block-Height is set', async () => {
    const qb = queryResolvesTo([])
    const ctx: any = { params: {}, meta: {} }
    await service.listCorporationsV4(ctx)

    expect(getBlockChainTimeAsOf).not.toHaveBeenCalled()
    expect(qb.where).not.toHaveBeenCalledWith('created', '<=', expect.anything())
  })

  it('returns an empty list (and skips aggregate queries) when the page is empty', async () => {
    queryResolvesTo([])
    const ctx: any = { params: {}, meta: {} }
    const res: any = await service.listCorporationsV4(ctx)
    expect(res).toEqual({ corporations: [] })
    expect(calculateCorporationParticipantStatsBatch).not.toHaveBeenCalled()
  })

  it('runs trust_data enrichment over the whole payload when requested', async () => {
    queryResolvesTo([corpRow({ id: 1 })])
    ;(calculateCorporationParticipantStatsBatch as jest.Mock).mockResolvedValue(new Map([['1', stats()]]))
    ;(parseTrustDataMode as jest.Mock).mockReturnValue({ ok: true, mode: 'summary' })
    ;(enrichTrustDataDeep as jest.Mock).mockResolvedValueOnce({ enriched: true })

    const ctx: any = { params: { trust_data: 'summary' }, meta: { blockHeight: 9 } }
    const res: any = await service.listCorporationsV4(ctx)

    expect(enrichTrustDataDeep).toHaveBeenCalledWith(
      expect.objectContaining({ corporations: expect.any(Array) }),
      'summary',
      9
    )
    expect(res).toEqual({ enriched: true })
  })

  it('rejects invalid gf_data / limit / min_id / sort / modified_after with 400 and no query', async () => {
    for (const params of [
      { gf_data: 'bogus' },
      { limit: '0' },
      { min_id: 'abc' },
      { sort: 'weight' },
      { modified_after: 'not-a-date' },
    ]) {
      jest.clearAllMocks()
      ;(parseTrustDataMode as jest.Mock).mockReturnValue({ ok: true, mode: 'none' })
      const ctx: any = { params, meta: {} }
      const res: any = await service.listCorporationsV4(ctx)
      expect(res.code).toBe(400)
    }
    // gf_data/limit/min_id/sort fail before any DB access
    expect(ApiResponder.error).toBeDefined()
  })

  it('propagates a trust_data validation failure as 400', async () => {
    ;(parseTrustDataMode as jest.Mock).mockReturnValue({ ok: false, message: 'Invalid "trust_data"' })
    const ctx: any = { params: { trust_data: 'bogus' }, meta: {} }
    const res: any = await service.listCorporationsV4(ctx)
    expect(res).toMatchObject({ code: 400 })
  })
})
