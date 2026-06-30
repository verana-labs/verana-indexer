jest.mock('../../../../src/models/corporation', () => ({ Corporation: { query: jest.fn() } }))
jest.mock('../../../../src/models/corporation_history', () => ({ CorporationHistory: { query: jest.fn() } }))
jest.mock('../../../../src/services/crawl-co/co_stats', () => ({
  calculateCorporationParticipantStats: jest.fn(),
  countControlledEcosystems: jest.fn(),
  getCorporationTrustDeposit: jest.fn(),
  deriveActiveVersion: jest.fn(),
  applyGfData: jest.fn(),
  getResolvedBlockHeight: jest.fn(async () => 0),
  parseGfDataMode: jest.fn((raw: string | undefined) => ({ ok: true, mode: raw ?? 'only_active' })),
}))
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

import { ServiceBroker } from 'moleculer'
import ApiResponder from '../../../../src/common/utils/apiResponse'
import { Corporation } from '../../../../src/models/corporation'
import CorporationApiService from '../../../../src/services/crawl-co/co_api.service'
import {
  applyGfData,
  calculateCorporationParticipantStats,
  countControlledEcosystems,
  deriveActiveVersion,
  getCorporationTrustDeposit,
  getResolvedBlockHeight,
  parseGfDataMode,
} from '../../../../src/services/crawl-co/co_stats'

function fetchReturns(corporation: unknown) {
  ;(Corporation.query as jest.Mock).mockReturnValue({
    findById: jest.fn().mockReturnValue({
      withGraphFetched: jest.fn().mockResolvedValue(corporation),
    }),
  })
}

describe('CorporationApiService.getCorporationV4', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new CorporationApiService(broker)

  beforeEach(() => jest.clearAllMocks())

  it('returns 404 when the corporation does not exist', async () => {
    fetchReturns(undefined)
    const ctx: any = { params: { id: '99' }, meta: {} }

    await service.getCorporationV4(ctx)

    expect(ApiResponder.error).toHaveBeenCalledWith(ctx, 'Corporation 99 not found', 404)
  })

  it('returns 400 for a non-numeric id (uint64 required) instead of crashing', async () => {
    const ctx: any = { params: { id: 'abc' }, meta: {} }

    await service.getCorporationV4(ctx)

    expect(ApiResponder.error).toHaveBeenCalledWith(ctx, expect.stringContaining('Invalid corporation id'), 400)
    expect(Corporation.query as jest.Mock).not.toHaveBeenCalled()
  })

  it('returns 400 (not all-versions) when gf_data fails validation', async () => {
    ;(parseGfDataMode as jest.Mock).mockReturnValueOnce({
      ok: false,
      message: 'Invalid "gf_data". Allowed values: none, only_active, all',
    })
    const ctx: any = { params: { id: '1', gf_data: 'bogus' }, meta: {} }

    await service.getCorporationV4(ctx)

    expect(ApiResponder.error).toHaveBeenCalledWith(ctx, expect.stringContaining('Invalid "gf_data"'), 400)
    expect(Corporation.query as jest.Mock).not.toHaveBeenCalled()
  })

  it('excludes EGF rows (ecosystem_id != 0) from active_version/versions — CGF only per spec', async () => {
    fetchReturns({
      toJSON: () => ({
        id: 1,
        did: 'did:example:co',
        corporation: 'verana1pol',
        governanceFrameworkVersions: [
          { version: 1, ecosystem_id: 0, active_since: '2024-01-01', documents: [] }, // CGF
          { version: 7, ecosystem_id: 5, active_since: '2024-06-01', documents: [] }, // EGF for a controlled ecosystem
        ],
      }),
    })
    ;(calculateCorporationParticipantStats as jest.Mock).mockResolvedValue({
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
    })
    ;(countControlledEcosystems as jest.Mock).mockResolvedValue(1)
    ;(getCorporationTrustDeposit as jest.Mock).mockResolvedValue({
      deposit: 0,
      share: 0,
      refunded: 0,
      slashed_deposit: 0,
      repaid_deposit: 0,
      slash_count: 0,
      last_slashed: null,
      last_repaid: null,
    })
    ;(deriveActiveVersion as jest.Mock).mockReturnValue(1)
    ;(applyGfData as jest.Mock).mockReturnValue([])

    const ctx: any = { params: { id: '1', gf_data: 'all' }, meta: {} }
    await service.getCorporationV4(ctx)

    // Both helpers must receive the CGF-only set (ecosystem_id falsy), not the EGF row.
    const cgfOnly = [{ version: 1, ecosystem_id: 0, active_since: '2024-01-01', documents: [] }]
    expect(deriveActiveVersion).toHaveBeenCalledWith(cgfOnly)
    expect(applyGfData).toHaveBeenCalledWith(cgfOnly, 'all', undefined)
  })

  it('builds the spec response object: { corporation } with on-chain fields + aggregates + versions', async () => {
    fetchReturns({
      toJSON: () => ({
        id: 1,
        did: 'did:example:co',
        policy_address: 'verana1pol',
        corporation: 'verana1pol',
        language: 'en',
        created: '2024-01-01',
        modified: '2024-02-01',
        governanceFrameworkVersions: [{ version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }] }],
      }),
    })
    ;(calculateCorporationParticipantStats as jest.Mock).mockResolvedValue({
      participants: 2,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 1,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 1,
    })
    ;(countControlledEcosystems as jest.Mock).mockResolvedValue(2)
    ;(getCorporationTrustDeposit as jest.Mock).mockResolvedValue({
      deposit: 100,
      share: 5,
      refunded: 9,
      slashed_deposit: 3,
      repaid_deposit: 2,
      slash_count: 1,
      last_slashed: null,
      last_repaid: null,
    })
    ;(deriveActiveVersion as jest.Mock).mockReturnValue(1)
    ;(applyGfData as jest.Mock).mockReturnValue([
      { version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }] },
    ])
    ;(getResolvedBlockHeight as jest.Mock).mockResolvedValue(175)

    const ctx: any = { params: { id: '1', gf_data: 'all' }, meta: {} }
    const res: any = await service.getCorporationV4(ctx)

    expect(res.corporation).toEqual({
      id: 1,
      did: 'did:example:co',
      policy_address: 'verana1pol',
      language: 'en',
      active_version: 1,
      archived: null,
      created: '2024-01-01',
      modified: '2024-02-01',
      controlled_ecosystems: 2,
      participants: 2,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 1,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 1,
      deposit: 100,
      share: 5,
      refunded: 9,
      slashed_deposit: 3,
      repaid_deposit: 2,
      slash_count: 1,
      last_slashed: null,
      last_repaid: null,
      versions: [{ version: 1, active_since: '2024-01-01', documents: [{ language: 'en' }] }],
    })
    expect(res.block_height).toBe(175)
    expect(getCorporationTrustDeposit).toHaveBeenCalledWith('verana1pol')
    expect(applyGfData).toHaveBeenCalledWith(expect.any(Array), 'all', undefined)
  })

  it('omits versions entirely when gf_data is "none"', async () => {
    fetchReturns({
      toJSON: () => ({ id: 1, did: 'did:example:co', corporation: 'verana1pol', governanceFrameworkVersions: [] }),
    })
    ;(calculateCorporationParticipantStats as jest.Mock).mockResolvedValue({
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
    })
    ;(countControlledEcosystems as jest.Mock).mockResolvedValue(0)
    ;(getCorporationTrustDeposit as jest.Mock).mockResolvedValue({
      deposit: 0,
      share: 0,
      refunded: 0,
      slashed_deposit: 0,
      repaid_deposit: 0,
      slash_count: 0,
      last_slashed: null,
      last_repaid: null,
    })
    ;(deriveActiveVersion as jest.Mock).mockReturnValue(null)
    ;(applyGfData as jest.Mock).mockReturnValue([])

    const ctx: any = { params: { id: '1', gf_data: 'none' }, meta: {} }
    const res: any = await service.getCorporationV4(ctx)

    expect('versions' in res.corporation).toBe(false)
  })

  it('resolves participant aggregates at the requested At-Block-Height and echoes it', async () => {
    fetchReturns({
      toJSON: () => ({ id: 1, did: 'did:example:co', corporation: 'verana1pol', governanceFrameworkVersions: [] }),
    })
    ;(calculateCorporationParticipantStats as jest.Mock).mockResolvedValue({
      participants: 0,
      participants_ecosystem: 0,
      participants_issuer_grantor: 0,
      participants_issuer: 0,
      participants_verifier_grantor: 0,
      participants_verifier: 0,
      participants_holder: 0,
    })
    ;(countControlledEcosystems as jest.Mock).mockResolvedValue(0)
    ;(getCorporationTrustDeposit as jest.Mock).mockResolvedValue({
      deposit: 0,
      share: 0,
      refunded: 0,
      slashed_deposit: 0,
      repaid_deposit: 0,
      slash_count: 0,
      last_slashed: null,
      last_repaid: null,
    })
    ;(deriveActiveVersion as jest.Mock).mockReturnValue(null)
    ;(getResolvedBlockHeight as jest.Mock).mockResolvedValue(5)

    const ctx: any = { params: { id: '1', gf_data: 'none' }, meta: { blockHeight: 5 } }
    await service.getCorporationV4(ctx)

    // participant counts are resolved at the requested At-Block-Height (block height threaded through)
    expect(calculateCorporationParticipantStats).toHaveBeenCalledWith(1, 5)
    // block_height resolves/echoes the requested At-Block-Height
    expect(getResolvedBlockHeight).toHaveBeenCalledWith(5)
  })
})
