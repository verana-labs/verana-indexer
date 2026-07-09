jest.mock('../../../../src/models/co_governance_framework_version', () => ({
  CoGovernanceFrameworkVersion: { query: jest.fn() },
}))
jest.mock('../../../../src/models/governance_framework_version', () => ({
  GovernanceFrameworkVersion: { query: jest.fn() },
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
import { CoGovernanceFrameworkVersion } from '../../../../src/models/co_governance_framework_version'
import { GovernanceFrameworkVersion } from '../../../../src/models/governance_framework_version'
import GovernanceFrameworkApiService from '../../../../src/services/crawl-gf/gf_api.service'

function tableResolvesTo(model: { query: jest.Mock }, row: Record<string, unknown> | undefined) {
  const qb: any = {}
  qb.where = jest.fn(() => qb)
  qb.withGraphFetched = jest.fn(() => qb)
  qb.first = jest.fn(async () => (row ? { toJSON: () => row } : undefined))
  model.query.mockReturnValue(qb)
  return qb
}

function gfvRow(over: Record<string, unknown> = {}) {
  return {
    id: 3,
    corporation_id: 2,
    ecosystem_id: 0,
    version: 2,
    created: '2024-01-10T00:00:00.000Z',
    active_since: '2024-02-01T00:00:00.000Z',
    gfv_id: 7,
    documents: [
      {
        id: 5,
        gfv_id: 3,
        gfd_id: 11,
        created: '2024-01-10T00:00:00.000Z',
        language: 'en',
        url: 'http://example.com/en.pdf',
        digest_sri: 'sha384-en',
      },
      {
        id: 6,
        gfv_id: 3,
        gfd_id: 12,
        created: '2024-01-11T00:00:00.000Z',
        language: 'fr',
        url: 'http://example.com/fr.pdf',
        digest_sri: 'sha384-fr',
      },
    ],
    ...over,
  }
}

describe('GovernanceFrameworkApiService.getGovernanceFrameworkVersionV4', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new GovernanceFrameworkApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    tableResolvesTo(CoGovernanceFrameworkVersion as any, undefined)
    tableResolvesTo(GovernanceFrameworkVersion as any, undefined)
  })

  it('returns a CGF version (ecosystem_id 0) with corporation_id set and chain ids', async () => {
    tableResolvesTo(CoGovernanceFrameworkVersion as any, gfvRow())

    const ctx: any = { params: { id: '7' }, meta: {} }
    const res: any = await service.getGovernanceFrameworkVersionV4(ctx)

    expect(res.version).toEqual({
      id: 7,
      ecosystem_id: null,
      corporation_id: 2,
      created: '2024-01-10T00:00:00.000Z',
      version: 2,
      active_since: '2024-02-01T00:00:00.000Z',
      documents: [
        {
          id: 11,
          gfv_id: 7,
          created: '2024-01-10T00:00:00.000Z',
          language: 'en',
          url: 'http://example.com/en.pdf',
          digest_sri: 'sha384-en',
        },
        {
          id: 12,
          gfv_id: 7,
          created: '2024-01-11T00:00:00.000Z',
          language: 'fr',
          url: 'http://example.com/fr.pdf',
          digest_sri: 'sha384-fr',
        },
      ],
    })
  })

  it('maps an ecosystem-scoped co-table row (ecosystem_id != 0) as an EGF', async () => {
    tableResolvesTo(CoGovernanceFrameworkVersion as any, gfvRow({ ecosystem_id: 4 }))

    const ctx: any = { params: { id: '7' }, meta: {} }
    const res: any = await service.getGovernanceFrameworkVersionV4(ctx)

    expect(res.version.ecosystem_id).toBe(4)
    expect(res.version.corporation_id).toBeNull()
  })

  it('falls back to the ecosystem table when the co table has no match', async () => {
    tableResolvesTo(
      GovernanceFrameworkVersion as any,
      gfvRow({ corporation_id: undefined, ecosystem_id: 9, gfv_id: 2, version: 1 })
    )

    const ctx: any = { params: { id: '2' }, meta: {} }
    const res: any = await service.getGovernanceFrameworkVersionV4(ctx)

    expect(res.version.id).toBe(2)
    expect(res.version.ecosystem_id).toBe(9)
    expect(res.version.corporation_id).toBeNull()
  })

  it('returns only the preferred_language document when present, all documents otherwise', async () => {
    tableResolvesTo(CoGovernanceFrameworkVersion as any, gfvRow())

    const hit: any = await service.getGovernanceFrameworkVersionV4({
      params: { id: '7', preferred_language: 'fr' },
      meta: {},
    } as any)
    expect(hit.version.documents).toHaveLength(1)
    expect(hit.version.documents[0].language).toBe('fr')

    tableResolvesTo(CoGovernanceFrameworkVersion as any, gfvRow())
    const miss: any = await service.getGovernanceFrameworkVersionV4({
      params: { id: '7', preferred_language: 'de' },
      meta: {},
    } as any)
    expect(miss.version.documents).toHaveLength(2)
  })

  it('applies At-Block-Height: filters later documents and masks a later active_since', async () => {
    tableResolvesTo(
      CoGovernanceFrameworkVersion as any,
      gfvRow({
        active_since: '2024-03-15T00:00:00.000Z',
        documents: [
          gfvRow().documents?.[0],
          { ...(gfvRow().documents?.[1] as object), created: '2024-03-10T00:00:00.000Z' },
        ],
      })
    )

    const ctx: any = { params: { id: '7' }, meta: { blockHeight: 50 } }
    const res: any = await service.getGovernanceFrameworkVersionV4(ctx)

    expect(res.version.active_since).toBeNull()
    expect(res.version.documents).toHaveLength(1)
    expect(res.version.documents[0].language).toBe('en')
  })

  it('returns 404 at a height before the version was created', async () => {
    tableResolvesTo(CoGovernanceFrameworkVersion as any, gfvRow({ created: '2024-03-20T00:00:00.000Z' }))

    const ctx: any = { params: { id: '7' }, meta: { blockHeight: 50 } }
    const res: any = await service.getGovernanceFrameworkVersionV4(ctx)

    expect(res.code).toBe(404)
  })

  it('returns 404 when neither table matches and 400 for a non-numeric id', async () => {
    const missing: any = await service.getGovernanceFrameworkVersionV4({ params: { id: '999' }, meta: {} } as any)
    expect(missing.code).toBe(404)

    const invalid: any = await service.getGovernanceFrameworkVersionV4({ params: { id: 'abc' }, meta: {} } as any)
    expect(invalid.code).toBe(400)
    expect(CoGovernanceFrameworkVersion.query).toHaveBeenCalledTimes(1)
  })
})
