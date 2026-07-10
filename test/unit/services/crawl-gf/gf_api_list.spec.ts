jest.mock('../../../../src/models/co_governance_framework_version', () => ({
  CoGovernanceFrameworkVersion: { query: jest.fn() },
}))
jest.mock('../../../../src/models/governance_framework_version', () => ({
  GovernanceFrameworkVersion: { query: jest.fn() },
}))
jest.mock('../../../../src/models/corporation', () => ({ Corporation: { query: jest.fn() } }))
jest.mock('../../../../src/models/ecosystem', () => ({ Ecosystem: { query: jest.fn() } }))

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
import { Corporation } from '../../../../src/models/corporation'
import { Ecosystem } from '../../../../src/models/ecosystem'
import { GovernanceFrameworkVersion } from '../../../../src/models/governance_framework_version'
import GovernanceFrameworkApiService from '../../../../src/services/crawl-gf/gf_api.service'

function listQb(rows: Record<string, unknown>[]) {
  const qb: any = {}
  qb.where = jest.fn(() => qb)
  qb.whereNotNull = jest.fn(() => qb)
  qb.withGraphFetched = jest.fn(() => qb)
  qb.orderBy = jest.fn(() => qb)
  qb.limit = jest.fn(async () => rows.map((r) => ({ toJSON: () => r })))
  return qb
}

function subjectQuery(activeVersion: number | null | undefined) {
  return {
    findById: jest.fn(async () => (activeVersion === undefined ? undefined : { active_version: activeVersion })),
  }
}

function gfvRow(over: Record<string, unknown> = {}) {
  return {
    corporation_id: 2,
    ecosystem_id: 0,
    version: 2,
    created: '2024-01-10T00:00:00.000Z',
    active_since: '2024-02-01T00:00:00.000Z',
    gfv_id: 7,
    documents: [
      { gfd_id: 11, gfv_id: 3, created: '2024-01-10T00:00:00.000Z', language: 'en', url: 'u', digest_sri: 's' },
    ],
    ...over,
  }
}

describe('GovernanceFrameworkApiService.listGovernanceFrameworkVersionsV4', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new GovernanceFrameworkApiService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    ;(CoGovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(listQb([]))
    ;(GovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(listQb([]))
  })

  it('rejects when neither ecosystem_id nor corporation_id is given', async () => {
    const res: any = await service.listGovernanceFrameworkVersionsV4({ params: {}, meta: {} } as any)
    expect(res.code).toBe(400)
  })

  it('rejects when both ecosystem_id and corporation_id are given', async () => {
    const res: any = await service.listGovernanceFrameworkVersionsV4({
      params: { ecosystem_id: '1', corporation_id: '2' },
      meta: {},
    } as any)
    expect(res.code).toBe(400)
  })

  it('rejects an invalid subject id', async () => {
    const res: any = await service.listGovernanceFrameworkVersionsV4({
      params: { corporation_id: 'abc' },
      meta: {},
    } as any)
    expect(res.code).toBe(400)
  })

  it('lists CGF for a corporation, scoping the co table to ecosystem_id 0 and non-null gfv_id', async () => {
    const qb = listQb([gfvRow()])
    ;(CoGovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(qb)

    const res: any = await service.listGovernanceFrameworkVersionsV4({
      params: { corporation_id: '2' },
      meta: {},
    } as any)

    expect(qb.where).toHaveBeenCalledWith('corporation_id', '2')
    expect(qb.where).toHaveBeenCalledWith('ecosystem_id', 0)
    expect(qb.whereNotNull).toHaveBeenCalledWith('gfv_id')
    expect(res.versions).toHaveLength(1)
    expect(res.versions[0]).toMatchObject({ id: 7, corporation_id: 2, ecosystem_id: null, version: 2 })
  })

  it('lists EGF for an ecosystem from the authoritative table', async () => {
    const qb = listQb([gfvRow({ corporation_id: undefined, ecosystem_id: 4, gfv_id: 8 })])
    ;(GovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(qb)

    const res: any = await service.listGovernanceFrameworkVersionsV4({ params: { ecosystem_id: '4' }, meta: {} } as any)

    expect(qb.where).toHaveBeenCalledWith('ecosystem_id', '4')
    expect(CoGovernanceFrameworkVersion.query).not.toHaveBeenCalled()
    expect(res.versions[0]).toMatchObject({ id: 8, ecosystem_id: 4, corporation_id: null })
  })

  it('active_only filters to the subject active_version', async () => {
    const qb = listQb([gfvRow()])
    ;(CoGovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(qb)
    ;(Corporation.query as jest.Mock).mockReturnValue(subjectQuery(2))

    await service.listGovernanceFrameworkVersionsV4({
      params: { corporation_id: '2', active_only: 'true' },
      meta: {},
    } as any)

    expect(qb.where).toHaveBeenCalledWith('version', 2)
  })

  it('active_only returns an empty list when the subject has no active_version', async () => {
    ;(Ecosystem.query as jest.Mock).mockReturnValue(subjectQuery(null))

    const res: any = await service.listGovernanceFrameworkVersionsV4({
      params: { ecosystem_id: '1', active_only: 'true' },
      meta: {},
    } as any)

    expect(res).toEqual({ versions: [] })
    expect(GovernanceFrameworkVersion.query).not.toHaveBeenCalled()
  })

  it('applies gfv_id cursor, sort and limit', async () => {
    const qb = listQb([])
    ;(CoGovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(qb)

    await service.listGovernanceFrameworkVersionsV4({
      params: { corporation_id: '2', min_id: '5', max_id: '20', sort: '+id', limit: '10' },
      meta: {},
    } as any)

    expect(qb.where).toHaveBeenCalledWith('gfv_id', '>=', '5')
    expect(qb.where).toHaveBeenCalledWith('gfv_id', '<', '20')
    expect(qb.orderBy).toHaveBeenCalledWith('gfv_id', 'asc')
    expect(qb.limit).toHaveBeenCalledWith(10)
  })

  it('defaults to gfv_id desc and limit 64', async () => {
    const qb = listQb([])
    ;(CoGovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(qb)

    await service.listGovernanceFrameworkVersionsV4({ params: { corporation_id: '2' }, meta: {} } as any)

    expect(qb.orderBy).toHaveBeenCalledWith('gfv_id', 'desc')
    expect(qb.limit).toHaveBeenCalledWith(64)
  })

  it('filters by created <= asOf at At-Block-Height', async () => {
    const qb = listQb([])
    ;(CoGovernanceFrameworkVersion.query as jest.Mock).mockReturnValue(qb)

    await service.listGovernanceFrameworkVersionsV4({
      params: { corporation_id: '2' },
      meta: { blockHeight: 50 },
    } as any)

    expect(qb.where).toHaveBeenCalledWith('created', '<=', '2024-03-01T00:00:00.000Z')
  })

  it('rejects an invalid active_only value', async () => {
    const res: any = await service.listGovernanceFrameworkVersionsV4({
      params: { corporation_id: '2', active_only: 'maybe' },
      meta: {},
    } as any)
    expect(res.code).toBe(400)
  })
})
