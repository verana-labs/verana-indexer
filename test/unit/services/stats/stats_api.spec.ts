const mockEpcFirst = jest.fn()
const mockEpcChain: any = {
  where: jest.fn(() => mockEpcChain),
  andWhere: jest.fn(() => mockEpcChain),
  orderBy: jest.fn(() => mockEpcChain),
  first: mockEpcFirst,
}

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: jest.fn(() => mockEpcChain),
}))

jest.mock('../../../../src/services/crawl-co/co_stats', () => ({
  getResolvedBlockHeight: jest.fn(async (height?: number) => height ?? 999),
}))

const mockComputeSnapshotMetrics = jest.fn()
const mockGetBlockTimeAtHeight = jest.fn()
jest.mock('../../../../src/services/stats/stats_snapshot', () => ({
  ...jest.requireActual('../../../../src/services/stats/stats_snapshot'),
  computeSnapshotMetrics: (...args: unknown[]) => mockComputeSnapshotMetrics(...args),
  getBlockTimeAtHeight: (...args: unknown[]) => mockGetBlockTimeAtHeight(...args),
}))

jest.mock('../../../../src/models/stats', () => ({
  __esModule: true,
  default: { query: jest.fn() },
}))

jest.mock('../../../../src/common/utils/apiResponse', () => ({
  __esModule: true,
  default: {
    success: jest.fn((_ctx: unknown, data: unknown) => data),
    error: jest.fn((_ctx: unknown, message: string, code: number) => ({ error: message, code })),
  },
}))

import { ServiceBroker } from 'moleculer'
import Stats from '../../../../src/models/stats'
import StatsAPIService from '../../../../src/services/stats/stats_api.service'

const PER_ROLE_FIELDS = [
  'cumulative_participants_ecosystem',
  'cumulative_participants_issuer_grantor',
  'cumulative_participants_issuer',
  'cumulative_participants_verifier_grantor',
  'cumulative_participants_verifier',
  'cumulative_participants_holder',
  'delta_participants_ecosystem',
  'delta_participants_issuer_grantor',
  'delta_participants_issuer',
  'delta_participants_verifier_grantor',
  'delta_participants_verifier',
  'delta_participants_holder',
]

function statsRow(over: Record<string, unknown> = {}) {
  const row: Record<string, unknown> = {
    id: 3,
    granularity: 'DAY',
    timestamp: new Date('2026-01-18T00:00:00.000Z'),
    entity_type: 'GLOBAL',
    entity_id: null,
    created_at: new Date('2026-01-19T00:00:00.000Z'),
    updated_at: new Date('2026-01-19T00:00:00.000Z'),
  }
  const metrics = [
    'participants',
    'active_ecosystems',
    'archived_ecosystems',
    'active_schemas',
    'archived_schemas',
    'weight',
    'issued',
    'verified',
    'ecosystem_slash_events',
    'ecosystem_slashed_amount',
    'ecosystem_slashed_amount_repaid',
    'network_slash_events',
    'network_slashed_amount',
    'network_slashed_amount_repaid',
  ]
  for (const m of metrics) {
    row[`cumulative_${m}`] = 1
    row[`delta_${m}`] = 1
  }
  for (const f of PER_ROLE_FIELDS) row[f] = 9
  return { ...row, ...over }
}

describe('StatsAPIService.get', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new StatsAPIService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
  })

  it('drops per-role and audit fields, keeping only the StatsEntry shape', async () => {
    ;(Stats.query as jest.Mock).mockReturnValue({ findById: jest.fn(async () => statsRow()) })

    const res: any = await service.get({ params: { id: 3 } } as any)

    for (const f of PER_ROLE_FIELDS) expect(res).not.toHaveProperty(f)
    expect(res).not.toHaveProperty('created_at')
    expect(res).not.toHaveProperty('updated_at')
    expect(Object.keys(res).length).toBe(33)
    expect(res).toMatchObject({ cumulative_active_ecosystems: 1, delta_archived_ecosystems: 1 })
    expect(res).toMatchObject({ id: 3, entity_type: 'GLOBAL', cumulative_participants: 1, delta_participants: 1 })
  })

  it('returns 404 when the id is not found', async () => {
    ;(Stats.query as jest.Mock).mockReturnValue({ findById: jest.fn(async () => undefined) })

    const res: any = await service.get({ params: { id: 99 } } as any)
    expect(res.code).toBe(404)
  })
})

describe('StatsAPIService.getParticipantsAtHeight', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new StatsAPIService(broker)

  beforeEach(() => {
    jest.clearAllMocks()
    mockEpcFirst.mockResolvedValue({ value: 7 })
  })

  it('uses the At-Block-Height header and returns block_height/participants', async () => {
    const res: any = await service.getParticipantsAtHeight({
      params: { entity_kind: 1, entity_id: '5', role_type: 3 },
      meta: { blockHeight: 42 },
    } as any)

    expect(res).toEqual({ entity_kind: 1, entity_id: 5, role_type: 3, block_height: 42, participants: 7 })
    expect(res).not.toHaveProperty('height')
    expect(res).not.toHaveProperty('value')
  })

  it('defaults to the latest indexed block when the header is absent', async () => {
    const res: any = await service.getParticipantsAtHeight({
      params: { entity_kind: 1, entity_id: '5', role_type: 3 },
      meta: {},
    } as any)

    expect(res.block_height).toBe(999)
  })

  it('returns entity_id null for GLOBAL', async () => {
    const res: any = await service.getParticipantsAtHeight({
      params: { entity_kind: 0, role_type: 0 },
      meta: { blockHeight: 10 },
    } as any)

    expect(res.entity_id).toBeNull()
    expect(res.block_height).toBe(10)
  })

  it('rejects a non-GLOBAL request without entity_id', async () => {
    const res: any = await service.getParticipantsAtHeight({
      params: { entity_kind: 2, role_type: 3 },
      meta: { blockHeight: 10 },
    } as any)

    expect(res.code).toBe(400)
  })
})

describe('StatsAPIService.getSnapshot', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new StatsAPIService(broker)
  const metrics = {
    active_ecosystems: 2,
    archived_ecosystems: 1,
    active_schemas: 5,
    archived_schemas: 0,
    weight: 1000,
    issued: 10,
    verified: 4,
    ecosystem_slash_events: 0,
    ecosystem_slashed_amount: 0,
    ecosystem_slashed_amount_repaid: 0,
    network_slash_events: 0,
    network_slashed_amount: 0,
    network_slashed_amount_repaid: 0,
  }

  beforeEach(() => {
    jest.clearAllMocks()
    mockEpcFirst.mockResolvedValue({ value: 7 })
    mockComputeSnapshotMetrics.mockResolvedValue(metrics)
    mockGetBlockTimeAtHeight.mockResolvedValue('2026-09-08T12:00:00.000Z')
  })

  it('returns the GLOBAL snapshot at the header height with entity_id null', async () => {
    const res: any = await service.getSnapshot({ params: {}, meta: { blockHeight: 42 } } as any)

    expect(mockComputeSnapshotMetrics).toHaveBeenCalledWith('GLOBAL', null, 42, true)
    expect(mockEpcFirst).toHaveBeenCalledTimes(7)
    expect(res).toEqual({
      entity_type: 'GLOBAL',
      entity_id: null,
      block_height: 42,
      timestamp: '2026-09-08T12:00:00.000Z',
      participants: 7,
      participants_ecosystem: 7,
      participants_issuer_grantor: 7,
      participants_issuer: 7,
      participants_verifier_grantor: 7,
      participants_verifier: 7,
      participants_holder: 7,
      ...metrics,
    })
  })

  it('resolves the latest block and reads live tables when the header is absent', async () => {
    const res: any = await service.getSnapshot({
      params: { entity_type: 'ECOSYSTEM', entity_id: '5' },
      meta: {},
    } as any)

    expect(mockComputeSnapshotMetrics).toHaveBeenCalledWith('ECOSYSTEM', 5, 999, false)
    expect(res.block_height).toBe(999)
    expect(res.entity_id).toBe(5)
  })

  it('rejects entity_id for GLOBAL', async () => {
    const res: any = await service.getSnapshot({ params: { entity_id: '1' }, meta: {} } as any)
    expect(res.code).toBe(400)
    expect(mockComputeSnapshotMetrics).not.toHaveBeenCalled()
  })

  it('requires entity_id for non-GLOBAL entity types', async () => {
    const res: any = await service.getSnapshot({ params: { entity_type: 'PARTICIPANT' }, meta: {} } as any)
    expect(res.code).toBe(400)
  })

  it('rejects a non-numeric entity_id', async () => {
    const res: any = await service.getSnapshot({
      params: { entity_type: 'CREDENTIAL_SCHEMA', entity_id: 'abc' },
      meta: {},
    } as any)
    expect(res.code).toBe(400)
  })

  it('returns 404 for an unknown entity', async () => {
    mockComputeSnapshotMetrics.mockResolvedValue(null)
    const res: any = await service.getSnapshot({
      params: { entity_type: 'CREDENTIAL_SCHEMA', entity_id: '77' },
      meta: {},
    } as any)
    expect(res.code).toBe(404)
    expect(mockEpcFirst).not.toHaveBeenCalled()
  })
})
