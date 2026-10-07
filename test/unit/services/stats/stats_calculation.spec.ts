const counterLog = new Map<string, number>()
const counterLogQueries: Array<Record<string, unknown>> = []
let participantRows: Array<Record<string, unknown>> = []
let participantRow: Record<string, unknown> | undefined

function counterKey(entityKind: unknown, entityId: unknown, roleType: unknown): string {
  return `${entityKind}:${entityId}:${roleType}`
}

function firstRow(table: string, filters: Record<string, unknown>): Record<string, unknown> | undefined {
  if (table === 'entity_participant_changes') {
    counterLogQueries.push({ ...filters })
    const value = counterLog.get(counterKey(filters.entity_kind, filters.entity_id, filters.type))
    return value === undefined ? undefined : { value }
  }
  if (table === 'participants') return participantRow
  if (table === 'credential_schemas') return filters.count ? { count: 0 } : { archived: null }
  if (table === 'ecosystem') return { active_ecosystems: 0, archived_ecosystems: 0 }
  return undefined
}

function createQueryBuilder(table: string) {
  const filters: Record<string, unknown> = {}
  const builder: any = {
    where: (column: unknown, operator?: unknown, value?: unknown) => {
      if (typeof column === 'string') filters[column] = value === undefined ? operator : value
      return builder
    },
    whereIn: () => builder,
    whereNull: (column: string) => {
      filters[column] = null
      return builder
    },
    whereNotNull: (column: string) => {
      filters[column] = 'not null'
      return builder
    },
    orderBy: () => builder,
    select: () => builder,
    count: () => {
      filters.count = true
      return builder
    },
    first: () => Promise.resolve(firstRow(table, filters)),
    then: (resolve: (rows: unknown) => unknown, reject: (error: unknown) => unknown) =>
      Promise.resolve(table === 'participants' ? participantRows : []).then(resolve, reject),
  }
  builder.andWhere = builder.where
  return builder
}

const knexMock: any = jest.fn((table: string) => createQueryBuilder(table))
knexMock.raw = (sql: string) => sql

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: knexMock,
}))

jest.mock('../../../../src/common/queue/queue-manager', () => ({
  __esModule: true,
  default: { getInstance: () => ({ bindQueueOwner: () => undefined }) },
}))

jest.mock('../../../../src/models/stats', () => ({
  __esModule: true,
  default: { query: () => createQueryBuilder('stats') },
}))

import { ServiceBroker } from 'moleculer'
import StatsCalculationService from '../../../../src/services/stats/stats_calculation.service'

const EVALUATION_HEIGHT = 1234
const BUCKET_TIMESTAMP = new Date('2026-01-18T00:00:00.000Z')

const INFLATED_SUBTREE_COUNTERS = {
  participants: 9,
  participants_ecosystem: 3,
  participants_issuer_grantor: 3,
  participants_issuer: 3,
  participants_verifier_grantor: 0,
  participants_verifier: 0,
  participants_holder: 0,
  weight: '0',
  issued: '0',
  verified: '0',
}

describe('StatsCalculationService participant counters', () => {
  let service: any

  beforeEach(() => {
    counterLog.clear()
    counterLogQueries.length = 0
    participantRows = [{ ...INFLATED_SUBTREE_COUNTERS }, { ...INFLATED_SUBTREE_COUNTERS }]
    participantRow = { ...INFLATED_SUBTREE_COUNTERS }
    service = new StatsCalculationService(new ServiceBroker({ logger: false }))
  })

  it('reads cumulative_participants for GLOBAL from the counter log at the evaluation height', async () => {
    counterLog.set(counterKey(0, 0, 0), 2)
    counterLog.set(counterKey(0, 0, 1), 1)
    counterLog.set(counterKey(0, 0, 3), 1)

    const stats = await service.computeGlobalStats(BUCKET_TIMESTAMP, EVALUATION_HEIGHT)

    expect(stats.cumulative_participants).toBe(2)
    expect(stats.cumulative_participants_ecosystem).toBe(1)
    expect(stats.cumulative_participants_issuer).toBe(1)
    expect(stats.delta_participants).toBe(2)
    expect(counterLogQueries).toHaveLength(7)
    expect(counterLogQueries[0]).toMatchObject({ entity_kind: 0, entity_id: 0, type: 0, height: EVALUATION_HEIGHT })
  })

  it('scopes the CREDENTIAL_SCHEMA lookup to the schema entity kind', async () => {
    counterLog.set(counterKey(2, 7, 0), 3)

    const stats = await service.computeCredentialSchemaStats('7', BUCKET_TIMESTAMP, EVALUATION_HEIGHT)

    expect(stats.cumulative_participants).toBe(3)
    expect(counterLogQueries[0]).toMatchObject({ entity_kind: 2, entity_id: 7, type: 0, height: EVALUATION_HEIGHT })
  })

  it('reads the PARTICIPANT sub-tree count from the counter log instead of the row counters', async () => {
    counterLog.set(counterKey(3, 5, 0), 4)

    const stats = await service.computeParticipantStats('5', '7', BUCKET_TIMESTAMP, EVALUATION_HEIGHT)

    expect(stats.cumulative_participants).toBe(4)
    expect(counterLogQueries[0]).toMatchObject({ entity_kind: 3, entity_id: 5, type: 0, height: EVALUATION_HEIGHT })
  })

  it('treats a missing counter-log row as zero', async () => {
    const stats = await service.computeGlobalStats(BUCKET_TIMESTAMP, EVALUATION_HEIGHT)

    expect(stats.cumulative_participants).toBe(0)
    expect(stats.cumulative_participants_holder).toBe(0)
  })
})
