import { ServiceBroker } from 'moleculer'
import { BULL_JOB_NAME } from '../../../../src/common/constant'
import IndexerMetaService from '../../../../src/services/manager/indexer_meta.service'

type QueryState = {
  table: string
  whereCol?: string
  whereOp?: string
  whereVal?: number
  whereStrVal?: string
  andWhereCol?: string
  andWhereOp?: string
  andWhereVal?: number
  orderCol?: string
  orderDir?: string
  limitVal?: number
}

const tableHeights: Record<string, number[]> = {
  ecosystem_history: [150],
  governance_framework_version_history: [],
  governance_framework_document_history: [],
  credential_schema_history: [],
  participant_history: [],
  participant_session_history: [],
  trust_deposit_history: [],
  module_params_history: [],
  block_checkpoint: [200],
}

const queryStates: QueryState[] = []

jest.mock('../../../../src/common/utils/db_connection', () => {
  const mockKnex: any = jest.fn((tableName: string) => {
    const state: QueryState = { table: tableName }
    queryStates.push(state)

    const qb: any = {}
    qb.select = jest.fn(() => qb)
    qb.where = jest.fn((col: string, opOrVal: any, maybeVal?: any) => {
      state.whereCol = col
      if (maybeVal === undefined) {
        state.whereOp = '='
        state.whereStrVal = String(opOrVal)
      } else {
        state.whereOp = String(opOrVal)
        state.whereVal = Number(maybeVal)
      }
      return qb
    })
    qb.andWhere = jest.fn((col: string, op: string, val: number) => {
      state.andWhereCol = col
      state.andWhereOp = op
      state.andWhereVal = Number(val)
      return qb
    })
    qb.orderBy = jest.fn((col: string, dir: string) => {
      state.orderCol = col
      state.orderDir = dir
      return qb
    })
    qb.limit = jest.fn((val: number) => {
      state.limitVal = val
      return qb
    })
    qb.first = jest.fn(async () => {
      const rows = tableHeights[tableName] ?? []
      if (tableName === 'block_checkpoint') {
        const matchesHandleTransactionCheckpoint =
          state.whereCol === 'job_name' &&
          state.whereOp === '=' &&
          state.whereStrVal === BULL_JOB_NAME.HANDLE_TRANSACTION
        if (!matchesHandleTransactionCheckpoint) {
          return undefined
        }
        const first = rows[0]
        return first !== undefined ? { height: first } : undefined
      }
      const threshold = state.whereVal ?? Number.NEGATIVE_INFINITY
      const max = state.andWhereVal ?? Number.POSITIVE_INFINITY
      const next = rows.filter((h) => h > threshold && h <= max).sort((a, b) => a - b)[0]
      return next !== undefined ? { height: next } : undefined
    })
    return qb
  })

  mockKnex.raw = jest.fn((sql: string) => sql)
  return mockKnex
})

describe('IndexerMetaService next_change_at', () => {
  let broker: ServiceBroker
  let service: any

  beforeAll(() => {
    broker = new ServiceBroker({ logger: false })
    service = new (IndexerMetaService as any)(broker)
  })

  beforeEach(() => {
    queryStates.length = 0
  })

  afterAll(async () => {
    await broker.stop()
  })

  it('returns the minimum height strictly greater than block_height', async () => {
    const next = await service.getNextChangeAt(105)
    expect(next).toBe(150)

    expect(queryStates.length).toBeGreaterThan(0)
    for (const q of queryStates) {
      if (q.table === 'block_checkpoint') continue
      expect(q.whereCol).toBe('height')
      expect(q.whereOp).toBe('>')
      expect(q.whereVal).toBe(105)
      expect(q.andWhereCol).toBe('height')
      expect(q.andWhereOp).toBe('<=')
      expect(q.andWhereVal).toBe(200)
      expect((q.orderDir || '').toLowerCase()).toBe('asc')
      expect(q.limitVal).toBe(1)
    }
  })

  it('returns null when no higher height exists', async () => {
    const next = await service.getNextChangeAt(1000)
    expect(next).toBeNull()
  })

  it('returns null when the next height is above current indexed checkpoint (reindex safety)', async () => {
    const prevEcosystem = tableHeights.ecosystem_history
    const prevCheckpoint = tableHeights.block_checkpoint
    try {
      tableHeights.ecosystem_history = [9999]
      tableHeights.block_checkpoint = [200]
      const next = await service.getNextChangeAt(105)
      expect(next).toBeNull()
    } finally {
      tableHeights.ecosystem_history = prevEcosystem
      tableHeights.block_checkpoint = prevCheckpoint
    }
  })

  it('returns null when checkpoint exists but no next change is within (block_height, checkpoint]', async () => {
    const next = await service.getNextChangeAt(150)
    expect(next).toBeNull()
  })

  it('returns null when no checkpoint row exists', async () => {
    const prevCheckpoint = tableHeights.block_checkpoint
    try {
      tableHeights.block_checkpoint = []
      const next = await service.getNextChangeAt(105)
      expect(next).toBeNull()
    } finally {
      tableHeights.block_checkpoint = prevCheckpoint
    }
  })
})
