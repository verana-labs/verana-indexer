import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Knex } from 'knex'
import { Context, Errors, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { BULL_JOB_NAME, SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import knex from '../../common/utils/db_connection'
import { isValidDid } from './api_shared'

const BLOCK_CHECKPOINT_JOB = BULL_JOB_NAME.HANDLE_TRANSACTION

async function fetchLatestIndexedHeight(): Promise<number> {
  const checkpoint = await knex('block_checkpoint').select('height').where('job_name', BLOCK_CHECKPOINT_JOB).first()
  const height = Number(checkpoint?.height ?? 0)
  return Number.isInteger(height) && height >= 0 ? height : 0
}

type SnapshotRow = Record<string, unknown>

type SnapshotResponse = {
  did: string
  block_height: number
  ecosystems: SnapshotRow[]
  schemas: SnapshotRow[]
  participants: SnapshotRow[]
  count: {
    ecosystems: number
    schemas: number
    participants: number
  }
}

type HistorySource = {
  table: string
  entityIdColumn: string
}

const ECOSYSTEM_HISTORY: HistorySource = { table: 'ecosystem_history', entityIdColumn: 'ecosystem_id' }
const CREDENTIAL_SCHEMA_HISTORY: HistorySource = {
  table: 'credential_schema_history',
  entityIdColumn: 'credential_schema_id',
}
const PARTICIPANT_HISTORY: HistorySource = { table: 'participant_history', entityIdColumn: 'participant_id' }

const HISTORY_SOURCES = [ECOSYSTEM_HISTORY, CREDENTIAL_SCHEMA_HISTORY, PARTICIPANT_HISTORY]

const HISTORY_BOOKKEEPING_COLUMNS = new Set(['event_type', 'action', 'changes', 'created_at'])

const TABLE_CHECK_TTL_MS = 60_000
let cachedTables: { expiresAt: number; value: Promise<Record<string, boolean>> } | null = null

async function getAvailableHistoryTables(): Promise<Record<string, boolean>> {
  const now = Date.now()
  if (cachedTables && cachedTables.expiresAt > now) return cachedTables.value

  const value = (async () => {
    const present = await Promise.all(HISTORY_SOURCES.map((source) => knex.schema.hasTable(source.table)))
    return Object.fromEntries(HISTORY_SOURCES.map((source, index) => [source.table, present[index]]))
  })()

  cachedTables = { expiresAt: now + TABLE_CHECK_TTL_MS, value }
  return value
}

function uniquePositiveIds(values: unknown[]): number[] {
  return Array.from(new Set(values.map((value) => Number(value ?? 0)).filter((id) => Number.isInteger(id) && id > 0)))
}

function toEntityRow(row: SnapshotRow, entityIdColumn: string): SnapshotRow {
  const entityRow: SnapshotRow = { id: Number(row[entityIdColumn] ?? 0) }
  for (const [column, value] of Object.entries(row)) {
    if (column === 'id' || column === entityIdColumn) continue
    if (HISTORY_BOOKKEEPING_COLUMNS.has(column)) continue
    entityRow[column] = value
  }
  return entityRow
}

async function fetchEntitiesAtHeight(args: {
  source: HistorySource
  blockHeight: number
  applyFilter: (query: Knex.QueryBuilder) => void
}): Promise<SnapshotRow[]> {
  const { source, blockHeight, applyFilter } = args

  const tables = await getAvailableHistoryTables()
  if (!tables[source.table]) return []

  const latestRowPerEntity = knex(source.table)
    .distinctOn(source.entityIdColumn)
    .select('*')
    .where('height', '<=', blockHeight)
    .orderBy([{ column: source.entityIdColumn }, { column: 'height', order: 'desc' }, { column: 'id', order: 'desc' }])

  const query = knex.from(latestRowPerEntity.as('state_at_height')).select('*')
  applyFilter(query)

  const rows: SnapshotRow[] = await query.orderBy(source.entityIdColumn, 'asc')
  return rows.map((row) => toEntityRow(row, source.entityIdColumn))
}

async function fetchEcosystemsByDidOrIds(did: string, ids: number[], blockHeight: number): Promise<SnapshotRow[]> {
  return fetchEntitiesAtHeight({
    source: ECOSYSTEM_HISTORY,
    blockHeight,
    applyFilter: (query) =>
      query.where((qb) => {
        qb.where('did', did)
        if (ids.length > 0) qb.orWhereIn('ecosystem_id', ids)
      }),
  })
}

async function fetchParticipantsByDid(did: string, blockHeight: number): Promise<SnapshotRow[]> {
  return fetchEntitiesAtHeight({
    source: PARTICIPANT_HISTORY,
    blockHeight,
    applyFilter: (query) => query.where('did', did),
  })
}

async function fetchCredentialSchemas(args: {
  ecosystemIds: number[]
  schemaIds: number[]
  blockHeight: number
}): Promise<SnapshotRow[]> {
  const { ecosystemIds, schemaIds, blockHeight } = args
  if (ecosystemIds.length === 0 && schemaIds.length === 0) return []

  return fetchEntitiesAtHeight({
    source: CREDENTIAL_SCHEMA_HISTORY,
    blockHeight,
    applyFilter: (query) =>
      query.where((qb) => {
        if (ecosystemIds.length > 0) qb.orWhereIn('ecosystem_id', ecosystemIds)
        if (schemaIds.length > 0) qb.orWhereIn('credential_schema_id', schemaIds)
      }),
  })
}

async function fetchParticipants(args: {
  did: string
  blockHeight: number
  schemaIds: number[]
  corporationIds: number[]
}): Promise<SnapshotRow[]> {
  const { did, blockHeight, schemaIds, corporationIds } = args

  return fetchEntitiesAtHeight({
    source: PARTICIPANT_HISTORY,
    blockHeight,
    applyFilter: (query) =>
      query.where((qb) => {
        qb.where('did', did)
        if (corporationIds.length > 0) qb.orWhereIn('corporation_id', corporationIds)
        if (schemaIds.length > 0) qb.orWhereIn('schema_id', schemaIds)
      }),
  })
}

export async function getDidSnapshotAtHeight(args: { did: string; blockHeight: number }): Promise<SnapshotResponse> {
  const { did, blockHeight } = args

  const [ecosystemsByDid, participantsByDid] = await Promise.all([
    fetchEcosystemsByDidOrIds(did, [], blockHeight),
    fetchParticipantsByDid(did, blockHeight),
  ])

  const schemas = await fetchCredentialSchemas({
    ecosystemIds: uniquePositiveIds(ecosystemsByDid.map((row) => row.id)),
    schemaIds: uniquePositiveIds(participantsByDid.map((row) => row.schema_id)),
    blockHeight,
  })

  const ecosystems = await fetchEcosystemsByDidOrIds(
    did,
    uniquePositiveIds(schemas.map((row) => row.ecosystem_id)),
    blockHeight
  )

  const corporationIds = uniquePositiveIds([
    ...ecosystems.map((row) => row.corporation_id),
    ...participantsByDid.map((row) => row.corporation_id),
  ])

  const participants = await fetchParticipants({
    did,
    blockHeight,
    schemaIds: uniquePositiveIds(schemas.map((row) => row.id)),
    corporationIds,
  })

  return {
    did,
    block_height: blockHeight,
    ecosystems,
    schemas,
    participants,
    count: {
      ecosystems: ecosystems.length,
      schemas: schemas.length,
      participants: participants.length,
    },
  }
}

@Service({
  name: SERVICE.V1.IndexerSnapshotService.key,
  version: 1,
})
export default class IndexerSnapshotService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({
    name: 'getSnapshot',
    params: {
      did: { type: 'string', optional: true, trim: true },
    },
    rest: 'GET /snapshot',
  })
  public async getSnapshot(ctx: Context<{ did?: string }>) {
    try {
      const did = typeof ctx.params.did === 'string' ? ctx.params.did.trim() : ''
      if (!did) return ApiResponder.error(ctx, 'Missing did', 400)
      if (!isValidDid(did)) return ApiResponder.error(ctx, 'Invalid did', 400)

      const headerHeight = (ctx.meta as { blockHeight?: number } | undefined)?.blockHeight
      const blockHeight =
        typeof headerHeight === 'number' && Number.isInteger(headerHeight) && headerHeight >= 0
          ? headerHeight
          : await fetchLatestIndexedHeight()

      const snapshot = await getDidSnapshotAtHeight({ did, blockHeight })
      return ApiResponder.success(ctx, snapshot, 200)
    } catch (err: unknown) {
      this.logger.error('[IndexerSnapshotService] Failed to build snapshot:', err)
      if (err instanceof Errors.MoleculerError) throw err
      throw new Errors.MoleculerError('Failed to build snapshot', 500, 'SNAPSHOT_FAILED')
    }
  }
}
