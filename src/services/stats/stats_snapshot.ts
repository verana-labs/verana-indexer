import knex from '../../common/utils/db_connection'
import type { EntityType } from '../../models/stats'

export const SNAPSHOT_ENTITY_KIND: Record<EntityType, number> = {
  GLOBAL: 0,
  ECOSYSTEM: 1,
  CREDENTIAL_SCHEMA: 2,
  PARTICIPANT: 3,
}

// index = entity_participant_changes role type (0 = ANY)
export const SNAPSHOT_PARTICIPANT_FIELDS = [
  'participants',
  'participants_ecosystem',
  'participants_issuer_grantor',
  'participants_issuer',
  'participants_verifier_grantor',
  'participants_verifier',
  'participants_holder',
] as const

const METRIC_COLUMNS = [
  'weight',
  'issued',
  'verified',
  'ecosystem_slash_events',
  'ecosystem_slashed_amount',
  'ecosystem_slashed_amount_repaid',
  'network_slash_events',
  'network_slashed_amount',
  'network_slashed_amount_repaid',
] as const

type MetricColumn = (typeof METRIC_COLUMNS)[number]
type Row = Record<string, unknown>

export type SnapshotMetrics = {
  active_ecosystems: number
  archived_ecosystems: number
  active_schemas: number
  archived_schemas: number
} & Record<MetricColumn, number | string>

// NUMERIC(38,0) arrives as a string; keep the digit string when it does not fit a safe integer
export function toMetricNumber(value: unknown): number | string {
  if (value === null || value === undefined || value === '') return 0
  const asString = String(value)
  if (!/^-?\d+$/.test(asString)) return Number(asString) || 0
  const asNumber = Number(asString)
  return Number.isSafeInteger(asNumber) ? asNumber : asString
}

function metricsFromRow(row: Row | undefined): Record<MetricColumn, number | string> {
  const out = {} as Record<MetricColumn, number | string>
  for (const column of METRIC_COLUMNS) out[column] = toMetricNumber(row?.[column])
  return out
}

function activeArchived(archived: unknown): [number, number] {
  return archived === null || archived === undefined ? [1, 0] : [0, 1]
}

function latestHistoryRow(table: string, idColumn: string, id: number, height: number, columns: string[]) {
  return knex(table)
    .select(columns)
    .where(idColumn, id)
    .where('height', '<=', height)
    .orderBy('height', 'desc')
    .orderBy('created_at', 'desc')
    .orderBy('id', 'desc')
    .first()
}

function latestHistoryRows(table: string, idColumn: string, height: number, columns: string[], alias: string) {
  return knex(table)
    .distinctOn(idColumn)
    .select(idColumn, ...columns)
    .where('height', '<=', height)
    .orderBy(idColumn, 'asc')
    .orderBy('height', 'desc')
    .orderBy('created_at', 'desc')
    .orderBy('id', 'desc')
    .as(alias)
}

async function globalMetrics(height: number, atHeight: boolean): Promise<SnapshotMetrics> {
  const ecosystems = atHeight
    ? latestHistoryRows('ecosystem_history', 'ecosystem_id', height, ['archived'], 'e')
    : knex('ecosystem').select('archived').as('e')
  const schemas = atHeight
    ? latestHistoryRows(
        'credential_schema_history',
        'credential_schema_id',
        height,
        ['archived', ...METRIC_COLUMNS],
        's'
      )
    : knex('credential_schemas')
        .select('archived', ...METRIC_COLUMNS)
        .as('s')

  const [ecosystemAgg, schemaAgg] = await Promise.all([
    knex
      .from(ecosystems)
      .select(
        knex.raw('COUNT(*) FILTER (WHERE archived IS NULL) as active_ecosystems'),
        knex.raw('COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_ecosystems')
      )
      .first() as Promise<Row | undefined>,
    knex
      .from(schemas)
      .select(
        knex.raw('COUNT(*) FILTER (WHERE archived IS NULL) as active_schemas'),
        knex.raw('COUNT(*) FILTER (WHERE archived IS NOT NULL) as archived_schemas'),
        ...METRIC_COLUMNS.map((column) => knex.raw('COALESCE(SUM(??), 0) as ??', [column, column]))
      )
      .first() as Promise<Row | undefined>,
  ])

  return {
    active_ecosystems: Number(ecosystemAgg?.active_ecosystems ?? 0),
    archived_ecosystems: Number(ecosystemAgg?.archived_ecosystems ?? 0),
    active_schemas: Number(schemaAgg?.active_schemas ?? 0),
    archived_schemas: Number(schemaAgg?.archived_schemas ?? 0),
    ...metricsFromRow(schemaAgg),
  }
}

async function ecosystemMetrics(id: number, height: number, atHeight: boolean): Promise<SnapshotMetrics | null> {
  const columns = ['archived', 'active_schemas', 'archived_schemas', ...METRIC_COLUMNS]
  const row: Row | undefined = atHeight
    ? await latestHistoryRow('ecosystem_history', 'ecosystem_id', id, height, columns)
    : await knex('ecosystem').select(columns).where('id', id).first()
  if (!row) return null
  const [active, archived] = activeArchived(row.archived)
  return {
    active_ecosystems: active,
    archived_ecosystems: archived,
    active_schemas: Number(row.active_schemas ?? 0),
    archived_schemas: Number(row.archived_schemas ?? 0),
    ...metricsFromRow(row),
  }
}

async function schemaArchivedState(schemaId: number, height: number, atHeight: boolean): Promise<[number, number]> {
  const row: Row | undefined = atHeight
    ? await latestHistoryRow('credential_schema_history', 'credential_schema_id', schemaId, height, ['archived'])
    : await knex('credential_schemas').select('archived').where('id', schemaId).first()
  return row ? activeArchived(row.archived) : [0, 0]
}

async function credentialSchemaMetrics(id: number, height: number, atHeight: boolean): Promise<SnapshotMetrics | null> {
  const columns = ['archived', ...METRIC_COLUMNS]
  const row: Row | undefined = atHeight
    ? await latestHistoryRow('credential_schema_history', 'credential_schema_id', id, height, columns)
    : await knex('credential_schemas').select(columns).where('id', id).first()
  if (!row) return null
  const [active, archived] = activeArchived(row.archived)
  return {
    active_ecosystems: 0,
    archived_ecosystems: 0,
    active_schemas: active,
    archived_schemas: archived,
    ...metricsFromRow(row),
  }
}

async function participantMetrics(id: number, height: number, atHeight: boolean): Promise<SnapshotMetrics | null> {
  const columns = ['schema_id', ...METRIC_COLUMNS]
  const row: Row | undefined = atHeight
    ? await latestHistoryRow('participant_history', 'participant_id', id, height, columns)
    : await knex('participants').select(columns).where('id', id).first()
  if (!row) return null
  const [active, archived] = await schemaArchivedState(Number(row.schema_id), height, atHeight)
  return {
    active_ecosystems: 0,
    archived_ecosystems: 0,
    active_schemas: active,
    archived_schemas: archived,
    ...metricsFromRow(row),
  }
}

export async function computeSnapshotMetrics(
  entityType: EntityType,
  entityId: number | null,
  height: number,
  atHeight: boolean
): Promise<SnapshotMetrics | null> {
  if (entityType === 'GLOBAL') return globalMetrics(height, atHeight)
  if (entityId === null) return null
  if (entityType === 'ECOSYSTEM') return ecosystemMetrics(entityId, height, atHeight)
  if (entityType === 'CREDENTIAL_SCHEMA') return credentialSchemaMetrics(entityId, height, atHeight)
  return participantMetrics(entityId, height, atHeight)
}

export async function getBlockTimeAtHeight(height: number): Promise<string | null> {
  const row = await knex('block').select('time').where('height', '<=', height).orderBy('height', 'desc').first()
  if (!row?.time) return null
  const time = new Date(row.time)
  return Number.isNaN(time.getTime()) ? null : time.toISOString()
}
