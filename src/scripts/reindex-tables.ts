import type { Knex } from 'knex'

// A reindex keeps these tables: stored blocks take days to crawl again, and the rest is bookkeeping.
const KEPT_TABLES = [
  'block',
  'block_signature',
  'block_checkpoint',
  'checkpoint',
  'knex_migrations',
  'knex_migrations_lock',
]
const KEPT_TABLE_PATTERNS = [/^block_partition_/]

export function isKeptTable(tableName: string): boolean {
  return KEPT_TABLES.includes(tableName) || KEPT_TABLE_PATTERNS.some((pattern) => pattern.test(tableName))
}

export async function resetDerivedTables(db: Knex): Promise<string[]> {
  const { rows } = await db.raw<{ rows: Array<{ tablename: string }> }>(
    `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`
  )
  const tables = rows.map((row) => row.tablename).filter((table) => !isKeptTable(table))
  if (tables.length === 0) return []

  // Without CASCADE the statement fails, and removes nothing, if a kept table references one of these.
  await db.raw(`TRUNCATE TABLE ${tables.map(() => '??').join(', ')} RESTART IDENTITY`, tables)
  return tables
}

// These jobs track a one-time data migration, not block progress.
const KEPT_CHECKPOINTS = ['job:create-event-attr-partition']

export async function resetCheckpoints(db: Knex): Promise<number> {
  const highest = Number((await db('block').max('height as max').first())?.max ?? 0)
  await db('block_checkpoint').whereNotIn('job_name', KEPT_CHECKPOINTS).update({ height: 0 })
  // The block crawler must not fetch the stored blocks again.
  await db('block_checkpoint').insert({ job_name: 'crawl:block', height: highest }).onConflict('job_name').merge()
  await db('checkpoint').update({ data: null })
  return highest
}

// One transaction: empty tables with old checkpoints would lose every block below those checkpoints.
export async function resetForReindex(db: Knex): Promise<{ tables: string[]; highestBlock: number }> {
  return db.transaction(async (trx) => {
    const tables = await resetDerivedTables(trx)
    const highestBlock = await resetCheckpoints(trx)
    return { tables, highestBlock }
  })
}
