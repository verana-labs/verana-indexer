import knex from '../../../src/common/utils/db_connection'
import { isKeptTable, resetDerivedTables, resetForReindex } from '../../../src/scripts/reindex-tables'

// A reindex keeps the stored blocks and rebuilds every other table from them, so it must not change the schema.
const CORPORATION = 4843
const now = new Date('2026-11-19T18:22:31.000Z')

const corporationRow = { id: CORPORATION, did: 'did:example:reindex', created: now, modified: now, height: 20 }
const historyRow = {
  corporation_id: CORPORATION,
  did: corporationRow.did,
  event_type: 'Create',
  height: 20,
  created_at: now,
}

const count = async (table: string) => Number((await knex(table).count('* as n').first())?.n)
const publicTables = () => knex('pg_tables').where({ schemaname: 'public' }).orderBy('tablename').pluck('tablename')

describe('reindex table reset', () => {
  afterAll(async () => {
    await knex('block_checkpoint').whereIn('job_name', ['reindex:spec', 'reindex:spec-job']).del()
    await knex('checkpoint').where({ job_name: 'reindex:spec' }).del()
    await knex.destroy()
  })

  it('keeps the block data, the checkpoints and the migration records', () => {
    const kept = [
      'block',
      'block_partition_0_100000000',
      'block_signature',
      'block_checkpoint',
      'checkpoint',
      'knex_migrations',
      'knex_migrations_lock',
    ]

    expect(kept.filter((table) => !isKeptTable(table))).toEqual([])
  })

  it('resets the tables that the indexer rebuilds from the blocks', () => {
    const rebuilt = ['transaction', 'transaction_partition_0_100000000', 'event', 'ecosystem', 'corporation', 'stats']

    expect(rebuilt.filter((table) => isKeptTable(table))).toEqual([])
  })

  it('empties the rebuilt tables, restarts their ids and leaves every table in place', async () => {
    const tablesBefore = await publicTables()
    const migrationsBefore = await count('knex_migrations')
    await knex('block_checkpoint').insert({ job_name: 'reindex:spec', height: 7 }).onConflict('job_name').merge()
    await knex('corporation').insert(corporationRow).onConflict('id').merge()
    await knex('corporation_history').insert(historyRow)

    const reset = await resetDerivedTables(knex)

    expect(reset).toEqual(tablesBefore.filter((table) => !isKeptTable(table)))
    expect(await count('corporation')).toBe(0)
    expect(await count('corporation_history')).toBe(0)
    expect(await knex('block_checkpoint').where({ job_name: 'reindex:spec' }).pluck('height')).toEqual([7])
    expect(await count('knex_migrations')).toBe(migrationsBefore)
    expect(await publicTables()).toEqual(tablesBefore)

    await knex('corporation').insert(corporationRow)
    const [first] = await knex('corporation_history').insert(historyRow).returning('id')
    expect(Number(first.id)).toBe(1)
    await knex('corporation').where({ id: CORPORATION }).del()
  })

  it('sends every job back to the start and the block crawler to the highest stored block', async () => {
    const highest = Number((await knex('block').max('height as max').first())?.max ?? 0)
    const partitionJob = { job_name: 'job:create-event-attr-partition', height: 13000000 }
    await knex('block_checkpoint').insert(partitionJob).onConflict('job_name').merge()
    await knex('block_checkpoint').insert({ job_name: 'reindex:spec-job', height: 50 }).onConflict('job_name').merge()
    await knex('block_checkpoint').insert({ job_name: 'crawl:block', height: 3 }).onConflict('job_name').merge()
    await knex('checkpoint')
      .insert({ job_name: 'reindex:spec', data: { cursor: 9 } })
      .onConflict('job_name')
      .merge()

    const result = await resetForReindex(knex)

    expect(result.highestBlock).toBe(highest)
    const heightOf = (job: string) => knex('block_checkpoint').where({ job_name: job }).pluck('height')
    expect(await heightOf('reindex:spec-job')).toEqual([0])
    expect(await heightOf('crawl:block')).toEqual([highest])
    expect(await heightOf('job:create-event-attr-partition')).toEqual([partitionJob.height])
    expect(await knex('checkpoint').where({ job_name: 'reindex:spec' }).pluck('data')).toEqual([null])
  })

  it('removes nothing when the checkpoint reset fails', async () => {
    await knex('corporation').insert(corporationRow).onConflict('id').merge()
    await knex.raw(
      `CREATE OR REPLACE FUNCTION reindex_spec_fail() RETURNS trigger AS $$ BEGIN RAISE EXCEPTION 'reindex spec failure'; END $$ LANGUAGE plpgsql`
    )
    await knex.raw(
      'CREATE TRIGGER reindex_spec_fail BEFORE UPDATE ON block_checkpoint FOR EACH STATEMENT EXECUTE FUNCTION reindex_spec_fail()'
    )

    try {
      await expect(resetForReindex(knex)).rejects.toThrow('reindex spec failure')
      expect(await count('corporation')).toBe(1)
    } finally {
      await knex.raw('DROP TRIGGER reindex_spec_fail ON block_checkpoint')
      await knex.raw('DROP FUNCTION reindex_spec_fail()')
      await knex('corporation').where({ id: CORPORATION }).del()
    }
  })
})
