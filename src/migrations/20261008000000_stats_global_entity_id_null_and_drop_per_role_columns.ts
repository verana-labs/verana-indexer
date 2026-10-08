import type { Knex } from 'knex'

const PER_ROLE_METRICS = [
  'participants_ecosystem',
  'participants_issuer_grantor',
  'participants_issuer',
  'participants_verifier_grantor',
  'participants_verifier',
  'participants_holder',
]

const PER_ROLE_COLUMNS = PER_ROLE_METRICS.flatMap((metric) => [`cumulative_${metric}`, `delta_${metric}`])

export async function up(knex: Knex): Promise<void> {
  await knex.raw("UPDATE stats SET entity_id = NULL WHERE entity_type = 'GLOBAL' AND entity_id = 0")
  await knex.raw('ALTER TABLE stats DROP CONSTRAINT IF EXISTS stats_unique_key')
  await knex.raw(
    'CREATE UNIQUE INDEX IF NOT EXISTS stats_unique_key ON stats (granularity, "timestamp", entity_type, entity_id) NULLS NOT DISTINCT'
  )

  for (const column of PER_ROLE_COLUMNS) {
    await knex.raw('ALTER TABLE stats DROP COLUMN IF EXISTS ??', [column])
  }
}

export async function down(knex: Knex): Promise<void> {
  for (const column of PER_ROLE_COLUMNS) {
    await knex.raw('ALTER TABLE stats ADD COLUMN IF NOT EXISTS ?? BIGINT NOT NULL DEFAULT 0', [column])
  }

  await knex.raw('DROP INDEX IF EXISTS stats_unique_key')
  await knex.raw("UPDATE stats SET entity_id = 0 WHERE entity_type = 'GLOBAL' AND entity_id IS NULL")
  await knex.raw(
    'ALTER TABLE stats ADD CONSTRAINT stats_unique_key UNIQUE (granularity, "timestamp", entity_type, entity_id)'
  )
}
