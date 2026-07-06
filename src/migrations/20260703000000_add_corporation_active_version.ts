import type { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable('corporation', (table) => {
    table.integer('active_version').nullable()
  })

  // Backfill from the highest activated CGF version (ecosystem_id = 0) per corporation.
  await knex.raw(`
    UPDATE corporation c
    SET active_version = sub.max_version
    FROM (
      SELECT corporation_id, MAX(version) AS max_version
      FROM co_governance_framework_version
      WHERE ecosystem_id = 0 AND active_since IS NOT NULL
      GROUP BY corporation_id
    ) sub
    WHERE sub.corporation_id = c.id
  `)
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable('corporation', (table) => {
    table.dropColumn('active_version')
  })
}
