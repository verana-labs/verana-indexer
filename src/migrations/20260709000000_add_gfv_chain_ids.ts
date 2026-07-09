import type { Knex } from 'knex'

// Chain-assigned global ids (x/gf shares one id sequence across EGF and CGF), captured from
// add_gf_document tx events. Null for rows indexed before this migration until a reindex.
export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable('co_governance_framework_version', (table) => {
    table.bigInteger('gfv_id').nullable()
  })
  await knex.schema.alterTable('governance_framework_version', (table) => {
    table.bigInteger('gfv_id').nullable()
  })
  await knex.schema.alterTable('co_governance_framework_document', (table) => {
    table.bigInteger('gfd_id').nullable()
  })
  await knex.schema.alterTable('governance_framework_document', (table) => {
    table.bigInteger('gfd_id').nullable()
  })
  await knex.raw(
    'CREATE UNIQUE INDEX co_gfv_chain_id_unique ON co_governance_framework_version (gfv_id) WHERE gfv_id IS NOT NULL'
  )
  await knex.raw(
    'CREATE UNIQUE INDEX gfv_chain_id_unique ON governance_framework_version (gfv_id) WHERE gfv_id IS NOT NULL'
  )
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw('DROP INDEX IF EXISTS co_gfv_chain_id_unique')
  await knex.raw('DROP INDEX IF EXISTS gfv_chain_id_unique')
  await knex.schema.alterTable('co_governance_framework_version', (table) => {
    table.dropColumn('gfv_id')
  })
  await knex.schema.alterTable('governance_framework_version', (table) => {
    table.dropColumn('gfv_id')
  })
  await knex.schema.alterTable('co_governance_framework_document', (table) => {
    table.dropColumn('gfd_id')
  })
  await knex.schema.alterTable('governance_framework_document', (table) => {
    table.dropColumn('gfd_id')
  })
}
