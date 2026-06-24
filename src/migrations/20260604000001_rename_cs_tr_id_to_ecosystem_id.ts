import { Knex } from 'knex'

// dev.13 renamed the credential schema's ecosystem reference `tr_id` -> `ecosystem_id`.
// Additive, idempotent ALTER ... RENAME: metadata-only (no table rewrite, indexes
// auto-follow). Guarded so it is a no-op on fresh installs where the create-table
// migration already produces `ecosystem_id`, and renames on databases still on `tr_id`.
//
// Scope: credential schema tables only. The `tr_id` columns in trust_registry_history
// / governance_framework_* belong to the trust_registry -> ecosystem rename (later PR).

const COLUMN_RENAMES: Array<{ table: string; from: string; to: string }> = [
  { table: 'credential_schemas', from: 'tr_id', to: 'ecosystem_id' },
  { table: 'credential_schema_history', from: 'tr_id', to: 'ecosystem_id' },
]

const INDEX_RENAMES: Array<{ from: string; to: string }> = [
  { from: 'credential_schemas_tr_id_index', to: 'credential_schemas_ecosystem_id_index' },
  { from: 'credential_schema_history_tr_id_index', to: 'credential_schema_history_ecosystem_id_index' },
  { from: 'idx_cs_tr_archived_modified_id', to: 'idx_cs_eco_archived_modified_id' },
  {
    from: 'idx_credential_schema_history_tr_schema_height_created_id_desc',
    to: 'idx_credential_schema_history_eco_schema_height_created_id_desc',
  },
]

async function renameColumns(knex: Knex, renames: Array<{ table: string; from: string; to: string }>): Promise<void> {
  for (const { table, from, to } of renames) {
    if (!(await knex.schema.hasTable(table))) continue
    const hasFrom = await knex.schema.hasColumn(table, from)
    const hasTo = await knex.schema.hasColumn(table, to)
    if (hasFrom && !hasTo) {
      await knex.schema.alterTable(table, (t) => t.renameColumn(from, to))
    }
  }
}

async function renameIndexes(knex: Knex, renames: Array<{ from: string; to: string }>): Promise<void> {
  for (const { from, to } of renames) {
    await knex.raw('ALTER INDEX IF EXISTS ?? RENAME TO ??', [from, to])
  }
}

export async function up(knex: Knex): Promise<void> {
  await knex.raw("SET lock_timeout = '5s'")
  await renameColumns(knex, COLUMN_RENAMES)
  await renameIndexes(knex, INDEX_RENAMES)
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw("SET lock_timeout = '5s'")
  await renameIndexes(
    knex,
    INDEX_RENAMES.map(({ from, to }) => ({ from: to, to: from }))
  )
  await renameColumns(
    knex,
    COLUMN_RENAMES.map(({ table, from, to }) => ({ table, from: to, to: from }))
  )
}
