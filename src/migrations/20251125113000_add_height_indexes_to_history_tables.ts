import { Knex } from "knex";

const HISTORY_TABLES = [
  "did_history",
  "trust_registry_history",
  "governance_framework_version_history",
  "governance_framework_document_history",
  "credential_schema_history",
  "permission_history",
  "permission_session_history",
  "trust_deposit_history",
  "module_params_history",
];

const INDEX_SUFFIX = "_height_idx";

export async function up(knex: Knex): Promise<void> {
  for (const table of HISTORY_TABLES) {
    const indexName = `${table}${INDEX_SUFFIX}`;
    const exists = await knex.schema.hasTable(table);
    if (!exists) continue;

    const hasIndex = await knex
      .raw(
        `SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?`,
        [indexName]
      )
      .then((res) => res.rowCount > 0);

    if (!hasIndex) {
      await knex.schema.alterTable(table, (tb) => {
        tb.index(["height"], indexName);
      });
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  for (const table of HISTORY_TABLES) {
    const indexName = `${table}${INDEX_SUFFIX}`;
    const exists = await knex.schema.hasTable(table);
    if (!exists) continue;

    await knex.schema.alterTable(table, (tb) => {
      tb.dropIndex(["height"], indexName);
    });
  }
}

