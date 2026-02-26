import { Knex } from "knex";

export const config = { transaction: false };

const INDEX_NAME = "idx_permissions_did_type_schema_modified";

export async function up(knex: Knex): Promise<void> {
  const client = (knex.client.config && (knex.client.config.client || "")).toString();

  const hasDid = await knex.schema.hasColumn("permissions", "did");
  const hasType = await knex.schema.hasColumn("permissions", "type");
  const hasSchemaId = await knex.schema.hasColumn("permissions", "schema_id");
  const hasModified = await knex.schema.hasColumn("permissions", "modified");

  if (!(hasDid && hasType && hasSchemaId && hasModified)) {
    return;
  }

  if (client.includes("pg")) {
    await knex.raw(`
      CREATE INDEX CONCURRENTLY IF NOT EXISTS ${INDEX_NAME}
      ON permissions (did, type, schema_id, modified DESC)
    `);
    return;
  }

  await knex.schema.table("permissions", (table) => {
    table.index(["did", "type", "schema_id", "modified"], INDEX_NAME);
  });
}

export async function down(knex: Knex): Promise<void> {
  const client = (knex.client.config && (knex.client.config.client || "")).toString();

  if (client.includes("pg")) {
    await knex.raw(`DROP INDEX IF EXISTS ${INDEX_NAME}`);
    return;
  }

  await knex.schema.table("permissions", (table) => {
    table.dropIndex(["did", "type", "schema_id", "modified"], INDEX_NAME);
  });
}
