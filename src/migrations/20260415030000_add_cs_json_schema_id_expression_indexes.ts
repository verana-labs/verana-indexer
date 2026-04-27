import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  try {
    await knex.schema.raw(
      `CREATE INDEX IF NOT EXISTS credential_schemas_json_schema_dollar_id_idx
       ON credential_schemas ((json_schema::jsonb->>'$id'))
       WHERE archived IS NULL`
    );
  } catch {
    console.warn("Failed to create index credential_schemas_json_schema_dollar_id_idx");
  }
  try {
    await knex.schema.raw(
      `CREATE INDEX IF NOT EXISTS credential_schemas_json_schema_id_idx
       ON credential_schemas ((json_schema::jsonb->>'id'))
       WHERE archived IS NULL`
    );
  } catch {
    console.warn("Failed to create index credential_schemas_json_schema_id_idx");
  }
  try {
    await knex.schema.raw(
      `CREATE INDEX IF NOT EXISTS credential_schemas_json_schema_at_id_idx
       ON credential_schemas ((json_schema::jsonb->>'@id'))
       WHERE archived IS NULL`
    );
  } catch {
    console.warn("Failed to create index credential_schemas_json_schema_at_id_idx");
  }
}

export async function down(knex: Knex): Promise<void> {
  try {
    await knex.schema.raw(`DROP INDEX IF EXISTS credential_schemas_json_schema_dollar_id_idx`);
  } catch {
    console.warn("Failed to drop index credential_schemas_json_schema_dollar_id_idx");
  }
  try {
    await knex.schema.raw(`DROP INDEX IF EXISTS credential_schemas_json_schema_id_idx`);
  } catch {
    console.warn("Failed to drop index credential_schemas_json_schema_id_idx");
  }
  try {
    await knex.schema.raw(`DROP INDEX IF EXISTS credential_schemas_json_schema_at_id_idx`);
  } catch {
    console.warn("Failed to drop index credential_schemas_json_schema_at_id_idx");
  }
}

