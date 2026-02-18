import { Knex } from "knex";


export async function up(knex: Knex): Promise<void> {
  await knex.raw(`
    ALTER TABLE credential_schemas
    ALTER COLUMN json_schema TYPE text USING json_schema::text;
  `);
  await knex.raw(`
    ALTER TABLE credential_schema_history
    ALTER COLUMN json_schema TYPE text USING json_schema::text;
  `);
}

export async function down(knex: Knex): Promise<void> {
  await knex.raw(`
    ALTER TABLE credential_schemas
    ALTER COLUMN json_schema TYPE jsonb USING json_schema::jsonb;
  `);
  await knex.raw(`
    ALTER TABLE credential_schema_history
    ALTER COLUMN json_schema TYPE jsonb USING json_schema::jsonb;
  `);
}
