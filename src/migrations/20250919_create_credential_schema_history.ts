import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("credential_schema_history", (table) => {
    table.increments("id").primary();
    table
      .integer("credential_schema_id")
      .notNullable()
      .references("id")
      .inTable("credential_schemas")
      .onDelete("CASCADE");

    table.integer("tr_id").notNullable();
    table.jsonb("json_schema").notNullable();
    table.specificType("deposit", "NUMERIC(38,0)").notNullable();

    table.integer("issuer_grantor_validation_validity_period").notNullable();
    table.integer("verifier_grantor_validation_validity_period").notNullable();
    table.integer("issuer_validation_validity_period").notNullable();
    table.integer("verifier_validation_validity_period").notNullable();
    table.integer("holder_validation_validity_period").notNullable();

    table.string("issuer_perm_management_mode").notNullable();
    table.string("verifier_perm_management_mode").notNullable();

    table.timestamp("archived").nullable();
    table.boolean("is_active").notNullable().defaultTo(false);
    table.timestamp("created").notNullable();
    table.timestamp("modified").notNullable();

    table.jsonb("changes").nullable();
    table.string("action").notNullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());
  });

  await knex.raw(`
    SELECT setval(
      pg_get_serial_sequence('credential_schema_history', 'id'),
      COALESCE((SELECT MAX(id) FROM credential_schema_history), 0) + 1,
      false
    );
  `);
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("credential_schema_history");
}
