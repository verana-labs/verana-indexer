import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("credential_schemas", (table) => {
    table.increments("id").primary();
    table.integer("tr_id").notNullable();
    table.jsonb("json_schema").notNullable();
    table.specificType("deposit", "NUMERIC(38,0)").notNullable();
    table.boolean("is_active").notNullable().defaultTo(false);

    table.integer("issuer_grantor_validation_validity_period").notNullable();
    table.integer("verifier_grantor_validation_validity_period").notNullable();
    table.integer("issuer_validation_validity_period").notNullable();
    table.integer("verifier_validation_validity_period").notNullable();
    table.integer("holder_validation_validity_period").notNullable();

    table.string("issuer_perm_management_mode").notNullable();
    table.string("verifier_perm_management_mode").notNullable();

    table.timestamp("archived").nullable();
    table.timestamp("created").defaultTo(knex.fn.now());
    table.timestamp("modified").defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("credential_schemas");
}
