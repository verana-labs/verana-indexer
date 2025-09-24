import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("credential_schemas", (table) => {
    table.increments("id").primary();
    table.string("tr_id").notNullable();
    table.jsonb("json_schema").notNullable();
    table.string("deposit").notNullable();
table.boolean("isActive").notNullable().defaultTo(false);

    table.integer("issuer_grantor_validation_validity_period").notNullable();
    table.integer("verifier_grantor_validation_validity_period").notNullable();
    table.integer("issuer_validation_validity_period").notNullable();
    table.integer("verifier_validation_validity_period").notNullable();
    table.integer("holder_validation_validity_period").notNullable();

    table.integer("issuer_perm_management_mode").notNullable();
    table.integer("verifier_perm_management_mode").notNullable();

    table.timestamp("archived").nullable();
    table.timestamp("created").defaultTo(knex.fn.now());
    table.timestamp("modified").defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("credential_schemas");
}
