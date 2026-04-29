import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("credential_schemas", (table) => {
    table.increments("id").primary();
    table.integer("tr_id").notNullable();
    table.text("json_schema").notNullable();
    table.boolean("is_active").notNullable().defaultTo(false);

    table.integer("issuer_grantor_validation_validity_period").notNullable();
    table.integer("verifier_grantor_validation_validity_period").notNullable();
    table.integer("issuer_validation_validity_period").notNullable();
    table.integer("verifier_validation_validity_period").notNullable();
    table.integer("holder_validation_validity_period").notNullable();

    table.string("issuer_onboarding_mode").notNullable();
    table.string("verifier_onboarding_mode").notNullable();
    table.string("holder_onboarding_mode").nullable();

    table.string("pricing_asset_type").nullable();
    table.string("pricing_asset").nullable();
    table.string("digest_algorithm").nullable();

    table.string("title").nullable();
    table.text("description").nullable();

    table.bigInteger("participants").notNullable().defaultTo(0);
    table.bigInteger("participants_ecosystem").notNullable().defaultTo(0);
    table.bigInteger("participants_issuer_grantor").notNullable().defaultTo(0);
    table.bigInteger("participants_issuer").notNullable().defaultTo(0);
    table.bigInteger("participants_verifier_grantor").notNullable().defaultTo(0);
    table.bigInteger("participants_verifier").notNullable().defaultTo(0);
    table.bigInteger("participants_holder").notNullable().defaultTo(0);

    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);

    table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);

    table.bigInteger("network_slash_events").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);

    table.timestamp("archived").nullable();
    table.timestamp("created").defaultTo(knex.fn.now());
    table.timestamp("modified").defaultTo(knex.fn.now());

    table.index(["tr_id"]);
    table.index(["archived"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("credential_schemas");
}
