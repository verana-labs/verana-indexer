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
    table.text("json_schema").notNullable();

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

    table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
    table.bigInteger("network_slash_events").notNullable().defaultTo(0);

    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);

    table.timestamp("archived").nullable();
    table.boolean("is_active").notNullable().defaultTo(false);
    table.timestamp("created").notNullable();
    table.timestamp("modified").notNullable();

    table.jsonb("changes").nullable();
    table.string("action").notNullable();
    table.bigInteger("height").notNullable().defaultTo(0);
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.index(["credential_schema_id"]);
    table.index(["height"]);
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
