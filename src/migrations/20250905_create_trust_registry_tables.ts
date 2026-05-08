import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("trust_registry", (table) => {
    table.bigIncrements("id").primary();
    table.string("did").notNullable();
    table.string("corporation").notNullable();
    table.timestamp("created").notNullable();
    table.timestamp("modified").notNullable();
    table.timestamp("archived").nullable();
    table.string("aka").nullable();
    table.string("language", 2).notNullable();
    table.integer("active_version").nullable();
    table.bigInteger("height").notNullable().unique();

    table.bigInteger("participants").notNullable().defaultTo(0);
    table.bigInteger("participants_ecosystem").notNullable().defaultTo(0);
    table.bigInteger("participants_issuer_grantor").notNullable().defaultTo(0);
    table.bigInteger("participants_issuer").notNullable().defaultTo(0);
    table.bigInteger("participants_verifier_grantor").notNullable().defaultTo(0);
    table.bigInteger("participants_verifier").notNullable().defaultTo(0);
    table.bigInteger("participants_holder").notNullable().defaultTo(0);

    table.bigInteger("active_schemas").notNullable().defaultTo(0);
    table.bigInteger("archived_schemas").notNullable().defaultTo(0);
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("network_slash_events").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);

    table.index(["corporation"], "idx_tr_corporation_archived_modified_id");
    table.index(["archived"]);
    table.index(["modified"]);
  });

  await knex.schema.createTable("governance_framework_version", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("tr_id").notNullable();
    table.timestamp("created").notNullable();
    table.integer("version").notNullable();
    table.timestamp("active_since").nullable();

    table
      .foreign("tr_id")
      .references("id")
      .inTable("trust_registry")
      .onDelete("CASCADE");

    table.unique(["tr_id", "version"], "gfv_trid_version_unique");
  });

  await knex.schema.createTable("governance_framework_document", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("gfv_id").notNullable();
    table.timestamp("created").notNullable();
    table.string("language", 2).notNullable();
    table.text("url").notNullable();
    table.text("digest_sri").notNullable();

    table
      .foreign("gfv_id")
      .references("id")
      .inTable("governance_framework_version")
      .onDelete("CASCADE");

  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("governance_framework_document");
  await knex.schema.dropTableIfExists("governance_framework_version");
  await knex.schema.dropTableIfExists("trust_registry");
}
