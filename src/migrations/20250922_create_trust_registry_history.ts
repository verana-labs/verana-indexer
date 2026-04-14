import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("trust_registry_history", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("tr_id").notNullable();
    table.string("did").notNullable();
    table.string("corporation").notNullable();
    table.timestamp("created").notNullable();
    table.timestamp("modified").notNullable();
    table.timestamp("archived").nullable();
    table.string("aka").nullable();
    table.string("language", 2).notNullable();
    table.integer("active_version").nullable();
    table.bigInteger("participants").notNullable().defaultTo(0);
    table.bigInteger("active_schemas").notNullable().defaultTo(0);
    table.bigInteger("archived_schemas").notNullable().defaultTo(0);
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("issued", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verified", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
    table
      .specificType("ecosystem_slashed_amount", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table
      .specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table.bigInteger("network_slash_events").notNullable().defaultTo(0);
    table
      .specificType("network_slashed_amount", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table
      .specificType("network_slashed_amount_repaid", "NUMERIC(38,0)")
      .notNullable()
      .defaultTo(0);
    table.text("event_type").notNullable();
    table.bigInteger("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());
  });

  await knex.schema.createTable("governance_framework_version_history", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("tr_id").notNullable();
    table.timestamp("created").notNullable();
    table.integer("version").notNullable();
    table.timestamp("active_since").nullable();
    table.text("event_type").notNullable();
    table.bigInteger("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());
  });

  await knex.schema.createTable("governance_framework_document_history", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("gfv_id").notNullable();
    table.bigInteger("tr_id").notNullable();
    table.timestamp("created").notNullable();
    table.string("language", 2).notNullable();
    table.text("url").notNullable();
    table.text("digest_sri").notNullable();
    table.text("event_type").notNullable();
    table.bigInteger("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("governance_framework_document_history");
  await knex.schema.dropTableIfExists("governance_framework_version_history");
  await knex.schema.dropTableIfExists("trust_registry_history");
}
