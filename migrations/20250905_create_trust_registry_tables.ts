import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("trust_registry", (table) => {
    table.bigIncrements("id").primary();
    table.string("did").notNullable();
    table.string("controller").notNullable();
    table.timestamp("created").notNullable();
    table.timestamp("modified").notNullable();
    table.timestamp("archived").nullable();
    table.decimal("deposit", 30, 0).notNullable();
    table.string("aka").nullable();
    table.string("language", 2).notNullable();
    table.integer("active_version").nullable();
    table.bigInteger("height").notNullable().unique();
  });

  await knex.schema.createTable("governance_framework_version", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("tr_id").notNullable();
    table.timestamp("created").notNullable();
    table.integer("version").notNullable();
    table.timestamp("active_since").notNullable();

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

    table.unique(["gfv_id", "url"], "gfd_gfvid_url_unique");
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("governance_framework_document");
  await knex.schema.dropTableIfExists("governance_framework_version");
  await knex.schema.dropTableIfExists("trust_registry");
}
