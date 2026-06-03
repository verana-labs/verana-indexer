import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("corporation", (table) => {
    table.bigIncrements("id").primary();
    // did is the globally-unique identifier of the Corporation (MsgCreateCorporation.did).
    table.string("did").notNullable().unique();
    // corporation is the on-chain group_policy_address assigned by x/group.
    // It is not present in MsgCreateCorporation content (assigned by the chain),
    // so it is nullable and back-filled from events / MsgUpdateCorporation.
    table.string("corporation").nullable();
    table.string("creator").nullable();
    table.string("language", 8).nullable();
    table.text("group_metadata").nullable();
    table.text("group_policy_metadata").nullable();
    table.jsonb("decision_policy").nullable();
    table.text("doc_url").nullable();
    table.text("doc_digest_sri").nullable();
    table.timestamp("created").notNullable();
    table.timestamp("modified").notNullable();
    table.bigInteger("height").notNullable();

    table.index(["corporation"]);
    table.index(["modified"]);
  });

  await knex.schema.createTable("corporation_member", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("corporation_id").notNullable();
    table.string("address").notNullable();
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.text("metadata").nullable();
    table.timestamp("created").notNullable();

    table
      .foreign("corporation_id")
      .references("id")
      .inTable("corporation")
      .onDelete("CASCADE");

    table.unique(["corporation_id", "address"], "corporation_member_unique");
  });

  await knex.schema.createTable("corporation_history", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("corporation_id").notNullable();
    table.string("did").nullable();
    table.string("corporation").nullable();
    table.string("language", 8).nullable();
    table.string("event_type").notNullable();
    table.bigInteger("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").notNullable();

    table
      .foreign("corporation_id")
      .references("id")
      .inTable("corporation")
      .onDelete("CASCADE");

    table.index(["corporation_id"]);
    table.index(["height"]);
  });

  // Governance Framework owned by a Corporation (verana.gf.v1).
  // ecosystem_id = 0 targets the Corporation's own CGF; otherwise an Ecosystem.
  await knex.schema.createTable("co_governance_framework_version", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("corporation_id").notNullable();
    table.bigInteger("ecosystem_id").notNullable().defaultTo(0);
    table.integer("version").notNullable();
    table.timestamp("created").notNullable();
    table.timestamp("active_since").nullable();

    table
      .foreign("corporation_id")
      .references("id")
      .inTable("corporation")
      .onDelete("CASCADE");

    table.unique(
      ["corporation_id", "ecosystem_id", "version"],
      "co_gfv_corp_ecosystem_version_unique"
    );
  });

  await knex.schema.createTable("co_governance_framework_document", (table) => {
    table.bigIncrements("id").primary();
    table.bigInteger("gfv_id").notNullable();
    table.string("language", 8).notNullable();
    table.text("url").notNullable();
    table.text("digest_sri").notNullable();
    table.timestamp("created").notNullable();

    table
      .foreign("gfv_id")
      .references("id")
      .inTable("co_governance_framework_version")
      .onDelete("CASCADE");
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("co_governance_framework_document");
  await knex.schema.dropTableIfExists("co_governance_framework_version");
  await knex.schema.dropTableIfExists("corporation_history");
  await knex.schema.dropTableIfExists("corporation_member");
  await knex.schema.dropTableIfExists("corporation");
}
