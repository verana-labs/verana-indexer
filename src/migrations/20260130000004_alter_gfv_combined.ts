import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.alterTable("governance_framework_version", (table) => {
    table.timestamp("active_since").nullable().alter();
  });

  await knex.schema.alterTable("governance_framework_version_history", (table) => {
    table.timestamp("active_since").nullable().alter();
  });

  await knex.schema.alterTable("governance_framework_document", (table) => {
    try {
      table.dropUnique(["gfv_id", "url"], "gfd_gfvid_url_unique");
    } catch (_) {}
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.alterTable("governance_framework_version", (table) => {
    table.timestamp("active_since").notNullable().alter();
  });

  await knex.schema.alterTable("governance_framework_version_history", (table) => {
    table.timestamp("active_since").notNullable().alter();
  });

  await knex.schema.alterTable("governance_framework_document", (table) => {
    table.unique(["gfv_id", "url"], "gfd_gfvid_url_unique");
  });
}
