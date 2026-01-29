import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const hasExpireSoonColumn = await knex.schema.hasColumn("permissions", "expire_soon");
  if (!hasExpireSoonColumn) {
    await knex.schema.alterTable("permissions", (table) => {
      table.boolean("expire_soon").nullable();
    });
  }

  
  const hasHistoryExpireSoonColumn = await knex.schema.hasColumn("permission_history", "expire_soon");
  if (hasHistoryExpireSoonColumn) {
    await knex.schema.alterTable("permission_history", (table) => {
      table.dropColumn("expire_soon");
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  const hasExpireSoonColumn = await knex.schema.hasColumn("permissions", "expire_soon");
  if (hasExpireSoonColumn) {
    await knex.schema.alterTable("permissions", (table) => {
      table.dropColumn("expire_soon");
    });
  }

  const hasHistoryExpireSoonColumn = await knex.schema.hasColumn("permission_history", "expire_soon");
  if (hasHistoryExpireSoonColumn) {
    await knex.schema.alterTable("permission_history", (table) => {
      table.dropColumn("expire_soon");
    });
  }
}
