import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("permission_session_history", (table) => {
    table.increments("id").primary();
    table.string("session_id", 255).notNullable();
    table.string("controller", 255).notNullable();
    table.string("agent_perm_id", 50).notNullable();
    table.string("wallet_agent_perm_id", 50).notNullable();
    table.jsonb("authz").notNullable().defaultTo("[]");
    table.timestamp("created").nullable();
    table.timestamp("modified").nullable();
    table.string("event_type").notNullable();
    table.integer("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.index(["session_id"]);
    table.index(["height"]);
    table.index(["event_type"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("permission_session_history");
}

