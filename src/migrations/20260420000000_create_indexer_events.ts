import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable("indexer_events");
  if (exists) return;

  await knex.schema.createTable("indexer_events", (table) => {
    table.bigIncrements("id").primary();
    table.text("event_type").notNullable();
    table.text("did").notNullable();
    table.bigInteger("block_height").notNullable();
    table.text("tx_hash").notNullable();
    table.integer("tx_index").notNullable().defaultTo(0);
    table.integer("message_index").notNullable().defaultTo(0);
    table.text("message_type").notNullable();
    table.text("module").notNullable();
    table.text("entity_type").nullable();
    table.text("entity_id").nullable();
    table.timestamp("timestamp").notNullable();
    table.jsonb("payload").notNullable().defaultTo("{}");
    table.timestamp("created_at").notNullable().defaultTo(knex.fn.now());

    table.unique(["did", "tx_hash", "message_index", "event_type"], "idx_events_did_tx_msg_type_unique");
    table.index(["did", "block_height", "tx_index", "message_index", "id"], "idx_events_did_replay_order");
    table.index(["block_height", "tx_index", "message_index", "id"], "idx_events_replay_order");
    table.index(["event_type"], "idx_events_event_type");
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("indexer_events");
}
