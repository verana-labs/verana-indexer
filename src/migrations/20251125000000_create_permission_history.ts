import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("permission_history", (table) => {
    table.increments("id").primary();
    table.integer("permission_id").notNullable();
    table.integer("schema_id").notNullable();
    table.specificType("type", "permission_type").notNullable();
    table.string("did", 255).nullable();
    table.string("corporation", 255).notNullable();
    table.text("vs_operator").nullable();
    table.timestamp("created").nullable();
    table.timestamp("modified").nullable();
    table.timestamp("adjusted").nullable();
    table.timestamp("slashed").nullable();
    table.timestamp("repaid").nullable();
    table.timestamp("effective_from").nullable();
    table.timestamp("effective_until").nullable();
    table.timestamp("revoked").nullable();
    table.specificType("validation_fees", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("issuance_fees", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("verification_fees", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("deposit", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("slashed_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("repaid_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.integer("validator_perm_id").nullable();
    table.specificType("vp_state", "validation_state").nullable();
    table.timestamp("vp_last_state_change").nullable();
    table.specificType("vp_current_fees", "NUMERIC(38,0)").defaultTo(0);
    table.specificType("vp_current_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.string("vp_summary_digest", 512).nullable();
    table.timestamp("vp_exp").nullable();
    table.specificType("vp_validator_deposit", "NUMERIC(38,0)").defaultTo(0);
    table.bigInteger("participants").defaultTo(0).notNullable();
    table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("ecosystem_slash_events").defaultTo(0).notNullable();
    table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("network_slash_events").defaultTo(0).notNullable();
    table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.bigInteger("issued").defaultTo(0).notNullable();
    table.bigInteger("verified").defaultTo(0).notNullable();
    table.specificType("issuance_fee_discount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.specificType("verification_fee_discount", "NUMERIC(38,0)").notNullable().defaultTo(0);
    table.boolean("expire_soon").nullable();

    table.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
    table.jsonb("vs_operator_authz_spend_limit").nullable();
    table.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
    table.jsonb("vs_operator_authz_fee_spend_limit").nullable();
    table.text("vs_operator_authz_spend_period").nullable();

    table.string("event_type").notNullable();
    table.integer("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.index(["permission_id"]);
    table.index(["height"]);
    table.index(["event_type"]);

    table.index(["schema_id", "height"], "idx_permission_history_schema_height_desc");
    table.index(["corporation", "height"], "idx_permission_history_corporation_height_desc");
    table.index(["permission_id", "height"], "idx_permission_history_permission_height_desc");
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("permission_history");
}

