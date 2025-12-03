import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable("permission_history", (table) => {
    table.increments("id").primary();
    table.string("permission_id", 50).notNullable();
    table.integer("schema_id").notNullable();
    table.specificType("type", "permission_type").notNullable();
    table.string("did", 255).nullable();
    table.string("grantee", 255).notNullable();
    table.string("created_by", 255).notNullable();
    table.timestamp("created").nullable();
    table.timestamp("modified").nullable();
    table.timestamp("extended").nullable();
    table.string("extended_by", 255).nullable();
    table.timestamp("slashed").nullable();
    table.string("slashed_by", 255).nullable();
    table.timestamp("repaid").nullable();
    table.string("repaid_by", 255).nullable();
    table.timestamp("effective_from").nullable();
    table.timestamp("effective_until").nullable();
    table.timestamp("revoked").nullable();
    table.string("revoked_by", 255).nullable();
    table.string("country", 2).nullable();
    table.string("validation_fees", 50).defaultTo("0");
    table.string("issuance_fees", 50).defaultTo("0");
    table.string("verification_fees", 50).defaultTo("0");
    table.string("deposit", 50).defaultTo("0");
    table.string("slashed_deposit", 50).defaultTo("0");
    table.string("repaid_deposit", 50).defaultTo("0");
    table.string("validator_perm_id", 50).nullable();
    table.specificType("vp_state", "validation_state").nullable();
    table.timestamp("vp_last_state_change").nullable();
    table.string("vp_current_fees", 50).defaultTo("0");
    table.string("vp_current_deposit", 50).defaultTo("0");
    table.string("vp_summary_digest_sri", 512).nullable();
    table.timestamp("vp_exp").nullable();
    table.string("vp_validator_deposit", 50).defaultTo("0");
    table.timestamp("vp_term_requested").nullable();
    table.string("event_type").notNullable();
    table.integer("height").notNullable();
    table.jsonb("changes").nullable();
    table.timestamp("created_at").defaultTo(knex.fn.now());

    table.index(["permission_id"]);
    table.index(["height"]);
    table.index(["event_type"]);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("permission_history");
}

