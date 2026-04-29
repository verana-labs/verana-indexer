import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  await knex.raw(`DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'permission_type') THEN
            CREATE TYPE permission_type AS ENUM (
                'ECOSYSTEM',
                'ISSUER_GRANTOR', 
                'VERIFIER_GRANTOR',
                'ISSUER',
                'VERIFIER',
                'HOLDER',
                'UNSPECIFIED'
            );
        END IF;
    END $$;`);

  await knex.raw(`DO $$ BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'validation_state') THEN
            CREATE TYPE validation_state AS ENUM (
                'VALIDATION_STATE_UNSPECIFIED',
                'PENDING',
                'VALIDATED',
                'TERMINATED'
            );
        END IF;
    END $$;`);

  const hasPermissions = await knex.schema.hasTable("permissions");
  if (!hasPermissions) {
    await knex.schema.createTable("permissions", (table) => {
      table.bigIncrements("id").primary();
      table.integer("schema_id").notNullable();
      table.specificType("type", "permission_type").notNullable();
      table.string("did", 255);
      table.string("corporation", 255).notNullable();
      table.text("vs_operator").nullable();
      table.timestamp("created").defaultTo(knex.fn.now());
      table.timestamp("modified").defaultTo(knex.fn.now());
      table.timestamp("adjusted").nullable();
      table.timestamp("slashed").nullable();
      table.timestamp("repaid").nullable();
      table.timestamp("effective_from").nullable();
      table.timestamp("effective_until").nullable();
      table.timestamp("revoked").nullable();
      table.specificType("validation_fees", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("issuance_fees", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("verification_fees", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("slashed_deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("repaid_deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.integer("validator_perm_id").nullable();
      table.specificType("vp_state", "validation_state").nullable();
      table.timestamp("vp_last_state_change").nullable();
      table.specificType("vp_current_fees", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("vp_current_deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.string("vp_summary_digest", 512).nullable();
      table.timestamp("vp_exp").nullable();
      table.specificType("vp_validator_deposit", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("issuance_fee_discount", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("verification_fee_discount", "NUMERIC(38,0)").notNullable().defaultTo(0);

      table.boolean("vs_operator_authz_enabled").notNullable().defaultTo(false);
      table.jsonb("vs_operator_authz_spend_limit").nullable();
      table.boolean("vs_operator_authz_with_feegrant").notNullable().defaultTo(false);
      table.jsonb("vs_operator_authz_fee_spend_limit").nullable();
      table.text("vs_operator_authz_spend_period").nullable();

      table.bigInteger("participants").notNullable().defaultTo(0);
      table.bigInteger("participants_ecosystem").notNullable().defaultTo(0);
      table.bigInteger("participants_issuer_grantor").notNullable().defaultTo(0);
      table.bigInteger("participants_issuer").notNullable().defaultTo(0);
      table.bigInteger("participants_verifier_grantor").notNullable().defaultTo(0);
      table.bigInteger("participants_verifier").notNullable().defaultTo(0);
      table.bigInteger("participants_holder").notNullable().defaultTo(0);
      table.specificType("weight", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.bigInteger("issued").notNullable().defaultTo(0);
      table.bigInteger("verified").notNullable().defaultTo(0);
      table.bigInteger("ecosystem_slash_events").notNullable().defaultTo(0);
      table.specificType("ecosystem_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("ecosystem_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.bigInteger("network_slash_events").notNullable().defaultTo(0);
      table.specificType("network_slashed_amount", "NUMERIC(38,0)").notNullable().defaultTo(0);
      table.specificType("network_slashed_amount_repaid", "NUMERIC(38,0)").notNullable().defaultTo(0);

      table.integer("last_valid_flip_version").notNullable().defaultTo(0);
      table.boolean("is_active_now").notNullable().defaultTo(false);

      table.boolean("expire_soon").nullable();

      table.index(["schema_id"]);
      table.index(["corporation"]);
      table.index(["corporation"], "idx_permissions_corporation");
      table.index(["type"]);
      table.index(["vp_state"]);
      table.index(["effective_until"]);
      table.index(["validator_perm_id"]);
      table.index(["validator_perm_id"], "idx_permissions_validator_perm_id");
    });
  }

  const hasSessions = await knex.schema.hasTable("permission_sessions");
  if (!hasSessions) {
    await knex.schema.createTable("permission_sessions", (table) => {
      table.string("id", 255).primary();
      table.string("corporation", 255).notNullable();
      table.integer("agent_perm_id").notNullable();
      table.integer("wallet_agent_perm_id").notNullable();
      table.jsonb("session_records").notNullable().defaultTo("[]");
      table.text("vs_operator").nullable();
      table.timestamp("created").defaultTo(knex.fn.now());
      table.timestamp("modified").defaultTo(knex.fn.now());
      table.index(["corporation"]);
      table.index(["agent_perm_id"]);
      table.index(["created"]);
    });
  }

  if (!(await knex.schema.hasTable("permission_scheduled_flips"))) {
    await knex.schema.createTable("permission_scheduled_flips", (table) => {
      table.bigInteger("perm_id")
        .notNullable()
        .references("id")
        .inTable("permissions")
        .onDelete("CASCADE");

      table.timestamp("flip_at_time", { useTz: true }).notNullable();
      table.specificType("flip_kind", "SMALLINT").notNullable();
      table.specificType("status", "SMALLINT").notNullable().defaultTo(0);
      table.specificType("version", "INTEGER").notNullable();
      table.bigInteger("applied_height").nullable();
      table.timestamp("applied_time", { useTz: true }).nullable();
      table.timestamp("created_at", { useTz: true }).notNullable().defaultTo(knex.fn.now());

      table.primary(["perm_id", "version", "flip_at_time", "flip_kind"]);
      table.index(["flip_at_time", "perm_id"], "psf_pending_idx");
      table.index(["status", "flip_at_time", "perm_id"], "psf_pending_status_time_idx");
    });
  }

  if (!(await knex.schema.hasTable("entity_participant_changes"))) {
    await knex.schema.createTable("entity_participant_changes", (table) => {
      table.bigInteger("height").notNullable();
      table.timestamp("block_time", { useTz: true }).notNullable();
      table.specificType("entity_kind", "SMALLINT").notNullable();
      table.bigInteger("entity_id").nullable();
      table.specificType("type", "SMALLINT").notNullable();
      table.bigInteger("value").notNullable();

      table.primary(["entity_kind", "entity_id", "type", "height"]);
      table.index(["entity_kind", "entity_id", "type", "height"], "epc_lookup_idx");
    });
  }
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("permission_sessions");
  await knex.schema.dropTableIfExists("permissions");
  await knex.raw("DROP TYPE IF EXISTS validation_state");
  await knex.raw("DROP TYPE IF EXISTS permission_type");
}
