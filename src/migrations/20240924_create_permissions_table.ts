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
      table.timestamp("created").defaultTo(knex.fn.now());
      table.timestamp("modified").defaultTo(knex.fn.now());
      table.timestamp("adjusted").nullable();
      table.string("adjusted_by", 255).nullable();
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
      table.index(["schema_id"]);
      table.index(["corporation"]);
      table.index(["type"]);
      table.index(["vp_state"]);
      table.index(["effective_until"]);
      table.index(["validator_perm_id"]);
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
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTableIfExists("permission_sessions");
  await knex.schema.dropTableIfExists("permissions");
  await knex.raw("DROP TYPE IF EXISTS validation_state");
  await knex.raw("DROP TYPE IF EXISTS permission_type");
}
