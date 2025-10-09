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
      table.string("schema_id", 50).notNullable();
      table.specificType("type", "permission_type").notNullable();
      table.string("did", 255);
      table.string("grantee", 255).notNullable();
      table.string("created_by", 255).notNullable();
      table.timestamp("created").defaultTo(knex.fn.now());
      table.timestamp("modified").defaultTo(knex.fn.now());
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
      table.index(["schema_id"]);
      table.index(["grantee"]);
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
      table.string("controller", 255).notNullable();
      table.string("agent_perm_id", 50).notNullable();
      table.string("wallet_agent_perm_id", 50).notNullable();
      table.jsonb("authz").notNullable().defaultTo("[]");
      table.timestamp("created").defaultTo(knex.fn.now());
      table.timestamp("modified").defaultTo(knex.fn.now());
      table.index(["controller"]);
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
