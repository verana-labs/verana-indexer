import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const hasTable = await knex.schema.hasTable("trust_results");
  if (!hasTable) {
    await knex.schema.createTable("trust_results", (table) => {
      table.increments("id").primary();
      table.string("did").notNullable();
      table.bigInteger("height").notNullable();

      table.jsonb("resolve_result").notNullable().defaultTo(knex.raw(`'{}'::jsonb`));

      table.jsonb("issuer_auth").nullable();
      table.jsonb("verifier_auth").nullable();
      table.jsonb("ecosystem_participant").nullable();

      table.text("trust_status").nullable();
      table.boolean("production").nullable();
      table.timestamp("evaluated_at", { useTz: true }).nullable();
      table.timestamp("expires_at", { useTz: true }).nullable();
      table.jsonb("full_result_json").nullable();

      table.timestamp("created_at", { useTz: true }).notNullable().defaultTo(knex.fn.now());

      table.unique(["did", "height"]);
    });
  }

  const hasTrustResults = await knex.schema.hasTable("trust_results");
  if (hasTrustResults) {
    const hasTrustStatus = await knex.schema.hasColumn("trust_results", "trust_status");
    if (!hasTrustStatus) {
      await knex.schema.alterTable("trust_results", (t) => {
        t.text("trust_status").nullable();
        t.boolean("production").nullable();
        t.timestamp("evaluated_at", { useTz: true }).nullable();
        t.timestamp("expires_at", { useTz: true }).nullable();
        t.jsonb("full_result_json").nullable();
      });
    }
    try {
      await knex.schema.raw(`CREATE INDEX IF NOT EXISTS trust_results_expires_at_idx ON trust_results (expires_at)`);
    } catch {
      console.warn("Failed to create index trust_results_expires_at_idx; this may impact performance of trust result refreshing.");
    }
  }

  const hasRetryTable = await knex.schema.hasTable("trust_reattemptable");
  if (!hasRetryTable) {
    await knex.schema.createTable("trust_reattemptable", (t) => {
      t.text("resource_id").primary();
      t.text("resource_type").notNullable();
      t.timestamp("first_failure", { useTz: true }).notNullable().defaultTo(knex.fn.now());
      t.timestamp("last_retry", { useTz: true }).nullable();
      t.text("error_type").nullable();
      t.integer("retry_count").notNullable().defaultTo(0);
    });
    try {
      await knex.schema.raw(
        `CREATE INDEX IF NOT EXISTS trust_reattemptable_last_retry_idx ON trust_reattemptable (last_retry)`
      );
    } catch {
      console.warn("Failed to create index trust_reattemptable_last_retry_idx; this may impact performance of trust result refreshing.");
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  if (await knex.schema.hasTable("trust_reattemptable")) {
    await knex.schema.dropTableIfExists("trust_reattemptable");
  }
  try {
    await knex.schema.raw(`DROP INDEX IF EXISTS trust_reattemptable_last_retry_idx`);
  } catch {
    console.warn("Failed to drop index trust_reattemptable_last_retry_idx; this may impact performance of trust result refreshing.");
  }

  if (await knex.schema.hasTable("trust_results")) {
    try {
      await knex.schema.raw(`DROP INDEX IF EXISTS trust_results_expires_at_idx`);
    } catch {
      console.warn("Failed to drop index trust_results_expires_at_idx; this may impact performance of trust result refreshing.");
    }
    await knex.schema.dropTableIfExists("trust_results");
  }
}

