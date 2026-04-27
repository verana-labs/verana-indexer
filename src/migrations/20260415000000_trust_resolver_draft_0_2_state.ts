import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const hasRetry = await knex.schema.hasTable("trust_reattemptable");
  if (hasRetry) {
    const hasLastError = await knex.schema.hasColumn("trust_reattemptable", "last_error");
    const hasLastFailure = await knex.schema.hasColumn("trust_reattemptable", "last_failure");
    const hasNextRetry = await knex.schema.hasColumn("trust_reattemptable", "next_retry");
    const hasUpdatedAt = await knex.schema.hasColumn("trust_reattemptable", "updated_at");

    if (!hasLastError || !hasLastFailure || !hasNextRetry || !hasUpdatedAt) {
      await knex.schema.alterTable("trust_reattemptable", (t) => {
        if (!hasLastFailure) t.timestamp("last_failure", { useTz: true }).nullable();
        if (!hasLastError) t.text("last_error").nullable();
        if (!hasNextRetry) t.timestamp("next_retry", { useTz: true }).nullable();
        if (!hasUpdatedAt) t.timestamp("updated_at", { useTz: true }).notNullable().defaultTo(knex.fn.now());
      });
    }

    try {
      await knex.schema.raw(
        `CREATE INDEX IF NOT EXISTS trust_reattemptable_next_retry_idx ON trust_reattemptable (next_retry)`
      );
    } catch {
      console.error("Failed to create index trust_reattemptable_next_retry_idx, maybe it already exists?")
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  try {
    await knex.schema.raw(`DROP INDEX IF EXISTS trust_reattemptable_next_retry_idx`);
  } catch {
    console.error("Failed to drop index trust_reattemptable_next_retry_idx, maybe it doesn't exist?")
  }
}

