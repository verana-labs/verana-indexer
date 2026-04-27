import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  const has = await knex.schema.hasTable("trust_deref_cache");
  if (!has) {
    await knex.schema.createTable("trust_deref_cache", (t) => {
      t.text("cache_key").primary();
      t.jsonb("cache_value").notNullable();
      t.text("value_hash").nullable(); // optional: content hash / etag / sri
      t.timestamp("cached_at", { useTz: true }).notNullable().defaultTo(knex.fn.now());
      t.timestamp("expires_at", { useTz: true }).notNullable();
      t.timestamp("updated_at", { useTz: true }).notNullable().defaultTo(knex.fn.now());
    });
    try {
      await knex.schema.raw(`CREATE INDEX IF NOT EXISTS trust_deref_cache_expires_at_idx ON trust_deref_cache (expires_at)`);
    } catch {
      console.warn("Failed to create index trust_deref_cache_expires_at_idx; this may impact performance of deref cache expiration.");
    }
  } else {
    try {
      await knex.schema.raw(`CREATE INDEX IF NOT EXISTS trust_deref_cache_expires_at_idx ON trust_deref_cache (expires_at)`);
    } catch {
      console.warn("Failed to create index trust_deref_cache_expires_at_idx; this may impact performance of deref cache expiration.");
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  try {
    await knex.schema.raw(`DROP INDEX IF EXISTS trust_deref_cache_expires_at_idx`);
  } catch {
    console.warn("Failed to drop index trust_deref_cache_expires_at_idx; this may impact performance of deref cache expiration.");
  }
  await knex.schema.dropTableIfExists("trust_deref_cache");
}

