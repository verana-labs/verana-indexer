import knex from "../../common/utils/db_connection";

export class DbDerefCache {
  private ttlMs: number;

  constructor(ttlMs: number) {
    this.ttlMs = Math.max(1000, Math.floor(ttlMs));
  }

  async get(key: string): Promise<any | undefined> {
    if (!key) return undefined;
    const now = new Date();
    const row = await knex("trust_deref_cache")
      .select("cache_value", "expires_at")
      .where({ cache_key: key })
      .first();
    if (!row) return undefined;
    const exp = new Date((row as any).expires_at);
    if (!Number.isFinite(exp.getTime()) || exp <= now) return undefined;
    return (row as any).cache_value;
  }

  async set(key: string, value: any): Promise<void> {
    if (!key) return;
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.ttlMs);
    await knex("trust_deref_cache")
      .insert({
        cache_key: key,
        cache_value: value,
        cached_at: now,
        expires_at: expiresAt,
        updated_at: now,
      })
      .onConflict("cache_key")
      .merge({
        cache_value: value,
        expires_at: expiresAt,
        updated_at: now,
      });
  }

  async delete(key: string): Promise<void> {
    if (!key) return;
    await knex("trust_deref_cache").where({ cache_key: key }).delete();
  }
}

