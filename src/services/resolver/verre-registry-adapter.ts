import type { IRegistryAdapter, PermissionType, VerifiablePublicRegistry } from "@verana-labs/verre";
import knex from "../../common/utils/db_connection";

type SchemaRow = {
  id: number;
  json_schema: unknown;
};

type AdapterOptions = {
  enabled: boolean;
};

function asSchemaJsonString(value: unknown): string {
  if (typeof value === "string") return value;
  return JSON.stringify(value ?? {});
}

function positiveInt(value: unknown): number | null {
  const n = Number(value);
  if (!Number.isInteger(n) || n <= 0) return null;
  return n;
}

function schemaIdFromUrl(url: string): number | null {
  const direct = positiveInt(url);
  if (direct) return direct;
  try {
    const segments = new URL(url).pathname.split("/").filter(Boolean);
    return positiveInt(segments.at(-1));
  } catch {
    return null;
  }
}

async function findSchemaByUrl(url: string): Promise<SchemaRow | null> {
  const trimmed = String(url ?? "").trim();
  if (!trimmed) return null;

  const directId = schemaIdFromUrl(trimmed);
  if (directId) {
    const row = await knex("credential_schemas")
      .select("id", "json_schema")
      .where("id", directId)
      .whereNull("archived")
      .first();
    if (row) return { id: Number((row as any).id), json_schema: (row as any).json_schema };
  }

  const isPg = String((knex as { client?: { config?: { client?: string } } }).client?.config?.client || "").includes("pg");
  if (isPg) {
    const row = await knex("credential_schemas")
      .select("id", "json_schema")
      .whereRaw(
        `
        (is_active IS NULL OR is_active = true)
        AND archived IS NULL
        AND (
          json_schema::jsonb->>'$id' = ?
          OR json_schema::jsonb->>'id' = ?
          OR json_schema::jsonb->>'@id' = ?
        )
        `,
        [trimmed, trimmed, trimmed]
      )
      .orderBy("id", "desc")
      .first();
    if (!row) return null;
    return { id: Number((row as any).id), json_schema: (row as any).json_schema };
  }

  const rows = await knex("credential_schemas")
    .select("id", "json_schema")
    .whereNull("archived")
    .limit(500);
  for (const row of rows as Array<{ id?: unknown; json_schema?: unknown }>) {
    const js = row.json_schema;
    const s = typeof js === "string" ? js : JSON.stringify(js ?? {});
    if (s.includes(trimmed)) {
      const id = Number(row.id);
      if (Number.isFinite(id) && id > 0) return { id, json_schema: js };
    }
  }
  return null;
}

class IndexerRegistryAdapter implements IRegistryAdapter {
  private readonly schemaCache = new Map<string, SchemaRow | null>();

  private readonly permissionCache = new Map<string, any>();

  private async cachedSchema(url: string): Promise<SchemaRow | null> {
    const key = String(url ?? "").trim();
    if (!key) return null;
    if (this.schemaCache.has(key)) return this.schemaCache.get(key) ?? null;
    const row = await findSchemaByUrl(key);
    this.schemaCache.set(key, row);
    return row;
  }

  async fetchSchema(url: string): Promise<string> {
    const row = await this.cachedSchema(url);
    if (!row) {
      throw new Error(`Schema not found for URL: ${url}`);
    }
    return asSchemaJsonString(row.json_schema);
  }

  async fetchPermission(schemaId: string, did: string, permissionType: PermissionType) {
    const cacheKey = `${schemaId}::${did}::${String(permissionType)}`;
    if (this.permissionCache.has(cacheKey)) return this.permissionCache.get(cacheKey);

    const dbSchemaId = schemaIdFromUrl(schemaId) ?? (await this.cachedSchema(schemaId))?.id;
    if (!dbSchemaId) {
      this.permissionCache.set(cacheKey, undefined);
      return undefined;
    }

    const row = await knex("permissions")
      .select("type", "created", "effective_from", "effective_until")
      .where("schema_id", dbSchemaId)
      .andWhere("did", did)
      .andWhere("type", String(permissionType))
      .whereNull("revoked")
      .whereNull("slashed")
      .whereNull("repaid")
      .orderBy("created", "desc")
      .first();

    const result = row
      ? {
          type: String((row as any).type),
          created: (row as any).created,
          effective_from: (row as any).effective_from ?? null,
          effective_until: (row as any).effective_until ?? null,
        }
      : undefined;
    this.permissionCache.set(cacheKey, result);
    return result;
  }
}

export function attachRegistryAdapters(
  registries: VerifiablePublicRegistry[],
  options: AdapterOptions
): VerifiablePublicRegistry[] {
  if (!options.enabled) return registries;
  const adapter = new IndexerRegistryAdapter();
  return registries.map((reg) => (reg.adapter ? reg : { ...reg, adapter }));
}
