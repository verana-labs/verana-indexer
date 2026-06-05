import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, Errors, ServiceBroker } from "moleculer";
import knex from "../../common/utils/db_connection";
import { SERVICE } from "../../common";
import ApiResponder from "../../common/utils/apiResponse";
import BaseService from "../../base/base.service";
import { isValidDid } from "./api_shared";

type SnapshotRow = Record<string, unknown>;

type SnapshotResponse = {
  did: string;
  block_height: number;
  trust_registries: SnapshotRow[];
  schemas: SnapshotRow[];
  permissions: SnapshotRow[];
  count: {
    trust_registries: number;
    schemas: number;
    permissions: number;
  };
};

type SnapshotTables = {
  hasTrustRegistry: boolean;
  hasCredentialSchemas: boolean;
  hasPermissions: boolean;
  trustRegistryHasHeight: boolean;
  credentialSchemasHasHeight: boolean;
  permissionsHasHeight: boolean;
};

const TABLE_CHECK_TTL_MS = 60_000;
let cachedTables: { expiresAt: number; value: Promise<SnapshotTables> } | null = null;

async function getSnapshotTables(): Promise<SnapshotTables> {
  const now = Date.now();
  if (cachedTables && cachedTables.expiresAt > now) return cachedTables.value;

  const value = (async () => {
    const [hasTrustRegistry, hasCredentialSchemas, hasPermissions] = await Promise.all([
      knex.schema.hasTable("trust_registry"),
      knex.schema.hasTable("credential_schemas"),
      knex.schema.hasTable("permissions"),
    ]);

    const [trustRegistryHasHeight, credentialSchemasHasHeight, permissionsHasHeight] = await Promise.all([
      hasTrustRegistry ? knex.schema.hasColumn("trust_registry", "height") : Promise.resolve(false),
      hasCredentialSchemas ? knex.schema.hasColumn("credential_schemas", "height") : Promise.resolve(false),
      hasPermissions ? knex.schema.hasColumn("permissions", "height") : Promise.resolve(false),
    ]);

    return {
      hasTrustRegistry,
      hasCredentialSchemas,
      hasPermissions,
      trustRegistryHasHeight,
      credentialSchemasHasHeight,
      permissionsHasHeight,
    };
  })();

  cachedTables = { expiresAt: now + TABLE_CHECK_TTL_MS, value };
  return value;
}

function parseNonNegativeInteger(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === "number" ? value : Number(String(value).trim());
  if (!Number.isInteger(n) || n < 0) return null;
  return n;
}

async function fetchTrustRegistriesAtHeight(did: string, height: number, tables: SnapshotTables): Promise<SnapshotRow[]> {
  if (!tables.hasTrustRegistry) return [];

  const query = knex("trust_registry")
    .select("*")
    .where("did", did);

  if (tables.trustRegistryHasHeight) {
    query.andWhere("height", "<=", height);
  }

  return query.orderBy("id", "asc");
}

async function fetchCredentialSchemasAtHeight(trIds: number[], _height: number, tables: SnapshotTables): Promise<SnapshotRow[]> {
  if (!tables.hasCredentialSchemas) return [];
  if (trIds.length === 0) return [];

  const query = knex("credential_schemas")
    .select("*")
    .whereIn("ecosystem_id", trIds)
    .orderBy("id", "asc");

  if (tables.credentialSchemasHasHeight) {
    query.andWhere("height", "<=", _height);
  }

  return query;
}

async function fetchPermissionsAtHeight(args: {
  did: string;
  blockHeight: number;
  schemaTrIds?: number[];
  corporationAddresses: string[];
  tables: SnapshotTables;
}): Promise<SnapshotRow[]> {
  const { did, schemaTrIds = [], corporationAddresses, tables, blockHeight } = args;
  if (!tables.hasPermissions) return [];

  const query = knex("permissions")
    .select("*")
    .where((qb) => {
      qb.where("did", did);
      if (corporationAddresses.length > 0) qb.orWhere((q) => q.whereIn("corporation", corporationAddresses));
      if (tables.hasCredentialSchemas && schemaTrIds.length > 0) {
        const schemaQuery = knex("credential_schemas")
          .select("id")
          .whereIn("ecosystem_id", schemaTrIds);
        if (tables.credentialSchemasHasHeight) {
          schemaQuery.andWhere("height", "<=", blockHeight);
        }
        qb.orWhereIn("schema_id", schemaQuery);
      }
    })
    .orderBy("id", "asc");

  if (tables.permissionsHasHeight) {
    query.andWhere("height", "<=", blockHeight);
  }

  return query;
}

export async function getDidSnapshotAtHeight(args: { did: string; blockHeight: number }): Promise<SnapshotResponse> {
  const { did, blockHeight } = args;
  const tables = await getSnapshotTables();

  const trustRegistries = await fetchTrustRegistriesAtHeight(did, blockHeight, tables);
  const trIds = trustRegistries
    .map((row) => Number(row.id))
    .filter((id) => Number.isInteger(id) && id >= 0);

  const corporationAddresses = Array.from(
    new Set(
      trustRegistries
        .map((row) => row.corporation)
        .filter((corp): corp is string => typeof corp === "string" && corp.trim().length > 0)
        .map((corp) => corp.trim())
    )
  );

  const [credentialSchemas, permissions] = await Promise.all([
    fetchCredentialSchemasAtHeight(trIds, blockHeight, tables),
    fetchPermissionsAtHeight({
      did,
      blockHeight,
      schemaTrIds: trIds,
      corporationAddresses,
      tables,
    }),
  ]);

  return {
    did,
    block_height: blockHeight,
    trust_registries: trustRegistries,
    schemas: credentialSchemas,
    permissions,
    count: {
      trust_registries: trustRegistries.length,
      schemas: credentialSchemas.length,
      permissions: permissions.length,
    },
  };
}

@Service({
  name: SERVICE.V1.IndexerSnapshotService.key,
  version: 1,
})
export default class IndexerSnapshotService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker);
  }

  @Action({
    name: "getSnapshot",
    params: {
      did: { type: "string", optional: true, trim: true },
      block_height: { type: "any", optional: true },
    },
    rest: "GET /snapshot",
  })
  public async getSnapshot(ctx: Context<{ did?: string; block_height?: unknown }>) {
    try {
      const did = typeof ctx.params.did === "string" ? ctx.params.did.trim() : "";
      if (!did) return ApiResponder.error(ctx, "Missing did", 400);
      if (!isValidDid(did)) return ApiResponder.error(ctx, "Invalid did", 400);

      const blockHeight = parseNonNegativeInteger(ctx.params.block_height);
      if (blockHeight === null) {
        if (ctx.params.block_height === undefined || ctx.params.block_height === null || String(ctx.params.block_height).trim() === "") {
          return ApiResponder.error(ctx, "Missing block_height", 400);
        }
        return ApiResponder.error(ctx, "Invalid block_height", 400);
      }

      const snapshot = await getDidSnapshotAtHeight({ did, blockHeight });
      return ApiResponder.success(ctx, snapshot, 200);
    } catch (err: unknown) {
      this.logger.error("[IndexerSnapshotService] Failed to build snapshot:", err);
      if (err instanceof Errors.MoleculerError) throw err;
      throw new Errors.MoleculerError("Failed to build snapshot", 500, "SNAPSHOT_FAILED");
    }
  }
}

